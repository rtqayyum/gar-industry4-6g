#!/usr/bin/env python3
"""Run production-handler tests and four deliberately faulty guard ablations.

No UDP performance or real-time service claim follows from these deterministic
checks. Mutations exist only here and are never exposed by the runtime CLI.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import platform
import sys
import unittest

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parents[1]
TESTS = PROJECT / "tests"
sys.path.insert(0, str(TESTS))
from test_gateway import GatewayTransactions, request
from transaction_guard import verify_candidate


def fixture():
    test = GatewayTransactions()
    test.setUp()
    return test


def ablation_cases():
    rows = []
    for name in ("prefix", "tail", "older_bookings", "version"):
        row = {"component": name}
        for remove in (False, True):
            test = fixture()
            try:
                if name == "prefix":
                    test.adapter.call("PROP 0 0 8 2 2 1 0")
                    test.clock = 1_000_000
                    command = test.command(target=8, tail=2, pending=[[3, 24]])
                    if remove:
                        def check(adapter, snapshot, cfg, cmd):
                            wrong = {**snapshot, "calendar": [snapshot["calendar"][0]] * len(snapshot["calendar"])}
                            return verify_candidate(adapter, wrong, cfg, cmd)
                        test.handler.candidate_check = check
                    ack, _ = test.handler.handle(command, {})
                    calendar = test.adapter.call("ADV 1 3")["calendar"]
                    violation = ack["accepted"] and sum(calendar) < 24
                    evidence = {"old_plan": {"target": 8, "expiry": 2, "tail": 2},
                                "observation_ms": 1, "activation_ms": 4,
                                "critical_pending_bytes": 24, "residual_deadline_ms": 3,
                                "actual_immutable_prefix": calendar,
                                "available_bytes_before_deadline": sum(calendar)}
                elif name == "tail":
                    test.cfg["envelopes"][0]["rho"] = 2
                    command = test.command(target=3, tail=1)
                    if remove:
                        test.handler.candidate_check = lambda adapter, snapshot, cfg, cmd: verify_candidate(
                            adapter, snapshot, cfg, {**cmd, "tail": cmd["target"]})
                    ack, _ = test.handler.handle(command, {})
                    tail = test.adapter.call("ADV 8 1")["units"]
                    violation = ack["accepted"] and tail < 2
                    evidence = {"temporary_target": 3, "continuing_tail": 1,
                                "arrival_rate_bytes_per_ms": 2, "installed_tail_service": tail}
                elif name == "older_bookings":
                    test.ledger.commit(request("old", end=12, units=4))
                    command = test.command(target=2, tail=6, term=2)
                    if remove:
                        test.handler.candidate_check = lambda adapter, snapshot, cfg, cmd: verify_candidate(
                            adapter, {**snapshot, "other_units": [2]*len(snapshot["calendar"])}, cfg, cmd)
                        test.ledger.other_at = lambda _: 2
                    ack, _ = test.handler.handle(command, {})
                    floor = test.adapter.call("ADV 8 1")["units"]
                    total = floor + 2 + test.ledger.booked.get(8, 0)
                    violation = ack["accepted"] and total > 10
                    evidence = {"capacity_units": 10, "incumbent_units": 2,
                                "older_booking_units": 4, "candidate_tail": 6,
                                "committed_units_at_ms_8": total}
                else:
                    command = test.command()
                    test.ledger.commit(request("intervening", units=1))
                    if remove:
                        real = test.ledger
                        class FrozenVersion:
                            version = 0
                            def __getattr__(self, key):
                                return getattr(real, key)
                        test.handler.ledger = FrozenVersion()
                    ack, _ = test.handler.handle(command, {})
                    violation = ack["accepted"] and command["ledger_version"] != test.ledger.version
                    evidence = {"certified_ledger_version": 0, "live_ledger_version": test.ledger.version,
                                "invariant": "ACK must refer to the certified unchanged ledger version",
                                "physical_capacity_violation_claimed": False}
                row["ablated" if remove else "full_guard"] = {
                    "accepted": ack["accepted"], "reason": ack["reason"],
                    "invariant_violated": bool(violation), "trace": evidence}
            finally:
                test.tearDown()
        assert not row["full_guard"]["accepted"] and not row["full_guard"]["invariant_violated"], row
        assert row["ablated"]["accepted"] and row["ablated"]["invariant_violated"], row
        rows.append(row)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    stream = io.StringIO()
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(GatewayTransactions)
    result = unittest.TextTestRunner(stream=stream, verbosity=2).run(suite)
    (args.output / "tests.txt").write_text(stream.getvalue())
    GatewayTransactions.setUpClass()
    try:
        examples = ablation_cases()
    finally:
        GatewayTransactions.tearDownClass()
    (args.output / "component_counterexamples.json").write_text(json.dumps(examples, indent=2) + "\n")
    paths = sorted([*HERE.glob("*.py"), *HERE.glob("*.cpp"), *HERE.glob("*.hpp"),
                    *TESTS.glob("test_gateway*.py")])
    hashes = {str(path.relative_to(PROJECT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    receipt = {"status": "PASS" if result.wasSuccessful() else "FAIL",
               "timestamp_utc": datetime.now(timezone.utc).isoformat(),
               "python": platform.python_version(), "platform": platform.platform(),
               "unit_test_methods": result.testsRun, "failures": len(result.failures),
               "errors": len(result.errors), "release_queue_grid_cases": 726,
               "invalid_release_schedule_cases": 7,
               "malformed_command_cases": 12,
               "invalid_policy_cases": 5,
               "changed_gate_selection_config_cases": 4,
               "invalid_units_or_service_config_cases": 10,
               "component_counterexamples": len(examples),
               "full_guard_invariant_violations": sum(x["full_guard"]["invariant_violated"] for x in examples),
               "ablated_invariant_violations": sum(x["ablated"]["invariant_violated"] for x in examples),
               "scope": "Production Python handler with compiled C++ executor/certificate; deterministic fault injection, not UDP or OS real-time validation",
               "counterexample_scope": "Three capacity/service failures and one stale-version semantic violation; not statistical performance trials or published-system reproductions",
               "source_sha256": hashes}
    (args.output / "verification.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps({key: value for key, value in receipt.items() if key != "source_sha256"}, indent=2))
    if not result.wasSuccessful():
        raise SystemExit(1)


if __name__ == "__main__":
    main()
