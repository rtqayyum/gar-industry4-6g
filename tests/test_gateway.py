"""Production Python handler against the real compiled C++ adapter.

Run: python -m unittest discover -s tests -p 'test_gateway.py' -v
"""
from copy import deepcopy
import itertools
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

RUNTIME = Path(__file__).resolve().parents[1] / "experiments/gateway"
sys.path.insert(0, str(RUNTIME))
from realtime_gateway import FutureAdmissionLedger
from transaction_guard import (AdmissionReplay, GatewayTransactionHandler,
                               bind_command, release_contract_check)


class Adapter:
    def __init__(self, executable):
        self.process = subprocess.Popen([str(executable)], stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, text=True)
        self.calls = []

    def call(self, request):
        self.calls.append(request)
        self.process.stdin.write(request + "\n")
        self.process.stdin.flush()
        value = json.loads(self.process.stdout.readline())
        if "error" in value:
            raise AssertionError(value["error"] + ": " + request)
        return value

    def close(self):
        self.process.stdin.write("QUIT\n")
        self.process.stdin.flush()
        self.process.wait(timeout=5)
        self.process.stdin.close()
        self.process.stdout.close()


def configuration():
    return {"capacity_bytes_per_ms": 10, "units": 10, "base_units": 1,
            "maximum_units": 8, "background_units": [1, 1],
            "envelopes": [{"min_deadline": 4, "sigma": 0, "rho": 1}],
            "guardband_ms": 0, "delay_ms": 3, "tenant_headroom": 1,
            "tenant_theta_ms": 0, "tenant_max_duration_ms": 100,
            "step_ms": 1, "command_price": 0}


def request(key="job", start=3, end=7, units=2, work=4, releases=None):
    return {"id": key, "start_ms": start, "end_ms": end, "units": units,
            "duration_ms": end-start, "work_bytes": work,
            "release_schedule": releases if releases is not None else [[0, work]]}


class GatewayTransactions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix="gar-gateway-")
        cls.executable = Path(cls.directory.name) / "planner_adapter"
        compiler = shutil.which("clang++") or shutil.which("g++")
        if compiler is None:
            raise RuntimeError("A C++17 compiler is required; do not silently skip adapter tests")
        subprocess.run([compiler, "-std=c++17", "-O2", str(RUNTIME / "realtime_planner_adapter.cpp"),
                        "-o", str(cls.executable)], check=True, capture_output=True, text=True)

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def setUp(self):
        self.cfg = configuration()
        self.adapter = Adapter(self.executable)
        self.adapter.call("INIT 2")
        self.clock = 0
        self.ledger = FutureAdmissionLedger(self.cfg)
        self.journal_patch = patch("realtime_gateway.journal")
        self.journal_patch.start()
        self.handler = GatewayTransactionHandler(self.cfg, self.ledger, self.adapter,
                                                 now_ns=lambda: self.clock)
        self.sequence = 0

    def tearDown(self):
        self.adapter.close()
        self.journal_patch.stop()

    def command(self, *, epoch=1, target=2, tail=2, term=4, joint=None, pending=None):
        now = self.clock // 1_000_000
        state = self.adapter.call(f"ADV {now} 64")
        self.sequence += 1
        snapshot = self.handler.issue_snapshot({"seq": self.sequence, "now_ms": now,
            "state_version": state["state_version"], "ledger_version": self.ledger.version,
            "calendar": state["calendar"], "other_units": self.ledger.profile(now, 64),
            "old_final_change": state["old_final_change"], "critical_pending": pending or []})
        cmd = {"type": "command", "epoch": epoch, "snapshot_seq": snapshot["seq"],
            "state_version": state["state_version"], "ledger_version": self.ledger.version,
            "observation_ms": now, "activate_ms": joint["start_ms"] if joint else now+self.cfg["delay_ms"],
            "target": target, "tail": tail, "term": term, "policy": "joint_guarded",
            "certificate_passed": True, "transmit_ns": self.clock}
        if joint:
            cmd["tenant_request"] = joint
        return bind_command(cmd, snapshot, self.cfg)

    def test_acceptance_commits_joint_state_before_ack(self):
        job = request()
        ack, state = self.handler.handle(self.command(joint=job), {job["id"]: job})
        self.assertTrue(ack["accepted"])
        self.assertEqual(self.ledger.version, 1)
        self.assertEqual(self.ledger.jobs[0]["work_bytes"], 4)
        self.assertTrue(state["pending"])
        self.assertEqual(ack["ledger_version"], 1)

    def test_identical_command_retry_after_lost_ack_is_success(self):
        job = request()
        command = self.command(joint=job)
        first, _ = self.handler.handle(command, {job["id"]: job})
        command["transmit_ns"] += 100
        second, state = self.handler.handle(command, {})
        self.assertTrue(first["accepted"] and second["accepted"] and second["replayed"])
        self.assertIsNone(state)  # No stale state injected into the gateway loop.
        self.assertEqual(self.ledger.version, 1)
        self.assertEqual(sum(x.startswith("PROP") for x in self.adapter.calls), 1)

    def test_changed_epoch_payload_is_not_replayed(self):
        command = self.command()
        self.handler.handle(command, {})
        command["target"] = 3
        ack, _ = self.handler.handle(command, {})
        self.assertFalse(ack["accepted"])
        self.assertEqual(ack["reason"], "conflicting_epoch_reuse")

    def test_old_retry_does_not_claim_a_later_plan_identity(self):
        first_command = self.command()
        first, _ = self.handler.handle(first_command, {})
        self.clock = 3_000_000
        second, _ = self.handler.handle(self.command(epoch=2, target=3), {})
        replay, state = self.handler.handle(first_command, {})
        self.assertTrue(first["accepted"] and second["accepted"] and replay["replayed"])
        self.assertNotEqual(first["executor_plan_id"], second["executor_plan_id"])
        self.assertEqual(replay["executor_plan_id"], first["executor_plan_id"])
        self.assertEqual(replay["state_version"], first["state_version"])
        self.assertIsNone(state)

    def test_candidate_tamper_is_rejected_without_mutation(self):
        command = self.command()
        command["target"] = 3
        ack, state = self.handler.handle(command, {})
        self.assertEqual(ack["reason"], "candidate_digest_mismatch")
        self.assertEqual(state["counters"]["accepted_plan_commands"], 0)

    def test_malformed_commands_fail_without_adapter_or_ledger_mutation(self):
        command = self.command()
        malformed = [None, [], 7, "command", {}, {**command, "epoch": []},
                     {**command, "term": False}, {**command, "term": -1},
                     {**command, "tenant_request": []},
                     {**command, "tenant_request": {"id": "missing-fields"}},
                     {**command, "tenant_request": {**request(), "id": []}},
                     {**command, "certificate_digest": None}]
        before = len(self.adapter.calls)
        for value in malformed:
            with self.subTest(value=value):
                ack, state = self.handler.handle(value, {})
                self.assertFalse(ack["accepted"])
                self.assertIsNone(state)
                self.assertEqual(len(self.adapter.calls), before)
                self.assertEqual(self.ledger.version, 0)

    def test_policy_cannot_bypass_candidate_check(self):
        command = self.command()
        for policy in (None, "", "unprotected", "static_soft"):
            with self.subTest(policy=policy):
                ack, _ = self.handler.handle({**command, "policy": policy}, {})
                self.assertEqual(ack["reason"], "invalid_or_changed_policy")
        self.handler.policy = "predictive"
        self.assertEqual(self.handler.handle(command, {})[0]["reason"], "invalid_or_changed_policy")

    def test_selection_and_gate_configuration_is_bound(self):
        command = self.command()
        for key in ("step_ms", "command_price", "gate_eta", "tenant_release_fraction"):
            before = deepcopy(self.cfg)
            self.cfg[key] = 999
            with self.subTest(key=key):
                self.assertEqual(self.handler.handle(command, {})[0]["reason"], "contracts_changed")
            self.cfg.clear()
            self.cfg.update(before)

    def test_release_units_and_service_divisor_are_validated(self):
        for units in (-1, 0, 11, True):
            self.assertFalse(release_contract_check(request(units=units), self.cfg)[0])
        self.assertFalse(release_contract_check(request(start=-1), self.cfg)[0])
        for changes in ({"units": 0}, {"units": -1}, {"capacity_bytes_per_ms": 0},
                        {"tenant_headroom": "NaN"}, {"tenant_headroom": "Infinity"}):
            self.assertFalse(release_contract_check(request(), {**self.cfg, **changes})[0])

    def test_unknown_snapshot_is_rejected(self):
        command = self.command()
        command["snapshot_seq"] = 999
        self.assertEqual(self.handler.handle(command, {})[0]["reason"], "unknown_or_changed_snapshot")

    def test_changed_contract_is_rejected(self):
        command = self.command()
        self.cfg["tenant_headroom"] = 0.5
        self.assertEqual(self.handler.handle(command, {})[0]["reason"], "contracts_changed")

    def test_intervening_booking_invalidates_snapshot(self):
        command = self.command()
        self.ledger.commit(request("intervening", units=1))
        self.assertEqual(self.handler.handle(command, {})[0]["reason"], "ledger_changed")

    def test_intervening_timer_invalidates_snapshot(self):
        self.adapter.call("PROP 0 0 2 1 3 1 0")
        command = self.command()
        self.clock = 1_000_000
        self.assertEqual(self.handler.handle(command, {})[0]["reason"], "state_changed")

    def test_expired_command_cannot_install(self):
        command = self.command()
        self.clock = 4_000_000
        self.assertEqual(self.handler.handle(command, {})[0]["reason"], "missed_activation")

    def test_changed_pending_request_is_rejected(self):
        job = request()
        command = self.command(joint=job)
        changed = {**job, "work_bytes": 3, "release_schedule": [[0, 3]]}
        self.assertEqual(self.handler.handle(command, {"job": changed})[0]["reason"],
                         "request_no_longer_pending_or_changed")

    def test_controller_stop_before_joint_commit_rejects(self):
        job = request()
        self.handler.heartbeat_fresh = lambda _: False
        ack, _ = self.handler.handle(self.command(joint=job), {"job": job})
        self.assertEqual(ack["reason"], "controller_heartbeat_stale")
        self.assertEqual(self.ledger.version, 0)

    def test_controller_stop_after_commit_preserves_future_calendar(self):
        job = request(end=9)
        ack, _ = self.handler.handle(self.command(joint=job, target=3, term=2, tail=2), {"job": job})
        self.assertTrue(ack["accepted"])
        self.handler.heartbeat_fresh = lambda _: False
        state = self.adapter.call("ADV 9 1")
        self.assertEqual(state["units"], 2)
        self.assertEqual([e["kind"] for e in state["events"]], ["installation", "fallback"])
        self.assertEqual(self.ledger.version, 1)

    def test_new_fallback_cannot_break_old_booking(self):
        self.ledger.commit(request("old", end=12, units=4))
        ack, _ = self.handler.handle(self.command(target=2, tail=6, term=2), {})
        self.assertFalse(ack["accepted"])
        self.assertEqual(self.ledger.version, 1)

    def test_old_fallback_during_lead_is_in_actual_certificate(self):
        self.adapter.call("PROP 0 0 8 2 2 1 0")
        self.clock = 1_000_000
        command = self.command(target=8, tail=2, pending=[[3, 24]])
        ack, _ = self.handler.handle(command, {})
        self.assertFalse(ack["accepted"])
        self.assertEqual(self.adapter.call("ADV 4 1")["units"], 2)

    def test_coincident_installation_supersedes_old_fallback(self):
        self.adapter.call("PROP 0 0 8 3 2 1 0")
        ack, _ = self.handler.handle(self.command(target=4, tail=2), {})
        self.assertTrue(ack["accepted"])
        state = self.adapter.call("ADV 3 1")
        self.assertEqual(state["units"], 4)
        self.assertEqual([e["kind"] for e in state["events"]], ["installation"])

    def test_unsustainable_tail_is_rechecked_not_trusted_boolean(self):
        self.cfg["envelopes"][0]["rho"] = 2
        ack, _ = self.handler.handle(self.command(target=3, tail=1), {})
        self.assertFalse(ack["accepted"])
        self.assertTrue(ack["reason"].startswith("candidate_recheck_failed"))

    def test_late_payload_is_rejected_in_real_handler(self):
        job = request(work=4, releases=[[3, 4]])
        ack, _ = self.handler.handle(self.command(joint=job), {"job": job})
        self.assertEqual(ack["reason"], "release_service_suffix_conflict")
        self.assertEqual(self.ledger.version, 0)

    def test_feasible_staged_payload_is_accepted_in_real_handler(self):
        job = request(work=4, releases=[[0, 2], [3, 2]])
        ack, _ = self.handler.handle(self.command(joint=job), {"job": job})
        self.assertTrue(ack["accepted"])

    def test_missing_payload_schedule_fails_closed(self):
        job = request()
        del job["release_schedule"]
        self.assertEqual(self.ledger.check(job, 0, [2]*64)[1], "missing_or_invalid_release_contract")

    def test_conflicting_admission_id_is_not_materialized_twice(self):
        replay = AdmissionReplay()
        job = request()
        replay.remember(job, {"accepted": True, "id": "job", "admission_ns": 10})
        self.assertTrue(replay.lookup(job)["replayed"])
        changed = {**job, "work_bytes": 5, "release_schedule": [[0, 5]]}
        self.assertEqual(replay.lookup(changed)["reason"], "conflicting_request_id_reuse")

    def test_latency_is_subtracted_from_each_suffix(self):
        self.cfg["tenant_theta_ms"] = 1
        bad = request(work=4, releases=[[0, 2], [3, 2]])
        good = request(work=4, releases=[[0, 2], [2, 2]])
        self.assertFalse(release_contract_check(bad, self.cfg)[0])
        self.assertTrue(release_contract_check(good, self.cfg)[0])

    def test_release_schedule_validation(self):
        malformed = ([], [[0, 0]], [[-1, 4]], [[4, 4]], [[1, 2], [1, 2]], [[0, True]], [[0, 3]])
        for schedule in malformed:
            with self.subTest(schedule=schedule):
                self.assertFalse(release_contract_check(request(releases=schedule), self.cfg)[0])

    def test_small_release_grid_matches_independent_queue(self):
        # (3^5 - 1) positive-payload vectors * 3 rates = 726 cases.
        for arrivals in itertools.product(range(3), repeat=5):
            if not sum(arrivals):
                continue
            for rate in range(3):
                job = request(end=8, units=max(rate, 1), work=sum(arrivals),
                              releases=[[t, b] for t, b in enumerate(arrivals) if b])
                cfg = {**self.cfg, "capacity_bytes_per_ms": rate*10, "units": 10}
                job["units"] = 1
                backlog = 0
                for amount in arrivals:
                    backlog = max(0, backlog + amount - rate)
                self.assertEqual(release_contract_check(job, cfg)[0], backlog == 0)


if __name__ == "__main__":
    unittest.main()
