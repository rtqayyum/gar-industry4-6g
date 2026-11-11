"""Replay two follow-up controls on the original 2,048 matched admission snapshots."""

import argparse
from collections import Counter, defaultdict
import csv
from itertools import product
import json
from pathlib import Path
import tempfile

from experiments.matched import run as matched


def replay_case(adapter, case):
    matched.validate_workload(case)
    answer = adapter.call(case)
    peak, keep, gar = answer["decisions"]
    if not (answer["eligible"] and answer["old_oracle"] and keep["release_pass"]):
        raise RuntimeError(f"Frozen snapshot differs: {case['id']}")

    constant = keep["accepted"] or answer["constant_tail"]
    if constant and not gar["accepted"]:
        raise RuntimeError(f"Constant-floor acceptance outside full GAR: {case['id']}")

    target = answer["zero_target"]
    replan = not keep["accepted"] and target >= 0
    accept = keep["accepted"] or replan
    actual = matched.execute(case, dict(accepted=accept, replaced=replan, target=target, tail=answer["tail"]))
    groups = actual["groups"]
    return {
        "id": case["id"],
        "seed": case["seed"],
        "stratum": case["stratum"],
        "lead_ms": case["lead"],
        "offered": 1,
        "peak": int(peak["accepted"]),
        "keep": int(keep["accepted"]),
        "gar": int(gar["accepted"]),
        "constant_tail": int(constant),
        "tail_units": answer["tail"],
        "constant_gain": int(constant and not keep["accepted"]),
        "timed_gain": int(gar["accepted"] and not constant),
        "zero_lead_accepted": int(accept),
        "zero_lead_replaced": int(replan),
        "actual_certificate_violations": int(replan and not answer["zero_actual_cert"]),
        "actual_ledger_violation_cases": int(actual["capacity_violations"] > 0),
        "actual_ledger_violation_slots": actual["capacity_violations"],
        "actual_incumbent_misses": actual["incumbent_failures"],
        "actual_old_tenant_misses": groups["old_tenant"]["misses"],
        "actual_new_tenant_misses": groups["new_tenant"]["accepted_job"] - groups["new_tenant"]["completed_job"],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="New directory for replay tables")
    args = parser.parse_args()
    matched.validate_protocol()
    protocol = matched.PROTOCOL
    start = protocol["heldout_seed_start"]
    seeds = range(start, start + protocol["heldout_seed_count"])
    replications = range(protocol["replicates_per_seed_stratum"])
    inputs = product(seeds, protocol["strata"], replications)

    with tempfile.TemporaryDirectory(prefix="gar-snapshot-controls-") as folder:
        adapter = matched.Adapter(folder, compiler_flags=("-DGAR_SNAPSHOT_CONTROLS",))
        try:
            rows = [replay_case(adapter, matched.generate(*inputs_for_case)) for inputs_for_case in inputs]
        finally:
            adapter.close()

    counts = defaultdict(Counter)
    descriptors = {"id", "seed", "stratum", "lead_ms", "tail_units"}
    for row in rows:
        totals = {key: value for key, value in row.items() if key not in descriptors}
        counts["all"].update(totals)
        counts[row["stratum"]].update(totals)

    published = {"offered": 2048, "peak": 743, "keep": 845, "gar": 2029}
    if any(counts["all"][key] != value for key, value in published.items()):
        raise RuntimeError("Replay failed to reproduce the original matched-snapshot counts")
    if {row["tail_units"] for row in rows} != {49}:
        raise RuntimeError("Replay did not reproduce the original continuing floor")

    args.output.mkdir(parents=True, exist_ok=False)
    with (args.output / "snapshot_diagnostics.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    summary = json.dumps(dict(counts), indent=2)
    (args.output / "snapshot_diagnostics.json").write_text(summary + "\n", encoding="utf-8")
    print(summary)


if __name__ == "__main__":
    main()
