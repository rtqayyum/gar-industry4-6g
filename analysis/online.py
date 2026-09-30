#!/usr/bin/env python3
"""Compute equal-context online policy means from the final comparison.

This analysis derives absolute means, not new experiments or confidence
intervals. The unit of aggregation is the family-context cluster:
first average the six traffic cells and three seeds within each context, then
average the 12 contexts equally. Paired intervals are evaluated by the experiment.
Only the Python standard library is required.
"""
from __future__ import annotations

import csv
import argparse
import hashlib
import json
import math
from collections import defaultdict
from pathlib import Path
from statistics import fmean

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "results/online/main_rows.csv"
SUMMARY = SOURCE.with_name("policy_summary.csv")
POLICIES = ("gar_h", "luu_joint_adapter", "luu_sequential_adapter", "yu_gateway_adapter")
FAMILIES = {"deadline_mix", "heavy_tenants", "regime_shift", "synchronized"}
METRICS = {"efficiency": "timely_payload_efficiency_pct",
           "background_failure": "background_failure_pct"}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def rows_from_runs(runs: list[dict]) -> list[dict]:
    """Validate the balanced frozen design and return four equal-context means."""
    if len(runs) != 864:
        raise ValueError("Expected 864 main comparison runs")
    grouped = defaultdict(list)
    identities = set()
    families = {}
    for run in runs:
        policy = run["policy_id"]
        if policy not in POLICIES:
            raise ValueError(f"Unexpected policy: {policy}")
        if run["stage"] != "main" or run["simulator_status"] != "PASS":
            raise ValueError("Only completed main comparison runs are allowed")
        if run["exit_code"] != "0" or run["configuration_infeasible"] != "False":
            raise ValueError("A failed or infeasible run cannot enter this overview")
        context = run["context_id"]
        family = run["family"]
        if family not in FAMILIES or (context in families and families[context] != family):
            raise ValueError("Unexpected or inconsistent workload family")
        families[context] = family
        identity = (policy, context, run["traffic_block"], run["seed"])
        if identity in identities:
            raise ValueError("Duplicate method/context/traffic/seed observation")
        identities.add(identity)
        for field in METRICS.values():
            value = float(run[field])
            if not math.isfinite(value) or not 0.0 <= value <= 100.0:
                raise ValueError(f"Invalid percentage metric: {field}")
        grouped[(policy, context)].append(run)
    if len(families) != 12 or any(list(families.values()).count(f) != 3 for f in FAMILIES):
        raise ValueError("Expected three contexts in each of four workload families")
    expected_pairs = None
    result = []
    for index, policy in enumerate(POLICIES, 1):
        context_values = defaultdict(list)
        for context in sorted(families):
            group = grouped[(policy, context)]
            pairs = {(r["traffic_block"], r["seed"]) for r in group}
            traffic = {pair[0] for pair in pairs}
            seeds = {pair[1] for pair in pairs}
            if len(group) != 18 or len(traffic) != 6 or len(seeds) != 3:
                raise ValueError("Each method/context must contain six traffic cells and three seeds")
            if pairs != {(t, s) for t in traffic for s in seeds}:
                raise ValueError("Unbalanced traffic/seed design")
            if expected_pairs is None:
                expected_pairs = pairs
            elif pairs != expected_pairs:
                raise ValueError("Methods and contexts must share the same traffic cells and seeds")
            for label, field in METRICS.items():
                context_values[label].append(fmean(float(r[field]) for r in group))
        result.append({"policy": index,
                       **{label: fmean(values) for label, values in context_values.items()}})
    return result


def verify_references(rows: list[dict], summary: list[dict]) -> None:
    """Cross-check the recorded policy summary independently of row ordering."""
    summaries = {item["policy_id"]: item for item in summary}
    if len(summary) != 4:
        raise ValueError("Expected exactly four reference policy means")
    if set(summaries) != set(POLICIES):
        raise ValueError("Reference policies differ from the frozen design")
    for row, policy in zip(rows, POLICIES):
        if int(summaries[policy]["context_clusters_n"]) != 12:
            raise ValueError("Reference summary must contain 12 contexts per method")
        for label, field in METRICS.items():
            if not math.isclose(row[label], float(summaries[policy][field]), rel_tol=0.0, abs_tol=1e-12):
                raise ValueError(f"Reference mismatch for {policy}: {field}")


def table_text(rows: list[dict]) -> str:
    return "policy efficiency background_failure\n" + "".join(
        f"{r['policy']} {r['efficiency']:.15f} {r['background_failure']:.15f}\n"
        for r in rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=SOURCE.parent)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = args.input / "main_rows.csv"
    summary = args.input / "policy_summary.csv"
    target = args.output / "policy_overview.dat"
    receipt_path = args.output / "analysis.json"
    runs = read_csv(source)
    rows = rows_from_runs(runs)
    verify_references(rows, read_csv(summary))
    source_hashes = {path.name: sha256(path) for path in (source, summary)}
    args.output.mkdir(parents=True, exist_ok=False)
    target.write_text(table_text(rows), encoding="utf-8")
    receipt = {
        "status": "PASS",
        "scope": "Equal-context summaries of the recorded final comparison.",
        "aggregation": "Equal mean of 12 context means per policy; each context averages six traffic cells and three seeds (18 runs).",
        "cohort": {"total_method_runs": 864, "policies": 4, "context_clusters": 12,
                   "workload_families": 4, "contexts_per_family": 3,
                   "traffic_cells_per_context": 6, "seeds_per_traffic_cell": 3},
        "intervals": "No new confidence intervals are estimated or plotted. Stored context-comparison results retain the existing paired efficiency-difference intervals.",
        "rows": [{"policy_id": policy, **row} for policy, row in zip(POLICIES, rows)],
        "source_sha256": source_hashes,
        "generator": str(Path(__file__).resolve().relative_to(ROOT)),
        "generator_sha256": sha256(Path(__file__)),
        "table": target.name,
        "table_sha256": sha256(target),
    }
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"status": "PASS", "rows": len(rows), "runs": len(runs),
                      "table": str(target), "receipt": str(receipt_path)}))


if __name__ == "__main__":
    main()
