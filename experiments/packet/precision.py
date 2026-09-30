#!/usr/bin/env python3
"""Recompute command timing, service-window rates, and context-level uncertainty.

This is numerical reanalysis of recorded packet experiments, not a live run.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import math
from collections import defaultdict
from pathlib import Path
import statistics

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
FOLLOWUP = "results/packet/controlled/"
HISTORICAL = "results/packet/historical/"
WINDOWS = "data/packet/service_probe_windows.csv.gz"


def open_csv(path):
    path = Path(path)
    return gzip.open(path, "rt", newline="") if path.suffix == ".gz" else path.open(newline="")


def decompressed_sha(path):
    digest = hashlib.sha256()
    with gzip.open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_csv(path):
    with open_csv(path) as stream:
        return list(csv.DictReader(stream))


def close(a, b, label="number", tolerance=1e-10):
    if not math.isclose(float(a), float(b), rel_tol=0, abs_tol=tolerance):
        raise AssertionError(f"{label}: {a!r} != {b!r}")


def quantile_linear(values, probability):
    values = sorted(values)
    if not values or not 0 <= probability <= 1:
        raise ValueError("Nonempty sample and probability in [0,1] required")
    index = (len(values) - 1) * probability
    lo, hi = math.floor(index), math.ceil(index)
    return values[lo] + (index - lo) * (values[hi] - values[lo])


def join_applications(commands, events):
    commands = [r for r in commands if r["kind"] == "targeted" and r["type"] == "ack"]
    accepted = {(r["name"], int(r["executor_plan_id"])): r
                for r in commands if r["accepted"] == "True"}
    assert len(accepted) == sum(r["accepted"] == "True" for r in commands)
    joined, seen = [], set()
    for event in events:
        if event["kind"] != "installation":
            continue
        key = event["name"], int(event["plan_id"])
        if key not in accepted:
            continue
        assert key not in seen, "Duplicate installation for accepted command"
        seen.add(key)
        command = accepted[key]
        actual = int(event["actual_application_ns"])
        delay = (actual - int(command["transmit_ns"])) / 1e6
        lateness = (actual - int(event["scheduled_ns"])) / 1e6
        close(lateness, float(event["timer_lateness_us"]) / 1000, "lateness timestamp")
        assert delay >= 0 and lateness >= 0
        joined.append({"policy": command["policy"], "trial": key[0], "plan_id": key[1],
                       "transmit_to_application_ms": delay, "application_lateness_ms": lateness})
    summaries = {}
    for policy in sorted({r["policy"] for r in commands}):
        group = [r for r in joined if r["policy"] == policy]
        policy_commands = [r for r in commands if r["policy"] == policy]
        count = sum(r["accepted"] == "True" for r in policy_commands)
        assert group
        summaries[policy] = {
            "accepted": count, "matched": len(group), "unmatched": count - len(group),
            "rejected": len(policy_commands) - count,
            "p99_transmit_to_application_ms": quantile_linear(
                [r["transmit_to_application_ms"] for r in group], .99),
            "p99_application_lateness_ms": quantile_linear(
                [r["application_lateness_ms"] for r in group], .99),
            "maximum_application_lateness_ms": max(r["application_lateness_ms"] for r in group),
        }
    return summaries


def command_audit(root):
    result = join_applications(read_csv(root / (FOLLOWUP + "commands.csv")),
                               read_csv(root / (FOLLOWUP + "executor_events.csv")))
    reference = {r["policy"]: r for r in read_csv(root / (FOLLOWUP + "command_timing_summary.csv"))}
    for policy, stats in result.items():
        row = reference[policy]
        assert stats["accepted"] == int(row["commands_accepted"])
        assert stats["matched"] == int(row["transmit_to_actual_application_observations"])
        close(stats["p99_transmit_to_application_ms"],
              float(row["transmit_to_actual_application_us_p99"]) / 1000, "activation p99")
    assert sum(v["accepted"] for v in result.values()) == 4603
    assert sum(v["matched"] for v in result.values()) == 4595
    return {"policies": result, "matched_fraction_pct": 100 * 4595 / 4603,
            "statistic": "Pooled matched-installation 99th percentile, linear interpolation",
            "event": "Recorded command application, not first packet service",
            "cohort": "48 controlled targeted trials; accepted installation commands only",
            "lateness_definition": "(actual_application_ns - scheduled_ns) / 1000000",
            "delay_definition": "(actual_application_ns - transmit_ns) / 1000000",
            "no_unconditional_p999_claim": True}


def probe_audit(root):
    # These rows are the frozen residence-filtered windows, not fresh probes.
    expected = json.loads((root / "data/packet/expected_outputs.json").read_text())
    assert decompressed_sha(root / WINDOWS) == expected["controlled_analysis"]["service_probe_windows.csv"]
    groups = defaultdict(list)
    with open_csv(root / WINDOWS) as stream:
        for row in csv.DictReader(stream):
            if int(row["floor"]) == 49 and int(row["window_ms"]) == 500:
                assert int(row["end_ms"]) - int(row["start_ms"]) == 500
                assert int(row["nominal_integer_floor_bytes"]) == 300 * 500
                groups[row["name"]].append(row)
    reference = {r["name"]: r for r in read_csv(root / (FOLLOWUP + "service_probe_500ms_executions.csv"))}
    execution = []
    for name, rows in sorted(groups.items()):
        starts = sorted(int(r["start_ms"]) for r in rows)
        assert len(starts) == len(set(starts))
        assert any(b - a < 500 for a, b in zip(starts, starts[1:])), "Windows must overlap"
        mean = sum(int(r["critical_egress_bytes"]) for r in rows) / (500 * len(rows))
        minimum = min(int(r["critical_egress_bytes"]) for r in rows) / 500
        old = reference[name]
        assert len(rows) == int(old["residence_covered_windows"])
        close(mean, old["critical_rate_mean"], "per-execution mean")
        close(minimum, old["critical_rate_min"], "per-execution minimum")
        execution.append({"name": name, "floor_units": 49, "packet_bytes": int(rows[0]["packet_bytes"]),
                          "seed": int(rows[0]["seed"]), "covered_windows": len(rows),
                          "mean_critical_departure_bytes_per_ms": mean,
                          "minimum_critical_departure_bytes_per_ms": minimum,
                          "first_start_ms": starts[0], "last_start_ms": starts[-1],
                          "input_trace_sha256": old["input_trace_sha256"]})
    assert len(execution) == 4
    return {"executions": execution, "execution_mean_min_bytes_per_ms": min(
                r["mean_critical_departure_bytes_per_ms"] for r in execution),
            "execution_mean_max_bytes_per_ms": max(
                r["mean_critical_departure_bytes_per_ms"] for r in execution),
            "nominal_integer_floor_bytes_per_ms": 650 * 49 // 106,
            "window_ms": 500, "start_grid_ms": 1,
            "selection": "Windows wholly covered by the union of observed critical ingress-to-egress/expiry residence, including serialization",
            "departure_interval": "(start, end]",
            "scope": "Four executions; two packet sizes with two repetitions of identical input bytes per size; overlapping windows are not independent trials",
            "not_established": ["continuously runnable kernel queue", "deterministic lower service curve"],
            "audit_scope": "Reaggregates frozen window records; original raw-residence reconstruction is covered by the preserved independent follow-up audit"}


def pooled_contrast(focal, comparator, denominator):
    if not len(focal) == len(comparator) == len(denominator) or not len(focal):
        raise ValueError("Matched nonempty arrays required")
    if any(d <= 0 for d in denominator):
        raise ValueError("Denominators must be positive")
    return 100 * (sum(focal) - sum(comparator)) / sum(denominator)


def complete_case_audit(root):
    rows = [r for r in read_csv(root / (HISTORICAL + "trial_results.csv")) if r["arm"] == "primary"]
    ordered = sorted({(r["family"], int(r["seed"])) for r in rows})
    lookup = {(r["family"], int(r["seed"]), r["policy"]): r for r in rows}
    assert len(ordered) == 10 and len(rows) == 40
    rng = np.random.default_rng(20260928)
    draws, offset = [], 0
    for family in sorted({f for f, _ in ordered}):
        count = sum(f == family for f, _ in ordered)
        draws.append(offset + rng.integers(0, count, size=(10000, count)))
        offset += count
    indices = np.concatenate(draws, axis=1)
    n, other, den, contexts = [], [], [], []
    for family, seed in ordered:
        a, b = lookup[family, seed, "gar_h"], lookup[family, seed, "predictive_h"]
        assert a["nominal_capacity_bytes"] == b["nominal_capacity_bytes"]
        n.append(int(a["strict_useful_bytes"]))
        other.append(int(b["strict_useful_bytes"]))
        den.append(int(a["nominal_capacity_bytes"]))
        contexts.append({"family": family, "seed": seed, "focal_useful_bytes": n[-1],
                         "comparator_useful_bytes": other[-1], "denominator": den[-1]})
    point = pooled_contrast(n, other, den)
    n, other, den = map(lambda a: np.asarray(a, dtype=float), [n, other, den])
    boot = 100 * (n[indices].sum(axis=1) - other[indices].sum(axis=1)) / den[indices].sum(axis=1)
    low, high = np.quantile(boot, [.025, .975])
    reference = next(r for r in read_csv(root / (HISTORICAL + "paired_cluster_comparisons.csv"))
                     if r["comparator"] == "predictive_h" and r["metric"] == "strict_capacity_efficiency")
    for key, value in [("pooled_difference", point), ("ci_low", low), ("ci_high", high)]:
        close(value, 100 * float(reference[key]), key)
    for row in contexts:
        row["pooled_weight"] = row["denominator"] / sum(den)
    return {"focal": "GAR-H", "comparator": "Predictive", "contexts": contexts,
            "difference_pp": point, "ci95_low_pp": float(low), "ci95_high_pp": float(high),
            "weighting": "Denominator-weighted context contrasts, equal to the difference of pooled timely-byte/capacity ratios",
            "uncertainty": "10000 paired context-bootstrap draws within four fixed families; seed 20260928; percentile interval",
            "equal_context_complete_case_mean_pp_for_distinction_only": float(np.mean(100 * (n - other) / den))}


def missing_bounds(observed, contexts, comparator):
    lower, upper, pairs = [], [], []
    def interval(family, seed, policy):
        value = observed.get((family, seed, policy))
        if value is None:
            return 0., 100.
        assert 0 <= value <= 100
        return value, value
    for family, seed in contexts:
        alo, ahi = interval(family, seed, "gar_h")
        blo, bhi = interval(family, seed, comparator)
        lower.append(alo - bhi)
        upper.append(ahi - blo)
        if alo == ahi and blo == bhi:
            pairs.append(alo - blo)
    return {"comparison_policy": comparator, "intended_primary_contexts": len(contexts),
            "pairwise_observed_contexts": len(pairs),
            "pairwise_complete_case_mean_gar_minus_comparator_pp": statistics.mean(pairs),
            "all_intended_contexts_lower_bound_pp": statistics.mean(lower),
            "all_intended_contexts_upper_bound_pp": statistics.mean(upper)}


def missing_audit(root):
    registry = read_csv(root / (HISTORICAL + "attempt_registry.csv"))
    rows = read_csv(root / (HISTORICAL + "all_completed_trial_results.csv"))
    primary = [r for r in registry if r["arm"] == "primary"]
    observed = {(r["family"], int(r["seed"]), r["policy"]): 100 * float(r["strict_capacity_efficiency"])
                for r in rows if r["arm"] == "primary"}
    contexts = sorted({(r["family"], int(r["seed"])) for r in primary})
    assert len(registry) == 66 and len(primary) == 48 and len(contexts) == 12 and len(observed) == 46
    missing = [r["trial_name"] for r in primary if (r["family"], int(r["seed"]), r["policy"]) not in observed]
    comparisons = [missing_bounds(observed, contexts, policy)
                   for policy in ["fixed_h", "predictive_h", "residual_priority_h"]]
    reference = json.loads((root / "results/packet/precision/precision.json").read_text())["historical_missing_bounds"]
    assert missing == reference["missing_primary_attempts"]
    for row, previous in zip(comparisons, reference["comparisons"]):
        assert row["comparison_policy"] == previous["comparison_policy"]
        for key in row.keys() - {"comparison_policy"}:
            close(row[key], previous[key], key)
    return {"comparisons": comparisons, "missing_primary_attempts": missing,
            "weighting": "Equal weight 1/12 for every intended primary context",
            "missing_efficiency_range_pct": [0, 100],
            "not_confidence_intervals": True,
            "different_estimand_from_complete_case": True}


def calculate(root=ROOT):
    return {"status": "PASS", "new_experiments": False,
            "command_timing": command_audit(root), "floor49_probe": probe_audit(root),
            "historical_complete_case": complete_case_audit(root),
            "historical_missing_bounds": missing_audit(root)}



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    target = args.output.expanduser().absolute()
    for part in (target, *target.parents):
        if part.is_symlink():
            raise ValueError("Output path must not contain a symlink")
    if target.exists():
        raise FileExistsError("Output must be a new directory")
    if not target.parent.is_dir():
        raise ValueError("Output parent must already exist")
    facts = calculate()
    expected = json.loads((ROOT / "results/packet/precision/precision.json").read_text())
    if facts != expected:
        raise ValueError("Precision results differ from the recorded numerical reference")
    target.mkdir()
    (target / "precision.json").write_text(json.dumps(facts, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"status": "PASS", "new_experiments": False,
                      "matches_reference": True, "output": str(target)}))


if __name__ == "__main__":
    main()
