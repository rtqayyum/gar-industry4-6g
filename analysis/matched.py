#!/usr/bin/env python3
"""Clustered admission effects and an independently checked calendar example.

Inference clusters the four within-stratum cases from each independent seed.
The calendar example is selected by a declared lexical rule, not its appearance.
All plotted margins are recomputed here by exhaustive integer interval checks,
independently of the optimized production certificate. The selected decision is
also replayed through the unchanged, hash-verified production C++ adapter.
"""
from __future__ import annotations

import csv
import argparse
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import random
import statistics
import tempfile

ROOT = Path(__file__).resolve().parents[1]
GUARD = ROOT / "results/matched"
SOURCE = ROOT / "experiments/matched"
OUT = None
ANALYSIS = None
STRATA = ["fallback_in_prefix", "fallback_in_window", "booking_boundary", "noncrossing"]
POLICIES = ["peak_reservation", "keep_calendar", "full_guard"]
ANALYSIS_PROTOCOL = {
    "data": "All 2048 recorded evaluation snapshots; no new workload or tuning",
    "cluster": "128 paired seeds; all four cases per seed and stratum stay together",
    "bootstrap_replicates": 10000,
    "bootstrap_seed": 918271,
    "interval": "Two-sided percentile 95%, linear interpolation; descriptive stratum intervals, not multiplicity-adjusted hypothesis tests",
    "resample": "One common seed-index bootstrap reused across the four balanced strata",
    "example_selection": "First lexicographic (seed, stratum, replicate) eligible fallback_in_window case with full_guard replacement and keep_calendar rejection",
    "finite_horizon_slots": 256,
    "example_slack": "S(b)=min_{0<=a<b}[sum_{n=a}^{b-1}c(u(n))-Dbar(a,b)]; every integer interval is enumerated",
    "example_margin": "M(n)=106-u(n)-other_units(n)-new_request_units(n)",
    "continuation": "Finite graph ends at H=256; constant-tail rate, deadline breakpoints, final-change horizon and constancy checked separately",
}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical_sha(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def load_records(path):
    with gzip.open(path, "rt") as stream:
        return [json.loads(line) for line in stream]


def quantile(values, p):
    ordered = sorted(values)
    z = (len(ordered) - 1) * p
    lo = int(z)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (z - lo)


def cluster_effects(rows):
    """All offers, including rejections, contribute to each seed's denominator."""
    seeds = sorted({row["seed"] for row in rows})
    assert len(seeds) == 128 and len(rows) == 2048
    assert len({row["id"] for row in rows}) == len(rows)
    assert set(seeds) == set(range(20001, 20129))
    rng = random.Random(ANALYSIS_PROTOCOL["bootstrap_seed"])
    samples = [rng.choices(range(len(seeds)), k=len(seeds))
               for _ in range(ANALYSIS_PROTOCOL["bootstrap_replicates"])]
    effects = []
    for index, stratum in enumerate(STRATA, 1):
        seed_effects = []
        accepted = {p: 0 for p in POLICIES}
        for seed in seeds:
            group = [r for r in rows if r["seed"] == seed and r["stratum"] == stratum]
            assert len(group) == 4 and {r["replicate"] for r in group} == {0, 1, 2, 3}
            for r in group:
                indicators = [int(r["policies"][p]["accepted"]) if r["eligible"] else 0 for p in POLICIES]
                assert indicators[0] <= indicators[1] <= indicators[2]
                for p, indicator in zip(POLICIES, indicators):
                    accepted[p] += indicator
            seed_effects.append(100 * statistics.fmean(
                int(r["policies"]["full_guard"]["accepted"]) - int(r["policies"]["keep_calendar"]["accepted"])
                if r["eligible"] else 0 for r in group))
        boot = [statistics.fmean(seed_effects[i] for i in sample) for sample in samples]
        mean = statistics.fmean(seed_effects)
        low, high = quantile(boot, .025), quantile(boot, .975)
        assert 0 <= low <= mean <= high <= 100
        effects.append({"index": index, "stratum": stratum, "clusters": len(seeds),
                        "cases_per_cluster": 4, "offered": 512, "accepted": accepted,
                        "gain_pp": mean, "ci_low_pp": low, "ci_high_pp": high})
    total = {p: sum(x["accepted"][p] for x in effects) for p in POLICIES}
    counts = [total[POLICIES[0]], total[POLICIES[1]] - total[POLICIES[0]],
              total[POLICIES[2]] - total[POLICIES[1]], len(rows) - total[POLICIES[2]]]
    assert counts == [743, 102, 1184, 19] and sum(counts) == 2048
    return effects, counts


def select_example(rows):
    return next(r for r in sorted(rows, key=lambda x: (x["seed"], x["stratum"], x["replicate"]))
                if r["eligible"] and r["stratum"] == "fallback_in_window"
                and r["policies"]["full_guard"]["replaced"]
                and not r["policies"]["keep_calendar"]["accepted"])


def demand(case, protocol, a, b):
    """Count admissible release slots explicitly instead of using header algebra."""
    assert 0 <= a < b
    work = sum(p["work"] for p in case["pending"] if a == 0 and p["certificate_deadline"] <= b)
    for m, burst, rate in zip(protocol["critical_certificate_deadlines"],
                              protocol["critical_sigma"], protocol["critical_rho"]):
        count = len(range(max(1, a), max(max(1, a), b - m + 1)))
        if count:
            work += burst + rate * count
    return work


def dense_slack(case, calendar, protocol):
    prefix = [0]
    for units in calendar:
        prefix.append(prefix[-1] + protocol["capacity_bytes_per_ms"] * units // protocol["units"])
    rows = []
    for b in range(1, len(calendar) + 1):
        slack, start = min((prefix[b] - prefix[a] - demand(case, protocol, a, b), a) for a in range(b))
        rows.append({"b": b, "slack": slack, "a": start})
    return rows


def tail_audit(case, calendar, final_change, protocol):
    service = protocol["capacity_bytes_per_ms"] * calendar[-1] // protocol["units"]
    deadlines = protocol["critical_certificate_deadlines"]
    required_horizon = max(final_change + max(deadlines) + 1,
                           max((p["certificate_deadline"] for p in case["pending"]), default=0))
    breaks = []
    for length in sorted(set(deadlines)):
        required = 0
        for m, burst, rate in zip(deadlines, protocol["critical_sigma"], protocol["critical_rho"]):
            release_count = len(range(0, max(0, length - m + 1)))
            if release_count:
                required += burst + rate * release_count
        breaks.append({"length_ms": length, "demand_bytes": required,
                       "service_bytes": service * length, "margin_bytes": service * length - required})
    rate_margin = service - sum(protocol["critical_rho"])
    constant = len(set(calendar[final_change:])) == 1
    passed = (rate_margin >= 0 and all(x["margin_bytes"] >= 0 for x in breaks)
              and constant and len(calendar) >= required_horizon)
    return {"pass": passed, "tail_units": calendar[-1], "tail_bytes_per_ms": service,
            "arrival_rate_bytes_per_ms": sum(protocol["critical_rho"]),
            "rate_margin_bytes_per_ms": rate_margin, "deadline_breakpoints": breaks,
            "constant_from_ms": final_change, "constant_tail": constant,
            "required_horizon_ms": required_horizon, "checked_horizon_ms": len(calendar)}


def example_analysis(case, outcome, protocol):
    assert canonical_sha(case) == outcome["workload_sha256"]
    H = protocol["horizon_ms"]
    assert H == 256 and len(case["old_calendar"]) == len(case["other_units"]) == H
    decision = outcome["policies"]["full_guard"]
    begin, end = case["lead"], case["lead"] + case["request"]["length"]
    old = case["old_calendar"]
    new = [old[n] if n < begin else decision["target"] if n < end else decision["tail"] for n in range(H)]
    assert new[:begin] == old[:begin]
    old_slack = dense_slack(case, old, protocol)
    new_slack = dense_slack(case, new, protocol)
    old_tail = tail_audit(case, old, case["old_final"], protocol)
    new_tail = tail_audit(case, new, end, protocol)
    request = [case["request"]["units"] if begin <= n < end else 0 for n in range(H)]
    original_margin = [106 - old[n] - case["other_units"][n] for n in range(H)]
    old_plus = [original_margin[n] - request[n] for n in range(H)]
    new_margin = [106 - new[n] - case["other_units"][n] - request[n] for n in range(H)]
    assert min(original_margin) >= 0 and min(old_plus) < 0 and min(new_margin) >= 0
    assert min(x["slack"] for x in old_slack + new_slack) >= 0
    assert old_tail["pass"] and new_tail["pass"]
    remaining = case["request"]["work"]
    release_slacks = []
    quota = protocol["capacity_bytes_per_ms"] * case["request"]["units"] // protocol["units"]
    for when, work in case["request"]["releases"]:
        margin = quota * (case["request"]["length"] - when) - remaining
        release_slacks.append({"relative_release_ms": when, "remaining_bytes": remaining, "margin_bytes": margin})
        assert margin >= 0
        remaining -= work
    assert remaining == 0
    return {"id": case["id"], "workload_sha256": canonical_sha(case),
            "outcome_sha256": canonical_sha(outcome), "activation_ms": begin,
            "request_end_ms": end, "old_fallback_ms": case["old_final"],
            "older_booking": case["older_booking"], "request": case["request"],
            "pending": case["pending"], "old_peak_units": max(old),
            "replacement_units": decision["target"], "continuing_units": decision["tail"],
            "intervals_per_calendar": H * (H + 1) // 2,
            "old_min_slack_bytes": min(x["slack"] for x in old_slack),
            "new_min_slack_bytes": min(x["slack"] for x in new_slack),
            "original_min_resource_margin_units": min(original_margin),
            "retained_plus_request_min_margin_units": min(old_plus),
            "new_min_resource_margin_units": min(new_margin),
            "old_tail": old_tail, "new_tail": new_tail,
            "release_suffixes": release_slacks,
            "incumbent_failures": decision["execution"]["incumbent_failures"],
            "accepted_new_completes": decision["execution"]["groups"]["new_tenant"]["completed_job"]}, (old, new, old_slack, new_slack, original_margin, old_plus, new_margin)


def replay_production(case, outcome):
    spec = importlib.util.spec_from_file_location("matched_guard_study", SOURCE / "run.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with tempfile.TemporaryDirectory(prefix="guard-replay-") as temporary:
        adapter = module.Adapter(temporary)
        try:
            current = adapter.call(case)
        finally:
            adapter.close()
    assert current["eligible"] == outcome["eligible"]
    for name, decision in zip(POLICIES, current["decisions"]):
        for key, value in decision.items():
            if key != "decision_us":
                assert value == outcome["policies"][name][key], (name, key)
    return {"pass": True, "comparison": "Every recorded C++ decision field except host-dependent timing", "policies": POLICIES}


def table(name, fields, rows):
    with (OUT / name).open("w", newline="") as stream:
        writer = csv.writer(stream, delimiter=" ", lineterminator="\n")
        writer.writerow(fields)
        writer.writerows([[format(x, ".12g") if isinstance(x, float) else x for x in row] for row in rows])


def main():
    global GUARD, OUT, ANALYSIS
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=GUARD)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    GUARD = args.input.resolve()
    OUT = ANALYSIS = args.output.resolve()
    OUT.mkdir(parents=True, exist_ok=False)
    rows = load_records(GUARD / "outcomes.jsonl.gz")
    cases = load_records(GUARD / "workloads.jsonl.gz")
    assert len(rows) == len(cases) == 2048
    cases_by_id = {x["id"]: x for x in cases}
    assert len(cases_by_id) == 2048
    for outcome in rows:
        assert canonical_sha(cases_by_id[outcome["id"]]) == outcome["workload_sha256"]
        assert not outcome["audit_errors"]
    effects, nested = cluster_effects(rows)
    selected = select_example(rows)
    case = cases_by_id[selected["id"]]
    protocol = json.loads((SOURCE / "protocol.json").read_text())
    example, values = example_analysis(case, selected, protocol)
    example["production_replay"] = replay_production(case, selected)
    old, new, old_slack, new_slack, original_margin, old_plus, new_margin = values
    table("guard_effects.dat", ["stratum", "gain_pp", "minus", "plus", "low", "high"],
          [(x["index"], x["gain_pp"], x["gain_pp"] - x["ci_low_pp"], x["ci_high_pp"] - x["gain_pp"],
            x["ci_low_pp"], x["ci_high_pp"]) for x in effects])
    cumulative = 0
    nested_rows = []
    for i, count in enumerate(nested, 1):
        nested_rows.append((i, count, count / 2048 * 100, cumulative, cumulative + count, cumulative + count / 2))
        cumulative += count
    table("guard_nested.dat", ["segment", "count", "percent", "start", "end", "center"], nested_rows)
    # One final boundary closes the staircase; it is not another service slot.
    table("certificate_calendar.dat", ["n", "old", "new", "older_booking", "new_request"],
          [(n, old[min(n, 255)], new[min(n, 255)],
            case["older_booking"]["units"] if case["older_booking"]["start"] <= n < case["older_booking"]["end"] else 0,
            case["request"]["units"] if case["lead"] <= n < case["lead"] + case["request"]["length"] else 0)
           for n in range(257)])
    table("certificate_slack.dat", ["b", "old", "new", "old_worst_a", "new_worst_a"],
          [(a["b"], a["slack"], b["slack"], a["a"], b["a"])
           for a, b in zip(old_slack, new_slack)])
    table("certificate_margin.dat", ["n", "original", "old_plus_request", "new"],
          [(n, original_margin[min(n, 255)], old_plus[min(n, 255)], new_margin[min(n, 255)])
           for n in range(257)])
    analysis = {"status": "PASS", "protocol": ANALYSIS_PROTOCOL,
                "all_offered_snapshots": 2048, "paired_seed_clusters": 128,
                "eligible_snapshots": sum(x["eligible"] for x in rows),
                "stratum_effects": effects, "nested_counts": nested,
                "selected_example": example}
    for name, value in [("protocol.json", ANALYSIS_PROTOCOL), ("analysis.json", analysis)]:
        (ANALYSIS / name).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    inputs = [GUARD / "outcomes.jsonl.gz", GUARD / "workloads.jsonl.gz", GUARD / "summary.json",
              SOURCE / "protocol.json", SOURCE / "run.py"]
    receipt = {"scope": "Recorded evidence; 128-seed paired uncertainty and one lexical-rule certificate example",
               "source_sha256": {p.name: sha(p) for p in inputs},
               "generator_sha256": sha(Path(__file__)),
               "output_sha256": {p.name: sha(p) for p in sorted(OUT.glob("*.dat"))}}
    assert len(receipt["output_sha256"]) == 5
    (OUT / "provenance.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"status": analysis["status"], "example": example,
                      "stratum_effects": effects}, indent=2))


if __name__ == "__main__":
    main()
