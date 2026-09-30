#!/usr/bin/env python3
"""Reproduce the frozen, matched admission-snapshot experiment locally.

Outputs are create-only. All attempted states, including ineligible ones, are
retained. The compiled search uses the production admission header.
"""
from __future__ import annotations
import argparse
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import platform
import random
import statistics
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parents[1]
PROTOCOL = json.loads((HERE / "protocol.json").read_text())
POLICIES = PROTOCOL["policies"]


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def rate(units):
    return 650 * units // 106


def source_hashes():
    paths = [*HERE.glob("*.py"), *HERE.glob("*.cpp"), HERE / "protocol.json",
             PROJECT / "tests/test_matched.py"]
    paths += [PROJECT / "experiments/gateway" / name for name in
              ("request_admission.hpp", "controller.hpp", "deadline_certificate.hpp")]
    return {str(p.relative_to(PROJECT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}


def validate_protocol():
    # These fixed executable assumptions are intentionally not a tuning API.
    expected = {"units": 106, "capacity_bytes_per_ms": 650, "base_units": 21,
                "maximum_units": 74, "critical_original_deadlines": [20, 50, 100],
                "critical_certificate_deadlines": [15, 45, 95], "critical_headroom_ms": 5,
                "critical_guardband_ms": 0, "critical_sigma": [480, 3328, 2560],
                "critical_rho": [94, 104, 100],
                "video": {"units": 27, "work_bytes": 1600, "period_ms": 10, "deadline_ms": 15},
                "monitor": {"units": 5, "work_bytes": 100, "period_ms": 10, "deadline_ms": 20},
                "policies": ["peak_reservation", "keep_calendar", "full_guard"]}
    for key, value in expected.items():
        if PROTOCOL[key] != value:
            raise ValueError(f"unsupported protocol change: {key}")


class Adapter:
    def __init__(self, directory):
        self.binary = Path(directory) / "guard_adapter"
        command = ["c++", "-std=c++17", "-O2", "-Wall", "-Wextra", str(HERE / "guard_adapter.cpp"), "-o", str(self.binary)]
        subprocess.run(command, check=True, capture_output=True, text=True)
        self.process = subprocess.Popen([str(self.binary)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, bufsize=1)

    def call(self, case):
        p = case["pending"]
        vals = [case["lead"], case["old_final"], case["request"]["units"], case["request"]["length"],
                len(p), len(case["old_calendar"]), len(case["request"]["releases"])]
        vals += [v for x in p for v in (x["certificate_deadline"], x["work"])]
        vals += case["old_calendar"] + case["other_units"]
        vals += [v for x in case["request"]["releases"] for v in x]
        self.process.stdin.write(" ".join(map(str, vals)) + "\n")
        self.process.stdin.flush()
        line = self.process.stdout.readline()
        if not line:
            raise RuntimeError("compiled adapter terminated")
        return json.loads(line)

    def close(self):
        self.process.stdin.close()
        self.process.wait(timeout=5)
        if self.process.returncode:
            raise RuntimeError(f"compiled adapter exit {self.process.returncode}")


def generate(seed, stratum, replicate):
    # No acceptance/rejection-dependent generation, retries, or future lookahead.
    rng = random.Random((seed * 100 + PROTOCOL["strata"].index(stratum)) * 10 + replicate)
    lead = rng.choice(PROTOCOL["leads_ms"])
    length = rng.choice(PROTOCOL["request_lengths_ms"])
    units = rng.choice(PROTOCOL["request_units"])
    peak = rng.choice(PROTOCOL["initial_peaks"])
    old_q = rng.choice(PROTOCOL["old_booking_units"])
    end = lead + length
    if stratum == "fallback_in_prefix":
        expiry = lead // 2
        old_start, old_end = 0, end + 7
    elif stratum == "fallback_in_window":
        expiry = lead + max(1, length // 2)
        old_start, old_end = 0, end + 7
    elif stratum == "booking_boundary":
        expiry = end + 5
        old_start, old_end = lead + max(1, length // 2), end + 7
    else:
        peak, expiry = 49, 0
        old_start, old_end = 0, end + 7
    calendar = [peak if t < expiry else 49 for t in range(PROTOCOL["horizon_ms"])]
    other = [32 + (old_q if old_start <= t < old_end else 0) for t in range(len(calendar))]
    pending = []
    for k, (deadline, sigma) in enumerate(zip(PROTOCOL["critical_certificate_deadlines"], PROTOCOL["critical_sigma"])):
        work = rng.randint(0, sigma)
        due = rng.choice([max(1, deadline // 2), deadline])
        if work:
            pending.append({"stream": k, "work": work, "certificate_deadline": due, "original_deadline": due + 5})
    staged = rng.choice(PROTOCOL["request_release_patterns"])
    work = rate(units) * length
    mid = length // 2
    releases = [[0, work]] if staged == "upfront" else [[0, rate(units) * mid], [mid, rate(units) * (length - mid)]]
    jobs = []
    def add(group, release, deadline, amount, stream=None):
        jobs.append({"id": len(jobs), "group": group, "release": release, "deadline": deadline,
                     "work": amount, "stream": stream})
    for p in pending:
        add("critical", 0, p["original_deadline"], p["work"], p["stream"])
    for k, (sigma, rho, deadline) in enumerate(zip(PROTOCOL["critical_sigma"], PROTOCOL["critical_rho"], PROTOCOL["critical_original_deadlines"])):
        tokens = sigma
        for t in range(1, PROTOCOL["original_release_stop_ms"]):
            tokens = min(sigma + rho, tokens + rho)
            amount = min(tokens, rng.randint(0, 2 * rho))
            tokens -= amount
            if amount:
                add("critical", t, t + deadline, amount, k)
    for group in ("video", "monitor"):
        spec = PROTOCOL[group]
        phase = rng.randrange(spec["period_ms"])
        for t in range(phase, PROTOCOL["original_release_stop_ms"], spec["period_ms"]):
            add(group, t, t + spec["deadline_ms"], spec["work_bytes"])
    old = {"start": old_start, "end": old_end, "units": old_q, "work": rate(old_q) * (old_end - old_start)}
    return {"id": f"{seed}-{stratum}-{replicate}", "seed": seed, "stratum": stratum, "replicate": replicate,
            "lead": lead, "old_peak": max(calendar), "old_final": expiry,
            "old_calendar": calendar, "other_units": other, "pending": pending, "older_booking": old,
            "request": {"units": units, "length": length, "work": work, "releases": releases, "pattern": staged},
            "original_jobs": jobs}


def validate_workload(case):
    """Independent all-window checks, not the generator's token bookkeeping."""
    for k, (sigma, rho) in enumerate(zip(PROTOCOL["critical_sigma"], PROTOCOL["critical_rho"])):
        arrivals = [0] * PROTOCOL["original_release_stop_ms"]
        for job in case["original_jobs"]:
            if job["group"] == "critical" and job["stream"] == k and job["release"] > 0:
                arrivals[job["release"]] += job["work"]
        prefix = [0]
        for amount in arrivals:
            prefix.append(prefix[-1] + amount)
        for a in range(1, len(arrivals)):
            for b in range(a + 1, len(arrivals) + 1):
                if prefix[b] - prefix[a] > sigma + rho * (b - a):
                    raise AssertionError("realized future critical envelope exceeded")
    for group in ("video", "monitor"):
        spec = PROTOCOL[group]
        if spec["work_bytes"] > rate(spec["units"]) * min(spec["period_ms"], spec["deadline_ms"]):
            raise AssertionError("incumbent constant-rate contract infeasible")
    req = case["request"]
    remaining = req["work"]
    for when, amount in req["releases"]:
        if remaining > rate(req["units"]) * (req["length"] - when):
            raise AssertionError("generated new release schedule infeasible")
        remaining -= amount
    if remaining:
        raise AssertionError("payload mismatch")
    if any(c + o > 106 for c, o in zip(case["old_calendar"], case["other_units"])):
        raise AssertionError("generated prior capacity oversubscribed")
    return True


def execute(case, decision):
    """Exact byte-preemptive guaranteed quotas, then global EDF surplus.

    Tenant chunks share one completion deadline and whole-job credit. Every
    original item retains its initial deadline; missed bytes are not redefined.
    """
    jobs = deepcopy(case["original_jobs"])
    old = case["older_booking"]
    jobs.append({"id": len(jobs), "group": "old_tenant", "release": old["start"],
                 "deadline": old["end"], "work": old["work"]})
    req = case["request"]
    if decision["accepted"]:
        for when, work in req["releases"]:
            jobs.append({"id": len(jobs), "group": "new_tenant", "release": case["lead"] + when,
                         "deadline": case["lead"] + req["length"], "work": work})
    releases = defaultdict(list)
    for job in jobs:
        job["remaining"] = job["work"]
        job["completed"] = None
        releases[job["release"]].append(job)
    queues = defaultdict(list)
    served = 0
    def serve(queue, budget, now):
        nonlocal served
        used = 0
        while budget and queue:
            job = queue[0]
            amount = min(budget, job["remaining"])
            job["remaining"] -= amount
            budget -= amount
            used += amount
            served += amount
            if not job["remaining"]:
                job["completed"] = now + 1
                queue.pop(0)
        return used
    capacity_violations = 0
    for t in range(PROTOCOL["execution_slots"]):
        for group in tuple(queues):
            queues[group] = [j for j in queues[group] if j["deadline"] > t]
        for job in releases[t]:
            queues[job["group"]].append(job)
        for queue in queues.values():
            queue.sort(key=lambda j: (j["deadline"], j["id"]))
        critical = case["old_calendar"][t]
        if decision["replaced"] and t >= case["lead"]:
            critical = decision["target"] if t < case["lead"] + req["length"] else decision["tail"]
        units = {"critical": critical, "video": 27, "monitor": 5,
                 "old_tenant": old["units"] if old["start"] <= t < old["end"] else 0,
                 "new_tenant": req["units"] if decision["accepted"] and case["lead"] <= t < case["lead"] + req["length"] else 0}
        capacity_violations += sum(units.values()) > 106
        left = 650
        for group, quota in units.items():
            # A faulty candidate must produce a recorded capacity failure,
            # never fictitious service beyond the physical server or a hang.
            left -= serve(queues[group], min(left, rate(quota)), t)
        while left > 0:
            active = [(q[0]["deadline"], q[0]["id"], group) for group, q in queues.items() if q]
            if not active:
                break
            _, _, group = min(active)
            left -= serve(queues[group], min(left, queues[group][0]["remaining"]), t)
    counts = {}
    for group in ("critical", "video", "monitor", "old_tenant", "new_tenant"):
        selected = [j for j in jobs if j["group"] == group]
        timely = [j for j in selected if j["completed"] is not None and j["completed"] <= j["deadline"]]
        complete = len(timely) == len(selected)
        counts[group] = {"items": len(selected), "misses": len(selected) - len(timely),
                         "offered_bytes": sum(j["work"] for j in selected),
                         "timely_bytes": sum(j["work"] for j in timely) if group not in ("old_tenant", "new_tenant") or complete else 0}
    # Rejection remains in offered new-work accounting, not an execution miss.
    counts["new_tenant"]["offered_bytes"] = req["work"]
    counts["new_tenant"]["accepted_job"] = int(decision["accepted"])
    counts["new_tenant"]["completed_job"] = int(bool(decision["accepted"]) and counts["new_tenant"]["misses"] == 0)
    return {"groups": counts, "served_bytes": served, "capacity_violations": capacity_violations,
            "original_timely_bytes": sum(counts[k]["timely_bytes"] for k in ("critical", "video", "monitor")),
            "original_offered_bytes": sum(counts[k]["offered_bytes"] for k in ("critical", "video", "monitor")),
            "incumbent_failures": sum(counts[k]["misses"] for k in ("critical", "video", "monitor"))}


def quantile(values, probability):
    ordered = sorted(values)
    z = (len(ordered) - 1) * probability
    lo = int(z)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] * (hi - z) + ordered[hi] * (z - lo) if hi != lo else ordered[lo]


def paired_ci(values):
    rng = random.Random(918271)
    means = [statistics.fmean(rng.choices(values, k=len(values))) for _ in range(2000)]
    return [quantile(means, .025), quantile(means, .975)]


def summarize(rows):
    result = {"attempted_snapshots": len(rows), "eligible_snapshots": sum(x["eligible"] for x in rows),
              "initial_state_rejections": [x["id"] for x in rows if not x["eligible"]],
              "audit_errors": [{"id": x["id"], "errors": x["audit_errors"]} for x in rows if x["audit_errors"]],
              "policies": {}, "strata": {}, "paired": {}}
    for name in ["all", *PROTOCOL["strata"]]:
        selected = rows if name == "all" else [x for x in rows if x["stratum"] == name]
        dest = result["policies"] if name == "all" else result["strata"].setdefault(name, {})
        for policy in POLICIES:
            data = [x["policies"][policy] for x in selected if x["eligible"]]
            seed_means = []
            for seed in sorted({x["seed"] for x in selected}):
                samples = [x["policies"][policy]["decision_us"] for x in selected if x["eligible"] and x["seed"] == seed]
                if samples:
                    seed_means.append(statistics.fmean(samples))
            dest[policy] = {"offered": len(selected), "eligible": len(data),
                "accepted": sum(x["accepted"] for x in data),
                "replaced": sum(x["replaced"] for x in data),
                "timely_tenant_bytes": sum(x["execution"]["groups"]["new_tenant"]["timely_bytes"] for x in data),
                "offered_tenant_bytes": sum(x["request_work"] for x in selected),
                "incumbent_failures": sum(x["execution"]["incumbent_failures"] for x in data),
                "incumbent_items": sum(sum(x["execution"]["groups"][k]["items"] for k in ("critical", "video", "monitor")) for x in data),
                "old_tenant_failures": sum(x["execution"]["groups"]["old_tenant"]["misses"] for x in data),
                "new_tenant_failures": sum(x["execution"]["groups"]["new_tenant"]["accepted_job"] - x["execution"]["groups"]["new_tenant"]["completed_job"] for x in data),
                "mean_decision_us": statistics.fmean(seed_means),
                "decision_seed_mean_ci_us": paired_ci(seed_means),
                "maximum_decision_us": max(x["decision_us"] for x in data),
                "p95_decision_us": quantile([x["decision_us"] for x in data], .95),
                "original_timely_bytes": sum(x["execution"]["original_timely_bytes"] for x in data),
                "original_offered_bytes": sum(x["execution"]["original_offered_bytes"] for x in data)}
    for baseline in POLICIES[:2]:
        samples = {"additional_commitments_per_snapshot": [], "additional_timely_tenant_bytes_per_snapshot": [], "additional_decision_us": []}
        for seed in sorted({x["seed"] for x in rows}):
            selected = [x for x in rows if x["seed"] == seed]
            for metric, field in [("additional_commitments_per_snapshot", "accepted"), ("additional_decision_us", "decision_us")]:
                samples[metric].append(statistics.fmean((x["policies"]["full_guard"][field] - x["policies"][baseline][field]) if x["eligible"] else 0 for x in selected))
            samples["additional_timely_tenant_bytes_per_snapshot"].append(statistics.fmean((x["policies"]["full_guard"]["execution"]["groups"]["new_tenant"]["timely_bytes"] - x["policies"][baseline]["execution"]["groups"]["new_tenant"]["timely_bytes"]) if x["eligible"] else 0 for x in selected))
        result["paired"][baseline] = {key: {"mean": statistics.fmean(vals), "seed_bootstrap_ci": paired_ci(vals)} for key, vals in samples.items()}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    validate_protocol()
    hashes = source_hashes()
    reference = json.loads((PROJECT / "results/matched/verification.json").read_text())
    if digest(PROTOCOL) != reference["protocol_sha256"]:
        raise SystemExit("The replay protocol differs from the reported experiment")
    args.output.mkdir(parents=True, exist_ok=False)
    test_run = subprocess.run([sys.executable, "-m", "unittest", "discover", "-s", str(PROJECT / "tests"),
                               "-p", "test_matched.py", "-v"], capture_output=True, text=True)
    (args.output / "tests.txt").write_text(test_run.stdout + test_run.stderr)
    if test_run.returncode:
        raise SystemExit("guard-value unit tests failed; see tests.txt")
    seeds = range(PROTOCOL["heldout_seed_start"], PROTOCOL["heldout_seed_start"] + PROTOCOL["heldout_seed_count"])
    rows = []
    began = datetime.now(timezone.utc).isoformat()
    with tempfile.TemporaryDirectory(prefix="matched-guard-") as directory, gzip.open(args.output / "workloads.jsonl.gz", "wt") as workload_file, gzip.open(args.output / "outcomes.jsonl.gz", "wt") as outcome_file:
        adapter = Adapter(directory)
        try:
            for seed in seeds:
                for stratum in PROTOCOL["strata"]:
                    for replicate in range(PROTOCOL["replicates_per_seed_stratum"]):
                        case = generate(seed, stratum, replicate)
                        validate_workload(case)
                        workload_file.write(json.dumps(case, sort_keys=True) + "\n")
                        answer = adapter.call(case)
                        row = {"id": case["id"], "seed": seed, "stratum": stratum, "replicate": replicate,
                               "eligible": answer["eligible"], "old_reason": answer["old_reason"], "request_work": case["request"]["work"],
                               "request_units": case["request"]["units"], "request_length": case["request"]["length"],
                               "workload_sha256": digest(case), "policies": {}, "audit_errors": [],
                               "old_certificate": answer["old_certificate"], "old_oracle": answer["old_oracle"],
                               "old_resources": answer["old_resources"]}
                        if answer["old_certificate"] != answer["old_oracle"]:
                            row["audit_errors"].append("production/dense initial-state disagreement")
                        for policy, decision in zip(POLICIES, answer["decisions"]):
                            if answer["eligible"]:
                                if not decision["calendar_oracle"] or not decision["resources"]:
                                    row["audit_errors"].append(f"{policy}: final calendar or capacity violates independent oracle")
                                decision["execution"] = execute(case, decision)
                                if decision["execution"]["capacity_violations"] or decision["execution"]["incumbent_failures"] or decision["execution"]["groups"]["old_tenant"]["misses"] or decision["execution"]["groups"]["new_tenant"]["misses"]:
                                    row["audit_errors"].append(f"{policy}: model commitment failure")
                            row["policies"][policy] = decision
                        rows.append(row)
                        outcome_file.write(json.dumps(row, sort_keys=True) + "\n")
                print(f"finished seed {seed}", flush=True)
        finally:
            adapter.close()
    summary = summarize(rows)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    receipt = {"status": "FAIL" if summary["audit_errors"] else "PASS", "phase": "reported_workload_replay", "started_utc": began,
               "completed_utc": datetime.now(timezone.utc).isoformat(), "python": platform.python_version(),
               "platform": platform.platform(), "compiler": subprocess.run(["c++", "--version"], capture_output=True, text=True, check=True).stdout.splitlines()[0],
               "source_sha256": hashes, "protocol_sha256": digest(PROTOCOL),
               "snapshot_count": len(rows), "policy_decisions": len(rows) * 3,
               "unit_test_methods": 10,
               "realized_workload_envelope_audits": len(rows), "dense_calendar_audits": len(rows) * 4,
               "output_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(args.output.iterdir()) if p.is_file()}}
    receipt["reference_receipt_sha256"] = hashlib.sha256((PROJECT / "results/matched/verification.json").read_bytes()).hexdigest()
    (args.output / "verification.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(summary["policies"], indent=2))
    if summary["audit_errors"]:
        raise SystemExit("audit/commitment failures retained in complete outcomes; no safety claim permitted")


if __name__ == "__main__":
    main()
