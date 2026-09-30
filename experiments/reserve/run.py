#!/usr/bin/env python3
"""Replay every reported incumbent-reservation cell and retain raw outcomes."""
from __future__ import annotations

import argparse
from collections import defaultdict
from contextlib import ExitStack
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import random
import statistics
import time

from model import canonical, configuration, digest, make_workload, run_model


HERE = Path(__file__).resolve().parent


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def interval(values: list[float], replicates: int, seed: int) -> dict[str, float | int]:
    rng = random.Random(seed)
    n = len(values)
    boot = sorted(sum(values[rng.randrange(n)] for _ in range(n)) / n for _ in range(replicates))
    return {"n": n, "mean": statistics.fmean(values),
            "low": boot[int(.025 * replicates)], "high": boot[min(replicates - 1, int(.975 * replicates))]}


def aggregate(protocol: dict, summaries: list[dict]) -> dict:
    groups = defaultdict(list)
    by_key = {}
    for row in summaries:
        groups[(row["load"], row["factor"], row["policy"])].append(row)
        by_key[(row["load"], row["factor"], row["policy"], row["seed"])] = row
    cells = []
    paired = []
    reps = protocol["statistics"]["replicates"]
    boot_seed = protocol["statistics"]["bootstrap_seed"]
    for (load, factor, policy), rows in sorted(groups.items()):
        cell = {"load": load, "factor": factor, "policy": policy, "runs": len(rows),
                "configuration": rows[0]["configuration"],
                "completed_runs": sum(r["status"] == "completed" for r in rows)}
        good = [r for r in rows if r["status"] == "completed"]
        if good:
            for metric in ["timely_payload_efficiency_nominal", "utilization", "accepted_tenants",
                           "reserve_lent_bytes", "unused_bytes"]:
                cell[metric] = interval([r[metric] for r in good], reps, boot_seed)
            cell["accepted_tenant_failures"] = sum(r["accepted_tenant_failures"] for r in good)
            cell["offered_tenants"] = sum(r["offered_tenants"] for r in good)
            cell["classes"] = {}
            for kind in good[0]["class_metrics"]:
                metrics = [r["class_metrics"][kind] for r in good]
                offered = sum(m["offered_jobs"] for m in metrics)
                failed = sum(m["failed_jobs"] for m in metrics)
                cell["classes"][kind] = {"offered": offered, "failures": failed,
                                         "failure_percent": 100 * failed / offered if offered else None,
                                         "p99_ms_mean": statistics.fmean(m["p99_ms"] for m in metrics if m["p99_ms"] is not None)
                                         if any(m["p99_ms"] is not None for m in metrics) else None,
                                         "p99_ms_max": max((m["p99_ms"] for m in metrics if m["p99_ms"] is not None), default=None)}
            cell["epochs"] = {}
            for epoch in protocol["epochs"]:
                cell["epochs"][epoch] = {}
                for kind in ("critical", "video", "monitor"):
                    ms = [r["epochs"][epoch][kind] for r in good]
                    cell["epochs"][epoch][kind] = {"offered": sum(m["offered_jobs"] for m in ms),
                                                  "failures": sum(m["failures"] for m in ms),
                                                  "p99_ms_mean": statistics.fmean(m["p99_ms"] for m in ms if m["p99_ms"] is not None)
                                                  if any(m["p99_ms"] is not None for m in ms) else None}
            if policy != "borrowable":
                pairs = [(by_key[(load, factor, "borrowable", r["seed"])], r) for r in good]
                pairs = [(a, b) for a, b in pairs if a["status"] == "completed"]
                if pairs:
                    comparison = {"load": load, "factor": factor, "contrast": f"borrowable_minus_{policy}"}
                    for metric in ["timely_payload_efficiency_nominal", "utilization", "accepted_tenants"]:
                        comparison[metric] = interval([a[metric] - b[metric] for a, b in pairs], reps, boot_seed)
                    if policy == "no_tenant_control":
                        comparison["incumbent_p99_delta_ms"] = {
                            kind: interval([a["class_metrics"][kind]["p99_ms"] - b["class_metrics"][kind]["p99_ms"]
                                            for a, b in pairs], reps, boot_seed)
                            for kind in ("critical", "video", "monitor")}
                    paired.append(comparison)
        cells.append(cell)
    return {"protocol_sha256": digest(protocol), "cells": cells, "paired_comparisons": paired,
            "interpretation": "Percentile intervals use independent seed-level paired differences, not individual packets. Zero failures is finite-model evidence under declared contracts."}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if output.exists():
        raise SystemExit(f"Refusing to overwrite evidence directory: {output}")
    output.mkdir(parents=True)
    protocol = json.loads((HERE / "protocol.json").read_text())
    sources = {p.name: sha(p) for p in sorted(HERE.glob("*.py"))}
    sources["protocol.json"] = sha(HERE / "protocol.json")
    source_dir = output / "sources"
    source_dir.mkdir()
    for name in sources:
        (source_dir / name).write_bytes((HERE / name).read_bytes())
    freeze = {"reproduction_started_utc": datetime.now(timezone.utc).isoformat(),
              "split": "reported_heldout_workloads", "protocol": protocol, "source_sha256": sources,
              "python_requirement": "Python >= 3.10; standard library only"}
    reference = json.loads((HERE.parents[1] / "results/reserve/summary.json").read_text())
    if digest(protocol) != reference["protocol_sha256"]:
        raise SystemExit("The replay protocol differs from the reported experiment")
    write_json(output / "configuration.json", freeze)
    spec = protocol["heldout_seeds"]
    seeds = range(spec["first"], spec["first"] + spec["count"])
    summaries = []
    started = time.monotonic()
    with ExitStack() as stack:
        summary_file = stack.enter_context((output / "runs.jsonl").open("w"))
        workload_file = stack.enter_context(gzip.open(output / "workloads.jsonl.gz", "wt", compresslevel=5))
        outcome_file = stack.enter_context(gzip.open(output / "outcomes.jsonl.gz", "wt", compresslevel=5))
        trace_file = stack.enter_context(gzip.open(output / "traces.jsonl.gz", "wt", compresslevel=5))
        for load in protocol["load_levels"]:
            for seed in seeds:
                jobs = make_workload(protocol, seed, load)
                workload_file.write(json.dumps({"seed": seed, "load": load, "jobs": [j.payload() for j in jobs]}, separators=(",", ":")) + "\n")
                for factor in protocol["delivered_service_factors"]:
                    for policy in protocol["policies"]:
                        summary, outcomes, trace = run_model(protocol, seed, load, factor, policy, jobs)
                        summaries.append(summary)
                        summary_file.write(json.dumps(summary, separators=(",", ":")) + "\n")
                        identifier = {"seed": seed, "load": load, "factor": factor, "policy": policy,
                                      "status": summary["status"]}
                        outcome_file.write(json.dumps({**identifier, "outcomes": outcomes}, separators=(",", ":")) + "\n")
                        trace_file.write(json.dumps({**identifier, "trace": trace}, separators=(",", ":")) + "\n")
            print(f"Completed load={load}; {len(summaries)} protocol cells; {time.monotonic()-started:.1f}s", flush=True)
    write_json(output / "summary.json", aggregate(protocol, summaries))
    completed = [r for r in summaries if r["status"] == "completed"]
    protected = [r for r in completed if r["policy"] != "no_video_contract"]
    validation = {"protocol_cells": len(summaries), "completed_cells": len(completed),
                  "provisioning_rejected_cells": len(summaries) - len(completed),
                  "protected_runs": len(protected),
                  "protected_incumbent_failures": sum(r["class_metrics"][k]["failed_jobs"] for r in protected for k in ("critical", "video", "monitor")),
                  "accepted_tenant_failures_all_policies": sum(r["accepted_tenant_failures"] for r in completed),
                  "capacity_invariant_errors": sum(len(r["invariant_errors"]) for r in completed),
                  "elapsed_seconds": time.monotonic() - started}
    write_json(output / "validation.json", validation)
    manifest = {str(p.relative_to(output)): {"sha256": sha(p), "bytes": p.stat().st_size}
                for p in sorted(output.rglob("*")) if p.is_file()}
    write_json(output / "manifest.json", manifest)
    print(json.dumps(validation, indent=2), flush=True)


if __name__ == "__main__":
    main()
