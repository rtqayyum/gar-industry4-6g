#!/usr/bin/env python3
"""Export exact existing results as JSON coordinates; no fitting or smoothing."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=ROOT / "results/reserve")
    parser.add_argument("--protocol", type=Path, default=ROOT / "experiments/reserve/protocol.json")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit("Refusing to overwrite plot evidence")
    args.output.mkdir(parents=True)
    summary = json.loads((args.input / "summary.json").read_text())
    protocol = json.loads(args.protocol.read_text())
    runs = [json.loads(line) for line in (args.input / "runs.jsonl").read_text().splitlines()]
    line_data = []
    epoch_data = []
    feasibility = []
    for cell in summary["cells"]:
        base = {k: cell[k] for k in ["load", "factor", "policy"]}
        feasibility.append({**base, **cell["configuration"], "completed_runs": cell["completed_runs"]})
        if not cell["completed_runs"]:
            continue
        line_data.append({**base, "video_failure_percent": cell["classes"]["video"]["failure_percent"],
                          "video_p99_ms_mean": cell["classes"]["video"]["p99_ms_mean"],
                          "accepted_tenants": cell["accepted_tenants"],
                          "efficiency": cell["timely_payload_efficiency_nominal"],
                          "utilization": cell["utilization"]})
        for epoch, kinds in cell["epochs"].items():
            for kind, values in kinds.items():
                epoch_data.append({**base, "epoch": epoch, "kind": kind, **values})
    scatter = []
    for row in runs:
        if row["status"] == "completed":
            scatter.append({k: row[k] for k in ["seed", "load", "factor", "policy", "accepted_tenants", "timely_payload_efficiency_nominal"]}
                           | {"video_failures": row["class_metrics"]["video"]["failed_jobs"],
                              "video_p99_ms": row["class_metrics"]["video"]["p99_ms"]})
    traces = []
    selected_seed = protocol["prespecified_example_seed"]
    with gzip.open(args.input / "traces.jsonl.gz", "rt") as handle:
        for line in handle:
            row = json.loads(line)
            if row["seed"] == selected_seed and row["factor"] == 1 and row["load"] == 1:
                traces.append(row)
    data = {"load_line": line_data, "epoch_bars": epoch_data, "seed_scatter": scatter,
            "feasibility_heatmap": feasibility, "prespecified_trace": traces,
            "paired_comparisons": summary["paired_comparisons"],
            "labels": {"video_p99_ms_mean": "Mean across seeds of per-run successful-job p99 completion delay (ms); failures shown separately",
                       "efficiency": "Timely byte efficiency relative to nominal 650 bytes/ms",
                       "infeasible": "Do not connect a protected-policy curve through rejected provisioning cells",
                       "confidence_intervals": "95% paired seed-bootstrap intervals; 128 independent seeds per held-out cell",
                       "scatter": "Each point is one seed, not one packet"}}
    out = args.output / "plot_data.json"
    out.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    manifest = {"source_inputs": {name: sha(args.input / name) for name in ["summary.json", "runs.jsonl", "traces.jsonl.gz"]},
                "protocol_sha256": sha(args.protocol),
                "exporter_sha256": sha(Path(__file__)), "output_sha256": sha(out)}
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"lines": len(line_data), "epoch_rows": len(epoch_data), "scatter_points": len(scatter), "example_traces": len(traces)}))


if __name__ == "__main__":
    main()
