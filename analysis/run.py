#!/usr/bin/env python3
"""Recompute the final numerical plotting inputs from recorded results."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--plots", action="store_true")
    args = parser.parse_args()
    destination = args.output.resolve()
    destination.mkdir(parents=True, exist_ok=False)
    comparisons = {}
    for study in ("matched", "online", "reserve"):
        target = destination / study
        subprocess.run([sys.executable, str(ROOT / "analysis" / (study + ".py")),
                        "--output", str(target)], cwd=ROOT, check=True)
        for reference in sorted((ROOT / "results/analysis" / study).iterdir()):
            if not reference.is_file():
                continue
            generated = target / reference.name
            if not generated.is_file() or sha(generated) != sha(reference):
                raise RuntimeError(f"Recomputed table differs: {study}/{reference.name}")
            comparisons[f"{study}/{reference.name}"] = sha(generated)
    subprocess.run([sys.executable, str(ROOT / "experiments/packet/precision.py"),
                    "--output", str(destination / "packet")], cwd=ROOT, check=True)
    reference = ROOT / "results/packet/precision/precision.json"
    generated = destination / "packet/precision.json"
    if sha(generated) != sha(reference):
        raise RuntimeError("Recomputed packet precision analysis differs")
    comparisons["packet/precision.json"] = sha(generated)
    if args.plots:
        subprocess.run([sys.executable, str(ROOT / "analysis/plots.py"),
                        "--data", str(destination), "--output", str(destination / "plots")],
                       cwd=ROOT, check=True)
    receipt = {"status": "PASS", "numeric_outputs_sha256": comparisons,
               "scope": "Recorded-data reanalysis; same inference units and no new experimental observations."}
    (destination / "verification.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
