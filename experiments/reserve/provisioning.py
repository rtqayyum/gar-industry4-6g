"""Arithmetic-only audit of incumbent provisioning; never runs/tunes a policy.

The alternative units are prospective initial contracts, not legal reductions
of the fixed unit entitlements. Exact rational arithmetic and exhaustive
integer-unit checks make the reported minimum independently reproducible.
"""

from __future__ import annotations

import argparse
from fractions import Fraction
import hashlib
import json
from pathlib import Path


def ceil_fraction(value: Fraction) -> int:
    return -(-value.numerator // value.denominator)


def rate(units: int, factor: Fraction) -> int:
    amount = Fraction(650 * units, 106) * factor
    return amount.numerator // amount.denominator


def minimum_units(work: int, period: int, deadline: int, factor: Fraction) -> int:
    needed_rate = ceil_fraction(Fraction(work, min(period, deadline)))
    units = ceil_fraction(Fraction(106 * needed_rate, 650) / factor)
    assert work <= rate(units, factor) * min(period, deadline)
    assert units == 0 or work > rate(units - 1, factor) * min(period, deadline)
    # Independent finite scan; the formula should identify the first passing q.
    assert units == next(q for q in range(107)
                         if work <= rate(q, factor) * min(period, deadline))
    return units


def audit() -> dict:
    rows = []
    for factor in (Fraction(1), Fraction(95, 100), Fraction(9, 10)):
        for load in (Fraction(6, 10), Fraction(8, 10), Fraction(1),
                     Fraction(12, 10), Fraction(14, 10)):
            video_work = ceil_fraction(2400 * load)
            units = {
                "critical_base": minimum_units(500, 5, 8, factor),
                "critical_peak": minimum_units(1450, 5, 8, factor),
                "video": minimum_units(video_work, 10, 15, factor),
                "monitor": minimum_units(100, 10, 20, factor),
            }
            original = {
                "critical_base": ceil_fraction(Fraction(21) / factor),
                "critical_peak": ceil_fraction(Fraction(49) / factor),
                "video": ceil_fraction(Fraction(ceil_fraction(40 * load)) / factor),
                "monitor": ceil_fraction(Fraction(5) / factor),
            }
            peak = sum(units[key] for key in ("critical_peak", "video", "monitor"))
            original_peak = sum(original[key] for key in
                                ("critical_peak", "video", "monitor"))
            rows.append({
                "service_factor": float(factor), "video_load": float(load),
                "video_max_bytes": video_work,
                "minimum_units": units,
                "minimum_rates_bytes_per_ms": {key: rate(q, factor)
                                                for key, q in units.items()},
                "minimum_peak_total": peak, "minimum_feasible": peak <= 106,
                "frozen_v13_units": original, "frozen_v13_peak_total": original_peak,
                "frozen_v13_feasible": original_peak <= 106,
                "units_above_minimum": original_peak - peak,
            })
    result = {
        "audit_version": "v14-provisioning-arithmetic-1",
        "scope": "Prospective independent minimum floor per declared stream; no simulation or policy retuning.",
        "condition": "W <= floor(650*factor*q/106)*min(T,D)",
        "formula": "q_min = ceil(106*ceil(W/min(T,D))/(650*factor))",
        "capacity_units": 106,
        "frozen_v13_feasible_cells": sum(row["frozen_v13_feasible"] for row in rows),
        "minimum_feasible_cells": sum(row["minimum_feasible"] for row in rows),
        "all_unit_minima_exhaustively_verified": True,
        "rows": rows,
        "interpretation": [
            "Minimum refers only to the stated per-stream constant-rate sufficient condition, not global schedule optimality.",
            "A lower prospective floor does not preserve an already contracted larger unit entitlement.",
            "No tenant counts, latency, efficiency, or execution reliability are inferred for the alternative provisioning.",
            "The frozen event begins after activation and ends before its last possible deadline plus return; arbitrary unannounced events are outside this audit.",
        ],
    }
    assert result["frozen_v13_feasible_cells"] == 10
    assert result["minimum_feasible_cells"] == 12
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = audit()
    result["generator_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    result["frozen_protocol_sha256"] = hashlib.sha256(
        Path(__file__).with_name("protocol.json").read_bytes()).hexdigest()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    # Create-only output: a changed audit gets a new path, preserving provenance.
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")
    print(json.dumps({"output": str(args.output),
                      "frozen_v13_feasible_cells": result["frozen_v13_feasible_cells"],
                      "minimum_feasible_cells": result["minimum_feasible_cells"]}))


if __name__ == "__main__":
    main()
