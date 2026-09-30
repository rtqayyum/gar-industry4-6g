#!/usr/bin/env python3
"""Independent checks for release-aware tenant commitments.

This is a deterministic finite-window verifier, not a real-time service monitor.
Arrivals become available at the beginning of an integer slot; service in that
slot completes at its end. Completion at the end of the window is timely.
The forward queue and backward certificate deliberately use separate logic.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import itertools
import json
from pathlib import Path


def suffix_certificate(arrivals: tuple[int, ...], service: tuple[int, ...]) -> bool:
    if len(arrivals) != len(service) or not arrivals:
        raise ValueError("Arrival and service vectors must have equal positive length")
    if any(not isinstance(x, int) or x < 0 for x in (*arrivals, *service)):
        raise ValueError("Use nonnegative integer byte counts")
    work = supply = 0
    for release, available in zip(reversed(arrivals), reversed(service)):
        work += release
        supply += available
        if work > supply:
            return False
    return True


def forward_executor(arrivals: tuple[int, ...], service: tuple[int, ...]) -> int:
    """Independent work-conserving byte queue; returns unfinished bytes."""
    pending = 0
    for slot in range(len(arrivals)):
        pending += arrivals[slot]
        transmitted = min(service[slot], pending)
        pending -= transmitted
    return pending


def lower_availability_certificate(total: int, available_before: tuple[int, ...],
                                   service: tuple[int, ...]) -> bool:
    """Robust test when before[a] lower-bounds bytes released strictly before a.

    before has H+1 entries, starts at zero, and must end at the full payload.
    The caller, not this arithmetic check, must enforce the availability bound
    and the per-slot delivered-service guarantee.
    """
    if len(available_before) != len(service) + 1:
        raise ValueError("Availability needs one entry per slot boundary")
    values = (total, *available_before, *service)
    if any(not isinstance(x, int) or x < 0 for x in values):
        raise ValueError("Use nonnegative integer byte counts")
    if available_before[0] != 0 or available_before[-1] != total:
        raise ValueError("The release contract must cover the entire payload")
    if any(x > y for x, y in zip(available_before, available_before[1:])):
        raise ValueError("Cumulative availability cannot decrease")
    supply = 0
    for boundary in range(len(service) - 1, -1, -1):
        supply += service[boundary]
        if total - available_before[boundary] > supply:
            return False
    return True


def before_vector(arrivals: tuple[int, ...]) -> tuple[int, ...]:
    return (0, *itertools.accumulate(arrivals))


def verify(output: Path) -> dict:
    output.mkdir(parents=True, exist_ok=False)
    tested = accepted = mismatches = 0
    vectors = tuple(itertools.product(range(3), repeat=5))
    for arrivals in vectors:
        for service in vectors:
            decision = suffix_certificate(arrivals, service)
            complete = forward_executor(arrivals, service) == 0
            robust = lower_availability_certificate(sum(arrivals), before_vector(arrivals), service)
            tested += 1
            accepted += decision
            mismatches += decision != complete or robust != decision

    # Enforcing an earlier-availability contract must preserve completion.
    monotonic_tests = monotonic_failures = 0
    small_vectors = tuple(itertools.product(range(3), repeat=4))
    for arrivals in small_vectors:
        for service in small_vectors:
            if not suffix_certificate(arrivals, service):
                continue
            for late in range(1, 4):
                if arrivals[late] == 0:
                    continue
                for early in range(late):
                    moved = list(arrivals)
                    moved[late] -= 1
                    moved[early] += 1
                    monotonic_tests += 1
                    monotonic_failures += forward_executor(tuple(moved), service) != 0

    cases = {
        "available_at_start": ((100,) + (0,) * 99, (1,) * 100),
        "two_equal_releases": ((50,) + (0,) * 49 + (50,) + (0,) * 49, (1,) * 100),
        "all_payload_in_last_slot": ((0,) * 99 + (100,), (1,) * 100),
        "insufficient_delivered_service": ((100,) + (0,) * 99, (1,) * 50 + (0,) * 50),
    }
    rows = []
    for name, (arrivals, service) in cases.items():
        pending = forward_executor(arrivals, service)
        rows.append({"case": name, "payload_bytes": sum(arrivals),
                     "reserved_bytes": 100, "delivered_service_bytes": sum(service),
                     "certificate_pass": suffix_certificate(arrivals, service),
                     "unfinished_bytes": pending})
    with (output / "release_cases.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    with (output / "release_timelines.csv").open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["case", "boundary_ms", "released_bytes", "service_bytes", "completed_bytes"])
        for name, (arrivals, service) in cases.items():
            released = supply = completed = pending = 0
            writer.writerow([name, 0, 0, 0, 0])
            for n, (release, amount) in enumerate(zip(arrivals, service), 1):
                pending += release
                served = min(pending, amount)
                pending -= served
                released += release
                supply += amount
                completed += served
                writer.writerow([name, n, released, supply, completed])
    result = {
        "status": "PASS" if mismatches == 0 and monotonic_failures == 0 else "FAIL",
        "scope": "Constructed finite slotted byte-service cases; not gateway measurements or a radio trial",
        "exhaustive_horizon_slots": 5,
        "per_slot_arrival_and_service_values": [0, 1, 2],
        "exhaustive_cases": tested,
        "certificate_acceptances": accepted,
        "forward_backward_disagreements": mismatches,
        "earlier_release_checks": monotonic_tests,
        "earlier_release_failures": monotonic_failures,
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "examples": rows,
    }
    (output / "verification.json").write_text(json.dumps(result, indent=2) + "\n")
    if result["status"] != "PASS":
        raise AssertionError(result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.output), indent=2))
