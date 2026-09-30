#!/usr/bin/env python3
"""Independently check maximal-calendar feasibility against frozen enumeration.

Reads original JSON fixtures and results without importing the original solver.
Re-enumerates exactly the existing profiles; no new experiment or tuning occurs.
"""
import argparse
import hashlib
import itertools
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "data/formal"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def check(condition, description):
    if not condition:
        raise AssertionError(description)


def service(case, pulse, horizon):
    floors = case["prefix"] + list(pulse) + [case["tail"]] * (
        horizon - case["delay"] - case["term"])
    return [case["capacity"] * value // case["units"] for value in floors]


def interval_demands(case, horizon):
    values = []
    for left in range(horizon):
        for right in range(left + 1, horizon + 1):
            work = sum(amount for due, amount in case["pending"]
                       if left == 0 and due <= right)
            for envelope in case["envelopes"]:
                releases = max(0, right - envelope["deadline"] - max(left, 1) + 1)
                if releases:
                    work += envelope["sigma"] + releases * envelope["rho"]
            values.append((left, right, work))
    return values


def finite_pass(byte_service, demands):
    # Explicit interval sums intentionally avoid the original solver's prefix sums.
    return all(sum(byte_service[left:right]) >= demand
               for left, right, demand in demands)


def tail_pass(case):
    byte_floor = case["capacity"] * case["tail"] // case["units"]
    if byte_floor < sum(e["rho"] for e in case["envelopes"]):
        return False
    for length in {e["deadline"] for e in case["envelopes"]}:
        required = sum(e["sigma"] + e["rho"] * (length - e["deadline"] + 1)
                       for e in case["envelopes"] if e["deadline"] <= length)
        if required > length * byte_floor:
            return False
    return True


def main(output):
    if output.exists():
        raise FileExistsError(f"Refusing to overwrite output directory: {output}")
    source_paths = [FIXTURES / "instances.json", FIXTURES / "results.json", FIXTURES / "receipt.json",
                    ROOT / "experiments/formal/reference.py"]
    before = {str(path.relative_to(ROOT)): digest(path) for path in source_paths}
    instances = json.loads(source_paths[0].read_text())
    originals = json.loads(source_paths[1].read_text())
    original_receipt = json.loads(source_paths[2].read_text())
    check(len(instances) == len(originals) == 30, "Original 30 snapshots retained")
    rows = []
    profile_count = 0
    dominance_checks = 0
    cost_nonoptimal_examples = 0
    for case, original in zip(instances, originals):
        check(case == original["case"], "Fixture/result identity")
        end = case["delay"] + case["term"]
        horizon = max(end + max((e["deadline"] for e in case["envelopes"]), default=0) + 1,
                      max((p[0] for p in case["pending"]), default=0), len(case["bookings"]))
        check(horizon == original["horizon"], "Identical boundary horizon")
        demands = interval_demands(case, horizon)
        common_tail = tail_pass(case)
        check(common_tail == original["tail_feasible"], "Same tail conditions")
        for prior in original["request_levels"]:
            q = prior["request_units"]
            caps = []
            resource_ok = True
            for slot in range(horizon):
                booking = case["bookings"][slot] if slot < len(case["bookings"]) else 0
                if case["delay"] <= slot < end:
                    cap = min(case["maximum"], case["units"] - case["background"] - booking - q)
                    resource_ok &= cap >= case["base"]
                    caps.append(cap)
                else:
                    fixed = case["prefix"][slot] if slot < case["delay"] else case["tail"]
                    resource_ok &= fixed + booking + case["background"] <= case["units"]
            maximal = (common_tail and resource_ok and
                       finite_pass(service(case, caps, horizon), demands))
            passing = []
            enumerated = 0
            if common_tail and resource_ok:
                upper_service = service(case, caps, horizon)
                for pulse in itertools.product(*(range(case["base"], cap + 1) for cap in caps)):
                    enumerated += 1
                    candidate_service = service(case, pulse, horizon)
                    for left, right, _ in demands:
                        check(sum(candidate_service[left:right]) <= sum(upper_service[left:right]),
                              "Maximal calendar must dominate each interval")
                        dominance_checks += 1
                    if finite_pass(candidate_service, demands):
                        cost = sum(value - case["base"] for value in case["prefix"] + list(pulse))
                        passing.append((cost, pulse))
            check(maximal == bool(passing) == prior["calendar_feasible"], "All feasibility decisions agree")
            check(enumerated == prior["profiles_enumerated"], "Original exact enumeration count")
            if passing:
                best_cost, best_pulse = min(passing)
                check(best_cost == prior["calendar_min_cost"], "Original minimum calendar cost")
                check(list(best_pulse) == prior["calendar_witness"], "Original minimum-cost witness")
                upper_cost = sum(value - case["base"] for value in case["prefix"] + caps)
                cost_nonoptimal_examples += upper_cost > best_cost
            else:
                check(prior["calendar_min_cost"] is None, "Infeasible has no minimum cost")
                upper_cost = best_cost = None
            constants = [entry for entry in passing if len(set(entry[1])) == 1]
            check(bool(constants) == prior["constant_feasible"], "Constant-family feasibility retained")
            if constants:
                check(min(constants)[0] == prior["constant_min_cost"], "Constant-family cost retained")
            rows.append({"case": case["name"], "request_units": q,
                         "common_resources_valid": resource_ok, "common_tail_valid": common_tail,
                         "maximal_calendar": caps, "maximal_certificate_passes": bool(maximal),
                         "enumeration_has_passing_calendar": bool(passing),
                         "profiles_enumerated": enumerated, "maximal_cost": upper_cost,
                         "minimum_calendar_cost": best_cost})
            profile_count += enumerated
    check(profile_count == original_receipt["profiles_enumerated"] == 14337, "All original profiles checked")
    check(before == {str(path.relative_to(ROOT)): digest(path) for path in source_paths},
          "Original artifacts must remain unchanged")

    # Same reservation and total payload, different release availability.
    # Contract: 100 bytes, one byte/slot in [0,100). All-at-start completes;
    # all-at-last-slot cannot use the prior 99 idle slots retrospectively.
    release_examples = {"bytes": 100, "service_bytes_per_slot": 1, "slots": 100,
                        "available_at_start_completed_bytes": 100,
                        "available_at_final_slot_completed_bytes": 1}
    check(sum(min(1, max(0, 100 - t)) for t in range(100)) == 100, "Available payload completes")
    check(sum(1 for t in range(100) if t >= 99) == 1, "Late payload cannot consume earlier service")

    output.mkdir(parents=True, exist_ok=False)
    (output / "maximal_calendar_rows.json").write_text(json.dumps(rows, indent=2) + "\n")
    receipt = {
        "status": "PASS", "snapshots": len(instances), "request_levels": len(rows),
        "profiles_enumerated": profile_count, "interval_dominance_checks": dominance_checks,
        "maximal_vs_exhaustive_feasibility_disagreements": 0,
        "original_minimum_cost_disagreements": 0,
        "feasible_levels_where_maximal_is_not_minimum_cost": cost_nonoptimal_examples,
        "scope": "Independent dense check of the same 30 constructed snapshots; no new traffic trial. "
                 "Unrestricted per-slot floor family with immutable prefix/tail and no switch/cost budget. "
                 "Exact only for the stated sufficient certificate, not arbitrary schedule feasibility.",
        "release_availability_example": release_examples,
        "unchanged_original_files_sha256": before,
        "verification_source_sha256": digest(Path(__file__)),
        "rows_sha256": digest(output / "maximal_calendar_rows.json")}
    (output / "verification_receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    main(parser.parse_args().output)
