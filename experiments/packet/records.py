#!/usr/bin/env python3
"""Verify, extract, or reanalyze the recorded UDP experiments offline."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
import gzip
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data/packet"
RESULTS = ROOT / "results/packet"
MAX_FILE_BYTES = 128 * 1024 * 1024
MAX_TOTAL_BYTES = 2 * 1024 * 1024 * 1024
POLICIES = ("fixed_h", "predictive_h", "gar_h", "residual_priority_h")
LABELS = {"fixed_h": "Fixed + gate", "predictive_h": "Predictive + gate",
          "gar_h": "GAR-H", "residual_priority_h": "Residual priority + gate"}
TYPES = {0: "Robot", 1: "PLC", 2: "AGV", 3: "Meter", 4: "Monitor", 5: "Alarm", 6: "Video"}


def sha(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def safe_path(base: Path, name: str, *, required: bool = False) -> Path:
    if not isinstance(name, str) or not name or "\\" in name:
        raise ValueError("Invalid relative path")
    value = PurePosixPath(name)
    if value.is_absolute() or value.as_posix() != name or any(p in {"", ".", ".."} for p in name.split("/")):
        raise ValueError("Unsafe relative path: " + name)
    target = base / Path(*value.parts)
    for path in (target, *target.parents):
        if path.is_symlink():
            raise ValueError("Symlinks are not permitted: " + str(path))
    if required and not target.is_file():
        raise ValueError("Missing input: " + name)
    return target


def new_directory(path: Path) -> Path:
    path = path.expanduser().absolute()
    for part in (path, *path.parents):
        if part.is_symlink():
            raise ValueError("Output path must not contain a symlink")
    if path.exists():
        raise FileExistsError("Output must be a new directory")
    if not path.parent.is_dir():
        raise ValueError("Output parent must already exist")
    return path


def load(path: Path) -> dict:
    return json.loads(path.read_text())


def write(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def manifest() -> dict:
    value = load(DATA / "manifest.json")
    if value.get("format") != "GAR-packet-data" or len(value.get("records", [])) != 1505:
        raise ValueError("Unexpected packet collection")
    names, groups, total, signatures = set(), Counter(), 0, {}
    for item in value["records"]:
        safe_path(DATA / "logical", item["path"])
        if item["path"] in names:
            raise ValueError("Duplicate logical record")
        names.add(item["path"])
        groups[item["group"]] += 1
        namespace = item["path"].split("/", 1)[0]
        if namespace not in ({"historical"} if item["group"] == "historical" else {"controlled", "protocol"}):
            raise ValueError("Unexpected record namespace")
        if not re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) or not re.fullmatch(r"[0-9a-f]{64}", item["gzip_sha256"]):
            raise ValueError("Invalid content digest")
        if item["blob"] != "blobs/" + item["sha256"] + ".gz":
            raise ValueError("Blob path does not match its content digest")
        if not 0 <= item["bytes"] <= MAX_FILE_BYTES or not 0 <= item["gzip_bytes"] <= MAX_FILE_BYTES:
            raise ValueError("Packet record exceeds expansion limit")
        total += item["bytes"]
        signature = (item["sha256"], item["bytes"], item["gzip_sha256"], item["gzip_bytes"])
        if item["blob"] in signatures and signatures[item["blob"]] != signature:
            raise ValueError("Inconsistent deduplicated blob metadata")
        signatures[item["blob"]] = signature
    if groups != {"historical": 40, "controlled": 1465} or total != value["logical_uncompressed_bytes"] or total > MAX_TOTAL_BYTES:
        raise ValueError("Unexpected cohort size")
    if sum(s[3] for s in signatures.values()) != value["unique_gzip_bytes"]:
        raise ValueError("Unexpected compressed size")
    return value


def verify() -> dict:
    info, checked = manifest(), set()
    for item in info["records"]:
        if item["blob"] in checked:
            continue
        path = safe_path(DATA, item["blob"], required=True)
        if path.stat().st_size != item["gzip_bytes"] or sha(path) != item["gzip_sha256"]:
            raise ValueError("Compressed identity mismatch: " + item["blob"])
        total, value = 0, hashlib.sha256()
        with gzip.open(path, "rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                total += len(block)
                if total > item["bytes"]:
                    raise ValueError("Decompression exceeds declared size")
                value.update(block)
        if total != item["bytes"] or value.hexdigest() != item["sha256"]:
            raise ValueError("Raw identity mismatch: " + item["path"])
        checked.add(item["blob"])
    if {str(p.relative_to(DATA)) for p in (DATA / "blobs").iterdir()} != checked:
        raise ValueError("Unexpected or missing blob")
    return {"status": "PASS", "logical_files": len(info["records"]), "unique_blobs": len(checked),
            "raw_bytes": info["logical_uncompressed_bytes"], "gzip_bytes": info["unique_gzip_bytes"]}


def extract(output: Path) -> dict:
    output = new_directory(output)
    receipt = verify()
    output.mkdir()
    for item in manifest()["records"]:
        target = safe_path(output, item["path"])
        target.parent.mkdir(parents=True, exist_ok=True)
        value, total = hashlib.sha256(), 0
        with gzip.open(safe_path(DATA, item["blob"], required=True), "rb") as stream, target.open("xb") as dest:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                total += len(block)
                if total > item["bytes"]:
                    raise ValueError("Extraction exceeds declared size")
                value.update(block)
                dest.write(block)
        if total != item["bytes"] or value.hexdigest() != item["sha256"]:
            raise ValueError("Extracted identity mismatch")
    write(output / "extraction_receipt.json", receipt)
    return receipt


def check_records(records: Path) -> None:
    for item in manifest()["records"]:
        path = safe_path(records, item["path"], required=True)
        if path.stat().st_size != item["bytes"] or sha(path) != item["sha256"]:
            raise ValueError("Extracted input differs from reference: " + item["path"])


def csv_rows(path: Path) -> list[dict]:
    with path.open(newline="") as stream:
        return list(csv.DictReader(stream))


def historical_deadlines(records: Path) -> dict:
    reference = load(RESULTS / "historical/deadline_outcomes.json")
    trials = [r for r in csv_rows(RESULTS / "historical/trial_results.csv") if r["arm"] == "primary"]
    type_reference = {(r["trial_name"], int(r["type_id"])): r for r in csv_rows(RESULTS / "historical/per_type.csv") if r["arm"] == "primary"}
    if Counter(r["policy"] for r in trials) != {p: 10 for p in POLICIES}:
        raise ValueError("Historical primary cohort changed")
    counts, by_type, deadlines, absent = defaultdict(Counter), defaultdict(Counter), defaultdict(set), defaultdict(Counter)
    for trial in trials:
        policy, name = trial["policy"], trial["trial_name"]
        local, local_types = defaultdict(Counter), defaultdict(Counter)
        path = safe_path(records, f"historical/{name}/packet_ledger.csv", required=True)
        with path.open(newline="") as stream:
            for row in csv.DictReader(stream):
                typ, group = int(row["type_id"]), {0: "critical", 1: "video", 2: "meter_monitor"}[int(row["class_id"])]
                if (typ == 6) != (group == "video"):
                    raise ValueError("Traffic class mismatch")
                release, deadline = int(row["intended_release_ns"]), int(row["original_deadline_ns"])
                deadlines[group].add((deadline-release)/1e6)
                if row["sink_receive_ns"]:
                    received = int(row["sink_receive_ns"])
                    timely = received <= deadline
                    if received < release or (int(row["delivered"]), int(row["on_time"]), int(row["late"]), int(row["missing"])) != (1, int(timely), int(not timely), 0):
                        raise ValueError("Inconsistent delivered packet outcome")
                    state = "on_time" if timely else "late"
                else:
                    if (int(row["delivered"]), int(row["on_time"]), int(row["late"]), int(row["missing"])) != (0, 0, 0, 1):
                        raise ValueError("Inconsistent absent packet outcome")
                    state = "absent"
                    absent[policy, group][row["gateway_status"]] += 1
                for target in (counts[policy, group], by_type[policy, typ], local[group], local_types[typ]):
                    target["offered"] += 1
                    target[state] += 1
        for group, column in (("critical", "critical"), ("video", "video"), ("meter_monitor", "meter")):
            stats = local[group]
            if (stats["offered"], stats["late"]+stats["absent"]) != (int(trial[column+"_offered"]), int(trial[column+"_failures"])):
                raise ValueError("Trial aggregate mismatch")
        for typ, stats in local_types.items():
            ref = type_reference[name, typ]
            if (stats["offered"], stats["late"]+stats["absent"]) != (int(ref["offered"]), int(ref["failures"])):
                raise ValueError("Per-type aggregate mismatch")
    def outcome(stats):
        value = {k: stats[k] for k in ("offered", "on_time", "late", "absent")}
        if value["offered"] != sum(value[k] for k in ("on_time", "late", "absent")):
            raise ValueError("Packet denominator mismatch")
        value["failures"] = value["late"]+value["absent"]
        value.update({k+"_pct": 100*value[k]/value["offered"] for k in ("on_time", "late", "absent", "failures")})
        return value
    outcomes = [{"policy": p, "label": LABELS[p], "traffic_group": g, **outcome(counts[p, g]),
                 "absent_gateway_status": dict(absent[p, g])} for g in ("critical", "video", "meter_monitor") for p in POLICIES]
    types = [{"policy": p, "label": LABELS[p], "type_id": typ, "traffic_type": TYPES[typ], **outcome(by_type[p, typ])}
             for p in POLICIES for typ in (0, 1, 2, 5, 3, 4, 6)]
    observed = {"outcomes": outcomes, "per_type": types,
                "relative_original_deadlines_ms": {key: sorted(values) for key, values in deadlines.items()}}
    for key, value in observed.items():
        if value != reference[key]:
            raise ValueError("Historical outcome differs from recorded analysis: " + key)
    return {**reference, **observed}


def reservation_provisioning(analysis: Path, output: Path) -> None:
    """Preserve all offers; measure accepted requests to actual booking commit."""
    commands = csv_rows(analysis / "commands.csv")
    jobs = csv_rows(analysis / "jobs.csv")
    commitments = {(r["name"], r["id"]): r for r in csv_rows(analysis / "commitments.csv") if r["event"] == "booking_commit"}
    targeted = {r["name"]: r for r in csv_rows(analysis / "attempts.csv") if r["kind"] == "targeted"}
    if len(targeted) != 48 or any(r["attempt_status"] != "COMPLETE" for r in targeted.values()):
        raise ValueError("Controlled trial accounting differs")
    if not all(r[field] == "True" for r in targeted.values() for field in ("integrated_execution_complete", "gateway_complete", "sink_complete")):
        raise ValueError("Incomplete controlled measurements")
    acks = {(r["name"], r["id"]): r for r in commands if r["type"] == "admission_ack" and r["kind"] == "targeted"}
    if len(acks) != 144:
        raise ValueError("Expected all 144 original offers")
    rows = []
    for job in jobs:
        if job["kind"] != "targeted":
            continue
        ack = acks[job["name"], job["id"]]
        accepted = job["accepted"] == "True"
        if accepted != (ack["accepted"] == "True"):
            raise ValueError("Job and acknowledgement disagree")
        row = {"trial": job["name"], "policy": job["policy"], "seed": int(job["seed"]),
               "mode": job["mode"], "condition": job["condition"], "request_id": job["id"],
               "accepted": accepted, "reason": ack["reason"], "broker_to_booking_commit_ms": "",
               "gateway_receive_to_decision_ms": (int(ack["decision_end_ns"])-int(ack["first_gateway_receive_ns"]))/1e6,
               "booking_headroom_before_start_ms": ""}
        if accepted:
            if int(job["commit_complete_ns"]) != int(commitments[job["name"], job["id"]]["commit_complete_ns"]):
                raise ValueError("Booking commit timestamps disagree")
            row["broker_to_booking_commit_ms"] = (int(job["commit_complete_ns"])-int(ack["broker_transmit_ns"]))/1e6
            row["booking_headroom_before_start_ms"] = -float(job["booking_commit_after_start_us"])/1000
            if row["broker_to_booking_commit_ms"] < 0 or row["booking_headroom_before_start_ms"] < 0 or job["booking_committed_by_original_start"] != "True":
                raise ValueError("Unexpected booking timing")
        rows.append(row)
    if len(rows) != 144 or sum(r["accepted"] for r in rows) != 96:
        raise ValueError("Offer denominator changed")
    with output.open("x", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def analyze(records: Path, output: Path) -> dict:
    records = records.expanduser().absolute()
    output = new_directory(output)
    if not records.is_dir() or records == output or records.is_relative_to(output) or output.is_relative_to(records):
        raise ValueError("Input and output directories must be separate")
    check_records(records)
    expected = load(DATA / "expected_outputs.json")
    output.mkdir()
    code = Path(__file__).resolve().parent
    commands = [[sys.executable, str(code / "analyze_followup.py"), "--protocol", str(records / "protocol/PROTOCOL.json"),
                 "--raw", str(records / "controlled"), "--output", str(output / "controlled")],
                [sys.executable, str(code / "report_followup.py"), "--analysis", str(output / "controlled")]]
    for command in commands:
        completed = subprocess.run(command, cwd=output, capture_output=True, text=True)
        if completed.returncode:
            raise RuntimeError("Packet analysis failed:\n" + completed.stderr[-5000:])
    for group in ("controlled_analysis", "controlled_summaries"):
        for name, digest in expected[group].items():
            if sha(safe_path(output / "controlled", name, required=True)) != digest:
                raise ValueError("Numeric reanalysis differs from recorded result: " + name)
    receipt = load(output / "controlled/analysis_receipt.json")
    if receipt["input_files"] != expected["controlled_input_files"] or receipt["protocol_sha256"] != expected["protocol_sha256"] or receipt["read_issues"]:
        raise ValueError("Controlled input selection or integrity changed")
    provisioning = output / "controlled/reservation_provisioning.csv"
    reservation_provisioning(output / "controlled", provisioning)
    if sha(provisioning) != "ea63e879fe4aca08afc559166627b95f09b30b0614db18604c2ad70c42f870dd":
        raise ValueError("Reservation provisioning values differ from reference")
    historical = historical_deadlines(records)
    (output / "historical").mkdir()
    write(output / "historical/deadline_outcomes.json", historical)
    receipt = {"status": "PASS", "controlled_numeric_outputs": len(expected["controlled_analysis"]),
               "controlled_summary_outputs": len(expected["controlled_summaries"]), "historical_ledgers": 40,
               "numeric_outputs_match_reference": True, "controlled_inputs_match_reference": True,
               "reservation_provisioning_offers": 144, "reservation_provisioning_accepted": 96,
               "scope": "Offline reanalysis of recorded experiments; no new live measurements."}
    write(output / "verification.json", receipt)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("verify")
    p = sub.add_parser("extract")
    p.add_argument("--output", type=Path, required=True)
    p = sub.add_parser("analyze")
    p.add_argument("--records", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = verify() if args.command == "verify" else extract(args.output) if args.command == "extract" else analyze(args.records, args.output)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
