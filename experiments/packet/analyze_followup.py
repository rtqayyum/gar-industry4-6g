#!/usr/bin/env python3
"""Independent read-only accounting for the instrumented 60-attempt follow-up.

This module imports no runtime implementation. All times remain in the recorded
monotonic epoch. Overlapping service windows and packets are descriptive records,
not independent repetitions. Failed and ineligible attempts remain in every index.
"""
import argparse
import csv
import hashlib
import json
import math
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path

import numpy as np


NS = 1_000_000
DEFAULTS = {"units": 106, "capacity_bytes_per_ms": 650, "base_units": 21,
            "background_units": [27, 5], "tenant_max_duration_ms": 500}


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def integer(row, key, default=None):
    value = row.get(key)
    return int(value) if value is not None and value != "" else default


def truth(value):
    return value is True or str(value).lower() in {"true", "1"}


def quantile(values, p):
    return float(np.quantile(values, p)) if len(values) else None


def write_csv(path, rows):
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=sorted({k for r in rows for k in r}))
        writer.writeheader()
        writer.writerows(rows)


def write_json(path, obj):
    path.write_text(json.dumps(obj, indent=2, allow_nan=False) + "\n")


class Evidence:
    def __init__(self, raw, protocol):
        self.raw, self.protocol = raw, protocol
        self.files, self.issues = {}, []

    def bind(self, path):
        path = Path(path)
        if path.exists():
            if path.is_relative_to(self.raw):
                key = "raw/" + str(path.relative_to(self.raw))
            elif path.is_relative_to(self.protocol.parent):
                key = "protocol/" + str(path.relative_to(self.protocol.parent))
            else:
                key = str(path)
            self.files[key] = sha(path)
            return True
        return False

    def json(self, path, default=None):
        if not self.bind(path):
            return {} if default is None else default
        try:
            return json.loads(path.read_text())
        except (ValueError, UnicodeError) as exc:
            self.issues.append({"path": str(path), "error": str(exc)})
            return {} if default is None else default

    def csv(self, path):
        if not self.bind(path):
            return []
        with path.open(newline="") as handle:
            rows = list(csv.DictReader(handle))
        good = [r for r in rows if None not in r and all(v is not None for v in r.values())]
        if len(good) != len(rows):
            self.issues.append({"path": str(path), "error": "incomplete CSV rows", "rows": len(rows)-len(good)})
        return good

    def jsonl(self, path):
        if not self.bind(path):
            return []
        rows = []
        for line, value in enumerate(path.read_text().splitlines(), 1):
            try:
                if value.strip():
                    rows.append(json.loads(value))
            except ValueError as exc:
                self.issues.append({"path": str(path), "line": line, "error": str(exc)})
        return rows

    def completion(self, folder, filename, rows, role):
        marker = self.json(folder / (filename + ".completion.json"))
        completion = self.json(folder / (role + "_completion.json"))
        path = folder / filename
        valid = bool(marker and path.exists() and truth(marker.get("complete"))
                     and marker.get("sha256") == sha(path) and marker.get("rows") == len(rows)
                     and marker.get("bytes") == path.stat().st_size)
        role_ok = completion.get("status") == "COMPLETE" and completion.get("exitcode") == 0
        return valid and role_ok, {"marker_valid": valid, "role_complete": role_ok,
                                  "marker": marker, "role": completion}


def reconstruct_requests(offered, cfg, first_wire):
    """Reconstruct fixed work/packet sizes without importing the engine helper."""
    requests, packets = [], {}
    C, N = cfg["capacity_bytes_per_ms"], cfg["units"]
    gamma = Decimal(str(cfg.get("tenant_headroom", 1)))
    rate = int(Decimal(str(C)) * gamma)
    theta = int(cfg.get("tenant_theta_ms", 0))
    next_wire = first_wire
    for index, r in enumerate(offered):
        q, L, requested = int(r["units"]), int(r["duration_ms"]), int(r["request_ms"])
        start = requested + int(cfg["delay_ms"])
        work = max(0, L-theta) * (rate*q//N)
        if L > cfg["tenant_max_duration_ms"]:
            work = 0
        supported = work >= 8 and theta < L <= cfg["tenant_max_duration_ms"]
        # Instrumented protocol invokes the engine's default 96-byte chunk cap.
        count = math.ceil(work/96) if supported else 0
        small, extra = divmod(work, count) if count else (0, 0)
        wires = list(range(next_wire, next_wire+count))
        for k, wire in enumerate(wires):
            phase = [0, .2, .5, .8][min(3, 4*k//count)] if cfg.get("tenant_release_fraction") else 0
            packets[wire] = {"wire_id": wire, "size_bytes": small + (k < extra),
                             "tenant_id": str(r["id"]), "release_ms": start+int(phase*L),
                             "deadline_ms": start+L, "class_id": 3+index}
        requests.append({"id": str(r["id"]), "request_ms": requested, "start_ms": start,
                         "end_ms": start+L, "units": q, "duration_ms": L,
                         "work_bytes": work, "nominal_request_bytes": L*(C*q//N),
                         "supported": supported, "wire_ids": wires})
        next_wire += count
    return requests, packets


def envelope(rows, timestamp, rho, sigma):
    events = defaultdict(int)
    for r in rows:
        t = integer(r, timestamp)
        if t is not None:
            events[t] += r["size_bytes"]
    debt, peak, previous, violating = 0., 0., None, set()
    for t, amount in sorted(events.items()):
        debt = max(0., debt-rho*(t-previous)/NS) if previous is not None else 0.
        debt += amount
        peak = max(peak, debt)
        if debt > sigma + 1e-7:
            violating.add(t)
        previous = t
    return {"observed_packets": sum(integer(r, timestamp) is not None for r in rows),
            "events": len(events), "peak_burst_bytes": peak, "sigma_bytes": sigma,
            "rho_bytes_per_ms": rho, "excess_bytes": max(0., peak-sigma),
            "violating_event_times": len(violating), "conforming_observed_stream": peak <= sigma+1e-7}, violating


def clock_analysis(base, receipt, attempt, witness, protocol):
    out = {**base, "clock_witness_rows": len(witness), "boot_identity": json.dumps(receipt.get("boot_identity"))}
    for scope, start, end in [("engine", receipt.get("start_clocks", {}), receipt.get("end_clocks", {})),
                              ("runner", attempt.get("started", {}), attempt.get("finished", {}))]:
        for key in ["monotonic_ns", "continuous_ns", "wall_ns"]:
            if start.get(key) is not None and end.get(key) is not None:
                out[f"{scope}_{key[:-3]}_elapsed_seconds"] = (end[key]-start[key])/1e9
    gaps = []
    for a, b in zip(witness, witness[1:]):
        if any(integer(x, k) is None for x in [a, b] for k in ["monotonic_ns", "wall_ns", "continuous_ns"]):
            continue
        d = {k: (int(b[k])-int(a[k]))/NS for k in ["monotonic_ns", "wall_ns", "continuous_ns"]}
        cm, wc = d["continuous_ns"]-d["monotonic_ns"], d["wall_ns"]-d["continuous_ns"]
        uncertainty = (integer(a, "read_span_ns", 0)+integer(b, "read_span_ns", 0))/NS
        gaps.append({**base, "start_monotonic_ns": int(a["monotonic_ns"]),
                     "end_monotonic_ns": int(b["monotonic_ns"]), "from_phase": a.get("phase"), "to_phase": b.get("phase"),
                     "monotonic_gap_ms": d["monotonic_ns"], "continuous_gap_ms": d["continuous_ns"],
                     "continuous_minus_monotonic_ms": cm, "wall_minus_continuous_ms": wc,
                     "read_span_sum_ms": uncertainty,
                     "clock_discrepancy_flag": max(abs(cm), abs(wc)) > protocol.get("clock_gap_threshold_ms", 50),
                     "sampling_gap_flag": d["continuous_ns"] > protocol.get("witness_max_interval_ms", 100)})
    for key in ["monotonic_gap_ms", "continuous_gap_ms", "read_span_sum_ms"]:
        out[key+"_max"] = max((x[key] for x in gaps), default=None)
    for key in ["continuous_minus_monotonic_ms", "wall_minus_continuous_ms"]:
        out[key+"_abs_max"] = max((abs(x[key]) for x in gaps), default=None)
    out["clock_discrepancy_pairs"] = sum(x["clock_discrepancy_flag"] for x in gaps)
    out["sampling_gap_pairs"] = sum(x["sampling_gap_flag"] for x in gaps)
    out["measurement_sampling_gap_pairs"] = sum(x["sampling_gap_flag"] and x["from_phase"] == "measurement" and x["to_phase"] == "measurement" for x in gaps)
    out["final_sampling_gap_pairs"] = sum(x["sampling_gap_flag"] and x["to_phase"] == "end" for x in gaps)
    return out, gaps


def boundary_state(job, tenant_packets, gateway, sink, boundary, origin):
    if boundary is None:
        return {}
    counts = Counter()
    for wire in job["wire_ids"]:
        p, g, s = tenant_packets[wire], gateway.get(wire, {}), sink.get(wire, {})
        size, rx = p["size_bytes"], integer(s, "received_ns")
        release, dispatch = integer(g, "tenant_release_observed_ns"), integer(g, "gateway_dispatch_ns")
        ack, egress = integer(g, "pacer_ack_observed_ns"), integer(g, "egress_ns")
        serial, expired = integer(g, "serialization_start_ns"), integer(g, "expired_ns")
        if rx is not None and rx <= boundary:
            category = "received"
        elif release is None or release > boundary:
            category = "unreleased_unreceived"
        elif expired is not None and expired <= boundary:
            category = "expired_unreceived"
        elif dispatch is None or dispatch > boundary:
            category = "released_not_dispatched_unreceived"
        elif ack is None or ack > boundary:
            category = "outstanding_ack_unreceived"
        else:
            category = "acked_unreceived"
        counts[category+"_bytes"] += size
        if release is not None and release <= boundary and (rx is None or rx > boundary):
            counts["released_unreceived_bytes"] += size
        if dispatch is not None and dispatch <= boundary and (ack is None or ack > boundary):
            counts["outstanding_ack_all_bytes"] += size
        if serial is not None and serial <= boundary and egress is not None and egress > boundary:
            counts["physical_serialization_inflight_bytes"] += size
        if rx is not None and boundary < rx <= origin+job["end_ms"]*NS:
            counts["post_boundary_on_time_received_bytes"] += size
        if rx is not None and rx > boundary:
            counts["post_boundary_received_bytes"] += size
    keys = ["received_bytes", "unreleased_unreceived_bytes", "expired_unreceived_bytes",
            "released_not_dispatched_unreceived_bytes", "outstanding_ack_unreceived_bytes",
            "acked_unreceived_bytes", "released_unreceived_bytes", "outstanding_ack_all_bytes",
            "physical_serialization_inflight_bytes", "post_boundary_on_time_received_bytes", "post_boundary_received_bytes"]
    result = {k: counts[k] for k in keys}
    result["unfinished_bytes"] = job["work_bytes"]-counts["received_bytes"]
    assert result["unfinished_bytes"] == sum(counts[k] for k in keys[1:6])
    return result


def probe_windows(base, job, packets, gateway, pacer, origin, cfg, clip_at_deadline=False):
    intervals, times, amounts = [], [], []
    for p in packets:
        if p["class_id"] != 0:
            continue
        g = gateway.get(p["wire_id"], {})
        begin = integer(g, "ingress_ns")
        end = integer(g, "egress_ns") or integer(g, "expired_ns")
        if clip_at_deadline and end is not None:
            end = min(end, origin+p["deadline_ms"]*NS)
        if begin is not None and end is not None and end > begin and g.get("status") not in {"overflow", "invalid_size"}:
            intervals.append(((begin-origin)/NS, (end-origin)/NS))
        if integer(g, "egress_ns") and integer(g, "sent_bytes", 0) == p["size_bytes"]:
            times.append((int(g["egress_ns"])-origin)/NS)
            amounts.append(p["size_bytes"])
    union = []
    for begin, end in sorted(intervals):
        if union and begin <= union[-1][1]:
            union[-1][1] = max(union[-1][1], end)
        else:
            union.append([begin, end])
    rows, summaries = [], []
    starts = np.array([x[0] for x in union]); ends = np.array([x[1] for x in union])
    order = np.argsort(times, kind="stable")
    times = np.array(times)[order]; prefix = np.r_[0, np.cumsum(np.array(amounts, dtype=int)[order])]
    all_times = np.array([(int(r["egress_ns"])-origin)/NS for r in pacer if integer(r, "egress_ns") is not None])
    all_amounts = np.array([integer(r, "sent_bytes", 0) for r in pacer if integer(r, "egress_ns") is not None])
    all_order = np.argsort(all_times, kind="stable"); all_times = all_times[all_order]
    all_prefix = np.r_[0, np.cumsum(all_amounts[all_order])]
    rate = cfg["capacity_bytes_per_ms"]*job["floor"]/cfg["units"]
    floor_rate = cfg["capacity_bytes_per_ms"]*job["floor"]//cfg["units"]
    for width in [5, 20, 100, 500]:
        candidates = np.arange(0, job["duration_ms"]-width+1)
        if len(union):
            idx = np.searchsorted(starts, candidates, side="right")-1
            mask = (idx >= 0) & (ends[np.maximum(idx, 0)] >= candidates+width)
        else:
            mask = np.zeros(len(candidates), dtype=bool)
        valid = candidates[mask]
        supplied = prefix[np.searchsorted(times, valid+width, side="right")]-prefix[np.searchsorted(times, valid, side="right")]
        all_supplied = all_prefix[np.searchsorted(all_times, valid+width, side="right")]-all_prefix[np.searchsorted(all_times, valid, side="right")]
        for t, delivered, total in zip(valid, supplied, all_supplied):
            rows.append({**base, "window_ms": width, "start_ms": int(t), "end_ms": int(t+width),
                         "critical_egress_bytes": int(delivered), "all_egress_bytes": int(total),
                         "nominal_fractional_floor_bytes": rate*width,
                         "nominal_integer_floor_bytes": floor_rate*width,
                         "fractional_floor_deficit_bytes": max(0., rate*width-delivered),
                         "integer_floor_deficit_bytes": max(0., floor_rate*width-delivered)})
        deficit = np.maximum(0, rate*width-supplied)
        summaries.append({**base, "window_ms": width, "candidate_windows": len(candidates),
                          "residence_covered_windows": len(valid), "uncovered_windows": len(candidates)-len(valid),
                          "nominal_fractional_floor_rate": rate, "nominal_integer_floor_rate": floor_rate,
                          "critical_rate_min": float(np.min(supplied/width)) if len(valid) else None,
                          "critical_rate_p01": quantile(supplied/width, .01),
                          "critical_rate_median": quantile(supplied/width, .5),
                          "critical_rate_p99": quantile(supplied/width, .99),
                          "critical_rate_mean": float(np.mean(supplied/width)) if len(valid) else None,
                          "all_payload_rate_mean": float(np.mean(all_supplied/width)) if len(valid) else None,
                          "fractional_deficit_positive_windows": int(np.sum(deficit > 1e-7)),
                          "fractional_deficit_bytes_max": float(max(deficit)) if len(valid) else None,
                          "fractional_deficit_bytes_p999": quantile(deficit, .999),
                          "residence_union_ms": sum(b-a for a, b in union)})
    return rows, summaries


def analyze(args):
    protocol_path, raw, out = args.protocol.resolve(), args.raw.resolve(), args.output.resolve()
    if out == raw or raw.is_relative_to(out):
        raise ValueError("Analysis output must not contain or equal the immutable raw directory")
    out.mkdir(parents=True, exist_ok=False)
    ev = Evidence(raw, protocol_path)
    protocol = ev.json(protocol_path)
    registry = ev.json(raw / "attempt_registry.json")
    execution = ev.json(raw / "FROZEN_EXECUTION.json")
    ev.json(raw / "receipt.json")
    attempts_by_name = {x["name"]: x for x in registry.get("attempts", [])}
    if len(attempts_by_name) != len(registry.get("attempts", [])):
        raise ValueError("Duplicate attempt names; require an explicit attempt identity map")
    input_checks = []
    for name, expected in protocol.get("input_sha256", {}).items():
        path = protocol_path.parent / name
        exists = ev.bind(path)
        input_checks.append({"file": name, "expected_sha256": expected,
                             "actual_sha256": sha(path) if exists else None, "matches": exists and sha(path) == expected})
    tables = defaultdict(list)
    tables["protocol_input_checks"] = input_checks
    for index, job in enumerate(protocol["trials"], 1):
        folder = raw / job["name"]
        attempt = attempts_by_name.get(job["name"], {})
        receipt = ev.json(folder / "receipt.json")
        base = {"index": index, **{k: job.get(k) for k in ["name", "kind", "seed", "mode", "policy", "condition", "packet_bytes", "floor"]}}
        status = attempt.get("status", "NOT_STARTED")
        cfg = {**DEFAULTS, **ev.json(protocol_path.parent / job["config"]), **receipt.get("config", {})}
        origin = receipt.get("origin_monotonic_ns")
        offered_path = folder / "offered_packets.csv"
        offered = ev.csv(offered_path if offered_path.exists() else protocol_path.parent / job["trace"])
        packets = [{**r, **{k: int(r[k]) for k in ["arrival_ms", "class_id", "size_bytes", "deadline_ms", "type_id"]}, "wire_id": n}
                   for n, r in enumerate(offered)]
        request_path = folder / "offered_requests.csv"
        offered_requests = ev.csv(request_path if request_path.exists() else protocol_path.parent / job["requests"]) if job.get("requests") else []
        jobs, tenant_packets = reconstruct_requests(offered_requests, cfg, len(packets))
        configured_jobs = cfg.get("tenant_requests")
        if configured_jobs is not None and jobs:
            # Verify the actual invocation used the expected default packetization.
            configured = {str(j["id"]): j for j in configured_jobs}
            for j in jobs:
                if j["id"] in configured:
                    for field in ["wire_ids", "work_bytes", "start_ms", "end_ms", "nominal_request_bytes"]:
                        if configured[j["id"]].get(field) != j[field]:
                            raise ValueError(f"Request reconstruction mismatch {job['name']} {j['id']} {field}")
        source = ev.csv(folder / "source.csv")
        sink_rows = ev.csv(folder / "sink.csv")
        source_complete, source_integrity = ev.completion(folder, "source.csv", source, "source")
        sink_complete, sink_integrity = ev.completion(folder, "sink.csv", sink_rows, "sink")
        source_map = {int(r["wire_id"]): r for r in source if integer(r, "wire_id") is not None}
        source_coverage = set(source_map) == set(range(len(packets))) and len(source) == len(packets)
        states = ev.jsonl(folder / "gateway_packet_states.jsonl")
        gateway = {int(r["wire_id"]): r for r in states if integer(r, "wire_id") is not None}
        for r in ev.csv(folder / "gateway_packets.csv"):
            gateway[int(r["wire_id"])] = {**gateway.get(int(r["wire_id"]), {}), **r}
        commands = ev.jsonl(folder / "gateway_commands_live.jsonl") or ev.csv(folder / "gateway_commands.csv")
        events = ev.jsonl(folder / "gateway_events_live.jsonl") or ev.csv(folder / "gateway_events.csv")
        commitments = ev.jsonl(folder / "gateway_commitments.jsonl")
        controller_records = ev.jsonl(folder / "controller.jsonl")
        joint_epochs = {integer(r, "epoch"): str(r["tenant_request"]["id"]) for r in controller_records
                        if r.get("type") == "command" and isinstance(r.get("tenant_request"), dict)}
        joint_plan_commits = {joint_epochs[integer(r, "epoch")]: r for r in commitments
                             if r.get("event") == "plan_commit_return" and integer(r, "epoch") in joint_epochs
                             and integer(r, "executor_accepted") == 1}
        for c in commitments:
            tables["commitments"].append({**base, **c, "joint_request_id": joint_epochs.get(integer(c, "epoch"))})
        for event in events:
            tables["executor_events"].append({**base, **event})
        pacer = ev.csv(folder / "pacer.csv")
        role = {r: ev.json(folder / (r+"_completion.json")) for r in ["gateway", "controller", "broker"]}
        gateway_complete = role["gateway"].get("status") == "COMPLETE" and role["gateway"].get("exitcode") == 0
        sink = {}
        expected_sizes = {p["wire_id"]: p["size_bytes"] for p in packets} | {w: p["size_bytes"] for w, p in tenant_packets.items()}
        invalid_receipts = duplicates = 0
        for s in sink_rows:
            wire = integer(s, "wire_id")
            valid = truth(s.get("valid")) and wire in expected_sizes and integer(s, "received_bytes") == expected_sizes[wire]
            if not valid:
                invalid_receipts += 1
                continue
            if wire in sink:
                duplicates += 1
                if int(s["received_ns"]) >= int(sink[wire]["received_ns"]):
                    continue
            sink[wire] = s
        trial = {**base, "attempt_status": status, "engine_status": receipt.get("status", "NO_RECEIPT"),
                 "runner_status": attempt.get("runner_status"), "engine_exitcode": attempt.get("exitcode"),
                 "origin_monotonic_ns": origin, "source_complete": source_complete,
                 "source_exact_row_coverage": source_coverage, "sink_complete": sink_complete,
                 "gateway_complete": gateway_complete, "sink_invalid_receipts": invalid_receipts,
                 "sink_duplicate_valid_receipts": duplicates, "offered_original_packets": len(packets),
                 "offered_jobs": len(jobs), "integrated_execution_complete": status == "COMPLETE",
                 "process_exitcodes": json.dumps(receipt.get("process_exitcodes")),
                 "processes_still_alive": json.dumps(receipt.get("processes_still_alive")),
                 "runner_cleanup": json.dumps(attempt.get("cleanup")),
                 "error": receipt.get("error", attempt.get("error"))}
        tables["completion_integrity"].append({**base,
            "source_marker_valid": source_integrity["marker_valid"], "source_role_complete": source_integrity["role_complete"],
            "sink_marker_valid": sink_integrity["marker_valid"], "sink_role_complete": sink_integrity["role_complete"]})
        # Bind and check durable role journal hashes without requiring scientific success.
        for role_name in ["source", "sink", "gateway", "controller", "broker"]:
            completion = ev.json(folder / (role_name+"_completion.json"))
            for filename, meta in completion.get("journals", {}).items():
                path = folder / filename
                exists = ev.bind(path)
                tables["journal_integrity"].append({**base, "role": role_name, "file": filename,
                    "matches": exists and sha(path) == meta.get("sha256") and path.stat().st_size == meta.get("bytes")})
        clocks, gaps = clock_analysis(base, receipt, attempt, ev.csv(folder / "clock_witness.csv"), protocol)
        tables["clocks"].append(clocks); tables["clock_intervals"].extend(gaps)
        if origin is None:
            trial.update(packet_outcome_scope="no_origin_record", accepted_jobs=None, critical_failures=None)
            tables["attempts"].append(trial)
            for j in jobs:
                tables["jobs"].append({**base, **{k: v for k, v in j.items() if k != "wire_ids"},
                                       "acceptance_status": "unknown_no_origin", "eligible_outage_case": False})
            continue
        C, N = cfg["capacity_bytes_per_ms"], cfg["units"]
        horizon = max([p["deadline_ms"] for p in packets]+[j["end_ms"] for j in jobs]+[job["duration_ms"]])
        trial.update(charged_horizon_ms=horizon, nominal_payload_capacity_bytes=C*horizon,
                     packet_outcome_scope="exact_completed_sink_horizon" if sink_complete else "recorded_receipts_only_missing_unknown")
        packet_results = []
        for p in packets:
            wire = p["wire_id"]; g, s, sent = gateway.get(wire, {}), sink.get(wire, {}), source_map.get(wire, {})
            deadline = origin+p["deadline_ms"]*NS; intended = origin+p["arrival_ms"]*NS
            rx, ingress, source_ns = integer(s, "received_ns"), integer(g, "ingress_ns"), integer(sent, "actual_send_ns")
            on_time = rx is not None and rx <= deadline
            failure = not on_time if sink_complete else (True if rx is not None and rx > deadline else None)
            r = {**base, **p, "offered_ns": intended, "deadline_ns": deadline, "source_send_ns": source_ns,
                 "ingress_ns": ingress, "sink_receive_ns": rx, "on_time": on_time,
                 "failure": failure, "absent_recorded_receipt": rx is None, "outcomes_complete": sink_complete,
                 "status": g.get("status", "no_gateway_record"), "egress_ns": integer(g, "egress_ns")}
            packet_results.append(r)
        for cls in [0, 1, 2]:
            group = [r for r in packet_results if r["class_id"] == cls]
            timely = sum(r["size_bytes"] for r in group if r["on_time"])
            trials_key = ["critical", "video", "monitor_meter"][cls]
            trial.update({trials_key+"_offered": len(group), trials_key+"_offered_bytes": sum(r["size_bytes"] for r in group),
                          trials_key+"_on_time": sum(r["on_time"] for r in group), trials_key+"_timely_bytes": timely,
                          trials_key+"_failures": sum(not r["on_time"] for r in group) if sink_complete else None,
                          trials_key+"_absent_recorded": sum(r["absent_recorded_receipt"] for r in group)})
        critical = [r for r in packet_results if r["class_id"] == 0]
        allowance = {}; endpoint_violations = set()
        groups = [[0], [1, 5], [2]]
        for group_id, kinds in enumerate(groups):
            group = [r for r in critical if r["type_id"] in kinds]
            if group_id >= len(cfg["envelopes"]):
                continue
            env = cfg["envelopes"][group_id]
            for r in group:
                allowance[r["wire_id"]] = env["min_deadline"]
            for stage, key in [("offered", "offered_ns"), ("source_send", "source_send_ns"), ("gateway_ingress", "ingress_ns")]:
                stat, bad = envelope(group, key, env["rho"], env["sigma"])
                tables["arrival_envelopes"].append({**base, "group": group_id, "stage": stage, **stat,
                    "offered_group_packets": len(group), "all_offered_have_stage_timestamp": stat["observed_packets"] == len(group),
                    "probe_deliberately_nonconforming": job["kind"] == "service_probe"})
                if stage == "gateway_ingress":
                    endpoint_violations.update(r["wire_id"] for r in group if r["ingress_ns"] in bad)
        for r in critical:
            ingress, deadline = r["ingress_ns"], r["deadline_ns"]
            allowed = allowance.get(r["wire_id"])
            r.update(ingress_residual_ms=(deadline-ingress)/NS if ingress is not None else None,
                     declared_min_ingress_deadline_ms=allowed,
                     ingress_allowance_violation=(deadline-ingress < allowed*NS) if ingress is not None and allowed is not None else None,
                     ingress_at_or_after_deadline=ingress >= deadline if ingress is not None else None,
                     source_sent_at_or_after_deadline=r["source_send_ns"] >= deadline if r["source_send_ns"] is not None else None,
                     ingress_envelope_violating_endpoint=r["wire_id"] in endpoint_violations)
        trial["critical_ingress_allowance_violations"] = sum(r["ingress_allowance_violation"] is True for r in critical)
        trial["critical_ingress_at_or_after_deadline"] = sum(r["ingress_at_or_after_deadline"] is True for r in critical)
        trial["critical_missing_ingress_records"] = sum(r["ingress_ns"] is None for r in critical)
        trial["critical_failures_with_allowance_violation"] = sum(r["failure"] is True and r["ingress_allowance_violation"] is True for r in critical)
        tables["critical_packets"].extend(critical)
        stop = (role["controller"].get("termination_received") or {}).get("monotonic_ns")
        invocation = receipt.get("controller_stop_actual_ns")
        return_ns = receipt.get("controller_stop_signal_return_ns")
        stop_boundary = stop if stop is not None else invocation
        stop_scope = "controller_termination_received" if stop is not None else ("signal_invocation_only_not_observed_stop" if invocation is not None else "no_controller_stop")
        commits = {str(r["id"]): r for r in commitments if r.get("event") == "booking_commit"}
        decisions = {str(r["id"]): r for r in commands if r.get("type") == "admission_ack"}
        trial_jobs = []
        for j in jobs:
            commit, ack = commits.get(j["id"], {}), decisions.get(j["id"], {})
            joint_commit = joint_plan_commits.get(j["id"], {})
            joint_required = ack.get("reason") == "joint_certified_plan_and_admission"
            accepted = bool(commit) or truth(ack.get("accepted"))
            acceptance_known = accepted or bool(ack) or gateway_complete
            deadline = origin+j["end_ms"]*NS; start = origin+j["start_ms"]*NS
            received = [(w, int(sink[w]["received_ns"])) for w in j["wire_ids"] if w in sink]
            timely = sum(tenant_packets[w]["size_bytes"] for w, t in received if t <= deadline)
            received_bytes = sum(tenant_packets[w]["size_bytes"] for w, t in received)
            completed = accepted and j["work_bytes"] > 0 and received_bytes == j["work_bytes"]
            timely_complete = completed and timely == j["work_bytes"]
            commit_end = integer(commit, "commit_complete_ns")
            row = {**base, **{k: v for k, v in j.items() if k != "wire_ids"},
                   "acceptance_status": "accepted" if accepted else ("not_accepted" if acceptance_known else "unknown"),
                   "accepted": accepted if acceptance_known else None, "booking_commit_recorded": bool(commit),
                   "admission_ack_accepted": truth(ack.get("accepted")), "admission_reason": ack.get("reason"),
                   "commit_begin_ns": integer(commit, "commit_begin_ns"), "commit_complete_ns": commit_end,
                   "booking_committed_by_original_start": commit_end <= start if commit_end is not None else None,
                   "booking_commit_after_start_us": (commit_end-start)/1000 if commit_end is not None else None,
                   "joint_plan_commit_recorded": bool(joint_commit),
                   "joint_plan_required_for_admission": joint_required,
                   "joint_plan_commit_complete_ns": integer(joint_commit, "commit_complete_ns"),
                   "joint_plan_committed_before_activation": truth(joint_commit.get("commit_complete_before_activation")) if joint_commit else None,
                   "received_bytes": received_bytes, "on_time_bytes": timely,
                   "completed": completed, "completed_on_time": timely_complete,
                   "completion_ns": max((t for _, t in received), default=None) if completed else None,
                   "whole_job_failure": not timely_complete if accepted and sink_complete else None,
                   "sink_outcomes_complete": sink_complete, "controller_stop_boundary_ns": stop_boundary,
                   "controller_stop_boundary_scope": stop_scope, "termination_received_ns": stop,
                   "signal_invocation_ns": invocation, "signal_return_ns": return_ns,
                   "stop_inside_original_window": start <= stop_boundary < deadline if stop_boundary is not None else None,
                   "booking_complete_by_stop": commit_end <= stop_boundary if commit_end is not None and stop_boundary is not None else None}
            cutoff = boundary_state(j, tenant_packets, gateway, sink, stop_boundary, origin)
            row.update({"at_stop_"+k: v for k, v in cutoff.items()})
            fallbacks = [r for r in events if r.get("kind") == "fallback" and integer(r, "actual_application_ns") is not None
                         and stop_boundary is not None and stop_boundary < int(r["actual_application_ns"]) < deadline
                         and start <= int(r["actual_application_ns"])]
            fallback = min(fallbacks, key=lambda r: int(r["actual_application_ns"])) if fallbacks else {}
            fallback_ns = integer(fallback, "actual_application_ns")
            row.update(fallback_applicable=job["policy"] in {"gar_h", "predictive_h"},
                       earliest_subsequent_fallback_ns=fallback_ns,
                       fallback_scheduled_ns=integer(fallback, "scheduled_ns"), fallback_units=integer(fallback, "units"),
                       fallback_plan_id=fallback.get("plan_id"),
                       has_subsequent_fallback_inside_window=bool(fallback))
            row.update({"at_fallback_"+k: v for k, v in boundary_state(j, tenant_packets, gateway, sink, fallback_ns, origin).items()})
            row["eligible_outage_case"] = bool(stop is not None and accepted and commit_end is not None
                and commit_end <= start and commit_end <= stop and start <= stop < deadline
                and cutoff.get("unfinished_bytes", 0) > 0 and sink_complete and gateway_complete
                and (not joint_required or (joint_commit and truth(joint_commit.get("commit_complete_before_activation")))))
            row["eligible_fallback_case"] = bool(row["eligible_outage_case"] and fallback
                and row.get("at_fallback_unfinished_bytes", 0) > 0 and row["fallback_applicable"])
            reasons = []
            if not accepted: reasons.append("not_accepted_or_unknown")
            if stop is None: reasons.append("no_observed_termination")
            if commit_end is None: reasons.append("no_booking_commit_record")
            elif commit_end > start: reasons.append("late_booking_commit")
            if joint_commit and not truth(joint_commit.get("commit_complete_before_activation")):
                reasons.append("late_joint_plan_commit")
            if joint_required and not joint_commit:
                reasons.append("required_joint_plan_commit_unavailable")
            if row["stop_inside_original_window"] is not True: reasons.append("stop_not_inside_original_window")
            if cutoff.get("unfinished_bytes") == 0: reasons.append("already_received_at_stop")
            if not sink_complete or not gateway_complete: reasons.append("incomplete_evidence")
            if not fallback: reasons.append("no_subsequent_fallback_inside_window")
            elif row.get("at_fallback_unfinished_bytes") == 0: reasons.append("already_received_at_fallback")
            row["eligibility_notes"] = ";".join(reasons)
            trial_jobs.append(row)
        tables["jobs"].extend(trial_jobs)
        accepted_jobs = [j for j in trial_jobs if j["accepted"] is True]
        whole_bytes = sum(j["work_bytes"] for j in accepted_jobs if j["completed_on_time"])
        partial_bytes = sum(j["on_time_bytes"] for j in accepted_jobs)
        nominal_work = sum(j["nominal_request_bytes"] for j in jobs)
        original_timely = sum(r["size_bytes"] for r in packet_results if r["on_time"])
        trial.update(accepted_jobs=len(accepted_jobs), completed_on_time_jobs=sum(j["completed_on_time"] for j in accepted_jobs),
                     admission_unknown_jobs=sum(j["accepted"] is None for j in trial_jobs),
                     nominal_offered_tenant_bytes=nominal_work, advertised_tenant_bytes=sum(j["work_bytes"] for j in jobs),
                     strict_completed_tenant_bytes=whole_bytes, partial_timely_tenant_bytes=partial_bytes,
                     original_timely_payload_bytes=original_timely,
                     strict_tenant_fraction_of_nominal=whole_bytes/nominal_work if nominal_work else None,
                     strict_timely_capacity_efficiency=(whole_bytes+original_timely)/(C*horizon),
                     partial_timely_capacity_efficiency=(partial_bytes+original_timely)/(C*horizon))
        tables["attempts"].append(trial)
        for r in pacer:
            wire = integer(r, "wire_id")
            if wire is None:
                continue
            g = gateway.get(wire, {}); ack = integer(g, "pacer_ack_observed_ns")
            item = {**base, "wire_id": wire, "class_id": packets[wire]["class_id"] if wire < len(packets) else tenant_packets.get(wire, {}).get("class_id"),
                    **{k: integer(r, k) for k in ["received_ns", "serialization_start_ns", "finish_target_ns", "egress_ns", "sent_bytes", "send_errno", "ack_send_begin_ns", "ack_send_end_ns", "ack_errno"]},
                    "ack_observed_ns": ack, "gateway_dispatch_ns": integer(g, "gateway_dispatch_ns")}
            components = {"dispatch_to_pacer_receive_us": (item["gateway_dispatch_ns"], item["received_ns"]),
                          "egress_to_ack_send_begin_us": (item["egress_ns"], item["ack_send_begin_ns"]),
                          "ack_send_syscall_us": (item["ack_send_begin_ns"], item["ack_send_end_ns"]),
                          "ack_send_end_to_observed_us": (item["ack_send_end_ns"], ack),
                          "egress_to_ack_observed_us": (item["egress_ns"], ack),
                          "serialization_lateness_us": (item["finish_target_ns"], item["egress_ns"])}
            item.update({k: (end-begin)/1000 if begin is not None and end is not None else None for k, (begin, end) in components.items()})
            tables["ack_packets"].append(item)
        for c in commands:
            if c.get("type") in {"ack", "admission_ack"} or "commit_complete_ns" in c:
                tables["commands"].append({**base, **c})
        if job["kind"] == "service_probe":
            windows, summary = probe_windows(base, job, packets, gateway, pacer, origin, cfg)
            for r in summary:
                r.update(sink_complete=sink_complete, gateway_complete=gateway_complete,
                         attempt_status=status, input_trace_sha256=sha(protocol_path.parent / job["trace"]))
            tables["service_probe_windows"].extend(windows); tables["service_probe_summary"].extend(summary)
            eligible_windows, eligible_summary = probe_windows(base, job, packets, gateway, pacer, origin, cfg, clip_at_deadline=True)
            for r in eligible_summary:
                r.update(sink_complete=sink_complete, gateway_complete=gateway_complete,
                         attempt_status=status, input_trace_sha256=sha(protocol_path.parent / job["trace"]),
                         residence_definition="ingress to min(egress_or_expiry, original_deadline); nonpositive intervals excluded")
            tables["eligible_service_probe_windows"].extend(eligible_windows)
            tables["eligible_service_probe_summary"].extend(eligible_summary)
    # All paired rows are retained. A missing/failed partner is labelled, never zero-filled.
    pair_groups = defaultdict(dict)
    for r in tables["attempts"]:
        if r["kind"] == "targeted":
            pair_groups[r["seed"], r["mode"], r["policy"]][r["condition"]] = r
    metrics = ["critical_failures", "video_failures", "monitor_meter_failures", "accepted_jobs", "completed_on_time_jobs",
               "strict_completed_tenant_bytes", "strict_tenant_fraction_of_nominal", "strict_timely_capacity_efficiency"]
    for (seed, mode, policy), partners in sorted(pair_groups.items()):
        present, killed = partners.get("controller_present", {}), partners.get("controller_killed", {})
        valid = all(r.get("integrated_execution_complete") and r.get("sink_complete") and r.get("gateway_complete") for r in [present, killed])
        row = {"seed": seed, "mode": mode, "policy": policy, "paired_complete": valid,
               "present_name": present.get("name"), "killed_name": killed.get("name"),
               "present_status": present.get("attempt_status"), "killed_status": killed.get("attempt_status")}
        for metric in metrics:
            a, b = present.get(metric), killed.get(metric)
            row.update({"present_"+metric: a, "killed_"+metric: b,
                        "killed_minus_present_"+metric: b-a if valid and a is not None and b is not None else None})
        tables["paired_controller_contrasts"].append(row)
    ack_groups = defaultdict(list)
    for r in tables["ack_packets"]:
        ack_groups[r["kind"], r["policy"], r["mode"], r["condition"]].append(r)
    for (kind, policy, mode, condition), rows in sorted(ack_groups.items(), key=lambda x: str(x[0])):
        base = dict(kind=kind, policy=policy, mode=mode, condition=condition, attempts=len({r["name"] for r in rows}), packets=len(rows))
        for metric in ["dispatch_to_pacer_receive_us", "egress_to_ack_send_begin_us", "ack_send_syscall_us",
                       "ack_send_end_to_observed_us", "egress_to_ack_observed_us", "serialization_lateness_us"]:
            values = [r[metric] for r in rows if r[metric] is not None]
            tables["ack_summary"].append({**base, "component": metric, "observations": len(values),
                "median_us": quantile(values, .5), "p99_us": quantile(values, .99),
                "max_us": max(values, default=None), "negative_differences": sum(v < 0 for v in values),
                "over_5ms": sum(v > 5000 for v in values), "over_10ms": sum(v > 10000 for v in values),
                "over_50ms": sum(v > 50000 for v in values)})
    target0 = [r for r in tables["jobs"] if r["id"] == "target-0"]
    tables["target0_outage_cases"] = target0
    accounting = Counter(r["attempt_status"] for r in tables["attempts"])
    summary = {"planned_attempts": len(protocol["trials"]), "registry_attempts": len(attempts_by_name),
               "analyzed_protocol_rows": len(tables["attempts"]), "attempt_status_counts": dict(accounting),
               "unrecognized_registry_names": sorted(set(attempts_by_name)-{j["name"] for j in protocol["trials"]}),
               "target0_rows": len(target0), "target0_killed_rows": sum(r["condition"] == "controller_killed" for r in target0),
               "eligible_target0_outage_cases": sum(r.get("eligible_outage_case", False) for r in target0),
               "eligible_target0_fallback_cases": sum(r.get("eligible_fallback_case", False) for r in target0),
               "paired_rows": len(tables["paired_controller_contrasts"]),
               "paired_complete_rows": sum(r["paired_complete"] for r in tables["paired_controller_contrasts"]),
               "input_hashes_all_match": all(r["matches"] for r in input_checks),
               "execution_protocol_hash_matches": execution.get("protocol_sha256") == sha(protocol_path),
               "read_issues": ev.issues,
               "scope": "Exact descriptive accounting, no packet/window independence or deterministic OS guarantee. Gamma changes tenant payload only; certificates use nominal capacity. Target0 eligibility is reported alongside all cases, never used to erase rejected or completed cases."}
    for name, rows in tables.items():
        write_csv(out / (name+".csv"), rows)
    write_json(out / "summary.json", summary)
    write_json(out / "analysis_receipt.json", {"analysis_source_sha256": sha(__file__),
        "protocol_sha256": sha(protocol_path), "input_files": ev.files,
        "outputs_sha256": {p.name: sha(p) for p in sorted(out.iterdir()) if p.is_file() and p.name != "analysis_receipt.json"},
        "readonly_raw": True, "read_issues": ev.issues})
    print(json.dumps(summary, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--protocol", type=Path, required=True)
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    analyze(parser.parse_args())


if __name__ == "__main__":
    main()
