#!/usr/bin/env python3
"""Descriptive summaries from the controlled UDP accounting.

No raw outcome is modified; failed/ineligible attempts remain in the base tables.
"""
import argparse
import csv
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

LABELS = {"fixed_h": "Fixed soft + gate", "predictive_h": "Predictive + gate",
          "gar_h": "GAR-H", "residual_priority_h": "Residual priority + gate"}
ORDER = list(LABELS)


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def write_json(p, data):
    p.write_text(json.dumps(data, indent=2, allow_nan=False)+"\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--analysis", type=Path, required=True)
    args = parser.parse_args(); out = args.analysis.resolve()
    inputs = {}
    def read(name):
        p = out/(name+".csv"); inputs[p.name] = sha(p)
        return pd.read_csv(p, low_memory=False)
    attempts, jobs, clocks = read("attempts"), read("jobs"), read("clocks")
    target, envelopes, service = read("target0_outage_cases"), read("arrival_envelopes"), read("service_probe_summary")
    ack, commands, commits, events = read("ack_packets"), read("commands"), read("commitments"), read("executor_events")
    critical = read("critical_packets")
    targeted = attempts[attempts.kind == "targeted"]
    matrix = []
    for policy in ORDER:
        r = targeted[targeted.policy == policy]
        t = target[(target.policy == policy) & (target.condition == "controller_killed")]
        staged = t[t["mode"] == "staged"]; bulk = t[t["mode"] == "bulk"]
        row = dict(policy=policy, label=LABELS[policy], trials=len(r),
                   critical_offered=int(r.critical_offered.sum()), critical_failures=int(r.critical_failures.sum()),
                   background_offered=int(r.video_offered.sum()+r.monitor_meter_offered.sum()),
                   background_failures=int(r.video_failures.sum()+r.monitor_meter_failures.sum()),
                   offered_jobs=int(r.offered_jobs.sum()), accepted_jobs=int(r.accepted_jobs.sum()),
                   completed_on_time_jobs=int(r.completed_on_time_jobs.sum()),
                   staged_killed_targets=len(staged), staged_unfinished_at_stop=int(staged.eligible_outage_case.sum()),
                   staged_with_unfinished_at_fallback=int(staged.eligible_fallback_case.sum()),
                   staged_fulfilled_after_stop=int((staged.eligible_outage_case & staged.completed_on_time).sum()),
                   bulk_killed_targets=len(bulk), bulk_already_complete=int((bulk.at_stop_unfinished_bytes == 0).sum()),
                   timely_capacity_efficiency=float((r.original_timely_payload_bytes.sum()+r.strict_completed_tenant_bytes.sum())/r.nominal_payload_capacity_bytes.sum()),
                   strict_tenant_fraction_of_nominal=float(r.strict_completed_tenant_bytes.sum()/r.nominal_offered_tenant_bytes.sum()),
                   critical_ingress_allowance_violations=int(r.critical_ingress_allowance_violations.sum()))
        for condition, label in [("controller_present", "present"), ("controller_killed", "killed")]:
            part = r[r.condition == condition]
            row.update({label+"_accepted_jobs": int(part.accepted_jobs.sum()),
                        label+"_on_time_jobs": int(part.completed_on_time_jobs.sum()),
                        label+"_critical_failures": int(part.critical_failures.sum()),
                        label+"_timely_capacity_efficiency": float((part.original_timely_payload_bytes.sum()+part.strict_completed_tenant_bytes.sum())/part.nominal_payload_capacity_bytes.sum())})
        matrix.append(row)
    pd.DataFrame(matrix).to_csv(out/"policy_mechanism_matrix.csv", index=False)
    # Preserve each of three phase blocks; there is no pooled-window confidence interval.
    columns = ["seed", "policy", "name", "work_bytes", "booking_committed_by_original_start",
               "controller_stop_boundary_scope", "termination_received_ns", "earliest_subsequent_fallback_ns",
               "completion_ns", "eligible_outage_case", "eligible_fallback_case", "completed_on_time"]
    columns += [k for k in target if k.startswith("at_stop_") or k.startswith("at_fallback_")]
    staged = target[(target["mode"] == "staged") & (target.condition == "controller_killed")]
    staged[columns].sort_values(["policy", "seed"]).to_csv(out/"staged_outage_mechanisms.csv", index=False)
    # Command timing uses actual commits and actual application stamps, not receipt-only approval.
    timing = []
    for policy in ORDER:
        c = commands[(commands.policy == policy) & (commands.kind == "targeted")]
        plans = c[c.type == "ack"]; ads = c[c.type == "admission_ack"]
        installed = events[(events.policy == policy) & (events.kind == "installation")]
        # executor_events.kind is the physical event kind, replacing the trial kind.
        plan_lookup = {(r["name"], int(r["executor_plan_id"])): r for r in plans.to_dict("records")
                       if r.get("accepted") is True and pd.notna(r.get("executor_plan_id"))}
        latencies = []
        for e in installed.to_dict("records"):
            c0 = plan_lookup.get((e["name"], int(e["plan_id"])))
            if c0 and pd.notna(c0.get("transmit_ns")):
                latencies.append((e["actual_application_ns"]-c0["transmit_ns"])/1000)
        plan_commits = commits[(commits.policy == policy) & (commits.event == "plan_commit_return")]
        row = {"policy": policy, "command_arrivals": len(plans), "commands_accepted": int((plans.accepted == True).sum()),
               "commands_rejected": int((plans.accepted == False).sum()),
               "admissions_accepted": int((ads.accepted == True).sum()), "admissions_rejected": int((ads.accepted == False).sum()),
               "accepted_plan_commit_after_activation": int(((plan_commits.executor_accepted == 1) & (plan_commits.commit_complete_before_activation == False)).sum()),
               "transmit_to_actual_application_observations": len(latencies),
               "transmit_to_actual_application_us_p99": float(np.quantile(latencies, .99)) if latencies else None,
               "transmit_to_actual_application_us_max": max(latencies) if latencies else None,
               "rejection_reasons": json.dumps(c[c.accepted == False].reason.value_counts().to_dict())}
        timing.append(row)
    pd.DataFrame(timing).to_csv(out/"command_timing_summary.csv", index=False)
    # Pooled packet quantiles describe the instrumentation, not sampling uncertainty.
    components = [k for k in ack if k.endswith("_us")]
    ack_rows = []
    for kind in ["targeted", "service_probe"]:
        for component in components:
            values = ack.loc[ack.kind == kind, component].dropna().to_numpy()
            ack_rows.append({"kind": kind, "component": component, "count": len(values),
                             "median_us": float(np.quantile(values, .5)), "p99_us": float(np.quantile(values, .99)),
                             "max_us": float(max(values)), "negative_values": int((values < 0).sum()),
                             "over_5ms": int((values > 5000).sum()), "over_10ms": int((values > 10000).sum()),
                             "over_50ms": int((values > 50000).sum())})
    pd.DataFrame(ack_rows).to_csv(out/"ack_pooled_summary.csv", index=False)
    misses = critical[(critical.kind == "targeted") & (critical.on_time == False)].copy()
    diagnostics = []
    for m in misses.to_dict("records"):
        overlapping = ack[(ack.name == m["name"]) & (ack.egress_ns <= m["deadline_ns"]) & (ack.ack_observed_ns >= m["offered_ns"])]
        bookings = jobs[(jobs.name == m["name"]) & (jobs.accepted == True)
                        & (jobs.start_ms <= m["deadline_ms"]) & (jobs.end_ms >= m["arrival_ms"])]
        largest = overlapping.sort_values("egress_to_ack_observed_us").iloc[-1].to_dict() if len(overlapping) else {}
        diagnostics.append({**m, "source_release_lateness_us": (m["source_send_ns"]-m["offered_ns"])/1000,
                            "booked_tenant_window_overlaps": len(bookings), "overlapping_ack_intervals": len(overlapping),
                            "max_overlapping_ack_wait_us": largest.get("egress_to_ack_observed_us"),
                            "overlapping_ack_wire_id": largest.get("wire_id"),
                            "overlapping_ack_send_begin_ns": largest.get("ack_send_begin_ns"),
                            "overlapping_ack_send_end_ns": largest.get("ack_send_end_ns"),
                            "overlapping_ack_observed_ns": largest.get("ack_observed_ns"),
                            "scope": "Temporal association only. Source send itself is overdue. No component/root-cause attribution from co-occurrence."})
    pd.DataFrame(diagnostics).to_csv(out/"targeted_critical_miss_diagnosis.csv", index=False)
    pooled = service[service.window_ms == 500].copy()
    pooled[["name", "seed", "packet_bytes", "floor", "input_trace_sha256", "residence_covered_windows",
            "nominal_fractional_floor_rate", "nominal_integer_floor_rate", "critical_rate_mean", "critical_rate_min",
            "all_payload_rate_mean", "fractional_deficit_bytes_max"]].to_csv(out/"service_probe_500ms_executions.csv", index=False)
    details = {
        "attempts": len(attempts), "targeted_trials": len(targeted), "service_probes": int((attempts.kind == "service_probe").sum()),
        "critical_packets": int(targeted.critical_offered.sum()), "critical_misses": int(targeted.critical_failures.sum()),
        "background_packets": int(targeted.video_offered.sum()+targeted.monitor_meter_offered.sum()),
        "background_misses": int(targeted.video_failures.sum()+targeted.monitor_meter_failures.sum()),
        "offered_jobs": int(targeted.offered_jobs.sum()), "accepted_jobs": int(targeted.accepted_jobs.sum()),
        "timely_jobs": int(targeted.completed_on_time_jobs.sum()),
        "critical_ingress_allowance_violations": int(targeted.critical_ingress_allowance_violations.sum()),
        "clock_discrepancy_flags": int(clocks.clock_discrepancy_pairs.sum()),
        "measurement_sampling_gap_flags": int(clocks.measurement_sampling_gap_pairs.sum()),
        "final_sampling_gap_flags": int(clocks.final_sampling_gap_pairs.sum()),
        "maximum_witness_gap_ms": float(clocks.continuous_gap_ms_max.max()),
        "max_abs_continuous_minus_monotonic_ms": float(clocks.continuous_minus_monotonic_ms_abs_max.max()),
        "max_abs_wall_minus_continuous_ms": float(clocks.wall_minus_continuous_ms_abs_max.max()),
        "arrival_envelopes_targeted": {stage: {"groups": len(r), "conforming_groups": int(r.conforming_observed_stream.sum())}
            for stage, r in envelopes[envelopes.kind == "targeted"].groupby("stage")},
        "service_probes_500ms_execution_mean_ranges": {str(int(floor)): {"minimum": float(r.critical_rate_mean.min()), "maximum": float(r.critical_rate_mean.max()),
            "nominal_fractional_rate": float(r.nominal_fractional_floor_rate.iloc[0])} for floor, r in pooled.groupby("floor")},
        "probe_replication_scope": "Two executions of identical input bytes at each packet-size/floor combination; overlapping windows are not independent repetitions.",
        "ack_observation_scope": "Gateway timestamp follows state/status/lateness journal writes, so send-to-observed includes transport, scheduling, processing and journal time; not pure transport delay."}
    write_json(out/"curated_facts.json", details)
    outputs = ["policy_mechanism_matrix.csv", "staged_outage_mechanisms.csv", "command_timing_summary.csv", "ack_pooled_summary.csv",
               "targeted_critical_miss_diagnosis.csv", "service_probe_500ms_executions.csv", "curated_facts.json"]
    write_json(out/"report_receipt.json", {"source_sha256": sha(__file__), "inputs_sha256": inputs,
        "outputs_sha256": {name: sha(out/name) for name in outputs}, "scope": "Presentation and descriptive summaries only, no raw changes or outcome exclusions."})
    print(json.dumps(details, indent=2))


if __name__ == "__main__":
    main()
