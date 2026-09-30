#!/usr/bin/env python3
"""Run the fixed online planning benchmark and context-level analysis.

All four policies receive identical generated traffic and the same gateway
admission guard. Failed and infeasible runs remain in the exported tables.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import itertools
import json
import platform
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np


HERE = Path(__file__).resolve().parent
PROJECT = HERE.parents[1]
SIMULATOR = HERE / "simulator.cpp"
CONTEXTS = PROJECT / "data/online"
PROTOCOL_PATH = CONTEXTS / "protocol.json"

POLICIES = {
    "gar_h": {
        "simulator_policy": "joint_guarded",
        "display_label": "GAR-H (study method)",
        "comparison_role": "study_method_local_control",
        "source_method_reproduced": False,
    },
    "luu_joint_adapter": {
        "simulator_policy": "luu_joint_style",
        "display_label": "Luu-J adapter",
        "comparison_role": "mechanism_aligned_adapter",
        "source_method_reproduced": False,
    },
    "luu_sequential_adapter": {
        "simulator_policy": "luu_sequential_style",
        "display_label": "Luu-S adapter",
        "comparison_role": "mechanism_aligned_adapter",
        "source_method_reproduced": False,
    },
    "yu_gateway_adapter": {
        "simulator_policy": "yu_gateway_style",
        "display_label": "Yu-inspired event-driven floor adapter (Y-E)",
        "comparison_role": "narrow_yu_inspired_mechanism_adapter",
        "source_method_reproduced": False,
    },
}

EXTERNAL_POLICIES = ["luu_joint_adapter", "luu_sequential_adapter", "yu_gateway_adapter"]
SAFETY_KEYS = ("critical_failures", "capacity_ledger_violations", "envelope_violation_endpoints")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def stable_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if columns is None:
        columns = sorted({key for row in rows for key in row})
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def numeric(value: Any, default: float = float("nan")) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def percent(numerator: Any, denominator: Any) -> float:
    denominator_value = numeric(denominator, 0.0)
    if denominator_value <= 0:
        return float("nan")
    return 100.0 * numeric(numerator, 0.0) / denominator_value


def mean_or_nan(values: Iterable[Any]) -> float:
    array = np.asarray([numeric(value) for value in values], dtype=float)
    array = array[np.isfinite(array)]
    return float(array.mean()) if array.size else float("nan")


def finite_or_blank(value: Any) -> Any:
    value = numeric(value)
    return value if np.isfinite(value) else ""


def row_key(row: dict[str, Any], fields: Iterable[str]) -> tuple[Any, ...]:
    return tuple(row.get(field) for field in fields)


def offer_trace_metadata(path: Path, duration_ms: int) -> dict[str, Any]:
    """Hash immutable pre-policy offer fields, independent of admission outcome."""
    if not path.exists():
        return {
            "immutable_offer_trace_path": "",
            "offer_trace_sha256": "",
            "offered_request_count": "",
            "actual_offered_request_rate_per_s": "",
        }
    fields = ("offer_ms", "start_ms", "end_ms", "units", "term_ms", "priority", "ordinal")
    with path.open(newline="", encoding="utf-8") as handle:
        records = [tuple(row[field] for field in fields) for row in csv.DictReader(handle)]
    canonical = "\n".join(",".join(record) for record in sorted(records)).encode("utf-8")
    return {
        "immutable_offer_trace_path": str(path),
        "offer_trace_sha256": hashlib.sha256(canonical).hexdigest(),
        "offered_request_count": len(records),
        "actual_offered_request_rate_per_s": 1000.0 * len(records) / duration_ms,
    }


@dataclass(frozen=True)
class Config:
    step_ms: int
    term_ms: int
    gate_eta: float

    @property
    def config_id(self) -> str:
        gate = "off" if self.gate_eta < 0 else f"{self.gate_eta:g}"
        return f"s{self.step_ms}_t{self.term_ms}_g{gate}"


@dataclass(frozen=True)
class Job:
    stage: str
    policy_id: str
    family: str
    suffix: int
    seed: int
    traffic_name: str
    load: float
    lease_rate: int
    config: Config
    expire_background: int

    @property
    def context_id(self) -> str:
        return f"{self.family}_{self.suffix}"

    @property
    def job_id(self) -> str:
        return "__".join(
            [
                self.stage,
                self.policy_id,
                self.context_id,
                self.traffic_name,
                f"seed{self.seed}",
                self.config.config_id,
                f"expiry{self.expire_background}",
            ]
        )


def output_metrics(payload: dict[str, Any]) -> dict[str, Any]:
    """Derive explicit, unit-labelled metrics without hiding missing output."""
    if payload.get("status") != "PASS":
        return {
            "timely_payload_bytes": "",
            "timely_payload_efficiency_pct": "",
            "tenant_acceptance_pct": "",
            "tenant_timely_pct": "",
            "background_failure_pct": "",
            "active_utilization_pct": "",
            "reserve_fraction_pct": "",
            "command_count": "",
            "planning_time_us_p95": "",
            "gate_time_us_p95": "",
            "controller_compute_us_per_control_tick": "",
            "critical_failure_pct": "",
            "safe_run": False,
        }
    classes = payload.get("classes", [])
    class_ontime = sum(numeric(item.get("on_time_bytes"), 0.0) for item in classes)
    timely_bytes = class_ontime + numeric(payload.get("lease_served_bytes"), 0.0)
    capacity_bytes = numeric(payload.get("capacity_bytes"), 0.0)
    background_generated = sum(numeric(item.get("generated"), 0.0) for item in classes[1:3])
    background_failures = sum(numeric(item.get("failures"), 0.0) for item in classes[1:3])
    control_ticks = numeric(payload.get("control_ticks"), 0.0)
    safety = all(numeric(payload.get(name), 1.0) == 0.0 for name in SAFETY_KEYS)
    return {
        "timely_payload_bytes": timely_bytes,
        "timely_payload_efficiency_pct": percent(timely_bytes, capacity_bytes),
        "tenant_acceptance_pct": percent(payload.get("lease_accepted"), payload.get("lease_offered")),
        "tenant_timely_pct": percent(payload.get("lease_on_time"), payload.get("lease_offered")),
        "background_failure_pct": percent(background_failures, background_generated),
        "active_utilization_pct": 100.0 * numeric(payload.get("active_utilization"), float("nan")),
        "reserve_fraction_pct": 100.0 * numeric(payload.get("reserve_fraction"), float("nan")),
        "command_count": numeric(payload.get("accepted_plan_commands"), float("nan")),
        "planning_time_us_p95": numeric(payload.get("planning_time_us_p95"), float("nan")),
        "gate_time_us_p95": numeric(payload.get("gate_time_us_p95"), float("nan")),
        "controller_compute_us_per_control_tick": (
            numeric(payload.get("controller_compute_time_us_total"), float("nan")) / control_ticks
            if control_ticks > 0
            else float("nan")
        ),
        "critical_failure_pct": percent(payload.get("critical_failures"), payload.get("critical_generated")),
        "safe_run": safety,
    }


def make_row(job: Job, result: dict[str, Any], protocol_hash: str, derivative_hash: str) -> dict[str, Any]:
    payload = result.get("payload", {})
    policy = POLICIES[job.policy_id]
    row: dict[str, Any] = {
        "job_id": job.job_id,
        "stage": job.stage,
        "policy_id": job.policy_id,
        "policy_label": policy["display_label"],
        "simulator_policy": policy["simulator_policy"],
        "comparison_role": policy["comparison_role"],
        "source_method_reproduced": policy["source_method_reproduced"],
        "family": job.family,
        "context_id": job.context_id,
        "context_suffix": job.suffix,
        "seed": job.seed,
        "traffic_block": job.traffic_name,
        "load": job.load,
        "tenant_offer_rate_per_s": job.lease_rate,
        "step_ms": job.config.step_ms,
        "term_ms": job.config.term_ms,
        "gate_eta": job.config.gate_eta,
        "config_id": job.config.config_id,
        "expire_background": job.expire_background,
        "exit_code": result["exit_code"],
        "wall_time_s": result["wall_time_s"],
        "raw_json_path": result["raw_json_path"],
        "lease_export_path": result["lease_export_path"],
        "immutable_offer_trace_path": result["immutable_offer_trace_path"],
        "offer_trace_sha256": result["offer_trace_sha256"],
        "offered_request_count": result["offered_request_count"],
        "actual_offered_request_rate_per_s": result["actual_offered_request_rate_per_s"],
        "batch_size": result["batch_size"],
        "offer_epoch_ms": result["offer_epoch_ms"],
        "common_headroom_gate": "enabled_eta_1" if job.config.gate_eta == 1.0 else "disabled_factorial_ablation",
        "stdout_log_path": result["stdout_log_path"],
        "stderr_log_path": result["stderr_log_path"],
        "error": result.get("error", ""),
        "simulator_status": payload.get("status", "missing_output"),
        "configuration_infeasible": payload.get("configuration_infeasible", ""),
        "failure_reason": payload.get("reason", ""),
        "protocol_sha256": protocol_hash,
        "derivative_sha256": derivative_hash,
    }
    for field in (
        "critical_failures",
        "critical_generated",
        "capacity_ledger_violations",
        "envelope_violation_endpoints",
        "lease_offered",
        "lease_accepted",
        "lease_on_time",
        "lease_rejected",
        "lease_admission_conflicts",
        "accepted_plan_commands",
        "plan_commands",
        "controller_compute_time_us_total",
        "luu_joint_batches",
        "luu_joint_subset_evaluations",
        "luu_sequential_batches",
        "yu_event_control_ticks",
        "yu_event_recomputations",
        "yu_arrival_recomputations",
        "yu_expiry_recomputations",
        "adapter_common_admission_guard_calls",
    ):
        row[field] = payload.get(field, "")
    row.update(output_metrics(payload))
    for name in ("timely_payload_efficiency_pct", "tenant_acceptance_pct", "tenant_timely_pct", "background_failure_pct",
                 "active_utilization_pct", "reserve_fraction_pct", "planning_time_us_p95", "gate_time_us_p95",
                 "controller_compute_us_per_control_tick", "critical_failure_pct"):
        row[name] = finite_or_blank(row[name])
    return row


def invoke(job: Job, binary: Path, output_root: Path, protocol_hash: str, derivative_hash: str, common: dict[str, Any]) -> dict[str, Any]:
    raw_json = output_root / "raw" / job.stage / f"{job.job_id}.json"
    lease_csv = output_root / "raw" / job.stage / f"{job.job_id}.leases.csv"
    offer_trace_csv = output_root / "raw" / job.stage / f"{job.job_id}.offers.csv"
    stdout_log = output_root / "logs" / job.stage / f"{job.job_id}.stdout.log"
    stderr_log = output_root / "logs" / job.stage / f"{job.job_id}.stderr.log"
    raw_json.parent.mkdir(parents=True, exist_ok=True)
    stdout_log.parent.mkdir(parents=True, exist_ok=True)
    context = CONTEXTS / f"{job.context_id}.csv"
    command = [
        str(binary),
        "--context", str(context),
        "--output", str(raw_json),
        "--policy", POLICIES[job.policy_id]["simulator_policy"],
        "--family", job.family,
        "--duration", str(common["duration_ms"]),
        "--seed", str(job.seed),
        "--load", str(job.load),
        "--burst-factor", str(common["burst_factor"]),
        "--capacity", str(common["capacity_bytes_per_ms"]),
        "--step", str(job.config.step_ms),
        "--delay", str(common["command_delay_ms"]),
        "--term", str(job.config.term_ms),
        "--lease-rate", str(job.lease_rate),
        "--lease-term", "0",
        "--gate-eta", str(job.config.gate_eta),
        "--expire-background", str(job.expire_background),
        "--batch-size", str(common["batch_size"]),
        "--lease-export", str(lease_csv),
        "--lease-offer-trace-export", str(offer_trace_csv),
    ]
    started = time.perf_counter()
    error = ""
    payload: dict[str, Any] = {}
    exit_code = -1
    try:
        process = subprocess.run(command, text=True, capture_output=True, check=False)
        exit_code = process.returncode
        stdout_log.write_text(process.stdout, encoding="utf-8")
        stderr_log.write_text(process.stderr, encoding="utf-8")
        if raw_json.exists():
            payload = json.loads(raw_json.read_text(encoding="utf-8"))
        else:
            error = "simulator produced no JSON output"
        if exit_code != 0:
            error = (error + "; " if error else "") + f"simulator exit code {exit_code}"
    except Exception as exc:  # records a failed process as evidence
        error = f"{type(exc).__name__}: {exc}"
        stdout_log.write_text("", encoding="utf-8")
        stderr_log.write_text(error + "\n", encoding="utf-8")
    result = {
        "exit_code": exit_code,
        "wall_time_s": time.perf_counter() - started,
        "raw_json_path": str(raw_json.relative_to(output_root)),
        "lease_export_path": str(lease_csv.relative_to(output_root)) if lease_csv.exists() else "",
        "immutable_offer_trace_path": str(offer_trace_csv.relative_to(output_root)) if offer_trace_csv.exists() else "",
        "stdout_log_path": str(stdout_log.relative_to(output_root)),
        "stderr_log_path": str(stderr_log.relative_to(output_root)),
        "payload": payload,
        "error": error,
        "batch_size": common["batch_size"],
        "offer_epoch_ms": common["offer_epoch_ms"],
    }
    trace = offer_trace_metadata(offer_trace_csv, int(common["duration_ms"]))
    result.update(trace)
    if lease_csv.exists():
        result["lease_export_path"] = str(lease_csv.relative_to(output_root))
    if offer_trace_csv.exists():
        result["immutable_offer_trace_path"] = str(offer_trace_csv.relative_to(output_root))
    return make_row(job, result, protocol_hash, derivative_hash)


def run_jobs(jobs: list[Job], binary: Path, output_root: Path, protocol_hash: str, derivative_hash: str,
             common: dict[str, Any], workers: int, stage: str) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    progress_path = output_root / "execution_progress.json"
    write_json(progress_path, {"stage": stage, "state": "running", "completed": 0, "total": len(jobs), "updated_at": utc_now()})
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(invoke, job, binary, output_root, protocol_hash, derivative_hash, common) for job in jobs]
        for completed, future in enumerate(as_completed(futures), start=1):
            results.append(future.result())
            if completed == len(jobs) or completed % 25 == 0:
                write_json(progress_path, {
                    "stage": stage,
                    "state": "running" if completed < len(jobs) else "complete",
                    "completed": completed,
                    "total": len(jobs),
                    "updated_at": utc_now(),
                })
    return sorted(results, key=lambda row: row["job_id"])


def offer_trace_audit(rows: list[dict[str, Any]], stage: str, include_config: bool) -> tuple[list[dict[str, Any]], list[str]]:
    """Verify the exported offered-work trace is policy-independent."""
    group_fields = ["family", "context_id", "seed", "traffic_block", "load", "tenant_offer_rate_per_s", "expire_background"]
    if include_config:
        group_fields.append("config_id")
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(row_key(row, group_fields), []).append(row)
    audit: list[dict[str, Any]] = []
    failures: list[str] = []
    for key, members in sorted(groups.items(), key=lambda item: str(item[0])):
        hashes = {member["offer_trace_sha256"] for member in members}
        counts = {member["offered_request_count"] for member in members}
        policy_ids = sorted(member["policy_id"] for member in members)
        passed = len(hashes) == 1 and "" not in hashes and len(counts) == 1 and set(policy_ids) == set(POLICIES)
        record = dict(zip(group_fields, key, strict=True))
        record.update({
            "stage": stage,
            "policy_ids": ";".join(policy_ids),
            "unique_offer_trace_hashes": len(hashes),
            "unique_offer_counts": len(counts),
            "offered_request_count": next(iter(counts)) if len(counts) == 1 else "",
            "offer_trace_sha256": next(iter(hashes)) if len(hashes) == 1 else "",
            "trace_equal_across_policies": passed,
        })
        audit.append(record)
        if not passed:
            failures.append(f"{stage}:{record['context_id']}:{record['seed']}:{record['traffic_block']}")
    return audit, failures


def selection_rows(tuning_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Config]]:
    selections: list[dict[str, Any]] = []
    selected: dict[str, Config] = {}
    for policy_id in POLICIES:
        by_config: dict[str, list[dict[str, Any]]] = {}
        for row in tuning_rows:
            if row["policy_id"] == policy_id:
                by_config.setdefault(row["config_id"], []).append(row)
        candidates: list[tuple[tuple[Any, ...], Config, dict[str, Any]]] = []
        for config_id, rows in by_config.items():
            safe = bool(rows) and all(
                row["simulator_status"] == "PASS" and row["safe_run"] is True and not row["error"] for row in rows
            )
            summary = {
                "policy_id": policy_id,
                "policy_label": POLICIES[policy_id]["display_label"],
                "config_id": config_id,
                "n_runs": len(rows),
                "all_tuning_runs_safe": safe,
                "mean_timely_payload_efficiency_pct": mean_or_nan(row["timely_payload_efficiency_pct"] for row in rows),
                "mean_background_failure_pct": mean_or_nan(row["background_failure_pct"] for row in rows),
                "mean_command_count": mean_or_nan(row["command_count"] for row in rows),
            }
            first = rows[0]
            config = Config(int(first["step_ms"]), int(first["term_ms"]), float(first["gate_eta"]))
            summary.update(asdict(config))
            selections.append(summary)
            if safe:
                candidates.append((
                    (-numeric(summary["mean_timely_payload_efficiency_pct"]),
                     numeric(summary["mean_background_failure_pct"]),
                     numeric(summary["mean_command_count"]),
                     config_id),
                    config,
                    summary,
                ))
        if candidates:
            candidates.sort(key=lambda item: item[0])
            _, chosen, summary = candidates[0]
            selected[policy_id] = chosen
            summary["selected"] = True
            summary["selection_reason"] = "fixed_common_configuration_safety_confirmation" if len(by_config) == 1 else "safe_lexicographic_protocol_rule"
        else:
            for summary in selections:
                if summary["policy_id"] == policy_id:
                    summary["selected"] = False
                    summary["selection_reason"] = "no_config_safe_in_every_tuning_run"
    for row in selections:
        row.setdefault("selected", False)
        row.setdefault("selection_reason", "not_selected")
        for key in ("mean_timely_payload_efficiency_pct", "mean_background_failure_pct", "mean_command_count"):
            row[key] = finite_or_blank(row[key])
    return sorted(selections, key=lambda row: (row["policy_id"], row["config_id"])), selected


def aggregate_seed_rows(main_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    group_fields = ["policy_id", "policy_label", "simulator_policy", "comparison_role", "source_method_reproduced",
                    "family", "context_id", "context_suffix", "traffic_block", "load", "tenant_offer_rate_per_s",
                    "config_id", "step_ms", "term_ms", "gate_eta", "expire_background"]
    metric_fields = [
        "timely_payload_efficiency_pct", "tenant_acceptance_pct", "tenant_timely_pct", "background_failure_pct",
        "active_utilization_pct", "reserve_fraction_pct", "command_count", "planning_time_us_p95", "gate_time_us_p95",
        "controller_compute_us_per_control_tick", "critical_failure_pct",
    ]
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in main_rows:
        groups.setdefault(row_key(row, group_fields), []).append(row)
    result: list[dict[str, Any]] = []
    for key, rows in groups.items():
        record = dict(zip(group_fields, key, strict=True))
        record["seed_n"] = len(rows)
        record["seed_runs_all_pass"] = all(row["simulator_status"] == "PASS" and not row["error"] for row in rows)
        record["seed_runs_all_safe"] = all(row["safe_run"] is True for row in rows)
        record["critical_failures_total"] = sum(numeric(row["critical_failures"], 0.0) for row in rows)
        record["capacity_ledger_violations_total"] = sum(numeric(row["capacity_ledger_violations"], 0.0) for row in rows)
        record["envelope_violation_endpoints_total"] = sum(numeric(row["envelope_violation_endpoints"], 0.0) for row in rows)
        for metric in metric_fields:
            record[metric] = finite_or_blank(mean_or_nan(row[metric] for row in rows))
        result.append(record)
    return sorted(result, key=lambda row: tuple(str(row[field]) for field in group_fields))


def aggregate_context_clusters(seed_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Average all prespecified traffic cells within each context cluster."""
    group_fields = ["policy_id", "policy_label", "simulator_policy", "comparison_role", "source_method_reproduced",
                    "family", "context_id", "context_suffix", "config_id", "step_ms", "term_ms", "gate_eta"]
    metric_fields = [
        "timely_payload_efficiency_pct", "tenant_acceptance_pct", "tenant_timely_pct", "background_failure_pct",
        "active_utilization_pct", "reserve_fraction_pct", "command_count", "planning_time_us_p95", "gate_time_us_p95",
        "controller_compute_us_per_control_tick", "critical_failure_pct",
    ]
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in seed_rows:
        groups.setdefault(row_key(row, group_fields), []).append(row)
    clusters: list[dict[str, Any]] = []
    for key, rows in groups.items():
        record = dict(zip(group_fields, key, strict=True))
        record["traffic_cells_n"] = len(rows)
        record["all_traffic_cells_pass"] = all(row["seed_runs_all_pass"] is True for row in rows)
        record["all_traffic_cells_safe"] = all(row["seed_runs_all_safe"] is True for row in rows)
        record["critical_failures_total"] = sum(numeric(row["critical_failures_total"], 0.0) for row in rows)
        record["capacity_ledger_violations_total"] = sum(numeric(row["capacity_ledger_violations_total"], 0.0) for row in rows)
        record["envelope_violation_endpoints_total"] = sum(numeric(row["envelope_violation_endpoints_total"], 0.0) for row in rows)
        for metric in metric_fields:
            record[metric] = finite_or_blank(mean_or_nan(row[metric] for row in rows))
        clusters.append(record)
    return sorted(clusters, key=lambda row: tuple(str(row[field]) for field in group_fields))


def policy_summary(context_clusters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    metrics = [
        "timely_payload_efficiency_pct", "tenant_acceptance_pct", "tenant_timely_pct", "background_failure_pct",
        "active_utilization_pct", "reserve_fraction_pct", "command_count", "planning_time_us_p95", "gate_time_us_p95",
        "controller_compute_us_per_control_tick", "critical_failure_pct",
    ]
    result = []
    for policy_id in POLICIES:
        rows = [row for row in context_clusters if row["policy_id"] == policy_id]
        record = {
            "policy_id": policy_id,
            "policy_label": POLICIES[policy_id]["display_label"],
            "comparison_role": POLICIES[policy_id]["comparison_role"],
            "source_method_reproduced": False,
            "aggregation": "equal mean over 12 family-context clusters after equal mean across six traffic blocks and three seeds",
            "context_clusters_n": len(rows),
            "clusters_all_safe": sum(row["all_traffic_cells_safe"] is True for row in rows),
            "critical_failures_total": sum(numeric(row["critical_failures_total"], 0.0) for row in rows),
            "capacity_ledger_violations_total": sum(numeric(row["capacity_ledger_violations_total"], 0.0) for row in rows),
            "envelope_violation_endpoints_total": sum(numeric(row["envelope_violation_endpoints_total"], 0.0) for row in rows),
        }
        for metric in metrics:
            record[metric] = finite_or_blank(mean_or_nan(row[metric] for row in rows))
        result.append(record)
    return result


def exact_sign_flip_pvalue(differences: np.ndarray) -> float:
    """Exact two-sided randomization test over whole context clusters (n<=12)."""
    values = np.asarray(differences, dtype=float)
    values = values[np.isfinite(values)]
    if values.size == 0:
        return float("nan")
    observed = abs(float(values.mean()))
    signs = np.asarray(list(itertools.product((-1.0, 1.0), repeat=values.size)), dtype=float)
    permuted = abs((signs * values).mean(axis=1))
    return float(np.mean(permuted >= observed - 1e-12))


def stratified_paired_bootstrap_interval(pairs: list[tuple[str, float]], seed: int,
                                        resamples: int = 20000) -> tuple[float, float]:
    """Resample the three context clusters within each fixed workload family."""
    strata: dict[str, list[float]] = {}
    for family, value in pairs:
        if np.isfinite(value):
            strata.setdefault(family, []).append(value)
    arrays = {family: np.asarray(values, dtype=float) for family, values in strata.items()}
    if not arrays:
        return float("nan"), float("nan")
    generator = np.random.default_rng(seed)
    family_means = []
    for values in arrays.values():
        indices = generator.integers(0, values.size, size=(resamples, values.size))
        family_means.append(values[indices].mean(axis=1))
    means = np.asarray(family_means).mean(axis=0)
    return float(np.quantile(means, 0.025)), float(np.quantile(means, 0.975))


def holm_adjust(pvalues: dict[str, float]) -> dict[str, float]:
    valid = sorted(((name, value) for name, value in pvalues.items() if np.isfinite(value)), key=lambda item: item[1])
    adjusted: dict[str, float] = {name: float("nan") for name in pvalues}
    prior = 0.0
    total = len(valid)
    for rank, (name, value) in enumerate(valid):
        corrected = min(1.0, (total - rank) * value)
        prior = max(prior, corrected)
        adjusted[name] = prior
    return adjusted


def paired_effects(context_clusters: list[dict[str, Any]], seed_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Primary whole-context comparison plus descriptive per-traffic summaries."""
    records: list[dict[str, Any]] = []
    primary_p: dict[str, float] = {}
    primary_records: dict[str, dict[str, Any]] = {}
    def effect_rows(rows: list[dict[str, Any]], scope: str, adapter: str, bootstrap_seed: int) -> dict[str, Any]:
        base = {row["context_id"]: row for row in rows if row["policy_id"] == "gar_h"}
        other = {row["context_id"]: row for row in rows if row["policy_id"] == adapter}
        keys = sorted(set(base).intersection(other))
        pairs_by_family = [
            (base[key]["family"], numeric(other[key]["timely_payload_efficiency_pct"]) - numeric(base[key]["timely_payload_efficiency_pct"]))
            for key in keys
        ]
        array = np.asarray([value for _, value in pairs_by_family], dtype=float)
        complete_safe = all(base[key]["all_traffic_cells_safe"] and other[key]["all_traffic_cells_safe"] for key in keys) if scope == "overall_all_traffic" else all(base[key]["seed_runs_all_safe"] and other[key]["seed_runs_all_safe"] for key in keys)
        lower, upper = stratified_paired_bootstrap_interval(pairs_by_family, bootstrap_seed)
        return {
            "analysis_scope": scope,
            "reference_policy": "gar_h",
            "reference_label": POLICIES["gar_h"]["display_label"],
            "comparison_policy": adapter,
            "comparison_label": POLICIES[adapter]["display_label"],
            "endpoint": "timely_payload_efficiency_pct",
            "effect_definition": "adapter minus GAR-H, percentage points",
            "paired_context_clusters_n": len(keys),
            "all_paired_clusters_safe": complete_safe,
            "mean_effect_pp": finite_or_blank(mean_or_nan(array)),
            "bootstrap_ci95_low_pp": finite_or_blank(lower),
            "bootstrap_ci95_high_pp": finite_or_blank(upper),
            "bootstrap_interval_note": "Pointwise family-stratified paired percentile bootstrap (three contexts resampled within each fixed family); descriptive, not multiplicity adjusted.",
            "sign_flip_p_two_sided": finite_or_blank(exact_sign_flip_pvalue(array)),
            "holm_adjusted_p": "",
            "inference_note": "Exact paired sign-flip over whole context clusters." if scope == "overall_all_traffic" else "Descriptive per-traffic paired contrast; no multiplicity-adjusted p-value.",
        }
    for index, adapter in enumerate(EXTERNAL_POLICIES):
        record = effect_rows(context_clusters, "overall_all_traffic", adapter, 9100 + index)
        primary_p[adapter] = numeric(record["sign_flip_p_two_sided"])
        primary_records[adapter] = record
        records.append(record)
    adjusted = holm_adjust(primary_p)
    for adapter, record in primary_records.items():
        record["holm_adjusted_p"] = finite_or_blank(adjusted[adapter])
    traffic_names = sorted({row["traffic_block"] for row in seed_rows})
    for traffic_index, traffic_name in enumerate(traffic_names):
        subset = [row for row in seed_rows if row["traffic_block"] == traffic_name]
        for index, adapter in enumerate(EXTERNAL_POLICIES):
            records.append(effect_rows(subset, f"traffic_block:{traffic_name}", adapter, 9200 + traffic_index * 10 + index))
    return records


def oracle_fixture() -> dict[str, Any]:
    """Independent four-slot exhaustive booking check for joint subset logic."""
    capacity = 12
    protected = [6, 6, 6, 6]
    offers = [
        {"id": "A", "start": 0, "end": 3, "units": 3, "priority": 100},
        {"id": "B", "start": 0, "end": 2, "units": 3, "priority": 60},
        {"id": "C", "start": 1, "end": 4, "units": 3, "priority": 80},
    ]
    feasible: list[dict[str, Any]] = []
    for mask in range(1 << len(offers)):
        chosen = [offer for index, offer in enumerate(offers) if mask & (1 << index)]
        slot_use = [sum(offer["units"] for offer in chosen if offer["start"] <= slot < offer["end"]) for slot in range(4)]
        okay = all(protected[slot] + slot_use[slot] <= capacity for slot in range(4))
        if okay:
            feasible.append({"selected": [offer["id"] for offer in chosen], "priority_sum": sum(offer["priority"] for offer in chosen), "slot_use": slot_use})
    optimum = max(feasible, key=lambda item: (item["priority_sum"], len(item["selected"]), tuple(item["selected"])))
    expected = {"selected": ["A", "C"], "priority_sum": 180}
    return {
        "purpose": "Finite static correctness diagnostic only; it is not an online or production oracle.",
        "capacity_units_per_slot": capacity,
        "protected_units_per_slot": protected,
        "offers": offers,
        "feasible_subsets": feasible,
        "selected_optimum": optimum,
        "expected_optimum": expected,
        "pass": optimum["selected"] == expected["selected"] and optimum["priority_sum"] == expected["priority_sum"],
    }


def smoke_tests(binary: Path, output_root: Path, protocol_hash: str, derivative_hash: str, common: dict[str, Any]) -> dict[str, Any]:
    """Run independent expected-behaviour checks before full study execution."""
    config = Config(step_ms=5, term_ms=40, gate_eta=1.0)
    jobs = [
        Job("tests", policy, "deadline_mix", 3, 9011, "stress_trace", 1.4, 100, config, 1)
        for policy in POLICIES
    ]
    rows = run_jobs(jobs, binary, output_root, protocol_hash, derivative_hash, common, workers=1, stage="tests")
    failures: list[str] = []
    for row in rows:
        if row["simulator_status"] != "PASS" or row["safe_run"] is not True:
            failures.append(f"{row['policy_id']} did not satisfy PASS/safety invariants")
        if numeric(row["lease_accepted"], 0.0) != numeric(row["lease_on_time"], 0.0):
            failures.append(f"{row['policy_id']} accepted lease did not complete on time")
    payloads = {row["policy_id"]: json.loads((output_root / row["raw_json_path"]).read_text(encoding="utf-8")) for row in rows}
    if numeric(payloads["luu_joint_adapter"].get("luu_joint_batches"), 0.0) <= 0 or numeric(payloads["luu_joint_adapter"].get("luu_joint_subset_evaluations"), 0.0) <= 0:
        failures.append("Luu-J adapter did not execute joint subset evaluation")
    if numeric(payloads["luu_sequential_adapter"].get("luu_sequential_batches"), 0.0) <= 0:
        failures.append("Luu-S adapter did not execute priority batch processing")
    if numeric(payloads["yu_gateway_adapter"].get("yu_event_recomputations"), 0.0) <= 0:
        failures.append("Y-E adapter did not execute arrival/expiry floor recomputation")
    oracle = oracle_fixture()
    if not oracle["pass"]:
        failures.append("small exact oracle selected an unexpected subset")
    # Per-job trace hashes are computed from offered-work fields only, after
    # sorting so sequential admission order cannot affect comparison.
    trace_hashes = {row["offer_trace_sha256"] for row in rows}
    trace_counts = {row["offered_request_count"] for row in rows}
    if "" in trace_hashes or len(trace_hashes) != 1 or len(trace_counts) != 1:
        failures.append("offered tenant trace differs across policies or is unavailable")
    write_json(output_root / "oracle_small_horizon.json", oracle)
    receipt = {
        "status": "PASS" if not failures else "FAIL",
        "checks": [
            "all four policies PASS common safety invariants",
            "accepted lease equals timely lease",
            "Luu-J joint subset activity observed",
            "Luu-S sequential priority activity observed",
            "Y-E arrival/expiry floor recomputation observed",
            "identical seeded offer-trace hash/count across policies",
            "independent small exact subset fixture",
        ],
        "failures": failures,
        "test_rows": rows,
    }
    write_json(output_root / "tests_receipt.json", receipt)
    return receipt


def prepare_output(output_root: Path) -> None:
    if output_root.exists() and any(output_root.iterdir()):
        raise SystemExit(f"Refusing to overwrite nonempty output directory: {output_root}")
    output_root.mkdir(parents=True, exist_ok=True)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as stream:
        return list(csv.DictReader(stream))


def scientific_outcomes(payload: dict[str, Any]) -> dict[str, Any]:
    """Exclude wall-clock timers and the location of the input context only."""
    return {key: value for key, value in payload.items()
            if "time_us" not in key and key != "context"}


def serial_timing(output: Path, binary: Path, protocol: dict[str, Any],
                  protocol_hash: str, simulator_hash: str) -> None:
    """Repeat fixed main-run cells serially to measure complete decision paths."""
    rows = []
    for cfg in protocol["timing_jobs"]:
        job = Job("main", cfg["policy"], cfg["family"], 3, 2101,
                  cfg["traffic"], cfg["load"], 100, Config(5, 40, 1.0), 1)
        row = invoke(job, binary, output / "timing", protocol_hash,
                     simulator_hash, protocol["common_conditions"])
        current = json.loads((output / "timing" / row["raw_json_path"]).read_text())
        full = json.loads((output / row["raw_json_path"]).read_text())
        if scientific_outcomes(current) != scientific_outcomes(full):
            raise AssertionError(f"Serial timing changed outcomes: {job.job_id}")
        row.update({key: value for key, value in current.items()
                    if key.startswith("complete_")})
        row["complete_decision_time_us_total"] = (
            current["complete_control_time_us_total"] + current["complete_batch_time_us_total"])
        row["same_scientific_outcome_as_full_rerun"] = True
        rows.append(row)
    (output / "timing_rows.jsonl").write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows))
    summary = []
    for policy in POLICIES:
        subset = [row for row in rows if row["policy_id"] == policy]
        summary.append({
            "policy": policy, "runs": len(subset),
            "mean_total_decision_ms_per_run": float(np.mean([
                row["complete_decision_time_us_total"] / 1000 for row in subset])),
            "mean_periodic_decision_p95_us": float(np.mean([
                row["complete_control_time_us_p95"] for row in subset])),
            "mean_batch_decision_p95_us": float(np.mean([
                row["complete_batch_time_us_p95"] for row in subset])),
            "max_batch_decision_us": max(row["complete_batch_time_us_max"] for row in subset),
            "complete_control_decisions": sum(row["complete_control_decisions"] for row in subset),
            "complete_batch_decisions": sum(row["complete_batch_decisions"] for row in subset),
        })
    write_json(output / "timing_summary.json", summary)


def context_outputs(output: Path) -> None:
    rows = read_csv(output / "context_cluster_summary.csv")
    index = {(row["policy_id"], row["context_id"]): row for row in rows}
    points = []
    for policy in EXTERNAL_POLICIES:
        for family in ("deadline_mix", "heavy_tenants", "regime_shift", "synchronized"):
            for suffix in (3, 4, 5):
                context = f"{family}_{suffix}"
                base, other = index["gar_h", context], index[policy, context]
                points.append({
                    "comparison_policy": policy, "context_id": context, "family": family,
                    "effect_adapter_minus_gar_pp": float(other["timely_payload_efficiency_pct"])
                        - float(base["timely_payload_efficiency_pct"]),
                    "gar_efficiency_pct": float(base["timely_payload_efficiency_pct"]),
                    "adapter_efficiency_pct": float(other["timely_payload_efficiency_pct"]),
                    "seed_count": 3, "traffic_block_count": 6,
                })
    (output / "context_points.jsonl").write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in points))
    write_json(output / "context_contrasts.json", [
        row for row in read_csv(output / "paired_effects.csv")
        if row["analysis_scope"] == "overall_all_traffic"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    if args.workers < 1 or args.workers > 8:
        raise SystemExit("--workers must be between 1 and 8")
    output_root = args.output.resolve()
    prepare_output(output_root)
    protocol = json.loads(PROTOCOL_PATH.read_text(encoding="utf-8"))
    protocol_hash = sha256(PROTOCOL_PATH)
    if sha256(SIMULATOR) != protocol["simulator_sha256"]:
        raise SystemExit("Simulator digest does not match the fixed protocol")
    for family in protocol["families"]:
        for suffix in protocol["contexts"]["tuning_suffixes"] + protocol["contexts"]["evaluation_suffixes"]:
            context = CONTEXTS / f"{family}_{suffix}.csv"
            if not context.exists():
                raise SystemExit(f"Missing required context: {context}")
    context_hashes = {
        f"{family}_{suffix}.csv": sha256(CONTEXTS / f"{family}_{suffix}.csv")
        for family in protocol["families"]
        for suffix in protocol["contexts"]["tuning_suffixes"] + protocol["contexts"]["evaluation_suffixes"]
    }
    compiler_version = subprocess.run(["c++", "--version"], capture_output=True, text=True, check=False).stdout.splitlines()
    freeze = {
        "study_id": protocol["study_id"],
        "frozen_at_utc": utc_now(),
        "protocol_sha256": protocol_hash,
        "simulator_sha256": sha256(SIMULATOR),
        "python": sys.version,
        "platform": platform.platform(),
        "compiler_version_first_line": compiler_version[0] if compiler_version else "unavailable",
        "context_sha256": context_hashes,
        "workers": args.workers,
        "note": "Written before compilation, smoke tests, calibration, or evaluation jobs.",
    }
    shutil.copy2(PROTOCOL_PATH, output_root / "protocol.json")
    write_json(output_root / "protocol_freeze.json", freeze)
    build_log = output_root / "logs" / "build.log"
    build_log.parent.mkdir(parents=True, exist_ok=True)
    derivative_hash = sha256(SIMULATOR)
    binary = output_root / "build" / "simulator"
    binary.parent.mkdir(parents=True, exist_ok=True)
    compile_command = ["c++", "-std=c++17", "-O2", "-I", str(HERE), str(SIMULATOR), "-o", str(binary)]
    compile_process = subprocess.run(compile_command, capture_output=True, text=True, check=False)
    with build_log.open("a", encoding="utf-8") as handle:
        handle.write("\n$ " + " ".join(compile_command) + "\n" + compile_process.stdout + compile_process.stderr)
    if compile_process.returncode != 0:
        raise SystemExit("Simulator compilation failed; see output logs/build.log")
    common = protocol["common_conditions"]
    oracle = oracle_fixture()
    if not oracle["pass"]:
        raise SystemExit("Internal exact oracle fixture is malformed")
    test_receipt = smoke_tests(binary, output_root, protocol_hash, derivative_hash, common)
    if test_receipt["status"] != "PASS":
        raise SystemExit("Smoke tests failed; evidence retained in the output directory")
    grid = [
        Config(step_ms=step, term_ms=term, gate_eta=gate)
        for step, term, gate in itertools.product(protocol["tuning_grid"]["step_ms"], protocol["tuning_grid"]["term_ms"], protocol["tuning_grid"]["gate_eta"])
    ]
    calibration = protocol["tuning_grid"]["traffic_block"]
    tuning_jobs = [
        Job("tuning", policy, family, suffix, seed, "calibration_mid", float(calibration["load"]), int(calibration["tenant_offer_rate_per_s"]), config, int(common["expire_background_main"]))
        for policy, family, suffix, seed, config in itertools.product(
            POLICIES, protocol["families"], protocol["contexts"]["tuning_suffixes"], protocol["seeds"], grid
        )
    ]
    manifest = {
        "study_id": protocol["study_id"],
        "protocol_sha256": protocol_hash,
        "derivative_sha256": derivative_hash,
        "simulator_sha256": sha256(SIMULATOR),
        "tuning_jobs_n": len(tuning_jobs),
        "main_jobs_n": 0,
        "factorial_jobs_n": 0,
        "job_manifest_sha256": stable_hash([asdict(job) for job in tuning_jobs]),
        "frozen_at_utc": freeze["frozen_at_utc"],
    }
    write_json(output_root / "run_manifest.json", manifest)
    tuning_rows = run_jobs(tuning_jobs, binary, output_root, protocol_hash, derivative_hash, common, args.workers, "tuning")
    write_csv(output_root / "tuning_rows.csv", tuning_rows)
    trace_audit_rows, trace_failures = offer_trace_audit(tuning_rows, "tuning", include_config=True)
    write_csv(output_root / "offer_trace_audit_tuning.csv", trace_audit_rows)
    if trace_failures:
        write_json(output_root / "receipt.json", {"status": "INVALID_WORKLOAD_TRACE", "stage": "tuning", "failed_groups": trace_failures})
        raise SystemExit("Tuning offer traces differ across policies; retained invalid evidence")
    selections, selected = selection_rows(tuning_rows)
    write_csv(output_root / "tuning_selection.csv", selections)
    if set(selected) != set(POLICIES):
        missing = sorted(set(POLICIES).difference(selected))
        write_json(output_root / "receipt.json", {"status": "INCOMPLETE_NO_SAFE_TUNING_CONFIGURATION", "missing_policies": missing})
        raise SystemExit(f"No safe tuning configuration for: {', '.join(missing)}")
    main_jobs = [
        Job("main", policy, family, suffix, seed, traffic["name"], float(traffic["load"]), int(traffic["tenant_offer_rate_per_s"]), selected[policy], int(common["expire_background_main"]))
        for policy, family, suffix, seed, traffic in itertools.product(
            POLICIES, protocol["families"], protocol["contexts"]["evaluation_suffixes"], protocol["seeds"], protocol["traffic_blocks"]
        )
    ]
    manifest["main_jobs_n"] = len(main_jobs)
    manifest["job_manifest_sha256"] = stable_hash([asdict(job) for job in tuning_jobs + main_jobs])
    write_json(output_root / "run_manifest.json", manifest)
    main_rows = run_jobs(main_jobs, binary, output_root, protocol_hash, derivative_hash, common, args.workers, "main")
    write_csv(output_root / "main_rows.csv", main_rows)
    trace_audit_rows, trace_failures = offer_trace_audit(main_rows, "main", include_config=False)
    write_csv(output_root / "offer_trace_audit_main.csv", trace_audit_rows)
    if trace_failures:
        write_json(output_root / "receipt.json", {"status": "INVALID_WORKLOAD_TRACE", "stage": "main", "failed_groups": trace_failures})
        raise SystemExit("Main offer traces differ across policies; retained invalid evidence")
    seed_rows = aggregate_seed_rows(main_rows)
    context_clusters = aggregate_context_clusters(seed_rows)
    write_csv(output_root / "seed_averaged_rows.csv", seed_rows)
    write_csv(output_root / "context_cluster_summary.csv", context_clusters)
    write_csv(output_root / "policy_summary.csv", policy_summary(context_clusters))
    write_csv(output_root / "paired_effects.csv", paired_effects(context_clusters, seed_rows))
    gar_config = selected["gar_h"]
    factor = protocol["factorial_diagnostic"]
    # The currently frozen wrapper accepts command delay through the common
    # condition.  Materialize each factorial cell with its declared delay by
    # encoding it in a temporary common dictionary rather than changing code.
    factorial_rows: list[dict[str, Any]] = []
    # Grouping by delay lets the wrapper pass the declared command lead while
    # retaining that factor explicitly in every exported row.
    factorial_jobs_by_delay: dict[int, list[Job]] = {int(delay): [] for delay in factor["factors"]["command_delay_ms"]}
    for family, suffix, seed, gate, expiry, delay in itertools.product(
        protocol["families"], protocol["contexts"]["evaluation_suffixes"], protocol["seeds"], factor["factors"]["gate_eta"],
        factor["factors"]["expire_background"], factor["factors"]["command_delay_ms"]
    ):
        factorial_jobs_by_delay[int(delay)].append(Job(
            "factorial", "gar_h", family, suffix, seed, f"factorial_stress_delay{delay}", float(factor["load"]), int(factor["tenant_offer_rate_per_s"]),
            Config(gar_config.step_ms, gar_config.term_ms, float(gate)), int(expiry),
        ))
    manifest["factorial_jobs_n"] = sum(len(jobs) for jobs in factorial_jobs_by_delay.values())
    manifest["job_manifest_sha256"] = stable_hash([asdict(job) for job in tuning_jobs + main_jobs + [item for jobs in factorial_jobs_by_delay.values() for item in jobs]])
    write_json(output_root / "run_manifest.json", manifest)
    for delay, jobs in factorial_jobs_by_delay.items():
        factorial_common = dict(common)
        factorial_common["command_delay_ms"] = delay
        rows = run_jobs(jobs, binary, output_root, protocol_hash, derivative_hash, factorial_common, args.workers, f"factorial_delay{delay}")
        for row in rows:
            row["factor_command_delay_ms"] = delay
        factorial_rows.extend(rows)
    write_csv(output_root / "factorial_diagnostics.csv", factorial_rows)
    failed_processes = [row for row in tuning_rows + main_rows + factorial_rows if row["error"] or row["exit_code"] != 0]
    infeasible_cells = [row for row in tuning_rows + main_rows + factorial_rows if row["simulator_status"] == "configuration_infeasible"]
    unsafe_pass_cells = [row for row in tuning_rows + main_rows + factorial_rows if row["simulator_status"] == "PASS" and row["safe_run"] is not True]
    receipt = {
        "status": "COMPLETE" if not failed_processes and not unsafe_pass_cells else "COMPLETE_WITH_RETAINED_FAILURES",
        "study_id": protocol["study_id"],
        "protocol_sha256": protocol_hash,
        "derivative_sha256": derivative_hash,
        "simulator_sha256": sha256(SIMULATOR),
        "test_status": test_receipt["status"],
        "tuning_jobs_n": len(tuning_rows),
        "main_jobs_n": len(main_rows),
        "factorial_jobs_n": len(factorial_rows),
        "failed_processes_n": len(failed_processes),
        "configuration_infeasible_cells_n": len(infeasible_cells),
        "unsafe_PASS_cells_n": len(unsafe_pass_cells),
        "evidence_scope": "Generated one-gateway packet-slot workloads; common-interface policy comparison with context-level statistical replication.",
        "heldout_qualification": protocol["contexts"]["evaluation_qualification"],
        "created_at_utc": utc_now(),
    }
    context_outputs(output_root)
    serial_timing(output_root, binary, protocol, protocol_hash, derivative_hash)
    receipt["serial_timing_jobs_n"] = len(protocol["timing_jobs"])
    write_json(output_root / "receipt.json", receipt)
    print(json.dumps(receipt, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
