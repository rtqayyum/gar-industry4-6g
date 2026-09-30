"""Small explicit-contract reserve model, using integer byte service.

This is a new simulation, not a replay of the packet gateway. Every policy sees
the same original deadlines and releases. A reserve is inside the 106-unit
budget, and accepted finite jobs never rely on its revocable loan.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from fractions import Fraction
import hashlib
import heapq
import json
import math
import random
from typing import Any


KINDS = ("critical", "video", "monitor", "best_effort", "tenant")


def validate_protocol(protocol: dict[str, Any]) -> None:
    """Reject semantic drift in this deliberately bounded, fixed-model study.

    Constants used by the independent certificate are not arbitrary knobs.
    Changing one requires a new reviewed model version, not just edited JSON.
    Metadata, seed ranges and bootstrap settings do not change model semantics.
    """
    required = {
        "resources": {"units": 106, "nominal_bytes_per_ms": 650, "slot_ms": 1},
        "duration_ms": 1250,
        "offer_until_ms": 1200,
        "protection": {"request_ms": 390, "activation_ms": 400, "event_start_ms": 410,
                       "event_end_ms": 800, "release_ms": 810, "installation_lead_ms": 10},
        "contracts": {
            "critical": {"period_ms": 5, "base_max_bytes": 500, "event_extra_max_bytes": 950,
                         "deadline_ms": 8, "nominal_base_units": 21, "nominal_active_units": 49},
            "video": {"period_ms": 10, "max_bytes_at_load_one": 2400, "deadline_ms": 15,
                      "nominal_units_at_load_one": 40},
            "monitor": {"period_ms": 10, "max_bytes": 100, "deadline_ms": 20, "nominal_units": 5},
            "best_effort": {"period_ms": 5, "min_bytes": 1500, "max_bytes": 2500,
                            "deadline_ms": 50, "protected": False}},
        "tenant": {"offer_start_ms": 100, "offer_end_ms": 1050,
                   "offer_probability_per_ms": "load/18", "lead_ms": 10,
                   "units": [4, 8, 12, 16, 20], "lengths_ms": [40, 80, 120],
                   "payload_fractions": [0.65, 0.85, 1.0],
                   "release_modes": ["all_at_start", "thirds", "late_half"],
                   "payload_basis": "fraction of nominal, not factor-adjusted, service; release certificate decides feasibility"},
    }
    for field, expected in required.items():
        if protocol.get(field) != expected:
            raise ValueError(f"Unsupported protocol semantics for {field}; revise and revalidate the model first")


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def quota(units: int, factor: float = 1.0) -> int:
    """Use exact decimal-rational arithmetic, avoiding binary-floor artifacts."""
    g = Fraction(str(factor))
    return (650 * g.numerator * units) // (106 * g.denominator)


def scaled_units(units: int, factor: float) -> int:
    return math.ceil(Fraction(units, 1) / Fraction(str(factor)))


def release_certificate(releases: list[int], service: list[int]) -> bool:
    """Exact common-deadline suffix test for preemptive integer-byte service."""
    if len(releases) != len(service) or any(x < 0 for x in releases + service):
        return False
    demand = supply = 0
    for arrivals, available in zip(reversed(releases), reversed(service)):
        demand += arrivals
        supply += available
        if demand > supply:
            return False
    return True


def forward_completion(releases: list[int], service: list[int]) -> bool:
    """Independent queue recursion used by tests, not admission."""
    if len(releases) != len(service) or any(x < 0 for x in releases + service):
        return False
    backlog = 0
    for arrivals, available in zip(releases, service):
        backlog = max(0, backlog + arrivals - available)
    return backlog == 0


def periodic_certificate(max_bytes: int, period: int, deadline: int, rate: int) -> bool:
    """Exact constant-rate condition for one sporadic stream's demand bound.

    For k jobs, k*W <= c*((k-1)*T+D). Checking W<=cD and W<=cT
    suffices for every integer k>=1. Simultaneous critical sources are bounded
    conservatively by their summed burst and common period/deadline.
    """
    return min(max_bytes, period, deadline, rate) > 0 and max_bytes <= rate * min(period, deadline)


@dataclass(frozen=True)
class Job:
    uid: int
    kind: str
    request: int
    start: int
    deadline: int
    total: int
    releases: tuple[tuple[int, int], ...]
    units: int = 0
    mode: str = "single"

    def payload(self) -> dict[str, Any]:
        return asdict(self)


def make_workload(protocol: dict[str, Any], seed: int, load: float) -> list[Job]:
    validate_protocol(protocol)
    rng = random.Random(seed * 100000 + round(load * 1000))
    jobs: list[Job] = []
    until = protocol["offer_until_ms"]
    contracts = protocol["contracts"]

    def append(kind: str, release: int, size: int, delay: int) -> None:
        jobs.append(Job(len(jobs), kind, release, release, release + delay,
                        size, ((release, size),)))

    def stream(kind: str, maximum: int, period: int, deadline: int,
               begin: int = 0, end: int = until) -> None:
        phase = rng.randrange(period)
        for t in range(begin + phase, end, period):
            # Deterministic seed-specific burst sizes, always inside the contract.
            size = rng.randint(math.ceil(0.85 * maximum), maximum)
            append(kind, t, size, deadline)

    cc = contracts["critical"]
    stream("critical", cc["base_max_bytes"], cc["period_ms"], cc["deadline_ms"])
    stream("critical", cc["event_extra_max_bytes"], cc["period_ms"], cc["deadline_ms"],
           protocol["protection"]["event_start_ms"], protocol["protection"]["event_end_ms"])
    vv = contracts["video"]
    stream("video", math.ceil(vv["max_bytes_at_load_one"] * load), vv["period_ms"], vv["deadline_ms"])
    mm = contracts["monitor"]
    stream("monitor", mm["max_bytes"], mm["period_ms"], mm["deadline_ms"])
    bb = contracts["best_effort"]
    for t in range(rng.randrange(bb["period_ms"]), until, bb["period_ms"]):
        append("best_effort", t, rng.randint(bb["min_bytes"], bb["max_bytes"]), bb["deadline_ms"])
    tt = protocol["tenant"]
    for t in range(tt["offer_start_ms"], tt["offer_end_ms"]):
        if rng.random() >= load / 18:
            continue
        units = rng.choice(tt["units"])
        length = rng.choice(tt["lengths_ms"])
        fraction = rng.choice(tt["payload_fractions"])
        total = math.floor(quota(units) * length * fraction)
        start = t + tt["lead_ms"]
        mode = rng.choice(tt["release_modes"])
        if mode == "all_at_start":
            releases = ((start, total),)
        elif mode == "thirds":
            first = total // 3
            releases = ((start, first), (start + length // 3, first),
                        (start + 2 * length // 3, total - 2 * first))
        else:
            first = total // 2
            releases = ((start, first), (start + 9 * length // 10, total - first))
        jobs.append(Job(len(jobs), "tenant", t, start, start + length, total,
                        releases, units, mode))
    return jobs


def configuration(protocol: dict[str, Any], load: float, factor: float,
                  policy: str) -> dict[str, Any]:
    validate_protocol(protocol)
    if factor <= 0 or load <= 0:
        raise ValueError("Service factor and offered load must be positive")
    cc = protocol["contracts"]["critical"]
    base = scaled_units(cc["nominal_base_units"], factor)
    active = scaled_units(cc["nominal_active_units"], factor)
    nominal_video = 27 if policy == "no_video_contract" else math.ceil(40 * load)
    video = scaled_units(nominal_video, factor)
    monitor = scaled_units(protocol["contracts"]["monitor"]["nominal_units"], factor)
    safe = periodic_certificate(cc["base_max_bytes"], 5, 8, quota(base, factor))
    safe &= periodic_certificate(cc["base_max_bytes"] + cc["event_extra_max_bytes"], 5, 8,
                                 quota(active, factor))
    video_ok = periodic_certificate(math.ceil(2400 * load), 10, 15, quota(video, factor))
    monitor_ok = periodic_certificate(100, 10, 20, quota(monitor, factor))
    required = active + video + monitor
    feasible = safe and monitor_ok and required <= 106 and (video_ok or policy == "no_video_contract")
    return {"base_units": base, "active_units": active, "reserve_units": active - base,
            "video_units": video, "monitor_units": monitor, "required_peak_units": required,
            "irrevocable_tenant_budget_units": max(0, 106 - required),
            "video_certificate": video_ok, "incumbent_certificate": bool(safe and video_ok and monitor_ok),
            "feasible": bool(feasible), "delivered_bytes_per_slot": quota(106, factor)}


def percentile(values: list[int | float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    # Empirical nearest-rank quantile: no interpolation conceals an actual tail.
    return float(ordered[max(0, math.ceil(probability * len(ordered)) - 1)])


def run_model(protocol: dict[str, Any], seed: int, load: float, factor: float,
              policy: str, jobs: list[Job] | None = None) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    if policy not in protocol["policies"]:
        raise ValueError(f"Unknown policy: {policy}")
    jobs = make_workload(protocol, seed, load) if jobs is None else jobs
    cfg = configuration(protocol, load, factor, policy)
    metadata = {"seed": seed, "load": load, "factor": factor, "policy": policy,
                "configuration": cfg, "workload_sha256": digest([j.payload() for j in jobs])}
    if not cfg["feasible"]:
        return {**metadata, "status": "incumbent_provisioning_rejected",
                "offered_tenants": sum(j.kind == "tenant" for j in jobs)}, [], []
    duration = protocol["duration_ms"]
    activation = protocol["protection"]["activation_ms"]
    release = protocol["protection"]["release_ms"]
    capacity = cfg["delivered_bytes_per_slot"]
    bookings = [0] * duration
    accepted: set[int] = set()
    admission: dict[int, str] = {}
    requests: dict[int, list[Job]] = {}
    arrivals: list[list[tuple[str, int, int]]] = [[] for _ in range(duration)]
    for job in jobs:
        if job.kind == "tenant":
            requests.setdefault(job.request, []).append(job)
        else:
            for t, amount in job.releases:
                arrivals[t].append((job.kind, job.uid, amount))
    # Heap elements contain remaining *released* bytes, not unreleased payload.
    queues: dict[str, list[list[int]]] = {kind: [] for kind in KINDS if kind != "tenant"}
    served = [0] * len(jobs)
    completed: list[int | None] = [None] * len(jobs)
    active_tenants: dict[str, Job] = {}
    trace: list[dict[str, Any]] = []
    totals = {kind: 0 for kind in KINDS}
    reserve_lent = blocked = unused = 0
    slice_service = {kind: 0 for kind in KINDS}
    slice_lent = slice_blocked = slice_unused = 0
    invariant_errors: list[dict[str, Any]] = []

    def send(key: str, budget: int, t: int) -> int:
        queue = queues[key]
        consumed = 0
        while queue and budget:
            deadline, uid, remaining = queue[0]
            if deadline <= t:
                heapq.heappop(queue)
                continue
            take = min(budget, remaining)
            budget -= take
            consumed += take
            served[uid] += take
            queue[0][2] -= take
            kind = jobs[uid].kind
            totals[kind] += take
            slice_service[kind] += take
            if queue[0][2] == 0:
                heapq.heappop(queue)
            if served[uid] == jobs[uid].total:
                completed[uid] = t + 1
        return consumed

    for t in range(duration):
        for job in requests.get(t, []):
            if policy == "no_tenant_control":
                admission[job.uid] = "control_disabled"
                continue
            releases = [0] * (job.deadline - job.start)
            for when, amount in job.releases:
                releases[when - job.start] += amount
            services = [quota(job.units, factor)] * len(releases)
            release_ok = release_certificate(releases, services)
            capacity_ok = all(bookings[u] + job.units <= cfg["irrevocable_tenant_budget_units"]
                              for u in range(job.start, job.deadline))
            if not release_ok:
                admission[job.uid] = "release_contract_rejected"
                continue
            if not capacity_ok:
                admission[job.uid] = "future_capacity_rejected"
                continue
            admission[job.uid] = "accepted"
            accepted.add(job.uid)
            for u in range(job.start, job.deadline):
                bookings[u] += job.units
            key = f"tenant_{job.uid}"
            queues[key] = []
            active_tenants[key] = job
            for when, amount in job.releases:
                arrivals[when].append((key, job.uid, amount))
        for key, uid, amount in arrivals[t]:
            heapq.heappush(queues[key], [jobs[uid].deadline, uid, amount])
        for key in list(active_tenants):
            if active_tenants[key].deadline <= t:
                queues.pop(key)
                active_tenants.pop(key)
        for queue in queues.values():
            while queue and queue[0][0] <= t:
                heapq.heappop(queue)
        current_critical = cfg["active_units"] if activation <= t < release else cfg["base_units"]
        allocation = {"critical": quota(current_critical, factor),
                      "video": quota(cfg["video_units"], factor),
                      "monitor": quota(cfg["monitor_units"], factor)}
        for key, job in active_tenants.items():
            if job.start <= t < job.deadline:
                allocation[key] = quota(job.units, factor)
        if sum(allocation.values()) > capacity:
            invariant_errors.append({"slot": t, "type": "byte_oversubscription"})
        used = 0
        critical_used = 0
        for key, budget in allocation.items():
            actual = send(key, budget, t)
            used += actual
            if key == "critical":
                critical_used = actual
        reserve_bytes = quota(cfg["active_units"], factor) - quota(cfg["base_units"], factor)
        utilized_reserve = max(0, critical_used - quota(cfg["base_units"], factor)) if activation <= t < release else 0
        unused_reserve = reserve_bytes - utilized_reserve
        blocked_now = unused_reserve if policy == "dedicated_idle" else 0
        leftover = capacity - blocked_now - used
        opportunistic = 0
        while leftover > 0:
            candidates = [(queue[0][0], queue[0][1], key) for key, queue in queues.items() if queue]
            if not candidates:
                break
            _, _, key = min(candidates)
            take = min(leftover, queues[key][0][2])
            actual = send(key, take, t)
            leftover -= actual
            opportunistic += actual
            used += actual
        # Non-reserve residual is consumed first: this is a conservative,
        # unambiguous accounting of opportunistic bytes using the reserve loan.
        nonreserve_slack = max(0, capacity - unused_reserve - (used - opportunistic))
        lent_now = max(0, opportunistic - nonreserve_slack) if policy != "dedicated_idle" else 0
        lent_now = min(lent_now, unused_reserve)
        reserve_lent += lent_now
        blocked += blocked_now
        unused += capacity - used
        slice_lent += lent_now
        slice_blocked += blocked_now
        slice_unused += capacity - used
        if t % 20 == 19 or t + 1 == duration:
            trace.append({"end_ms": t + 1, "service_bytes": dict(slice_service),
                          "reserve_lent_bytes": slice_lent, "reserve_blocked_bytes": slice_blocked,
                          "unused_bytes": slice_unused,
                          "backlog_bytes": {kind: sum(item[2] for key, queue in queues.items()
                                                       for item in queue if jobs[item[1]].kind == kind)
                                            for kind in KINDS}})
            slice_service = {kind: 0 for kind in KINDS}
            slice_lent = slice_blocked = slice_unused = 0
    outcomes: list[dict[str, Any]] = []
    for job in jobs:
        end = completed[job.uid]
        status = admission.get(job.uid, "incumbent" if job.kind != "best_effort" else "unprotected")
        success = end is not None and end <= job.deadline
        outcomes.append({"uid": job.uid, "kind": job.kind, "status": status,
                         "total_bytes": job.total, "served_bytes": served[job.uid],
                         "completion_ms": end, "timely": success,
                         "latency_ms": end - job.start if success else None})
    class_metrics: dict[str, Any] = {}
    for kind in KINDS:
        selected = [o for o in outcomes if o["kind"] == kind]
        delivered = [o for o in selected if o["timely"]]
        # Partial service consumes capacity but is not a complete timely item.
        credited = sum(o["total_bytes"] for o in delivered)
        latencies = [o["latency_ms"] for o in delivered]
        class_metrics[kind] = {"offered_jobs": len(selected), "offered_bytes": sum(o["total_bytes"] for o in selected),
                               "timely_jobs": len(delivered), "failed_jobs": len(selected) - len(delivered),
                               "timely_bytes": credited,
                               "on_time_partial_service_bytes": sum(o["served_bytes"] for o in selected),
                               "p95_ms": percentile(latencies, .95),
                               "p99_ms": percentile(latencies, .99), "max_ms": max(latencies, default=None)}
    epochs: dict[str, Any] = {}
    for label, (begin, end) in protocol["epochs"].items():
        epochs[label] = {}
        for kind in KINDS:
            selected = [o for o in outcomes if o["kind"] == kind and begin <= jobs[o["uid"]].start < end]
            latencies = [o["latency_ms"] for o in selected if o["timely"]]
            epochs[label][kind] = {"offered_jobs": len(selected),
                                   "failures": sum(not o["timely"] for o in selected),
                                   "p99_ms": percentile(latencies, .99),
                                   "timely_bytes": sum(o["total_bytes"] for o in selected if o["timely"])}
    accepted_outcomes = [o for o in outcomes if o["status"] == "accepted"]
    admitted_failed = sum(not o["timely"] for o in accepted_outcomes)
    timely_bytes = sum(m["timely_bytes"] for m in class_metrics.values())
    summary = {**metadata, "status": "completed", "class_metrics": class_metrics,
               "epochs": epochs, "offered_tenants": class_metrics["tenant"]["offered_jobs"],
               "accepted_tenants": len(accepted), "accepted_tenant_failures": admitted_failed,
               "tenant_reasons": {reason: list(admission.values()).count(reason) for reason in sorted(set(admission.values()))},
               "timely_payload_efficiency_nominal": timely_bytes / (650 * duration),
               "timely_payload_efficiency_delivered": timely_bytes / (capacity * duration),
               "partial_byte_service_fraction_nominal": sum(totals.values()) / (650 * duration),
               "utilization": sum(totals.values()) / (capacity * duration),
               "reserve_lent_bytes": reserve_lent, "reserve_blocked_bytes": blocked,
               "unused_bytes": unused, "invariant_errors": invariant_errors,
               "outcome_sha256": digest(outcomes)}
    return summary, outcomes, trace
