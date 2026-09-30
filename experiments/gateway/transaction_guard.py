"""Production transaction checks shared by the UDP gateway and its tests.

Hashes bind trusted local planner messages; they do not authenticate a hostile
controller. State and replay records are in memory, not crash-durable storage.
The declared rate–latency model remains an assumption of the packet prototype.
"""
from __future__ import annotations

from copy import deepcopy
from decimal import Decimal, InvalidOperation
import hashlib
import json


def canonical_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def contract_configuration(cfg):
    # Configuration is immutable within a run. Bind all fields, including gate,
    # candidate-selection, release-pattern, and otherwise future-added fields.
    return deepcopy(cfg)


def normalized_snapshot(snapshot):
    value = deepcopy(snapshot)
    for key in ("calendar", "other_units"):
        if key + "_runs" in value:
            value[key] = [item for length, item in value.pop(key + "_runs")
                          for _ in range(length)]
    value.pop("snapshot_digest", None)
    return value


def request_contract(request):
    return {key: deepcopy(request[key]) for key in
            ("id", "start_ms", "end_ms", "units", "work_bytes", "release_schedule")}


def command_payload(command):
    """Exclude transport stamps: identical retries can have a new send time."""
    return {key: deepcopy(value) for key, value in command.items()
            if key not in ("transmit_ns", "certificate_digest")}


def bind_command(command, snapshot, cfg):
    command = deepcopy(command)
    command["snapshot_digest"] = snapshot["snapshot_digest"]
    command["contracts_digest"] = canonical_digest(contract_configuration(cfg))
    command["certificate_digest"] = canonical_digest(command_payload(command))
    return command


def verify_candidate(adapter, snapshot, cfg, command):
    """Recheck the bound candidate, not merely its claimed certification flag."""
    pending = snapshot["critical_pending"]
    if any(deadline <= 0 and amount > 0 for deadline, amount in pending):
        return False, "guardband_overdue_pending"
    env = []
    for item in cfg["envelopes"]:
        deadline = int(item["min_deadline"]) - cfg["guardband_ms"]
        if deadline < 1:
            return False, "invalid_guardband_contract"
        env.extend((deadline, item["sigma"], item["rho"]))
    values = [cfg["capacity_bytes_per_ms"], cfg["units"], cfg["base_units"],
              cfg["maximum_units"], cfg["step_ms"],
              command["activate_ms"] - snapshot["now_ms"], command["term"],
              snapshot["old_final_change"], cfg["command_price"],
              len(cfg["envelopes"]), len(pending), len(snapshot["calendar"]), *env]
    for item in pending:
        values.extend(item)
    values.extend(snapshot["calendar"])
    values.extend(snapshot["other_units"])
    values.extend(command[key] for key in ("target", "term", "tail"))
    result = adapter.call("CHECK " + " ".join(map(str, values)))
    return bool(result.get("pass")), result.get("reason", "candidate_check_error")


def release_contract_check(request, cfg):
    """Check every release suffix against R * max(0, suffix_length - theta).

Release batches are available at the start of their integer slot; the job
deadline is the end of the final slot. A latency deduction is applied to each
suffix, not just to the start of the whole window. This is conservative for a
rate–latency service curve and exact for theta=0 slotted constant-rate service.
"""
    try:
        if not isinstance(request, dict) or not isinstance(cfg, dict):
            return False, "missing_or_invalid_release_contract"
        start, end, units, work = (request[k] for k in
                                  ("start_ms", "end_ms", "units", "work_bytes"))
        if any(type(x) is not int for x in (start, end, units, work)):
            return False, "noninteger_release_contract"
        duration = end - start
        if start < 0 or units < 1 or duration <= 0 or duration > cfg.get("tenant_max_duration_ms", 500) or work < 1:
            return False, "invalid_release_contract"
        schedule = request["release_schedule"]
        if not isinstance(schedule, list) or not schedule:
            return False, "missing_release_schedule"
        previous = -1
        for batch in schedule:
            if not isinstance(batch, (list, tuple)) or len(batch) != 2:
                return False, "invalid_release_schedule"
            when, amount = batch
            if type(when) is not int or type(amount) is not int or not previous < when < duration or amount < 1:
                return False, "invalid_release_schedule"
            previous = when
        if sum(batch[1] for batch in schedule) != work:
            return False, "release_payload_mismatch"
        headroom = Decimal(str(cfg.get("tenant_headroom", 1)))
        theta = cfg.get("tenant_theta_ms", 0)
        capacity, total = cfg["capacity_bytes_per_ms"], cfg["units"]
        if not headroom.is_finite() or not 0 < headroom <= 1 or type(theta) is not int or theta < 0:
            return False, "invalid_service_model"
        if type(capacity) is not int or type(total) is not int or capacity < 1 or total < 1:
            return False, "invalid_service_model"
        if units > total:
            return False, "invalid_release_contract"
        rate_capacity = int(Decimal(str(cfg["capacity_bytes_per_ms"])) * headroom)
        rate = rate_capacity * units // cfg["units"]
        remaining = work
        for when, amount in schedule:
            if remaining > rate * max(0, duration - when - theta):
                return False, "release_service_suffix_conflict"
            remaining -= amount
        return True, "release_contract_pass"
    except (KeyError, TypeError, ValueError, InvalidOperation):
        return False, "missing_or_invalid_release_contract"


class AdmissionReplay:
    """Successful ACK replay, with no second booking or materialization."""
    def __init__(self):
        self.accepted = {}

    def lookup(self, request):
        key = str(request.get("id"))
        if key not in self.accepted:
            return None
        fingerprint, ack = self.accepted[key]
        try:
            current = canonical_digest(request_contract(request))
        except KeyError:
            current = "invalid"
        if current != fingerprint:
            return {"type": "admission_ack", "id": key, "accepted": False,
                    "reason": "conflicting_request_id_reuse", "replayed": False}
        return {**deepcopy(ack), "replayed": True}

    def remember(self, request, ack):
        if ack["accepted"]:
            self.accepted[str(request["id"])] = (
                canonical_digest(request_contract(request)), deepcopy(ack))


class GatewayTransactionHandler:
    """Single-gateway command path with deterministic injectable time/adapter.

The adapter must implement ADV and PROP. The gateway invokes this handler and
only emits acceptance after it returns. A controller outage does not remove
the committed calendar. A gateway/process crash is outside this transaction
model: adapter and ledger do not share a durable write-ahead log.
"""
    def __init__(self, cfg, ledger, adapter, *, now_ns, origin_ns=0,
                 on_events=lambda state, actual: None, heartbeat_fresh=lambda now: True,
                 admission_screen=lambda request: (True, "disabled"),
                 candidate_check=verify_candidate, policy=None):
        self.cfg = cfg
        self.ledger = ledger
        self.adapter = adapter
        self.now_ns = now_ns
        self.origin = origin_ns
        self.on_events = on_events
        self.heartbeat_fresh = heartbeat_fresh
        self.admission_screen = admission_screen
        self.candidate_check = candidate_check
        self.policy = policy
        self.snapshots = {}
        self.committed = {}
        self.last_epoch = 0

    def issue_snapshot(self, snapshot):
        value = normalized_snapshot(snapshot)
        digest = canonical_digest(value)
        self.snapshots[value["seq"]] = (digest, value)
        # Bound memory; late commands for evicted snapshots fail closed.
        while len(self.snapshots) > 1024:
            self.snapshots.pop(next(iter(self.snapshots)))
        return {**snapshot, "snapshot_digest": digest}

    def handle(self, command, pending_admissions):
        def invalid(reason):
            return {"type": "ack", "epoch": command.get("epoch") if isinstance(command, dict) else None,
                    "accepted": False, "reason": reason, "replayed": False}, None
        if not isinstance(command, dict):
            return invalid("malformed_command")
        integer_fields = ("epoch", "snapshot_seq", "state_version", "ledger_version",
                          "observation_ms", "activate_ms", "target", "tail", "term")
        if any(type(command.get(key)) is not int for key in integer_fields):
            return invalid("malformed_command")
        if any(command[key] < 0 for key in integer_fields) or command["epoch"] < 1 or command["term"] < 1:
            return invalid("malformed_command")
        allowed = ("guarded", "predictive", "joint_guarded", "priority_joint")
        if command.get("policy") not in allowed or (self.policy is not None and command["policy"] != self.policy):
            return invalid("invalid_or_changed_policy")
        if command.get("certificate_passed") is not True:
            return invalid("uncertified_guarded_command")
        if any(not isinstance(command.get(key), str) or len(command[key]) != 64 for key in
               ("snapshot_digest", "contracts_digest", "certificate_digest")):
            return invalid("malformed_command_digest")
        joint = command.get("tenant_request")
        if joint is not None:
            if not isinstance(joint, dict) or not isinstance(joint.get("id"), str) or not joint["id"]:
                return invalid("malformed_tenant_request")
            passed, why = release_contract_check(joint, self.cfg)
            if not passed:
                return invalid(why)
        cmd = deepcopy(command)
        began = self.now_ns()
        epoch = cmd.get("epoch")
        try:
            fingerprint = canonical_digest(command_payload(cmd))
        except (TypeError, ValueError):
            return invalid("malformed_command")
        if epoch in self.committed:
            old_fingerprint, old_ack, old_state = self.committed[epoch]
            if fingerprint == old_fingerprint and cmd.get("certificate_digest") == fingerprint:
                return {**deepcopy(old_ack), "replayed": True}, None
            return {"type": "ack", "epoch": epoch, "accepted": False,
                    "reason": "conflicting_epoch_reuse", "replayed": False}, None
        ms = max(0, (began - self.origin) // 1_000_000)
        joint = cmd.get("tenant_request")
        horizon = max(1, self.ledger.max_end - ms,
                      joint["end_ms"] - ms if joint else 0)
        state = self.adapter.call(f"ADV {ms} {horizon}")
        self.on_events(state, self.now_ns())
        reason = "accepted"
        issued = self.snapshots.get(cmd.get("snapshot_seq"))
        if type(epoch) is not int or epoch <= self.last_epoch:
            reason = "stale_epoch"
        elif not issued or cmd.get("snapshot_digest") != issued[0]:
            reason = "unknown_or_changed_snapshot"
        elif cmd.get("contracts_digest") != canonical_digest(contract_configuration(self.cfg)):
            reason = "contracts_changed"
        elif cmd.get("certificate_digest") != fingerprint:
            reason = "candidate_digest_mismatch"
        elif any(cmd.get(key) != issued[1].get(key) for key in ("state_version", "ledger_version")) or cmd.get("observation_ms") != issued[1]["now_ms"]:
            reason = "snapshot_field_mismatch"
        elif cmd["state_version"] != state["state_version"]:
            reason = "state_changed"
        elif cmd["ledger_version"] != self.ledger.version:
            reason = "ledger_changed"
        elif any(type(cmd.get(k)) is not int for k in ("activate_ms", "target", "tail", "term")) or cmd["term"] < 1:
            reason = "invalid_candidate"
        elif cmd["activate_ms"] != (joint["start_ms"] if joint else issued[1]["now_ms"] + self.cfg["delay_ms"]):
            reason = "activation_not_bound_to_snapshot"
        elif began > self.origin + cmd["activate_ms"] * 1_000_000:
            reason = "missed_activation"
        elif state["pending"]:
            reason = "pending_command"
        elif not all(self.cfg["base_units"] <= cmd[k] <= self.cfg["maximum_units"] for k in ("target", "tail")):
            reason = "invalid_floor"
        elif cmd.get("policy") in ("guarded", "predictive", "joint_guarded", "priority_joint") and cmd.get("certificate_passed") is not True:
            reason = "uncertified_guarded_command"
        if joint and reason == "accepted":
            expected = pending_admissions.get(joint["id"])
            expected = expected[0] if isinstance(expected, tuple) else expected
            if not expected or request_contract(joint) != request_contract(expected):
                reason = "request_no_longer_pending_or_changed"
            elif not self.heartbeat_fresh(self.now_ns()):
                reason = "controller_heartbeat_stale"
            else:
                passed, why = self.admission_screen(joint)
                if not passed:
                    reason = why
        if reason == "accepted" and cmd.get("policy") in ("guarded", "predictive", "joint_guarded", "priority_joint"):
            passed, why = self.candidate_check(self.adapter, issued[1], self.cfg, cmd)
            if not passed:
                reason = "candidate_recheck_failed:" + why
        calendar = []
        if reason == "accepted":
            calendar = [state["calendar"][n] if ms + n < cmd["activate_ms"]
                        else cmd["target"] if ms + n < cmd["activate_ms"] + cmd["term"]
                        else cmd["tail"] for n in range(horizon)]
            for t in range(ms, self.ledger.max_end):
                if calendar[t - ms] + self.ledger.other_at(t) > self.cfg["units"]:
                    reason = "future_rights_conflict"
                    break
        if joint and reason == "accepted":
            passed, why = self.ledger.check(joint, ms, calendar)
            if not passed:
                reason = why
        commit_begin = commit_complete = None
        if reason == "accepted" and self.now_ns() > self.origin + cmd["activate_ms"] * 1_000_000:
            reason = "missed_activation_before_commit"
        if reason == "accepted":
            # No yielding or network I/O occurs between successful PROP and
            # ledger insertion. This is serialized in-process, not crash atomic.
            commit_begin = self.now_ns()
            state = self.adapter.call(f"PROP {ms} {cmd['activate_ms']-ms} {cmd['target']} {cmd['term']} {cmd['tail']} 1 0")
            commit_complete = self.now_ns()
            self.on_events(state, commit_complete)
            if state["accepted"] != 1:
                reason = "executor_rejected"
            else:
                if joint:
                    self.ledger.commit(joint)
                self.last_epoch = epoch
        ack = {"type": "ack", "epoch": epoch, "snapshot_seq": cmd.get("snapshot_seq"),
               "accepted": reason == "accepted", "reason": reason, "replayed": False,
               "snapshot_digest": cmd.get("snapshot_digest"),
               "certificate_digest": cmd.get("certificate_digest"),
               "contracts_digest": cmd.get("contracts_digest"),
               "gateway_receive_ns": began, "commit_begin_ns": commit_begin,
               "commit_complete_ns": commit_complete,
               "commit_complete_before_activation": commit_complete <= self.origin + cmd["activate_ms"] * 1_000_000 if commit_complete is not None else None,
               "executor_plan_id": state["pending_plan_id"] if state["pending_plan_id"] >= 0 else state["active_plan_id"],
               "state_version": state["state_version"], "ledger_version": self.ledger.version}
        if ack["accepted"]:
            self.committed[epoch] = (fingerprint, deepcopy(ack), deepcopy(state))
        return ack, state
