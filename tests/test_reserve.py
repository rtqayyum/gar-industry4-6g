"""Independent arithmetic and end-to-end invariants for incumbent reservation."""
import importlib.util
import copy
import itertools
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "experiments/reserve/model.py"
spec = importlib.util.spec_from_file_location("reserve_model", SOURCE)
m = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = m
spec.loader.exec_module(m)
PROTOCOL = json.loads(SOURCE.with_name("protocol.json").read_text())


class ReserveTests(unittest.TestCase):
    def test_all_four_slot_release_vectors_against_independent_forward_oracle(self):
        vectors = list(itertools.product(range(3), repeat=4))
        cases = 0
        for arrivals in vectors:
            for service in vectors:
                backlog = 0
                for a, s in zip(arrivals, service):
                    backlog += a
                    backlog -= min(backlog, s)
                self.assertEqual(m.release_certificate(list(arrivals), list(service)), backlog == 0)
                cases += 1
        self.assertEqual(cases, 6561)

    def test_late_release_is_not_total_capacity(self):
        self.assertFalse(m.release_certificate([0, 0, 0, 4], [1, 1, 1, 1]))
        self.assertTrue(m.release_certificate([4, 0, 0, 0], [1, 1, 1, 1]))

    def test_periodic_certificate_against_demand_bound(self):
        for amount, period, deadline, rate in itertools.product(range(1, 8), range(1, 8), range(1, 8), range(1, 8)):
            oracle = all(k * amount <= rate * ((k - 1) * period + deadline) for k in range(1, 65))
            self.assertEqual(m.periodic_certificate(amount, period, deadline, rate), oracle)

    def test_fractional_service_is_shared_by_every_contract(self):
        self.assertEqual(m.quota(106, .95), 617)
        self.assertEqual(m.quota(106, .9), 585)
        for factor in PROTOCOL["delivered_service_factors"]:
            cfg = m.configuration(PROTOCOL, 1, factor, "borrowable")
            self.assertGreaterEqual(m.quota(cfg["video_units"], factor) * 10, 2400)

    def test_impossible_peak_rejected_without_revoking_video(self):
        for policy in ["borrowable", "dedicated_idle", "no_tenant_control"]:
            row, outcomes, trace = m.run_model(PROTOCOL, 3, 1.4, 1, policy)
            self.assertEqual(row["status"], "incumbent_provisioning_rejected")
            self.assertEqual(outcomes, [])
            self.assertEqual(trace, [])

    def test_control_and_reserve_use_same_offered_work(self):
        rows = {}
        for policy in PROTOCOL["policies"]:
            row, outcomes, _ = m.run_model(PROTOCOL, 3, 1, 1, policy)
            rows[policy] = row
            self.assertEqual(row["invariant_errors"], [])
            self.assertEqual(row["accepted_tenant_failures"], 0)
            if policy != "no_video_contract":
                for kind in ["critical", "video", "monitor"]:
                    self.assertEqual(row["class_metrics"][kind]["failed_jobs"], 0)
            self.assertEqual(len(outcomes), sum(x["offered_jobs"] for x in row["class_metrics"].values()))
        self.assertEqual(len({row["workload_sha256"] for row in rows.values()}), 1)
        self.assertEqual(rows["borrowable"]["accepted_tenants"], rows["dedicated_idle"]["accepted_tenants"])
        self.assertEqual(rows["dedicated_idle"]["reserve_lent_bytes"], 0)
        self.assertGreater(rows["borrowable"]["reserve_lent_bytes"], 0)
        self.assertGreater(rows["borrowable"]["utilization"], rows["dedicated_idle"]["utilization"])

    def test_all_declared_feasible_cells_preserve_incumbents(self):
        for load, factor in itertools.product(PROTOCOL["load_levels"], PROTOCOL["delivered_service_factors"]):
            row, _, _ = m.run_model(PROTOCOL, 5, load, factor, "borrowable")
            if row["status"] == "completed":
                self.assertEqual(row["accepted_tenant_failures"], 0)
                for kind in ["critical", "video", "monitor"]:
                    self.assertEqual(row["class_metrics"][kind]["failed_jobs"], 0)

    def test_worst_case_control_phases_against_independent_edf(self):
        # Independent list-based EDF; no production queue, certificate, or
        # workload generator. Full maximum bursts exercise both transitions.
        cases = 0
        for factor, normal_phase, extra_phase in itertools.product(
                PROTOCOL["delivered_service_factors"], range(5), range(5)):
            cfg = m.configuration(PROTOCOL, .6, factor, "borrowable")
            arrivals = {}
            for t in range(normal_phase, 1200, 5):
                arrivals.setdefault(t, []).append([t + 8, 500])
            for t in range(410 + extra_phase, 800, 5):
                arrivals.setdefault(t, []).append([t + 8, 950])
            pending = []
            for t in range(1250):
                pending.extend(arrivals.get(t, []))
                self.assertFalse(any(d <= t and w for d, w in pending))
                pending.sort()
                units = cfg["active_units"] if 400 <= t < 810 else cfg["base_units"]
                credit = m.quota(units, factor)
                for item in pending:
                    used = min(credit, item[1])
                    item[1] -= used
                    credit -= used
                pending = [item for item in pending if item[1]]
            self.assertEqual(pending, [])
            cases += 1
        self.assertEqual(cases, 75)

    def test_accepted_ledger_against_independent_dense_unit_budget(self):
        jobs = m.make_workload(PROTOCOL, 6, 1)
        row, outcomes, _ = m.run_model(PROTOCOL, 6, 1, 1, "borrowable", jobs)
        accepted = {o["uid"] for o in outcomes if o["status"] == "accepted"}
        for t in range(PROTOCOL["duration_ms"]):
            irrevocable = sum(j.units for j in jobs if j.uid in accepted and j.start <= t < j.deadline)
            self.assertLessEqual(row["configuration"]["required_peak_units"] + irrevocable, 106)

    def test_incomplete_original_work_has_zero_primary_credit_but_charged_service(self):
        row, outcomes, _ = m.run_model(PROTOCOL, 3, 1.2, 1, "no_video_contract")
        for kind in ["video", "best_effort"]:
            selected = [o for o in outcomes if o["kind"] == kind]
            self.assertTrue(any(not o["timely"] and o["served_bytes"] > 0 for o in selected))
            expected = sum(o["total_bytes"] for o in selected if o["timely"])
            self.assertEqual(row["class_metrics"][kind]["timely_bytes"], expected)
            self.assertGreater(row["class_metrics"][kind]["on_time_partial_service_bytes"], expected)
        expected = sum(o["total_bytes"] for o in outcomes if o["timely"])
        self.assertEqual(row["timely_payload_efficiency_nominal"], expected / (650 * PROTOCOL["duration_ms"]))
        self.assertGreater(row["utilization"], row["timely_payload_efficiency_delivered"])

    def test_semantically_modified_protocol_fails_closed(self):
        mutations = [("resources", "units", 107), ("resources", "nominal_bytes_per_ms", 700),
                     ("protection", "release_ms", 800), ("tenant", "offer_probability_per_ms", "load/1")]
        for section, field, value in mutations:
            changed = copy.deepcopy(PROTOCOL)
            changed[section][field] = value
            with self.assertRaises(ValueError):
                m.configuration(changed, 1, 1, "borrowable")
            with self.assertRaises(ValueError):
                m.make_workload(changed, 3, 1)
        changed = copy.deepcopy(PROTOCOL)
        changed["contracts"]["critical"]["deadline_ms"] = 9
        with self.assertRaises(ValueError):
            m.run_model(changed, 3, 1, 1, "borrowable")


if __name__ == "__main__":
    unittest.main()
