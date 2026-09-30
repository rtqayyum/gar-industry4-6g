"""Regression tests for the matched guard-value study, not host validation."""
from copy import deepcopy
import importlib.util
from pathlib import Path
import tempfile
import unittest

MODULE = Path(__file__).resolve().parents[1] / "experiments/matched/run.py"
spec = importlib.util.spec_from_file_location("guard_value", MODULE)
study = importlib.util.module_from_spec(spec)
spec.loader.exec_module(study)


class MatchedGuardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory(prefix="matched-guard-tests-")
        cls.adapter = study.Adapter(cls.tmp.name)

    @classmethod
    def tearDownClass(cls):
        cls.adapter.close()
        cls.tmp.cleanup()

    def test_fixed_protocol_and_effective_deadlines(self):
        p = study.PROTOCOL
        study.validate_protocol()
        self.assertEqual([x - p["critical_headroom_ms"] - p["critical_guardband_ms"] for x in p["critical_original_deadlines"]], p["critical_certificate_deadlines"])
        self.assertEqual(p["units"], 106)
        self.assertEqual(p["capacity_bytes_per_ms"], 650)
        for group in ("video", "monitor"):
            x = p[group]
            self.assertLessEqual(x["work_bytes"], study.rate(x["units"]) * min(x["period_ms"], x["deadline_ms"]))
        old = deepcopy(p["video"])
        p["video"]["units"] = 1
        try:
            with self.assertRaisesRegex(ValueError, "unsupported"):
                study.validate_protocol()
        finally:
            p["video"] = old

    def test_generator_is_reproducible_and_future_envelopes_hold(self):
        for stratum in study.PROTOCOL["strata"]:
            for rep in range(4):
                case = study.generate(7, stratum, rep)
                self.assertEqual(case, study.generate(7, stratum, rep))
                self.assertTrue(study.validate_workload(case))

    def test_independent_envelope_audit_detects_invalid_trace(self):
        case = study.generate(3, "noncrossing", 0)
        case["original_jobs"].append({"group": "critical", "stream": 0, "release": 1, "work": 99999})
        with self.assertRaisesRegex(AssertionError, "envelope"):
            study.validate_workload(case)

    def test_exact_full_payload_and_staged_suffix(self):
        for stratum in study.PROTOCOL["strata"]:
            for rep in range(4):
                case = study.generate(5, stratum, rep)
                req = case["request"]
                self.assertEqual(req["work"], study.rate(req["units"]) * req["length"])
                self.assertEqual(req["work"], sum(x[1] for x in req["releases"]))
                remaining = req["work"]
                for when, amount in req["releases"]:
                    self.assertEqual(remaining, study.rate(req["units"]) * (req["length"] - when))
                    remaining -= amount

    def test_late_full_payload_fails_all_policies(self):
        case = study.generate(1, "noncrossing", 0)
        req = case["request"]
        req["releases"] = [[req["length"] - 1, req["work"]]]
        answer = self.adapter.call(case)
        for decision in answer["decisions"]:
            self.assertFalse(decision["accepted"])
            self.assertFalse(decision["release_pass"])

    def test_old_plan_validation_is_not_acceptance_selected(self):
        case = study.generate(1, "noncrossing", 0)
        case["pending"] = [{"certificate_deadline": 1, "original_deadline": 6, "work": 999999, "stream": 0}]
        answer = self.adapter.call(case)
        self.assertFalse(answer["eligible"])
        self.assertFalse(answer["old_certificate"])
        self.assertFalse(answer["old_oracle"])
        self.assertTrue(all(not x["accepted"] for x in answer["decisions"]))

    def test_boundary_strata_and_existing_booking_crossings(self):
        for stratum in study.PROTOCOL["strata"]:
            case = study.generate(6, stratum, 2)
            lead = case["lead"]
            end = lead + case["request"]["length"]
            self.assertGreater(case["older_booking"]["end"], end)
            if stratum == "fallback_in_prefix":
                self.assertLess(case["old_final"], lead)
            elif stratum == "fallback_in_window":
                self.assertLess(lead, case["old_final"])
                self.assertLess(case["old_final"], end)
            elif stratum == "booking_boundary":
                self.assertLess(lead, case["older_booking"]["start"])
                self.assertLess(case["older_booking"]["start"], end)
            else:
                self.assertEqual(set(case["old_calendar"]), {49})

    def test_compiled_guard_dense_oracle_nested_controls_and_queue(self):
        for stratum in study.PROTOCOL["strata"]:
            for rep in range(4):
                case = study.generate(8, stratum, rep)
                answer = self.adapter.call(case)
                self.assertEqual(answer["old_certificate"], answer["old_oracle"])
                if not answer["eligible"]:
                    continue
                peak, keep, guard = answer["decisions"]
                self.assertLessEqual(peak["accepted"], keep["accepted"])
                self.assertLessEqual(keep["accepted"], guard["accepted"])
                for decision in answer["decisions"]:
                    self.assertTrue(decision["calendar_oracle"])
                    self.assertTrue(decision["resources"])
                    execution = study.execute(case, decision)
                    self.assertEqual(execution["capacity_violations"], 0)
                    self.assertEqual(execution["incumbent_failures"], 0)
                    self.assertEqual(execution["groups"]["old_tenant"]["misses"], 0)
                    self.assertEqual(execution["groups"]["new_tenant"]["misses"], 0)
                    self.assertEqual(execution["groups"]["new_tenant"]["timely_bytes"], case["request"]["work"] * decision["accepted"])

    def test_rejected_work_keeps_original_offered_denominator(self):
        case = study.generate(8, "booking_boundary", 3)
        decision = {"accepted": False, "replaced": False}
        execution = study.execute(case, decision)
        self.assertEqual(execution["groups"]["new_tenant"]["offered_bytes"], case["request"]["work"])
        self.assertEqual(execution["groups"]["new_tenant"]["timely_bytes"], 0)
        self.assertEqual(execution["groups"]["new_tenant"]["completed_job"], 0)

    def test_full_original_item_credit_not_partial_utilization(self):
        case = study.generate(3, "noncrossing", 0)
        case["original_jobs"] = [{"id": 0, "group": "video", "release": 0, "deadline": 1, "work": 1000}]
        result = study.execute(case, {"accepted": False, "replaced": False})
        self.assertGreater(result["served_bytes"], 0)
        self.assertEqual(result["groups"]["video"]["timely_bytes"], 0)
        self.assertEqual(result["groups"]["video"]["misses"], 1)
        faulty = study.generate(4, "booking_boundary", 0)
        faulty["request"]["units"] = 120  # Deliberately impossible accepted decision.
        result = study.execute(faulty, {"accepted": True, "replaced": False})
        self.assertGreater(result["capacity_violations"], 0)
        self.assertLessEqual(result["served_bytes"], 650 * study.PROTOCOL["execution_slots"])


if __name__ == "__main__":
    unittest.main()
