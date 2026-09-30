"""Statistical-design and reporting regressions for final result analysis."""
import importlib.util
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]


def module(name):
    spec = importlib.util.spec_from_file_location("analysis_" + name, ROOT / "analysis" / (name + ".py"))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


class AnalysisTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.online = module("online")
        cls.matched = module("matched")
        cls.rows = cls.online.read_csv(ROOT / "results/online/main_rows.csv")

    def test_equal_context_policy_means(self):
        rows = self.online.rows_from_runs(self.rows)
        reference = (ROOT / "results/analysis/online/policy_overview.dat").read_text()
        self.assertEqual(self.online.table_text(rows), reference)

    def test_duplicate_observation_rejected(self):
        rows = list(self.rows)
        rows[1] = dict(rows[0])
        with self.assertRaises(ValueError):
            self.online.rows_from_runs(rows)

    def test_failed_observation_not_silently_excluded(self):
        rows = [dict(row) for row in self.rows]
        rows[0]["simulator_status"] = "FAIL"
        with self.assertRaises(ValueError):
            self.online.rows_from_runs(rows)

    def test_invalid_metric_rejected(self):
        rows = [dict(row) for row in self.rows]
        rows[0]["background_failure_pct"] = "nan"
        with self.assertRaises(ValueError):
            self.online.rows_from_runs(rows)

    def test_lexical_example_and_independent_certificate(self):
        cases = self.matched.load_records(ROOT / "results/matched/workloads.jsonl.gz")
        rows = self.matched.load_records(ROOT / "results/matched/outcomes.jsonl.gz")
        selected = self.matched.select_example(rows)
        case = next(item for item in cases if item["id"] == selected["id"])
        protocol = json.loads((ROOT / "experiments/matched/protocol.json").read_text())
        example, _ = self.matched.example_analysis(case, selected, protocol)
        self.assertEqual(example["new_min_slack_bytes"], 128)
        self.assertEqual(example["new_min_resource_margin_units"], 1)
        self.assertEqual(example["retained_plus_request_min_margin_units"], -3)
        self.assertTrue(example["new_tail"]["pass"])


if __name__ == "__main__":
    unittest.main()
