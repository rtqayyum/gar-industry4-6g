"""Numerical reporting checks for the recorded packet experiments."""
import importlib.util
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("packet_precision", ROOT / "experiments/packet/precision.py")
precision = importlib.util.module_from_spec(spec)
spec.loader.exec_module(precision)


class PacketPrecisionTests(unittest.TestCase):
    def test_all_reported_precision_values(self):
        reference = json.loads((ROOT / "results/packet/precision/precision.json").read_text())
        self.assertEqual(precision.calculate(), reference)

    def test_recorded_application_accounting(self):
        result = precision.command_audit(ROOT)["policies"]
        self.assertEqual(sum(r["accepted"] for r in result.values()), 4603)
        self.assertEqual(sum(r["matched"] for r in result.values()), 4595)
        self.assertEqual(sum(r["unmatched"] for r in result.values()), 8)

    def test_linear_quantile(self):
        self.assertEqual(precision.quantile_linear([30, 0, 10, 20], .5), 15)
        self.assertEqual(precision.quantile_linear([1, 5], 0), 1)
        self.assertEqual(precision.quantile_linear([1, 5], 1), 5)
        for values, probability in (([], .5), ([1], -1), ([1], 1.1)):
            with self.assertRaises(ValueError):
                precision.quantile_linear(values, probability)

    def test_pooled_weights_are_not_equal_context_weights(self):
        # The pooled estimator weights each context by its capacity denominator.
        self.assertEqual(precision.pooled_contrast([100, 0], [0, 0], [100, 900]), 10)
        for focal, comparator, denominator in (([], [], []), ([1], [], [1]), ([1], [0], [0])):
            with self.assertRaises(ValueError):
                precision.pooled_contrast(focal, comparator, denominator)

    def test_missing_observations_use_full_allowed_range(self):
        observed = {("family", 1, "gar_h"): 60., ("family", 1, "predictive_h"): 40.,
                    ("family", 2, "predictive_h"): 50.}
        result = precision.missing_bounds(observed, [("family", 1), ("family", 2)], "predictive_h")
        self.assertEqual(result["pairwise_observed_contexts"], 1)
        self.assertEqual(result["pairwise_complete_case_mean_gar_minus_comparator_pp"], 20)
        self.assertEqual(result["all_intended_contexts_lower_bound_pp"], -15)
        self.assertEqual(result["all_intended_contexts_upper_bound_pp"], 35)

    def test_compressed_window_identity(self):
        expected = json.loads((ROOT / "data/packet/expected_outputs.json").read_text())
        self.assertEqual(precision.decompressed_sha(ROOT / precision.WINDOWS),
                         expected["controlled_analysis"]["service_probe_windows.csv"])


if __name__ == "__main__":
    unittest.main()
