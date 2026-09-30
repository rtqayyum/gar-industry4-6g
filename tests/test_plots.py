"""Semantic panel mappings and numerical bounds for experimental plots."""
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch

import matplotlib.pyplot as plt
from matplotlib.container import BarContainer
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("final_plots", ROOT / "analysis/plots.py")
PLOTS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PLOTS)
DATA = ROOT / "results/analysis"


class PlotTests(unittest.TestCase):
    def tearDown(self):
        plt.close("all")

    def draw(self, function):
        with patch.object(PLOTS, "finish") as finish:
            if function is PLOTS.packets:
                function(Path("unused"))
            else:
                function(DATA, Path("unused"))
            finish.assert_called_once()
            return finish.call_args.args[0]

    @staticmethod
    def bars(axis):
        return {item.get_label(): item for item in axis.containers if isinstance(item, BarContainer)}

    def test_nested_categories_are_four_vertical_count_bars(self):
        fig = self.draw(PLOTS.admission)
        bars = next(iter(self.bars(fig.axes[1]).values()))
        self.assertEqual([bar.get_height() for bar in bars], [743, 102, 1184, 19])
        self.assertEqual(sum(bar.get_height() for bar in bars), 2048)
        self.assertTrue(all(bar.get_y() == 0 for bar in bars))
        self.assertEqual(len(fig.axes[1].texts), 4)
        self.assertTrue(fig.axes[0].yaxis_inverted())

    def test_reserve_policy_membership_and_each_own_interval(self):
        evidence = json.loads((DATA / "reserve/plot_data.json").read_text())
        fig = self.draw(PLOTS.reserve)
        groups = self.bars(fig.axes[0])
        self.assertEqual(set(groups), set(PLOTS.LABELS))
        for policy, label in zip(PLOTS.POLICIES, PLOTS.LABELS):
            rows = {r["load"]: r for r in evidence["load_line"] if r["factor"] == 1 and r["policy"] == policy}
            bars = groups[label]
            segments = bars.errorbar.lines[2][0].get_segments()
            for load, bar, segment in zip([.6, .8, 1, 1.2, 1.4], bars, segments):
                if load not in rows:
                    self.assertTrue(np.isnan(bar.get_height()))
                    self.assertEqual(len(segment), 0)
                    continue
                metric = rows[load]["efficiency"]
                self.assertAlmostEqual(bar.get_height(), 100 * metric["mean"])
                np.testing.assert_allclose(segment[:, 1], [100 * metric["low"], 100 * metric["high"]])
                self.assertLessEqual(metric["low"], metric["mean"])
                self.assertLessEqual(metric["mean"], metric["high"])
        self.assertEqual(set(self.bars(fig.axes[1])), {"Borrowable", "No video contract"})
        self.assertEqual(set(self.bars(fig.axes[2])), {"Borrowable", "No tenants"})
        self.assertTrue(any("Not provisioned" in text.get_text() for text in fig.axes[1].texts))

    def test_video_denominator_epochs_and_all_paired_seeds(self):
        fig = self.draw(PLOTS.reserve)
        failure = self.bars(fig.axes[1])
        self.assertEqual([bar.get_height() for bar in failure["Borrowable"][:4]], [0, 0, 0, 0])
        self.assertTrue(np.isnan(failure["Borrowable"][-1].get_height()))
        self.assertAlmostEqual(failure["No video contract"][2].get_height(), 100 * 5819 / 15360)
        for bars in self.bars(fig.axes[2]).values():
            self.assertEqual([bar.get_height() for bar in bars], [10, 10, 10])
        deadline = next(line for line in fig.axes[2].lines if line.get_label() == "Video deadline")
        np.testing.assert_array_equal(deadline.get_ydata(), [15, 15])
        self.assertEqual(len(fig.axes[3].collections[0].get_offsets()), 128)

    def test_packet_percentages_counts_and_condition_markers(self):
        fig = self.draw(PLOTS.packets)
        bars = self.bars(fig.axes[0])
        self.assertEqual(set(bars), {"Late", "No receipt"})
        for i, total in enumerate([26, 34, 69, 40]):
            width = bars["Late"][i].get_width() + bars["No receipt"][i].get_width()
            self.assertAlmostEqual(width, total / 255212 * 100)
        self.assertEqual([t.get_text() for t in fig.axes[0].texts], ["26", "34", "69", "40"])
        self.assertTrue(fig.axes[0].yaxis_inverted())
        self.assertEqual(sum(len(c.get_offsets()) for c in fig.axes[1].collections), 96)
        self.assertEqual([t.get_text() for t in fig.axes[1].get_legend().get_texts()],
                         ["Controller present", "Controller-stop trial"])
        # Stop-trial diamonds are hollow, not fabricated extra observations.
        self.assertTrue(all(len(c.get_facecolors()) == 0 for c in fig.axes[1].collections[1::2]))

    def test_certificate_and_online_scope_remain_unchanged(self):
        fig = self.draw(PLOTS.certificate)
        self.assertEqual(fig.axes[1].get_xscale(), "log")
        self.assertEqual(fig.axes[1].get_xlim(), (1, 256))
        labels = fig.axes[2].get_legend_handles_labels()[1]
        self.assertEqual(labels, ["Old + request", "Committed"])
        fig = self.draw(PLOTS.online)
        expected = PLOTS.table(DATA / "online/policy_overview.dat")
        for axis, metric in zip(fig.axes, ["efficiency", "background_failure"]):
            bars = next(iter(self.bars(axis).values()))
            np.testing.assert_allclose([b.get_width() for b in bars], expected[metric])


if __name__ == "__main__":
    unittest.main()
