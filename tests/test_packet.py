"""Packet-data identity, accounting, and safe extraction checks."""
from collections import Counter
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("packet_records", ROOT / "experiments/packet/records.py")
records = importlib.util.module_from_spec(spec)
spec.loader.exec_module(records)


class PacketTests(unittest.TestCase):
    def test_record_cohorts(self):
        value = records.manifest()
        self.assertEqual(Counter(r["group"] for r in value["records"]), {"historical": 40, "controlled": 1465})
        self.assertEqual(len({r["blob"] for r in value["records"]}), 1358)

    def test_unsafe_relative_paths(self):
        for name in ("../x", "/x", "x/../y", "x//y", "./x", "x\\y", ""):
            with self.subTest(name=name), self.assertRaises(ValueError):
                records.safe_path(Path("/tmp/packet-test"), name)

    def test_existing_output_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp, self.assertRaises(FileExistsError):
            records.new_directory(Path(tmp).resolve())

    def test_symlink_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "link").symlink_to(base, target_is_directory=True)
            with self.assertRaises(ValueError):
                records.safe_path(base, "link/new")

    def test_historical_failures_are_retained(self):
        attempts = records.csv_rows(ROOT / "results/packet/historical/attempt_registry.csv")
        self.assertEqual(len(attempts), 66)
        self.assertEqual(sum(r["status"] != "COMPLETE" for r in attempts), 2)
        outcomes = records.load(ROOT / "results/packet/historical/deadline_outcomes.json")["outcomes"]
        gar = {r["traffic_group"]: r for r in outcomes if r["policy"] == "gar_h"}
        self.assertEqual(gar["critical"]["failures"], 69)
        self.assertEqual(gar["video"]["failures"], 69025)

    def test_controlled_accounting(self):
        facts = records.load(ROOT / "results/packet/controlled/curated_facts.json")
        self.assertEqual((facts["targeted_trials"], facts["service_probes"]), (48, 12))
        self.assertEqual((facts["offered_jobs"], facts["accepted_jobs"], facts["timely_jobs"]), (144, 96, 96))
        self.assertEqual(facts["critical_misses"], 1)

    def test_published_numeric_reference_hashes(self):
        expected = records.load(ROOT / "data/packet/expected_outputs.json")
        self.assertEqual(len(expected["controlled_analysis"]), 21)
        self.assertEqual(len(expected["controlled_summaries"]), 7)
        for group in ("controlled_analysis", "controlled_summaries"):
            for name, digest in expected[group].items():
                path = ROOT / "results/packet/controlled" / name
                if path.exists():
                    self.assertEqual(records.sha(path), digest, name)

    def test_reservation_provisioning(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "provisioning.csv"
            records.reservation_provisioning(ROOT / "results/packet/controlled", target)
            self.assertEqual(records.sha(target), "ea63e879fe4aca08afc559166627b95f09b30b0614db18604c2ad70c42f870dd")


if __name__ == "__main__":
    unittest.main()
