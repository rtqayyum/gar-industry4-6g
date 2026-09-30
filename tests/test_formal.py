"""Regression checks for independent finite-calendar and payload certificates."""
import importlib.util
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


def module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "experiments/formal" / f"{name}.py")
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


class FormalTests(unittest.TestCase):
    def test_calendar_exhaustive_reference(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "calendar"
            with redirect_stdout(io.StringIO()):
                module("verify_calendar").main(output)
            receipt = json.loads((output / "verification_receipt.json").read_text())
            self.assertEqual(receipt["profiles_enumerated"], 14337)
            self.assertEqual(receipt["interval_dominance_checks"], 613974)
            self.assertEqual(receipt["maximal_vs_exhaustive_feasibility_disagreements"], 0)
            self.assertEqual((output / "maximal_calendar_rows.json").read_bytes(),
                             (ROOT / "results/formal/calendar/maximal_calendar_rows.json").read_bytes())

    def test_payload_exhaustive_reference(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "payload"
            result = module("verify_payload").verify(output)
            self.assertEqual(result["exhaustive_cases"], 59049)
            self.assertEqual(result["earlier_release_checks"], 7909)
            self.assertEqual(result["forward_backward_disagreements"], 0)
            for name in ("release_cases.csv", "release_timelines.csv"):
                self.assertEqual((output / name).read_bytes(),
                                 (ROOT / "results/formal/payload" / name).read_bytes())

    def test_payload_input_contract(self):
        verifier = module("verify_payload")
        for arrivals, service in (((), ()), ((1,), (1, 1)), ((-1,), (1,))):
            with self.assertRaises(ValueError):
                verifier.suffix_certificate(arrivals, service)
        with self.assertRaises(ValueError):
            verifier.lower_availability_certificate(2, (0, 1), (2,))

    def test_outputs_are_create_only(self):
        with tempfile.TemporaryDirectory() as temporary:
            for name in ("verify_calendar", "verify_payload"):
                with self.assertRaises(FileExistsError):
                    verifier = module(name)
                    (verifier.main if name == "verify_calendar" else verifier.verify)(Path(temporary))


if __name__ == "__main__":
    unittest.main()
