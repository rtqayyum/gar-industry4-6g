"""Safe output destinations and a single-description, code-only layout."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("repository", ROOT / "run.py")
REPO = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPO)


class RepositoryTests(unittest.TestCase):
    def test_output_must_not_replace_evidence(self):
        for target in (ROOT, ROOT.parent, ROOT / "results/new", ROOT / "experiments/new"):
            with self.subTest(target=target), self.assertRaises(ValueError):
                REPO.new_output(target)

    def test_new_run_directory_is_allowed(self):
        target = ROOT / "runs/unit-test-output"
        self.assertEqual(REPO.new_output(target), target)

    def test_existing_destination_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError):
                REPO.new_output(Path(directory))

    def test_no_publication_files_or_old_revision_directories(self):
        prohibited = {".tex", ".tikz", ".bib", ".pdf", ".docx", ".sty", ".cls", ".bst"}
        for path in REPO.files():
            self.assertNotIn(path.suffix.lower(), prohibited, str(path))
            self.assertFalse(any(part.startswith(("revision_", "latex_overleaf")) for part in path.parts))

    def test_one_description(self):
        descriptions = [p.relative_to(ROOT).as_posix() for p in REPO.files() if p.suffix == ".md"]
        self.assertEqual(descriptions, ["README.md"])


if __name__ == "__main__":
    unittest.main()
