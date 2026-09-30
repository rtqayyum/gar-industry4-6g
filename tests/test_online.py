"""Online protocol, metrics, input-integrity and C++ admission checks."""
import hashlib
import importlib.util
import itertools
import json
from pathlib import Path
import random
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'experiments/online'
spec = importlib.util.spec_from_file_location('online_study', SOURCE / 'run.py')
study = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = study
spec.loader.exec_module(study)


class OnlineProtocolTests(unittest.TestCase):
    def test_context_integrity(self):
        manifest = json.loads((ROOT / 'data/online/contexts.json').read_text())
        self.assertEqual(len(manifest['files']), 24)
        for name, expected in manifest['files'].items():
            path = ROOT / 'data/online' / name
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), expected['sha256'])

    def test_fixed_job_counts_and_source(self):
        p = json.loads(study.PROTOCOL_PATH.read_text())
        policies, families, seeds = len(study.POLICIES), len(p['families']), len(p['seeds'])
        grid = len(list(itertools.product(*(p['tuning_grid'][k]
                    for k in ('step_ms', 'term_ms', 'gate_eta')))))
        self.assertEqual(policies * families * seeds * len(p['contexts']['tuning_suffixes']) * grid, 144)
        self.assertEqual(policies * families * seeds * len(p['contexts']['evaluation_suffixes'])
                         * len(p['traffic_blocks']), 864)
        factors = p['factorial_diagnostic']['factors']
        self.assertEqual(families * seeds * len(p['contexts']['evaluation_suffixes'])
                         * len(list(itertools.product(*factors.values()))), 288)
        self.assertEqual(len(p['timing_jobs']), 32)
        self.assertEqual(study.sha256(study.SIMULATOR), p['simulator_sha256'])

    def test_static_subset_oracle(self):
        self.assertTrue(study.oracle_fixture()['pass'])

    def test_context_level_statistics(self):
        self.assertEqual(study.exact_sign_flip_pvalue([1, 1, 1]), 0.25)
        self.assertEqual(study.exact_sign_flip_pvalue([0, 0, 0]), 1.0)
        self.assertEqual(study.holm_adjust({'a': 0.01, 'b': 0.04, 'c': 0.03}),
                         {'a': 0.03, 'b': 0.06, 'c': 0.06})
        self.assertEqual(study.stratified_paired_bootstrap_interval([('a', 2), ('b', 2)], 41), (2.0, 2.0))

    def test_output_refuses_existing_evidence(self):
        with tempfile.TemporaryDirectory(prefix='online_output_') as directory:
            path = Path(directory)
            (path / 'evidence.json').write_text('{}')
            with self.assertRaises(SystemExit):
                study.prepare_output(path)

    def test_main_summary_keeps_all_policies_and_tradeoffs(self):
        rows = study.read_csv(ROOT / 'results/online/policy_summary.csv')
        by_id = {row['policy_id']: row for row in rows}
        self.assertEqual(set(by_id), set(study.POLICIES))
        self.assertTrue(all(int(row['context_clusters_n']) == 12 for row in rows))
        self.assertAlmostEqual(float(by_id['gar_h']['timely_payload_efficiency_pct']), 42.2624, places=3)
        self.assertAlmostEqual(float(by_id['yu_gateway_adapter']['timely_payload_efficiency_pct']), 46.1581, places=3)
        self.assertLess(float(by_id['gar_h']['background_failure_pct']),
                        float(by_id['yu_gateway_adapter']['background_failure_pct']))


class OnlineAdmissionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix='online_cpp_tests_')
        cls.output = Path(cls.temp.name)
        cls.context = ROOT / 'data/online/deadline_mix_3.csv'
        for name in ('batch_driver', 'selector_driver'):
            subprocess.run(['c++', '-std=c++17', '-O2', '-I', str(SOURCE),
                str(SOURCE / f'{name}.cpp'), '-o', str(cls.output / name)],
                check=True, text=True, capture_output=True)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def test_batch_headroom_and_atomic_commit(self):
        result = subprocess.run([str(self.output / 'batch_driver'), str(self.context)],
                                check=True, text=True, capture_output=True)
        self.assertIn('PASS aggregate fixtures', result.stdout)

    def test_subset_ordering_against_exhaustive_oracle(self):
        cases = [([5, 9], [21, 21, -999], 2),
                 ([10, 5, 5], [21, -999, -999, -999, -999, 21, -999], 6),
                 ([7, 7], [30, 21, -999], 2), ([7, 7], [21, 21, -999], 1),
                 ([7, 7], [-1, -1, -999], 1), ([7, 7], [-999, -999, -999], 0),
                 ([], [], 0)]
        rng = random.Random(130927)
        for _ in range(2000):
            n = rng.randrange(1, 7)
            priorities = [rng.randrange(1, 30) for _ in range(n)]
            targets = [rng.choice([-999, -1, 21, 22, 30]) for _ in range((1 << n) - 1)]
            eligible = [mask for mask in range(1, 1 << n) if targets[mask - 1] != -999]
            expected = max(eligible, key=lambda mask: (
                sum(priorities[i] for i in range(n) if mask & (1 << i)),
                mask.bit_count(), -targets[mask - 1], -mask)) if eligible else 0
            cases.append((priorities, targets, expected))
        data = '\n'.join(' '.join(map(str, [len(p)] + p + t)) for p, t, _ in cases) + '\n'
        result = subprocess.run([str(self.output / 'selector_driver'), str(self.context)],
            input=data, check=True, text=True, capture_output=True)
        actual = [int(line.split()[0]) for line in result.stdout.splitlines()]
        self.assertEqual(actual, [expected for _, _, expected in cases])


if __name__ == '__main__':
    unittest.main()
