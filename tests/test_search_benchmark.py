"""Validate search workload selection, independent oracle, and reporting guards."""
import copy
import json
from pathlib import Path
import sys
import unittest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / 'tools'))
import search_benchmark as search


def fixture(k=3, backend='local'):
    data, data_hash = search.vectors(42, 20, 4)
    queries, query_hash = search.vectors(42 ^ search.QUERY_XOR, 5, 4)
    return {
        'schema_version': 2, 'feature': 'm5-search', 'phase': 'baseline',
        'comparison_group': 'm5-search-smoke', 'git_revision': 'fixture',
        'generator': 'splitmix64-high24-uniform-f32-v1', 'metric': 'squared_euclidean',
        'measurement_protocol': 'local-v2-individual-query-timers-rusage',
        'config': dict(rows=20, dimensions=4, k=k, queries=5, samples=2, seed=42, scenario='search'),
        'environment': dict(source_sha256='fixture'),
        'results': [dict(scenario='search', backend=backend,
                         warmup_queries=5, cache='warm in-memory query pass',
                         query_latency=dict(raw_sample_ns=[1] * 10),
                         timing=dict(raw_sample_ns=[5] * 2),
                         measured_store_calls=dict(get_calls=0, get_payload_bytes=0),
                         measured_http_requests=dict(get=0, list=0, put=0, delete=0),
                         inventory=dict(logical_objects=21),
                         dataset_seed=42, query_seed=42 ^ search.QUERY_XOR,
                         dataset_sha256=data_hash, query_sha256=query_hash,
                         exact_neighbor_ids=[search.oracle(data, q, k) for q in queries])],
    }


class SearchMatrix(unittest.TestCase):
    def test_generator_matches_archived_rust_fingerprint(self):
        path = REPO / 'benchmarks/runs/73aef1437e76edc44d9f2e0282c28d66da452bf9ad1fba9054f33a69c8d5d4c9.json'
        original = json.loads(path.read_text())
        _, fingerprint = search.vectors(42, 100, 32)
        self.assertEqual(fingerprint, original['results'][0]['dataset_sha256'])
        self.assertEqual(search.oracle([[1., 0.], [-1., 0.], [0., 0.]], [0., 0.], 3), [2, 0, 1])

    def test_cases_change_one_factor_and_keep_cargo_protocol(self):
        anchor = search.CASES[0]
        for case in search.CASES[1:]:
            self.assertEqual(sum(a != b for a, b in zip(anchor, case)), 1)
        command = search.command(anchor, 'local', '/test', 'fixture')
        self.assertEqual(command[:3], ['cargo', 'bench', '--locked'])
        self.assertEqual(command[command.index('--scenario') + 1], 'search')
        self.assertEqual(command[command.index('--seed') + 1], '42')
        self.assertIn('--features', search.command(anchor, 's3', '/test', 'fixture'))

    def test_validation_rejects_changed_inputs_oracle_and_storage_requests(self):
        good = fixture()
        self.assertEqual(search.validate(good, (20, 4, 3), 'local', True), [0, 1, 2])
        for key, value in [('dataset_sha256', 'wrong'), ('query_sha256', 'wrong'),
                           ('cache', 'cold'), ('warmup_queries', 0)]:
            bad = copy.deepcopy(good)
            bad['results'][0][key] = value
            with self.subTest(key=key), self.assertRaises(AssertionError):
                search.validate(bad, (20, 4, 3), 'local', True)
        bad = copy.deepcopy(good)
        bad['results'][0]['exact_neighbor_ids'][0].reverse()
        with self.assertRaisesRegex(AssertionError, 'oracle'):
            search.validate(bad, (20, 4, 3), 'local', True)
        bad = fixture(backend='s3')
        bad['results'][0]['measured_http_requests']['get'] = 1
        with self.assertRaises(AssertionError):
            search.validate(bad, (20, 4, 3), 's3', True)
        with self.assertRaisesRegex(AssertionError, 'credential'):
            search.validate(good, (20, 4, 3), 'local', True, credentials=['fixture'])

    def test_repeat_and_cross_k_answers_must_agree(self):
        a, b = fixture(), fixture(k=10)
        search.validate_related([a, b, copy.deepcopy(a)])
        b['results'][0]['exact_neighbor_ids'][4].reverse()
        with self.assertRaisesRegex(AssertionError, 'cross-k'):
            search.validate_related([a, b])


if __name__ == '__main__':
    unittest.main()
