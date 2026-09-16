import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
import ann_benchmark as ann
import benchmarks
from test_search_benchmark import fixture
from test_benchmarks import report


class AnnComparison(unittest.TestCase):
    def test_quality_validation_rejects_wrong_recall_full_probe_and_io(self):
        d = fixture()
        d['config'].update(ivf_partitions=4, ivf_probes=4, ivf_iterations=8)
        r = d['results'][0]
        r['ann'] = dict(algorithm='ivf-flat-v1', build_ns=1,
                        returned_neighbor_ids=copy.deepcopy(r['exact_neighbor_ids']),
                        recall_at_k=[1.] * 5,
                        distance_evaluations=[dict(centroid=4, vector=20) for _ in range(5)])
        ann.validate(d)
        for change in ('recall', 'answer', 'count', 'io'):
            bad = copy.deepcopy(d)
            a = bad['results'][0]['ann']
            if change == 'recall': a['recall_at_k'][0] = 0.5
            if change == 'answer': a['returned_neighbor_ids'][0].reverse()
            if change == 'count': a['distance_evaluations'][0]['vector'] = 19
            if change == 'io': bad['results'][0]['measured_store_calls']['get_calls'] = 1
            with self.subTest(change=change), self.assertRaises(AssertionError):
                ann.validate(bad)

    def test_clustered_inputs_have_independent_queries_and_reproducible_fingerprints(self):
        data, fingerprint = ann.inputs(42, 20, 4, 'clustered')
        self.assertEqual((data, fingerprint), ann.inputs(42, 20, 4, 'clustered'))
        queries, query_hash = ann.inputs(42, 20, 4, 'clustered', True)
        self.assertNotEqual(data, queries)
        self.assertNotEqual(fingerprint, query_hash)

    def test_regression_comparison_reports_quality_and_does_not_gate_build_time(self):
        d = report()
        d['config'].update(ivf_partitions=4, ivf_probes=1, ivf_iterations=8)
        d['results'][0]['ann'] = dict(build_ns=10, recall_at_k=[0.5, 1.],
                                     distance_evaluations=[dict(vector=3, centroid=4)] * 2)
        with tempfile.TemporaryDirectory() as folder:
            a, b = Path(folder) / 'a.json', Path(folder) / 'b.json'
            a.write_text(json.dumps(d))
            d['results'][0]['ann']['build_ns'] = 100
            b.write_text(json.dumps(d))
            comparison = benchmarks.compare_files(a, b, True)
            self.assertFalse(comparison['counter_check_failed'])
            d['results'][0]['ann']['recall_at_k'][0] = 0.25
            b.write_text(json.dumps(d))
            self.assertTrue(benchmarks.compare_files(a, b, True)['counter_check_failed'])


if __name__ == '__main__':
    unittest.main()
