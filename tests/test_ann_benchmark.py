import copy
import hashlib
import json
from pathlib import Path
import struct
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
import ann_benchmark as ann
import benchmarks
from test_search_benchmark import fixture
from test_benchmarks import report


class AnnComparison(unittest.TestCase):
    def test_filtered_quality_validation_uses_original_ids_and_rejects_bad_counts(self):
        document = fixture()
        config, result = document['config'], document['results'][0]
        config.update(filter_every=2, ivf_partitions=4, ivf_probes=4, ivf_iterations=8)
        data, _ = ann.inputs(42, 20, 4, '')
        queries, _ = ann.inputs(42, 5, 4, '', True)
        exact = [[i for i in ann.oracle(data, q, 20) if i % 2 == 0][:3] for q in queries]
        result['exact_neighbor_ids'] = exact
        result['filter'] = {
            'predicate': {'selected': 'true'}, 'eligible_documents': 10,
            'selected_ids_sha256': hashlib.sha256(
                b''.join(struct.pack('<Q', i) for i in range(0, 20, 2))).hexdigest(),
        }
        result['ann'] = dict(algorithm='ivf-flat-v1', build_ns=1,
                             returned_neighbor_ids=copy.deepcopy(exact),
                             recall_at_k=[1.] * 5,
                             distance_evaluations=[dict(centroid=4, vector=10) for _ in range(5)])
        ann.validate(document)
        for change in ('fingerprint', 'oracle', 'answer', 'count'):
            bad = copy.deepcopy(document)
            if change == 'fingerprint': bad['results'][0]['filter']['selected_ids_sha256'] = 'wrong'
            if change == 'oracle': bad['results'][0]['exact_neighbor_ids'][0].reverse()
            if change == 'answer': bad['results'][0]['ann']['returned_neighbor_ids'][0] = [1]
            if change == 'count': bad['results'][0]['ann']['distance_evaluations'][0]['vector'] = 20
            with self.subTest(change=change), self.assertRaises(AssertionError):
                ann.validate(bad)

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
