import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from tenants_benchmark import assemble_report, percentiles, recall_at_k, tenant_rows  # noqa: E402


class TenantHelperTests(unittest.TestCase):
    def test_nearest_rank_percentiles(self):
        self.assertEqual(percentiles([]), {"p50": None, "p95": None, "p99": None})
        self.assertEqual(percentiles([4, 1, 3, 2]), {"p50": 2, "p95": 4, "p99": 4})
        self.assertEqual(percentiles(list(range(1, 101)), (5, 50, 95, 99)),
                         {"p5": 5, "p50": 50, "p95": 95, "p99": 99})

    def test_recall_uses_exact_top_k(self):
        self.assertEqual(recall_at_k([5, 2, 3], [1, 2, 3], 3), 2 / 3)
        self.assertEqual(recall_at_k([1, 2, 3], [1, 2, 3], 2), 1.0)
        self.assertEqual(recall_at_k([], [], 10), 1.0)

    def test_tenant_slicing_and_partial_batch(self):
        self.assertEqual(tenant_rows(2, 5, 2),
                         [[(0, 10), (1, 11)], [(2, 12), (3, 13)], [(4, 14)]])

    def test_report_correctness_gate(self):
        config = {"tenants": 2}
        good = assemble_report(config, {}, {"verified_tenants": 2, "mismatch_count": 0}, {})
        self.assertTrue(good["passed"])
        self.assertEqual(good["measurement_protocol"], "tenants-v1-http-client-wall-clock")
        for verification in ({"verified_tenants": 1, "mismatch_count": 0},
                             {"verified_tenants": 1, "mismatch_count": 1}):
            self.assertFalse(assemble_report(config, {}, verification, {})["passed"])


if __name__ == "__main__":
    unittest.main()
