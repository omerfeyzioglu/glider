import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from soak_summary import summarize, MIB


def point(seconds, rss):
    return {'elapsed_seconds': seconds, 'current_rss_bytes': None if rss is None else rss * MIB,
            'peak_rss_bytes': 300 * MIB, 'engine_metrics': None, 'probe_overloaded': True,
            'commands': 8}


def report(points):
    return {'progress': points, 'serve': {'elapsed_seconds': 1800},
            'accepted': False, 'gates': {'memory': False, 'durability': True}}


class SoakSummaryTests(unittest.TestCase):
    def test_trend_uses_current_rss_minutes_and_ignores_early_transient(self):
        result = summarize(report([point(1, 300), point(600, 100),
                                   point(1200, 150), point(1799, 100 + 5 * (1799 / 60 - 10))]))
        self.assertAlmostEqual(result['late_current_rss_ols_mib_per_minute'], 5)
        self.assertEqual(result['failed_historical_gates'], ['memory'])

    def test_missing_observations_are_unknown_not_zero(self):
        result = summarize(report([point(601, None), point(1201, 150)]))
        self.assertIsNone(result['late_current_rss_ols_mib_per_minute'])
        self.assertIsNone(result['five_minute_windows'][0]['current_rss_median_mib'])
        window = result['five_minute_windows'][2]
        self.assertEqual(window['samples'], 1)
        self.assertEqual(window['rss_samples'], 0)
        self.assertIsNone(window['tail_objects_max'])
        self.assertEqual(window['probe_overloaded'], 1)

    def test_window_boundary_and_nonincreasing_current_rss(self):
        result = summarize(report([point(299, 250), point(300, 200),
                                   point(600, 180), point(1200, 180)]))
        self.assertEqual(result['five_minute_windows'][0]['current_rss_median_mib'], 250)
        self.assertEqual(result['five_minute_windows'][1]['current_rss_median_mib'], 200)
        self.assertEqual(result['late_current_rss_ols_mib_per_minute'], 0)


if __name__ == '__main__':
    unittest.main()
