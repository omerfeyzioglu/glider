#!/usr/bin/env python3
"""Describe recorded soak windows; no new pass/fail thresholds or extrapolation."""
import argparse
import json
import math
from pathlib import Path
import statistics

MIB = 1024 * 1024


def summarize(report):
    trace = report['progress']
    end = report['serve']['elapsed_seconds']
    windows = []
    for start in range(0, math.ceil(end), 300):
        points = [p for p in trace if start <= p['elapsed_seconds'] < start + 300]
        rss = [p['current_rss_bytes'] / MIB for p in points if p['current_rss_bytes'] is not None]
        metrics = [dict(p['engine_metrics']) for p in points if p['engine_metrics'] is not None]
        windows.append({
            'start_seconds': start, 'end_seconds': min(start + 300, end),
            'samples': len(points), 'rss_samples': len(rss),
            'current_rss_median_mib': statistics.median(rss) if rss else None,
            'current_rss_min_mib': min(rss) if rss else None,
            'current_rss_max_mib': max(rss) if rss else None,
            'peak_rss_max_mib': max((p['peak_rss_bytes'] / MIB for p in points), default=None),
            'tail_objects_max': max((m['probe_tail_objects'] for m in metrics), default=None),
            'admission_mib_max': max((m['glider_index_admission_bytes'] / MIB for m in metrics), default=None),
            'last_seal_steps': metrics[-1]['glider_segmented_seal_steps_total'] if metrics else None,
            'probe_overloaded': sum(p['probe_overloaded'] for p in points),
            'queue_commands_max': max((p['commands'] for p in points), default=None),
        })
    late = [(p['elapsed_seconds'] / 60, p['current_rss_bytes'] / MIB)
            for p in trace if p['elapsed_seconds'] >= 600 and p['current_rss_bytes'] is not None]
    slope = None
    if len(late) >= 2:
        xbar = statistics.mean(x for x, _ in late)
        ybar = statistics.mean(y for _, y in late)
        divisor = sum((x - xbar) ** 2 for x, _ in late)
        if divisor:
            slope = sum((x - xbar) * (y - ybar) for x, y in late) / divisor
    return {'five_minute_windows': windows, 'late_rss_samples': len(late),
            'late_current_rss_ols_mib_per_minute': slope,
            'trend_definition': 'OLS of current RSS versus elapsed minutes, samples at/after minute 10; descriptive, includes harness retention.',
            'acceptance': report['accepted'],
            'failed_historical_gates': [name for name, passed in report['gates'].items() if not passed]}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report', type=Path)
    args = parser.parse_args()
    print(json.dumps(summarize(json.loads(args.report.read_text())), indent=2))
