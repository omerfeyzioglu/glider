#!/usr/bin/env python3
"""Short exact/IVF trade-off comparison; ordinary historical benchmarks are unchanged."""
import argparse
import hashlib
import json
from pathlib import Path
import struct
import subprocess

from search_benchmark import vectors, oracle


def inputs(seed, rows, dimensions, distribution, queries=False):
    points, digest = vectors(seed ^ 0xd1b54a32d192ed03 if queries else seed, rows, dimensions)
    if distribution == 'clustered':
        centers, _ = vectors(seed ^ 0xa0761d6478bd642f, 16, dimensions)
        f32 = lambda x: struct.unpack('<f', struct.pack('<f', x))[0]
        points = [[f32(c + f32(v * f32(0.1))) for c, v in zip(centers[i % 16], point)]
                  for i, point in enumerate(points)]
        digest = hashlib.sha256(b''.join(struct.pack('<f', x) for p in points for x in p)).hexdigest()
    return points, digest


def validate(report):
    c, r = report['config'], report['results'][0]
    assert r['scenario'] == 'search' and r['backend'] in ('local', 's3')
    distribution = c.get('distribution', '')
    expected_generator = ('splitmix64-16-centers-plus-uniform-noise-0.1-f32-v1'
                          if distribution else 'splitmix64-high24-uniform-f32-v1')
    assert report['generator'] == expected_generator
    assert report['metric'] == 'squared_euclidean'
    assert report['environment']['source_sha256'] and report['git_revision']
    data, data_hash = inputs(c['seed'], c['rows'], c['dimensions'], distribution)
    queries, query_hash = inputs(c['seed'], c['queries'], c['dimensions'], distribution, True)
    assert r['dataset_sha256'] == data_hash and r['query_sha256'] == query_hash
    exact = [oracle(data, q, c['k']) for q in queries]
    assert r['exact_neighbor_ids'] == exact, f"seed={c['seed']}: exact oracle differs"
    assert all(v == 0 for v in r['measured_store_calls'].values())
    assert all(v == 0 for v in r.get('measured_http_requests', {}).values())
    assert len(r['query_latency']['raw_sample_ns']) == c['queries'] * c['samples']
    if c.get('ivf_partitions', 0):
        ann = r['ann']
        assert ann['algorithm'] == 'ivf-flat-v1' and ann['build_ns'] >= 0
        assert len(ann['returned_neighbor_ids']) == len(exact)
        assert len(ann['recall_at_k']) == len(exact) == len(ann['distance_evaluations'])
        for expected, ids, recall, counts in zip(exact, ann['returned_neighbor_ids'], ann['recall_at_k'], ann['distance_evaluations']):
            assert len(ids) <= min(c['k'], c['rows']) and len(set(ids)) == len(ids)
            assert all(isinstance(i, int) and 0 <= i < c['rows'] for i in ids)
            assert recall == len(set(ids) & set(expected)) / len(expected)
            assert counts['centroid'] == min(c['ivf_partitions'], c['rows'])
            assert len(ids) <= counts['vector'] <= c['rows']
            if c['ivf_probes'] >= counts['centroid']:
                assert ids == expected and counts['vector'] == c['rows']
    else:
        assert 'ann' not in r


def summary(documents):
    lines = ['# Exact versus IVF-Flat', '',
             'Warm synthetic search; two invocations per setting. Ranges show both runs.',
             'Latency includes result allocation/destruction. Build time is excluded from query latency.',
             'Recall uses the exact oracle; distance counts include centroid routing. No timing gates.', '',
             '| Data / algorithm | p50 µs | p95 µs | Recall@k % | Distances/query | Build ms | Raw runs |',
             '|---|---:|---:|---:|---:|---:|---|']
    groups = {}
    for name, d in documents:
        c, r = d['config'], d['results'][0]
        algorithm = f"IVF probes={c['ivf_probes']}" if c.get('ivf_partitions') else 'exact'
        groups.setdefault((c.get('distribution') or 'uniform', algorithm), []).append((name, r, c))
    for (distribution, algorithm), values in groups.items():
        def span(values, scale=1):
            return f'{min(values)/scale:.2f}–{max(values)/scale:.2f}'
        recalls, distances, builds = [], [], []
        for _, r, c in values:
            ann = r.get('ann')
            recalls.append(sum(ann['recall_at_k']) / c['queries'] if ann else 1)
            distances.append(sum(x['centroid'] + x['vector'] for x in ann['distance_evaluations']) / c['queries'] if ann else c['rows'])
            builds.append(ann['build_ns'] if ann else 0)
        links = ', '.join(f'[r{i+1}]({name})' for i, (name, _, _) in enumerate(values))
        lines.append(f'| {distribution} / {algorithm} | {span([r["query_latency"]["p50_sample_ns"] for _, r, _ in values], 1000)} | {span([r["query_latency"]["p95_sample_ns"] for _, r, _ in values], 1000)} | {span(recalls, 0.01)} | {span(distances)} | {span(builds, 1e6)} | {links} |')
    c = documents[0][1]['config']
    lines += ['', f'Rows={c["rows"]}, dimensions={c["dimensions"]}, k={c["k"]}, seed={c["seed"]}, queries={c["queries"]}, batches={c["samples"]}.',
              'Raw reports retain source/commit/environment, input fingerprints, all timings, answers, resources and backend counters.',
              'Synthetic clustered data favors partitioning; uniform data is a stress case. Neither establishes production recall or tail latency.']
    return '\n'.join(lines) + '\n'


def run(output, smoke=False, archive=False):
    output.mkdir(parents=True, exist_ok=False)
    rows, dimensions, queries, samples, partitions = (48, 8, 8, 2, 4) if smoke else (512, 64, 24, 3, 16)
    documents = []
    for distribution in ('uniform', 'clustered'):
        settings = [0, 1, max(2, partitions // 4), partitions]
        previous = {}
        for repeat in (1, 2):
            # Reverse order on the second invocation to expose ordering/load effects.
            for probes in settings if repeat == 1 else reversed(settings):
                name = f'{distribution}-p{probes}-r{repeat}.json'
                command = ['cargo', 'bench', '--locked', '--bench', 'baseline', '--',
                           '--scenario', 'search', '--rows', str(rows), '--dimensions', str(dimensions),
                           '--queries', str(queries), '--samples', str(samples), '--k', '10', '--seed', '42',
                           '--root', 'target', '--feature', 'ivf-flat', '--phase', 'baseline',
                           '--comparison-group', 'ivf-smoke-v1' if smoke else 'ivf-quick-v1',
                           '--label', 'synthetic ANN tradeoff; desktop load and power uncontrolled']
                if distribution == 'clustered':
                    command += ['--distribution', 'clustered']
                if probes:
                    command += ['--ivf-partitions', str(partitions), '--ivf-probes', str(probes), '--ivf-iterations', '8']
                print(name, flush=True)
                raw = subprocess.run(command, check=True, text=True, stdout=subprocess.PIPE).stdout
                d = json.loads(raw)
                validate(d)
                r = d['results'][0]
                stable = (r['exact_neighbor_ids'], r.get('ann', {}).get('returned_neighbor_ids'),
                          r.get('ann', {}).get('distance_evaluations'), r['build_store_calls'], r['inventory'])
                if probes in previous:
                    assert stable == previous[probes], f'seed=42 {name}: deterministic results/counters changed'
                previous[probes] = stable
                path = output / name
                path.write_text(raw)
                if archive:
                    subprocess.run(['python3', 'tools/benchmarks.py', 'archive', str(path)], check=True, stdout=subprocess.DEVNULL)
                    name = 'runs/' + hashlib.sha256(raw.encode()).hexdigest() + '.json'
                documents.append((name, d))
    report = summary(documents)
    (Path('benchmarks/ANN.md') if archive else output / 'SUMMARY.md').write_text(report)
    print(report)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True, help='new output directory')
    parser.add_argument('--smoke', action='store_true', help='tiny CI correctness workload; not performance evidence')
    parser.add_argument('--archive', action='store_true', help='retain raw history and update benchmarks/ANN.md')
    args = parser.parse_args()
    run(args.output, args.smoke, args.archive)
