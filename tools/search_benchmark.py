#!/usr/bin/env python3
"""M5 warm exact-search characterization using the unchanged Cargo harness."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess

from benchmarks import compare_files, reports

MASK = (1 << 64) - 1
QUERY_XOR = 0xd1b54a32d192ed03
# One-factor-at-a-time around (1,000 rows, 128 dimensions, k=10).
CASES = [(1000, 128, 10), (10000, 128, 10), (1000, 32, 10),
         (1000, 768, 10), (1000, 128, 100)]
SMOKE_CASES = [(20, 4, 3), (20, 4, 10)]


def vectors(seed, rows, dimensions):
    state = seed
    values, digest = [], hashlib.sha256()
    for _ in range(rows):
        vector = []
        for _ in range(dimensions):
            state = (state + 0x9e3779b97f4a7c15) & MASK
            z = ((state ^ (state >> 30)) * 0xbf58476d1ce4e5b9) & MASK
            z = ((z ^ (z >> 27)) * 0x94d049bb133111eb) & MASK
            z ^= z >> 31
            value = (z >> 40) / 8388608.0 - 1.0
            vector.append(value)
            digest.update(struct.pack('<f', value))
        values.append(vector)
    return values, digest.hexdigest()


def oracle(data, query, k):
    ranked = []
    for identifier, vector in enumerate(data):
        score = 0.0
        for a, b in zip(query, vector):
            d = a - b
            score += d * d
        ranked.append((score, identifier))
    return [identifier for _, identifier in sorted(ranked)[:k]]


def command(case, backend, root, label, smoke=False):
    rows, dimensions, k = case
    args = ['cargo', 'bench', '--locked']
    if backend == 's3':
        args += ['--features', 's3']
    return args + ['--bench', 'baseline', '--', '--scenario', 'search', '--backend', backend,
                   '--rows', str(rows), '--dimensions', str(dimensions), '--k', str(k),
                   '--queries', '5' if smoke else '100', '--samples', '2' if smoke else '5',
                   '--seed', '42', '--feature', 'm5-search', '--phase', 'baseline',
                   '--comparison-group', 'm5-search-smoke' if smoke else 'm5-search-v1',
                   '--root', str(root), '--label', label]


def validate(document, case, backend, smoke=False, credentials=()):
    reports(document)
    encoded = json.dumps(document)
    for credential in credentials:
        assert not credential or credential not in encoded, 'credential in report'
    rows, dimensions, k = case
    config = document['config']
    expected = dict(rows=rows, dimensions=dimensions, k=k, queries=5 if smoke else 100,
                    samples=2 if smoke else 5, seed=42, scenario='search')
    for name, value in expected.items():
        assert config[name] == value, f'seed=42: wrong {name}'
    assert len(document['results']) == 1
    result = document['results'][0]
    assert result['scenario'] == 'search' and result['backend'] == backend
    assert document['generator'] == 'splitmix64-high24-uniform-f32-v1'
    assert document['metric'] == 'squared_euclidean'
    assert document['environment']['source_sha256'] and document['git_revision']
    assert result['warmup_queries'] == expected['queries']
    assert result['cache'] == 'warm in-memory query pass'
    assert len(result['query_latency']['raw_sample_ns']) == expected['queries'] * expected['samples']
    assert len(result['timing']['raw_sample_ns']) == expected['samples']
    assert all(value == 0 for value in result['measured_store_calls'].values())
    assert result['measured_store_calls']['get_payload_bytes'] == 0
    if backend == 's3':
        assert all(value == 0 for value in result['measured_http_requests'].values())
    assert result['inventory']['logical_objects'] == rows + 1
    data, data_hash = vectors(42, rows, dimensions)
    queries, query_hash = vectors(42 ^ QUERY_XOR, expected['queries'], dimensions)
    assert result['dataset_seed'] == 42 and result['query_seed'] == 42 ^ QUERY_XOR
    assert result['dataset_sha256'] == data_hash, 'seed=42: dataset fingerprint differs'
    assert result['query_sha256'] == query_hash, 'seed=42: query fingerprint differs'
    saved = result['exact_neighbor_ids']
    assert len(saved) == len(queries)
    for ids in saved:
        assert len(ids) == min(k, rows) and len(set(ids)) == len(ids)
        assert all(isinstance(i, int) and 0 <= i < rows for i in ids)
    # Independent scalar f64 oracle, outside the Rust process and every timer.
    checked = list(range(min(3, len(queries))))
    for index in checked:
        assert saved[index] == oracle(data, queries[index], k), f'seed=42 query={index}: exact oracle differs'
    return checked


def validate_related(documents):
    for i, a in enumerate(documents):
        for b in documents[i + 1:]:
            ar, br = a['results'][0], b['results'][0]
            if ar['dataset_sha256'] == br['dataset_sha256'] and ar['query_sha256'] == br['query_sha256']:
                if a['config']['k'] > b['config']['k']:
                    ar, br = br, ar
                assert all(x == y[:len(x)] for x, y in zip(ar['exact_neighbor_ids'], br['exact_neighbor_ids'])), 'seed=42: repeated or cross-k oracle differs'


def run(output, backend='local', root=Path('target'), label='M5 synthetic baseline; desktop load and power uncontrolled', smoke=False, repeats=2):
    if repeats < 2:
        raise ValueError('at least two invocations are required to expose repeatability')
    if not root.is_dir():
        raise ValueError('root must be an existing directory on the measured filesystem')
    output.mkdir(parents=True, exist_ok=False)
    cases = SMOKE_CASES if smoke else CASES
    manifest, documents = [], []
    credentials = [os.environ.get(k, '') for k in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN')]
    # Separate processes keep lifetime RSS comparable; never parallelize timed runs.
    for case in cases:
        previous = None
        for repeat in range(repeats):
            name = f'n{case[0]}-d{case[1]}-k{case[2]}-r{repeat + 1}.json'
            print(f'M5 {name}', flush=True)
            raw = subprocess.run(command(case, backend, root, label, smoke), check=True,
                                 text=True, stdout=subprocess.PIPE).stdout
            document = json.loads(raw)
            checked = validate(document, case, backend, smoke, credentials)
            if documents:
                assert document['environment']['source_sha256'] == documents[0]['environment']['source_sha256'], 'source changed during matrix'
                assert document['git_revision'] == documents[0]['git_revision'], 'revision changed during matrix'
            path = output / name
            with path.open('x') as file:
                file.write(raw)
            if previous:
                comparison = compare_files(previous, path, check_counters=True)
                assert not comparison['counter_check_failed'], 'deterministic search counters changed'
            documents.append(document)
            manifest.append(dict(file=name, sha256=hashlib.sha256(raw.encode()).hexdigest(),
                                 checked_query_indices=checked))
            previous = path
    validate_related(documents)
    with (output / 'matrix.json').open('x') as file:
        json.dump(dict(schema_version=1, backend=backend, smoke=smoke, repeats=repeats,
                       cache='warm query pass; cold CPU/OS/storage cache not measured', runs=manifest), file, indent=2)
        file.write('\n')
    print(f'Validated {len(manifest)} runs: fingerprints, exact oracle, counter repeatability and cross-k prefixes.', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True, help='new directory for immutable raw reports and matrix inventory')
    parser.add_argument('--backend', choices=('local', 's3'), default='local')
    parser.add_argument('--root', type=Path, default=Path('target'))
    parser.add_argument('--label', default='M5 synthetic baseline; desktop load and power uncontrolled')
    parser.add_argument('--smoke', action='store_true', help='small CI correctness workload, not performance evidence')
    parser.add_argument('--repeats', type=int, default=2)
    args = parser.parse_args()
    run(args.output, args.backend, args.root, args.label, args.smoke, args.repeats)


if __name__ == '__main__':
    main()
