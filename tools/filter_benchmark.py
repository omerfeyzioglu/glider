#!/usr/bin/env python3
"""Reproducible filtered exact/IVF quality comparison; timings are diagnostic."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

from ann_benchmark import validate


def summary(documents):
    lines = [
        '# Filtered exact versus IVF-Flat', '',
        'Synthetic vector-independent equality filter; every Nth document matches.',
        'Recall uses an independent filtered exact oracle. Distance counts include centroid routing.',
        'The short runs characterize quality and work counts, not production latency.', '',
        '| Every Nth ID | Algorithm | Eligible | Mean recall@k % | Distances/query | Queries below exact | Raw run |',
        '|---:|---|---:|---:|---:|---:|---|',
    ]
    for name, document in documents:
        config, result = document['config'], document['results'][0]
        ann = result.get('ann')
        eligible = result['filter']['eligible_documents']
        if ann:
            algorithm = f"IVF probes={config['ivf_probes']}"
            recall = sum(ann['recall_at_k']) / config['queries']
            distance = sum(x['centroid'] + x['vector'] for x in ann['distance_evaluations']) / config['queries']
            short = sum(len(ids) < len(exact) for ids, exact in
                        zip(ann['returned_neighbor_ids'], result['exact_neighbor_ids']))
        else:
            algorithm, recall, distance, short = 'exact', 1.0, eligible, 0
        lines.append(f"| {config['filter_every']} | {algorithm} | {eligible} | {recall * 100:.2f} | {distance:.2f} | {short}/{config['queries']} | [raw]({name}) |")
    config = documents[0][1]['config']
    lines += [
        '',
        f"Rows={config['rows']}, dimensions={config['dimensions']}, k={config['k']}, seed={config['seed']}, queries={config['queries']}, batches={config['samples']}.",
        'All runs use the local backend and warm in-memory queries. Raw reports include source, environment, inputs, answers, counters and timing samples.',
        'A vector-independent filter can be harder for IVF than a correlated one; no workload-wide recall or latency target follows from these results.',
    ]
    return '\n'.join(lines) + '\n'


def run(output, smoke=False, archive=False):
    output.mkdir(parents=True, exist_ok=False)
    rows, dimensions, queries, k, partitions, every, probes = (
        (48, 8, 8, 6, 4, (1, 8), (0, 1, 4)) if smoke else
        (512, 64, 24, 10, 16, (1, 8, 32), (0, 1, 4, 16))
    )
    documents = []
    for modulus in every:
        for probe in probes:
            name = f'filter-{modulus}-p{probe}.json'
            command = [
                'cargo', 'bench', '--locked', '--bench', 'baseline', '--',
                '--scenario', 'search', '--rows', str(rows), '--dimensions', str(dimensions),
                '--queries', str(queries), '--samples', '2', '--k', str(k), '--seed', '42',
                '--filter-every', str(modulus), '--root', 'target',
                '--feature', 'metadata-filtering', '--phase', 'baseline',
                '--comparison-group', 'filter-smoke-v1' if smoke else 'filter-quick-v1',
                '--label', 'synthetic filter quality; desktop load and power uncontrolled',
            ]
            if probe:
                command += ['--ivf-partitions', str(partitions), '--ivf-probes', str(probe), '--ivf-iterations', '8']
            print(name, flush=True)
            raw = subprocess.run(command, check=True, text=True, stdout=subprocess.PIPE).stdout
            document = json.loads(raw)
            validate(document)
            path = output / name
            path.write_text(raw)
            if archive:
                subprocess.run(['python3', 'tools/benchmarks.py', 'archive', str(path),
                                '--archive', 'benchmarks/filtering'],
                               check=True, stdout=subprocess.DEVNULL)
                name = 'filtering/runs/' + hashlib.sha256(raw.encode()).hexdigest() + '.json'
            documents.append((name, document))
    report = summary(documents)
    (Path('benchmarks/FILTERING.md') if archive else output / 'SUMMARY.md').write_text(report)
    print(report)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True, help='new output directory')
    parser.add_argument('--smoke', action='store_true', help='small quality check')
    parser.add_argument('--archive', action='store_true', help='retain raw runs and update benchmarks/FILTERING.md')
    args = parser.parse_args()
    run(args.output, args.smoke, args.archive)
