#!/usr/bin/env python3
"""Paired local write-admission regression measurements; never contacts AWS."""
import argparse
import hashlib
import json
import platform
import re
import statistics
import subprocess
import tempfile
import time
from pathlib import Path


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--before-binary', type=Path, required=True)
    parser.add_argument('--after-binary', type=Path, required=True)
    parser.add_argument('--before-revision', required=True)
    parser.add_argument('--after-revision', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--trials', type=int, default=3)
    args = parser.parse_args()
    if args.trials < 1 or args.output.exists():
        parser.error('positive trials and a new output path are required')
    binaries = {'before': args.before_binary.resolve(), 'after': args.after_binary.resolve()}
    for binary in binaries.values():
        if not binary.is_file():
            parser.error(f'missing binary: {binary}')
    report = {
        'environment': platform.platform(),
        'hardware': subprocess.check_output(['sysctl', '-n', 'machdep.cpu.brand_string'], text=True).strip() if platform.system() == 'Darwin' else platform.processor(),
        'rustc': subprocess.check_output(['rustc', '--version'], text=True).strip(),
        'backend': 'LocalStore, temporary directories on the local filesystem',
        'configuration': {'rows': 10000, 'dimensions': [128, 768], 'seed': 42, 'batch': 100, 'queries': 50, 'cache': False, 'conversion': False, 'index_watermark_bytes': 134217728},
        'protocol': 'Alternate paired before/after order. Individual acknowledged writes exclude input generation. Query samples include selective search and result allocation, exclude the exact oracle. Maintenance and reopen timed separately. Whole-process peak RSS includes setup and oracle work. No concurrent test or compile workload.',
        'revisions': {'before': args.before_revision, 'after': args.after_revision},
        'binary_sha256': {phase: digest(binary) for phase, binary in binaries.items()},
        'probe_source_sha256': digest(Path('examples/index_admission_probe.rs')),
        'runs': [],
    }
    with tempfile.TemporaryDirectory(prefix='glider-index-admission-') as root:
        for dimensions in report['configuration']['dimensions']:
            for trial in range(1, args.trials + 1):
                pair = {}
                for phase in (('before', 'after') if trial % 2 else ('after', 'before')):
                    command = [str(binaries[phase]), str(dimensions), str(Path(root) / f'{dimensions}-{trial}-{phase}')]
                    if platform.system() == 'Darwin':
                        command = ['/usr/bin/time', '-l', *command]
                    started = time.monotonic()
                    result = subprocess.run(command, capture_output=True, text=True, check=True)
                    measured = json.loads(result.stdout)
                    if measured['dimensions'] != dimensions or measured['rows'] != 10000:
                        raise ValueError('unexpected probe configuration')
                    peak = re.search(r'(\d+)\s+maximum resident set size', result.stderr)
                    measured.update(phase=phase, trial=trial, elapsed_s=time.monotonic() - started,
                                    peak_rss_bytes=int(peak.group(1)) if peak else None)
                    report['runs'].append(measured)
                    pair[phase] = measured
                    print(f'{dimensions} dimensions, trial {trial}, {phase} complete', flush=True)
                for field in ('result_hash', 'recall_at_10', 'index_bytes'):
                    if pair['before'][field] != pair['after'][field]:
                        raise ValueError(f'before/after result mismatch: {field}')
    fields = ['write_p50_ms', 'write_p95_ms', 'query_p50_ms', 'query_p95_ms', 'maintenance_ms', 'reopen_ms', 'peak_rss_bytes']
    report['medians'] = {}
    for dimensions in report['configuration']['dimensions']:
        report['medians'][str(dimensions)] = {
            phase: {field: statistics.median(run[field] for run in report['runs'] if run['dimensions'] == dimensions and run['phase'] == phase and run[field] is not None)
                    for field in fields if any(run[field] is not None for run in report['runs'])}
            for phase in binaries
        }
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__':
    main()
