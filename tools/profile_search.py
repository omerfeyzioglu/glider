#!/usr/bin/env python3
"""Sample the actual warm search path on macOS; diagnostic timings are not baselines."""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True, help='new diagnostic output directory')
    parser.add_argument('--rows', type=int, default=10000)
    parser.add_argument('--dimensions', type=int, default=128)
    parser.add_argument('--k', type=int, default=10)
    args = parser.parse_args()
    if sys.platform != 'darwin' or not shutil.which('sample'):
        parser.error('this runner requires macOS sample; use the profile-ready marker with a native profiler elsewhere')
    args.output.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, CARGO_PROFILE_BENCH_DEBUG='1')
    command = ['cargo', 'bench', '--locked', '--bench', 'baseline', '--', '--scenario', 'search',
               '--rows', str(args.rows), '--dimensions', str(args.dimensions), '--k', str(args.k),
               '--queries', '100', '--samples', '5', '--seed', '42', '--profile-seconds', '8',
               '--feature', 'search-profile', '--phase', 'baseline', '--comparison-group', 'native-sample-v1',
               '--root', 'target', '--label', 'diagnostic sampling; not an unprofiled performance baseline']
    with (args.output / 'diagnostic.json').open('x') as output:
        process = subprocess.Popen(command, env=env, stdout=output, stderr=subprocess.PIPE, text=True)
        sampled = False
        try:
            for line in process.stderr:
                print(line, end='', flush=True)
                if line.startswith('PROFILE_READY '):
                    pid = int(line.split()[1])
                    subprocess.run(['sample', str(pid), '5', '-file', str(args.output / 'sample.txt')], check=True)
                    sampled = True
            if process.wait() or not sampled:
                raise RuntimeError('search profiling did not complete')
        finally:
            process.stderr.close()
            if process.poll() is None:
                process.terminate()
                process.wait()


if __name__ == '__main__':
    main()
