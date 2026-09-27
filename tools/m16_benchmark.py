#!/usr/bin/env python3
"""One fixed mutex/worker comparison on disposable MinIO; short CI correctness smoke."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import secrets

from minio_harness import container_scope, ready, run
from test_s3 import IMAGE


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path)
    parser.add_argument('--smoke', action='store_true')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    env = {k: v for k, v in os.environ.items() if not k.startswith(('AWS_', 'GLIDER_S3_', 'MINIO_'))}
    run('cargo', 'build', '--release', '--locked', '--features', 's3', '--example', 'm16_concurrency', env=env)
    source = hashlib.sha256()
    paths = sorted(Path('src').rglob('*.rs')) + [Path('examples/m16_concurrency.rs'), Path('tools/m16_benchmark.py')]
    for path in paths:
        source.update(path.as_posix().encode()); source.update(path.read_bytes())
    name = 'glider-m16-' + secrets.token_hex(6)
    with container_scope(name):
        env.update(MINIO_ROOT_USER='glider-' + secrets.token_hex(8), MINIO_ROOT_PASSWORD=secrets.token_hex(24))
        run('docker', 'run', '-d', '--name', name, '-p', '127.0.0.1::9000', '-e', 'MINIO_ROOT_USER', '-e', 'MINIO_ROOT_PASSWORD', IMAGE, 'server', '/data', env=env, capture=True)
        port = run('docker', 'port', name, '9000/tcp', capture=True).strip().split(':')[-1]
        endpoint = 'http://127.0.0.1:' + port
        ready(endpoint, name)
        run('docker', 'exec', name, 'mc', 'mb', 'test/glider-test', capture=True)
        env.update(GLIDER_S3_ENDPOINT=endpoint, GLIDER_S3_BUCKET='glider-test', AWS_ACCESS_KEY_ID=env['MINIO_ROOT_USER'], AWS_SECRET_ACCESS_KEY=env['MINIO_ROOT_PASSWORD'])
        info = dict(version=1, git_revision=run('git', 'rev-parse', 'HEAD', capture=True).strip(),
                    git_status=run('git', 'status', '--short', capture=True).strip(), source_sha256=source.hexdigest(),
                    platform=platform.platform(), machine=platform.machine(), cpu_count=os.cpu_count(),
                    rust=run('rustc', '--version', capture=True).strip(), minio_image=IMAGE,
                    docker=run('docker', 'version', '--format', '{{.Server.Version}}', capture=True).strip(),
                    smoke=args.smoke, workload='4 clients, 100-operation batch + 10 queries/client/round, seed 42',
                    cache_control='uncontrolled OS/service caches and competing load; same container, separate namespaces/processes')
        (args.output / 'run.json').write_text(json.dumps(info, indent=2) + '\n')
        for mode in ('mutex', 'worker'):
            run('target/release/examples/m16_concurrency', mode, '2' if args.smoke else '50',
                'smoke' if args.smoke else 'paced', str(args.output / (mode + '.json')), env=env,
                timeout=180, stage='M16 ' + mode)
        result = json.loads((args.output / 'worker.json').read_text())
        if not args.smoke and not result['performance_accepted']:
            raise RuntimeError('M16 worker breached a declared budget; inspect retained report')
    print('M16 comparison passed; reports:', args.output)


if __name__ == '__main__':
    main()
