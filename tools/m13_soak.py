#!/usr/bin/env python3
"""Paced 30-minute MinIO serving soak with six processes; --smoke is unpaced."""
import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess
import time
from test_s3 import IMAGE, ready, run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path)
    parser.add_argument('--smoke', action='store_true')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    env = {k: v for k, v in os.environ.items() if not k.startswith(('AWS_', 'GLIDER_S3_', 'MINIO_', 'GLIDER_SOAK_'))}
    env.update(MINIO_ROOT_USER='glider-'+secrets.token_hex(8), MINIO_ROOT_PASSWORD=secrets.token_hex(24))
    if args.smoke:
        env['GLIDER_SOAK_UNPACED'] = '1'
    name = 'glider-m13-'+secrets.token_hex(6)
    run('cargo', 'build', '--release', '--locked', '--features', 's3', '--example', 'm13_soak', env=env)
    revision = run('git', 'rev-parse', 'HEAD', capture=True).strip()
    status = run('git', 'status', '--short', capture=True).strip()
    start = time.monotonic()
    try:
        run('docker', 'run', '-d', '--name', name, '-p', '127.0.0.1::9000', '-e', 'MINIO_ROOT_USER', '-e', 'MINIO_ROOT_PASSWORD', IMAGE, 'server', '/data', env=env, capture=True)
        port = run('docker', 'port', name, '9000/tcp', capture=True).strip().split(':')[-1]
        endpoint = 'http://127.0.0.1:'+port
        ready(endpoint, name)
        run('docker', 'exec', name, 'mc', 'mb', 'test/glider-test', capture=True)
        env.update(AWS_ACCESS_KEY_ID=env['MINIO_ROOT_USER'], AWS_SECRET_ACCESS_KEY=env['MINIO_ROOT_PASSWORD'], GLIDER_S3_ENDPOINT=endpoint, GLIDER_S3_BUCKET='glider-test', GLIDER_S3_NAMESPACE='soak-live')
        reports = []
        cycles = 70 if args.smoke else 300
        for epoch in range(2 if args.smoke else 6):
            raw = run('target/release/examples/m13_soak', str(epoch*cycles), str(cycles), env=env, capture=True)
            assert not any(secret in raw for secret in (env['AWS_ACCESS_KEY_ID'], env['AWS_SECRET_ACCESS_KEY']))
            r = json.loads(raw)
            (args.output / f'epoch-{epoch}.json').write_text(raw)
            reports.append(r)
            env['GLIDER_S3_NAMESPACE'] = r['active_prefix']
            assert r['peak_process_rss_bytes'] <= 64*1024*1024
            assert r['max_visible_engine_objects'] <= 128
            if not args.smoke:
                assert r['unfiltered']['p95_ns'] <= 5_000_000 and r['filtered']['p95_ns'] <= 5_000_000
                assert r['batch']['p95_ns'] <= 100_000_000
                assert r['cycles']*400/r['elapsed_seconds'] >= 100
            for opened in r['opens']:
                assert opened['http_gets'] <= 64 and opened['http_lists'] <= 4
                if not args.smoke:
                    assert opened['ns'] <= 500_000_000
            for event in r['events']:
                assert event['ns'] <= 30_000_000_000
            print(f'epoch={epoch} passed; elapsed={time.monotonic()-start:.1f}s', flush=True)
        assert len({r['source_sha256'] for r in reports}) == 1
        mutation = sum(r['mutation_payload_bytes'] for r in reports)
        maintenance = sum(r['maintenance_payload_bytes'] for r in reports)
        assert maintenance <= 2*mutation
        if not args.smoke:
            assert sum(r['elapsed_seconds'] for r in reports) >= 1800
        summary = dict(git_revision=revision, git_status=status, service=IMAGE, rustc=run('rustc','--version',capture=True).strip(), os=run('uname','-srvm',capture=True).strip(), cpu=run('sysctl','-n','machdep.cpu.brand_string',capture=True).strip() if os.uname().sysname=='Darwin' else os.uname().machine, smoke=args.smoke, elapsed_seconds=time.monotonic()-start, mutation_payload_bytes=mutation, maintenance_payload_bytes=maintenance, additional_maintenance_write_ratio=maintenance/mutation, epochs=len(reports), cycles=cycles*len(reports), queries=cycles*len(reports)*100, logical_mutations=cycles*len(reports)*400)
        (args.output/'summary.json').write_text(json.dumps(summary, indent=2)+'\n')
    finally:
        subprocess.run(['docker', 'rm', '-fv', name], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == '__main__':
    main()
