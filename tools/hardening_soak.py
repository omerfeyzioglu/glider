#!/usr/bin/env python3
"""Disposable MinIO HTTP soak; acknowledged-state oracle and SIGKILL/cache loss.

Synthetic integer f32 vectors, seed 42, single collection. No AWS credentials
are inherited. Full vectors/metadata/absence are checked after both restarts.
Latencies include Python encoding, HTTP and decoding; sampled RSS is not HWM.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

from minio_harness import container_scope, ready, run
from test_s3 import IMAGE
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'clients/python'))
from glider_client import Client, GliderError


def vector(identifier, generation, dimensions):
    rng = random.Random(42 + identifier * 0x9e3779b1 + generation * 0xd1b54a32)
    return [rng.randrange(-125, 126) for _ in range(dimensions)]


def percentiles(values):
    values = sorted(values)
    return {f'p{p}_ms': values[max(0, math.ceil(len(values) * p / 100) - 1)]
            if values else None for p in (50, 95, 99)}


def hash_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as source:
        for part in iter(lambda: source.read(1 << 20), b''):
            h.update(part)
    return h.hexdigest()


def execute(args):
    if args.rows < 200 or args.rows % 200 or args.dimensions < 1 or args.seconds < 1:
        raise ValueError('rows must be a positive multiple of 200; dimensions/seconds positive')
    args.output.mkdir(parents=True, exist_ok=False)
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(('AWS_', 'GLIDER_', 'MINIO_'))}
    env.update(MINIO_ROOT_USER='glider-' + secrets.token_hex(8),
               MINIO_ROOT_PASSWORD=secrets.token_hex(24))
    name = 'glider-hardening-' + secrets.token_hex(6)
    report = {'version': 1, 'protocol': 'hardening-http-v1', 'backend': 'loopback-minio',
              'minio_image': IMAGE, 'seed': 42, 'rows': args.rows,
              'dimensions': args.dimensions, 'seconds': args.seconds,
              'writers': 2, 'readers': 2, 'batch': 100, 'index_bytes': 128 << 20,
              'auto_cluster_rows': 0, 'local_blocks': 0,
              'git_revision': run('git', 'rev-parse', 'HEAD', capture=True).strip(),
              'source_sha256': hash_file(__file__), 'server_sha256': hash_file(args.server),
              'environment': platform.platform(), 'phases': [], 'passed': False,
              'rss_note': 'RSS sampled with ps every second; includes server only, not an exact peak'}
    oracle = [None] * args.rows
    process = None
    samples = []
    sampler_stop = threading.Event()
    def sample():
        while not sampler_stop.wait(1):
            current = process
            if current is not None and current.poll() is None:
                value = subprocess.run(['ps', '-o', 'rss=', '-p', str(current.pid)],
                                       capture_output=True, text=True).stdout.strip()
                if value.isdigit():
                    samples.append(int(value) * 1024)
    sampler = threading.Thread(target=sample, daemon=True)
    with container_scope(name), tempfile.TemporaryDirectory(prefix='glider-hardening-') as directory:
        cache = Path(directory) / 'cache'
        log = (args.output / 'server.log').open('w')
        def start():
            nonlocal process
            began = time.monotonic()
            process = subprocess.Popen([str(args.server.resolve())], env=env, stdout=log, stderr=log)
            client = Client('http://' + env['GLIDER_LISTEN'], timeout=120, max_retries=0)
            while time.monotonic() - began < 60:
                if process.poll() is not None:
                    raise RuntimeError('server exited during startup; see server.log')
                if client.health():
                    return client, time.monotonic() - began
                time.sleep(0.05)
            raise TimeoutError('startup did not complete')
        def stop(kill):
            nonlocal process
            if process is not None:
                process.send_signal(signal.SIGKILL if kill else signal.SIGTERM)
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise
                process = None
        def check(client, upper):
            began = time.monotonic()
            for first in range(0, upper, 100):
                found = client.get_many(list(range(first, min(first + 100, upper))))
                for identifier, point in zip(range(first, min(first + 100, upper)), found):
                    generation = oracle[identifier]
                    if generation is None:
                        assert point is None, f'unexpected id {identifier}; seed 42'
                    else:
                        assert point is not None, f'lost ack id {identifier}; seed 42'
                        assert point.metadata == {'generation': str(generation)}, identifier
                        assert point.vector == vector(identifier, generation, args.dimensions), identifier
            expected = sum(g is not None for g in oracle)
            assert client.count() == expected
            assert list(client.scan(page_size=10000)) == [i for i, g in enumerate(oracle) if g is not None]
            return {'checked_ids': upper, 'live': expected, 'vector_metadata_mismatches': 0,
                    'seconds': time.monotonic() - began}
        try:
            run('docker', 'run', '-d', '--name', name, '-p', '127.0.0.1::9000',
                '-e', 'MINIO_ROOT_USER', '-e', 'MINIO_ROOT_PASSWORD', IMAGE, 'server', '/data', env=env, capture=True)
            port = run('docker', 'port', name, '9000/tcp', capture=True).strip().split(':')[-1]
            endpoint = 'http://127.0.0.1:' + port
            ready(endpoint, name)
            run('docker', 'exec', name, 'mc', 'mb', 'test/glider-test', capture=True)
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                listen_port = sock.getsockname()[1]
            env.update(AWS_ACCESS_KEY_ID=env['MINIO_ROOT_USER'],
                       AWS_SECRET_ACCESS_KEY=env['MINIO_ROOT_PASSWORD'], GLIDER_S3_ENDPOINT=endpoint,
                       GLIDER_S3_BUCKET='glider-test', GLIDER_S3_NAMESPACE='soak', GLIDER_S3_REGION='us-east-1',
                       GLIDER_DIMENSIONS=str(args.dimensions), GLIDER_CACHE_DIR=str(cache),
                       GLIDER_CACHE_BYTES=str(256 << 20), GLIDER_INDEX_BYTES=str(128 << 20),
                       GLIDER_AUTO_CLUSTER_ROWS='0', GLIDER_AUTO_RECLUSTER_FACTOR='0', GLIDER_LOCAL_BLOCKS='0',
                       GLIDER_LEASE_SECONDS='1', GLIDER_LISTEN=f'127.0.0.1:{listen_port}', GLIDER_CONSOLE='0')
            client, startup = start()
            sampler.start()
            report['startup_seconds'] = startup
            began = time.monotonic()
            latencies = []
            for first in range(0, args.rows, 100):
                points = [{'id': i, 'vector': vector(i, 0, args.dimensions),
                           'metadata': {'generation': '0'}} for i in range(first, first + 100)]
                tick = time.monotonic()
                client.write(points)
                latencies.append((time.monotonic() - tick) * 1000)
                oracle[first:first + 100] = [0] * 100
                if first + 100 == args.rows // 2:
                    stop(True)
                    shutil.rmtree(cache, ignore_errors=True)
                    client, startup = start()
                    verified = check(client, args.rows // 2)
                    report['phases'].append({'phase': 'ingest_sigkill_cache_loss',
                                             'restart_seconds': startup, **verified})
                if (first + 100) % 10000 == 0:
                    print(f'loaded {first + 100}/{args.rows}', flush=True)
            report['phases'].append({'phase': 'load', 'seconds': time.monotonic() - began,
                                     'acknowledged_batches': len(latencies), **percentiles(latencies)})
            began = time.monotonic()
            done = threading.Event()
            barrier = threading.Barrier(4)
            def writer(worker):
                rng = random.Random(42 + worker)
                latencies, rejected, accepted = [], 0, 0
                bound = Client(client._scheme + '://' + client._netloc, timeout=120, max_retries=0)
                low, high = worker * (args.rows // 2), (worker + 1) * (args.rows // 2)
                barrier.wait()
                while time.monotonic() - began < args.seconds:
                    selected = rng.sample(range(low, high), 100)
                    deleted = [i for i in selected if rng.randrange(10) == 0]
                    puts = [i for i in selected if i not in deleted]
                    generations = {i: (oracle[i] or 0) + 1 for i in puts}
                    points = [{'id': i, 'vector': vector(i, generations[i], args.dimensions),
                               'metadata': {'generation': str(generations[i])}} for i in puts]
                    tick = time.monotonic()
                    try:
                        bound.write(points, deleted)
                    except GliderError as error:
                        if error.status != 429:
                            raise
                        rejected += 1
                        continue
                    latencies.append((time.monotonic() - tick) * 1000)
                    accepted += 1
                    for i, generation in generations.items():
                        oracle[i] = generation
                    for i in deleted:
                        oracle[i] = None
                return {'acknowledged_batches': accepted, 'http_429': rejected, **percentiles(latencies)}
            def reader(worker):
                rng = random.Random(4242 + worker)
                latencies, rejected = [], 0
                bound = Client(client._scheme + '://' + client._netloc, timeout=120, max_retries=0)
                barrier.wait()
                while not done.is_set():
                    query = vector(rng.randrange(args.rows), rng.randrange(10), args.dimensions)
                    tick = time.monotonic()
                    try:
                        hits = bound.query(query, include_vector=True, include_metadata=True)
                    except GliderError as error:
                        if error.status != 429:
                            raise
                        rejected += 1
                        continue
                    latencies.append((time.monotonic() - tick) * 1000)
                    assert len(hits) == 10
                    for hit in hits:
                        generation = int(hit.metadata['generation'])
                        assert hit.vector == vector(hit.id, generation, args.dimensions)
                        assert hit.distance == sum((x - y) ** 2 for x, y in zip(query, hit.vector))
                return {'queries': len(latencies), 'http_429': rejected, **percentiles(latencies)}
            with ThreadPoolExecutor(max_workers=4) as pool:
                readers = [pool.submit(reader, i) for i in range(2)]
                writers = [pool.submit(writer, i) for i in range(2)]
                try:
                    writes = [f.result() for f in writers]
                finally:
                    done.set()
                reads = [f.result() for f in readers]
            report['phases'].append({'phase': 'mixed', 'seconds': time.monotonic() - began,
                                     'writes': writes, 'reads': reads})
            (args.output / 'status-before-kill.json').write_text(json.dumps(client.status(), indent=2) + '\n')
            quality = []
            exact_results = []
            for index in range(5):
                query = vector(args.rows + index, 99, args.dimensions)
                exact = [hit.id for hit in client.query(query, exact=True)]
                approximate = [hit.id for hit in client.query(query)]
                exact_results.append((query, exact))
                quality.append(len(set(exact) & set(approximate)) / 10)
            report['sample_recall_at_10'] = quality
            stop(True)
            shutil.rmtree(cache, ignore_errors=True)
            client, startup = start()
            verified = check(client, args.rows)
            for query, exact in exact_results:
                assert [hit.id for hit in client.query(query, exact=True)] == exact
            report['phases'].append({'phase': 'mixed_sigkill_cache_loss', 'restart_seconds': startup,
                                     'exact_top10_queries_preserved': len(exact_results), **verified})
            stop(False)
            report['passed'] = True
        finally:
            sampler_stop.set()
            if sampler.is_alive():
                sampler.join(timeout=5)
            stop(False)
            log.close()
            report['sampled_server_peak_rss_bytes'] = max(samples) if samples else None
            (args.output / 'run.json').write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
    print(f"passed={report['passed']}; {args.output / 'run.json'}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', type=Path, default=Path('target/release/glider-server'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--rows', type=int, default=100000)
    parser.add_argument('--dimensions', type=int, default=768)
    parser.add_argument('--seconds', type=int, default=120)
    execute(parser.parse_args())


if __name__ == '__main__':
    main()
