#!/usr/bin/env python3
"""Compare catalog sweep request counts on disposable MinIO; no AWS access."""
import argparse
import hashlib
import http.client
import json
import os
import platform
import secrets
import signal
import socket
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from minio_harness import container_scope, ready, run
from test_s3 import IMAGE


class Proxy(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *args):
        pass

    def forward(self):
        parsed = urlsplit(self.path)
        query = parse_qs(parsed.query)
        kind = 'list' if 'list-type' in query else self.command.lower()
        prefix = query.get('prefix', [''])[0] if kind == 'list' else parsed.path
        zone = next((name for name, fragment in [('history', 'catalog-history/'), ('catalog', 'catalog/'), ('data', 'data/')]
                     if fragment in prefix), 'other')
        with self.server.count_lock:
            key = f'{kind}_{zone}'
            self.server.counts[key] = self.server.counts.get(key, 0) + 1
        body = self.rfile.read(int(self.headers.get('content-length', '0')))
        client = http.client.HTTPConnection('127.0.0.1', self.server.upstream, timeout=30)
        try:
            # Preserve Host and all signed headers while changing only the
            # connection destination to the disposable MinIO port.
            client.request(self.command, self.path, body=body, headers=dict(self.headers))
            response = client.getresponse()
            payload = response.read()
            self.send_response(response.status)
            for key, value in response.getheaders():
                if key.lower() not in ('transfer-encoding', 'content-length', 'connection'):
                    self.send_header(key, value)
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        finally:
            client.close()

    do_GET = do_PUT = do_DELETE = do_HEAD = forward


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--before-server', type=Path, required=True)
    parser.add_argument('--after-server', type=Path, required=True)
    parser.add_argument('--seed-binary', type=Path, default=Path('target/release/examples/catalog_sweep_seed'))
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    for binary in [args.before_server, args.after_server, args.seed_binary]:
        if not binary.is_file():
            parser.error(f'missing binary: {binary}')
    if args.output.exists():
        parser.error('output already exists')
    name = 'glider-hardening-sweep-' + secrets.token_hex(5)
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(('AWS_', 'GLIDER_', 'MINIO_'))}
    env.update(MINIO_ROOT_USER='glider-' + secrets.token_hex(5), MINIO_ROOT_PASSWORD=secrets.token_hex(20))
    report = {
        'environment': platform.platform(), 'minio_image': IMAGE,
        'fixture': {'active_generations': 100, 'orphan_generations': 100, 'objects_per_generation': 1,
                    'dimensions': 128, 'seed': 'deterministic ascending names and generation IDs'},
        'measurement': '62 seconds from process spawn, includes startup and first minute tick; setup excluded',
        'runs': [],
    }
    with tempfile.TemporaryDirectory(prefix='glider-sweep-') as logs, container_scope(name):
        run('docker', 'run', '-d', '--name', name, '-p', '127.0.0.1::9000', '-e', 'MINIO_ROOT_USER',
            '-e', 'MINIO_ROOT_PASSWORD', IMAGE, 'server', '/data', env=env, capture=True)
        upstream = int(run('docker', 'port', name, '9000/tcp', capture=True).strip().split(':')[-1])
        ready('http://127.0.0.1:' + str(upstream), name)
        run('docker', 'exec', name, 'mc', 'mb', 'test/glider-test', capture=True)
        proxy = ThreadingHTTPServer(('127.0.0.1', 0), Proxy)
        proxy.upstream, proxy.counts, proxy.count_lock = upstream, {}, threading.Lock()
        threading.Thread(target=proxy.serve_forever, daemon=True).start()
        try:
            env.update(AWS_ACCESS_KEY_ID=env['MINIO_ROOT_USER'], AWS_SECRET_ACCESS_KEY=env['MINIO_ROOT_PASSWORD'],
                       GLIDER_S3_BUCKET='glider-test', GLIDER_S3_ENDPOINT='http://127.0.0.1:' + str(proxy.server_port),
                       GLIDER_COLLECTION_IDLE_SECONDS='0')
            for phase, binary in [('before', args.before_server.resolve()), ('after', args.after_server.resolve())]:
                env['GLIDER_S3_NAMESPACE'] = 'sweep-' + phase
                subprocess.run([str(args.seed_binary.resolve())], env=env, check=True)
                with socket.socket() as port:
                    port.bind(('127.0.0.1', 0))
                    listen = port.getsockname()[1]
                env['GLIDER_LISTEN'] = '127.0.0.1:' + str(listen)
                with proxy.count_lock:
                    proxy.counts.clear()
                started = time.monotonic()
                with open(Path(logs) / (phase + '.log'), 'w') as log:
                    proc = subprocess.Popen([str(binary)], env=env, stdout=log, stderr=log)
                    try:
                        while time.monotonic() - started < 62:
                            if proc.poll() is not None:
                                raise RuntimeError('server exited before the measurement completed')
                            time.sleep(.2)
                    finally:
                        if proc.poll() is None:
                            proc.send_signal(signal.SIGTERM)
                        try:
                            proc.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            proc.wait()
                            raise
                    if proc.returncode != 0:
                        raise RuntimeError(f'server exit status {proc.returncode}')
                with proxy.count_lock:
                    measured = dict(proxy.counts)
                report['runs'].append({'phase': phase, 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
                                       'elapsed_s': time.monotonic() - started, 'requests': measured})
                print(phase, measured, flush=True)
        finally:
            proxy.shutdown()
            proxy.server_close()
    args.output.write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
