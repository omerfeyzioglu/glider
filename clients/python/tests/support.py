"""Shared test helpers: a scriptable fake HTTP server and a real-server launcher."""

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from glider_client import Client  # noqa: E402

DROP = "drop"  # close the connection without answering


class FakeServer:
    """Scripted HTTP server. Responses are queued per (method, path).

    ``script`` items are ``(status, json_body)``, ``DROP``, or a callable
    ``(request_body) -> (status, json_body)``. When a queue has one item left it
    keeps being used; an unscripted route answers 599 so tests fail loudly.
    """

    def __init__(self):
        self.requests = []  # (method, path, headers, parsed body)
        self._scripts = {}
        self._lock = threading.Lock()
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _handle(self):
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                body = json.loads(raw) if raw else None
                with outer._lock:
                    outer.requests.append((self.command, self.path, dict(self.headers), body))
                    queue = outer._scripts.get((self.command, self.path))
                    item = None
                    if queue:
                        item = queue.pop(0) if len(queue) > 1 else queue[0]
                if item is None:
                    item = (599, {"error": f"unscripted {self.command} {self.path}"})
                if item == DROP:
                    self.close_connection = True
                    self.connection.close()
                    return
                if callable(item):
                    item = item(body)
                status, payload = item
                data = b"" if payload is None else json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = do_POST = _handle

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._httpd.daemon_threads = True
        self.url = f"http://127.0.0.1:{self._httpd.server_address[1]}"
        self._thread = threading.Thread(target=self._httpd.serve_forever,
                                        kwargs={"poll_interval": 0.01}, daemon=True)
        self._thread.start()

    def script(self, method, path, *items):
        with self._lock:
            self._scripts[(method, path)] = list(items)

    def calls(self, method, path):
        return [r for r in self.requests if r[0] == method and r[1] == path]

    def close(self):
        self._httpd.shutdown()
        self._httpd.server_close()


def fast_client(url, **kwargs):
    """A client that never sleeps, so retry tests run instantly."""
    client = Client(url, **kwargs)
    client._sleep = lambda seconds: None
    return client


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


SERVER_BIN = os.environ.get("GLIDER_SERVER_BIN")
requires_server = unittest.skipUnless(
    SERVER_BIN and os.path.exists(SERVER_BIN),
    "set GLIDER_SERVER_BIN to a glider-server binary to run integration tests",
)


class RealServer:
    """A glider-server process on a local data directory."""

    def __init__(self, dimensions, metric=None, token=None):
        self.dir = tempfile.mkdtemp(prefix="glider-py-test-")
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        env = {
            "PATH": os.environ.get("PATH", ""),
            "GLIDER_DATA_DIR": os.path.join(self.dir, "data"),
            "GLIDER_DIMENSIONS": str(dimensions),
            "GLIDER_LISTEN": f"127.0.0.1:{self.port}",
            "GLIDER_CACHE_DIR": os.path.join(self.dir, "cache"),
        }
        if metric:
            env["GLIDER_METRIC"] = metric
        if token:
            env["GLIDER_API_TOKEN"] = token
        self.log = open(os.path.join(self.dir, "server.log"), "wb")
        self.proc = subprocess.Popen(
            [SERVER_BIN], env=env, stdout=self.log, stderr=subprocess.STDOUT
        )
        try:
            self._wait_healthy()
        except BaseException:
            self.stop()
            raise

    def _wait_healthy(self):
        deadline = time.monotonic() + 30
        probe = Client(self.url, timeout=2)
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"glider-server exited with {self.proc.returncode}")
            if probe.health():
                return
            time.sleep(0.05)
        raise RuntimeError("glider-server did not become healthy within 30s")

    def stop(self):
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.log.close()
        shutil.rmtree(self.dir, ignore_errors=True)


def supports(client, method, path, body):
    """True when the server accepts this route and body (not 404/405/422)."""
    from glider_client import GliderError

    try:
        client._request(method, path, body)
    except GliderError as exc:
        return exc.status not in (404, 405, 422)
    return True


DIMS = 8


def fake_embed(texts):
    """Deterministic bag-of-words embedding: texts sharing words are close."""
    import hashlib

    vectors = []
    for text in texts:
        vector = [0.0] * DIMS
        for word in text.lower().split():
            vector[hashlib.sha256(word.encode()).digest()[0] % DIMS] += 1.0
        if not any(vector):
            vector[0] = 1.0
        vectors.append(vector)
    return vectors
