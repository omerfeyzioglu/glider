#!/usr/bin/env python3
"""Local release-build crash, cache-loss, and backup/restore drills."""
import argparse
import http.client
import json
import os
from pathlib import Path
import random
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "target/release/glider-server"
ADMIN = ROOT / "target/release/glider-admin"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def request(port, method, path, body=None):
    payload = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", data=payload, method=method,
        headers={"content-type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.load(response)


def environment(data, cache, port):
    values = {**os.environ, "GLIDER_DATA_DIR": str(data), "GLIDER_CACHE_DIR": str(cache),
              "GLIDER_DIMENSIONS": "3", "GLIDER_METRIC": "squared_euclidean",
              "GLIDER_RESIDENT_FILTER": "drill=yes",
              "GLIDER_LISTEN": f"127.0.0.1:{port}"}
    values.pop("GLIDER_API_TOKEN", None)
    values.pop("GLIDER_CACHE_BYTES", None)
    return values


def start(env, log):
    process = subprocess.Popen([str(SERVER)], cwd=ROOT, env=env,
                               stdout=log, stderr=log)
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"server exited during startup: {process.returncode}")
        try:
            req = urllib.request.Request(f"http://{env['GLIDER_LISTEN']}/healthz")
            with urllib.request.urlopen(req, timeout=1) as response:
                if response.status == 200:
                    return process
        except urllib.error.URLError:
            time.sleep(0.05)
    raise RuntimeError("server did not become healthy")


def stop(process):
    process.send_signal(signal.SIGTERM)
    if process.wait(timeout=20) != 0:
        raise RuntimeError("server did not stop cleanly")


def admin(env, *args, expect_success=True):
    result = subprocess.run([str(ADMIN), *map(str, args)], cwd=ROOT, env=env,
                            text=True, capture_output=True, timeout=30)
    try:
        output = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"admin output was not JSON: {result.stdout!r}; {result.stderr}") from exc
    if expect_success and result.returncode:
        raise RuntimeError(f"admin {args}: {result.stderr.strip()}")
    if not expect_success and result.returncode == 0:
        raise RuntimeError(f"admin {args} unexpectedly succeeded")
    return output


def verify(port, ids):
    for point_id in ids:
        point = request(port, "GET", f"/v1/points/{point_id}")
        assert point["vector"] == [float(point_id), 0.0, 0.0], point
        assert point["metadata"] == {"drill": "yes"}, point
    hits = request(port, "POST", "/v1/query",
                   {"vector": [0, 0, 0], "k": 32, "filter": {"drill": "yes"}})
    found = {item["id"] for item in hits["results"]}
    assert set(ids) <= found, (ids, found)


def write_body(point_id, request_id):
    return {"upsert": [{"id": point_id, "vector": [point_id, 0, 0],
                        "metadata": {"drill": "yes"}}], "request_id": request_id}


def drill(seed):
    rng = random.Random(seed)
    with tempfile.TemporaryDirectory(prefix="glider-drills-") as temporary:
        base = Path(temporary)
        cache = base / "cache"
        old = base / "old"
        staged = base / "staged"
        backup = base / "backup"
        restored = base / "restored"
        log = (base / "server.log").open("w+")
        process = None
        stage = "kill"
        try:
            port = free_port()
            env = environment(old, cache, port)
            process = start(env, log)
            owner = admin(env, "status", expect_success=False)
            assert "already has an owner" in owner["error"], owner
            acknowledged = []
            for point_id in range(1, 9):
                boundary = request(port, "GET", "/v1/status")["sequence"]
                rid = {"boundary": boundary, "nonce": f"{rng.getrandbits(128):032x}"}
                answer = request(port, "POST", "/v1/write", write_body(point_id, rid))
                assert answer["conflict"] is None, answer
                acknowledged.append(point_id)
            print(f"kill drill: {len(acknowledged)} writes acknowledged before SIGKILL", flush=True)
            boundary = request(port, "GET", "/v1/status")["sequence"]
            uncertain_id = {"boundary": boundary, "nonce": f"{rng.getrandbits(128):032x}"}
            uncertain_body = write_body(9, uncertain_id)
            sent = threading.Event()
            submitted = threading.Event()
            submission_error = []
            def in_flight():
                connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
                try:
                    connection.request("POST", "/v1/write", body=json.dumps(uncertain_body),
                                       headers={"content-type": "application/json"})
                    submitted.set()
                    sent.set()
                    connection.getresponse().read()
                except OSError as error:
                    if not submitted.is_set():
                        submission_error.append(error)
                finally:
                    sent.set()
                    connection.close()

            thread = threading.Thread(target=in_flight)
            thread.start()
            if not sent.wait(2) or submission_error or not submitted.is_set():
                raise RuntimeError(f"uncertain request was not submitted: {submission_error}")
            process.kill()
            process.wait(timeout=10)
            thread.join(timeout=12)
            process = None
            # A forced exit leaves a claim. Restore to a fresh isolated namespace.
            staged_env = environment(staged, cache, port)
            admin(staged_env, "restore", old)
            process = start(staged_env, log)
            verify(port, acknowledged)
            first = request(port, "POST", "/v1/write", uncertain_body)
            second = request(port, "POST", "/v1/write", uncertain_body)
            assert first == second and first["conflict"] is None, (first, second)
            stable_ids = sorted(set(acknowledged + [9]))
            verify(port, stable_ids)
            print(f"PASS kill seed={seed}", flush=True)

            stage = "cache-loss"
            stop(process)
            process = None
            shutil.rmtree(cache, ignore_errors=True)
            process = start(staged_env, log)
            verify(port, stable_ids)
            print(f"PASS cache-loss seed={seed}", flush=True)

            stage = "backup/restore"
            stop(process)
            process = None
            result = admin(staged_env, "backup", backup)
            assert result["objects_copied"] > 0, result
            restored_env = environment(restored, cache, port)
            result = admin(restored_env, "restore", backup)
            assert result["sequence"] >= len(stable_ids), result
            process = start(restored_env, log)
            verify(port, stable_ids)
            stop(process)
            process = None
            print(f"PASS backup/restore seed={seed}", flush=True)
        except Exception as exc:
            log.flush()
            log.seek(0)
            print(f"FAIL {stage} seed={seed}: {exc}\nserver log:\n{log.read()}", file=sys.stderr)
            raise
        finally:
            if process is not None and process.poll() is None:
                process.kill()
                process.wait(timeout=10)
            log.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=29)
    args = parser.parse_args()
    subprocess.run(["cargo", "build", "--release", "--offline", "--features", "server",
                    "--bin", "glider-server", "--bin", "glider-admin"], cwd=ROOT, check=True)
    drill(args.seed)


if __name__ == "__main__":
    main()
