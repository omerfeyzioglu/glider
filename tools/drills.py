#!/usr/bin/env python3
"""Local release-build crash, paused-writer, cache-loss, backup/restore, conversion and
automatic conversion drills."""
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
              "GLIDER_RESIDENT_FILTER": "drill=yes", "GLIDER_LEASE_SECONDS": "1",
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
            # The listener accepts connections before the server holds the
            # lease; until then requests time out.
            req = urllib.request.Request(f"http://{env['GLIDER_LISTEN']}/healthz")
            with urllib.request.urlopen(req, timeout=1) as response:
                if response.status == 200:
                    return process
        except OSError:
            time.sleep(0.05)
    process.kill()
    process.wait(timeout=10)
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


def absent(port, point_id):
    try:
        request(port, "GET", f"/v1/points/{point_id}")
    except urllib.error.HTTPError as error:
        return error.code == 404
    return False


def write_body(point_id, request_id):
    return {"upsert": [{"id": point_id, "vector": [point_id, 0, 0],
                        "metadata": {"drill": "yes"}}], "request_id": request_id}


def write_refusal(port, body):
    """How the server refused the write, or None if it acknowledged it."""
    try:
        request(port, "POST", "/v1/write", body)
    except urllib.error.HTTPError as error:
        return f"HTTP {error.code}: {error.read().decode(errors='replace')}"
    except OSError as error:
        return f"connection failed: {error}"
    return None


def drill(seed):
    rng = random.Random(seed)

    def request_id(port):
        boundary = request(port, "GET", "/v1/status")["sequence"]
        return {"boundary": boundary, "nonce": f"{rng.getrandbits(128):032x}"}

    with tempfile.TemporaryDirectory(prefix="glider-drills-") as temporary:
        base = Path(temporary)
        cache = base / "cache"
        data = base / "data"
        backup = base / "backup"
        restored = base / "restored"
        log = (base / "server.log").open("w+")
        process = None
        paused = None
        stage = "kill"
        try:
            port = free_port()
            env = environment(data, cache, port)
            process = start(env, log)
            owner = admin(env, "status", expect_success=False)
            assert "lease" in owner["error"], owner
            acknowledged = []
            for point_id in range(1, 9):
                answer = request(port, "POST", "/v1/write",
                                 write_body(point_id, request_id(port)))
                assert "conflict" not in answer, answer
                acknowledged.append(point_id)
            print(f"kill drill: {len(acknowledged)} writes acknowledged before SIGKILL", flush=True)
            uncertain_body = write_body(9, request_id(port))
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
            # No operator step: the restart waits out the killed server's
            # lease, then takes over the same namespace and fences it.
            process = start(env, log)
            verify(port, acknowledged)
            first = request(port, "POST", "/v1/write", uncertain_body)
            second = request(port, "POST", "/v1/write", uncertain_body)
            assert first == second and "conflict" not in first, (first, second)
            stable_ids = sorted(set(acknowledged + [9]))
            verify(port, stable_ids)
            print(f"PASS kill seed={seed}", flush=True)

            stage = "paused-writer"
            # Freeze the server beyond its lease; a second server takes over
            # the same namespace. The resumed server must not publish.
            process.send_signal(signal.SIGSTOP)
            paused, process = process, None
            old_port, port = port, free_port()
            cache = base / "cache-2"
            env = environment(data, cache, port)
            process = start(env, log)
            verify(port, stable_ids)
            answer = request(port, "POST", "/v1/write", write_body(10, request_id(port)))
            assert "conflict" not in answer, answer
            stable_ids.append(10)
            fenced_body = write_body(11, {"boundary": 0,
                                          "nonce": f"{rng.getrandbits(128):032x}"})
            paused.send_signal(signal.SIGCONT)
            refusal = write_refusal(old_port, fenced_body)
            assert refusal is not None, "a fenced server acknowledged a write"
            print(f"paused-writer drill: resumed server refused the write ({refusal})", flush=True)
            # Its next lease renewal finds the takeover, so it stops.
            code = paused.wait(timeout=20)
            assert code != 0, code
            paused = None
            verify(port, stable_ids)
            assert absent(port, 11), "the fenced write exists"
            print(f"PASS paused-writer seed={seed}", flush=True)

            stage = "cache-loss"
            stop(process)
            process = None
            shutil.rmtree(cache, ignore_errors=True)
            process = start(env, log)
            verify(port, stable_ids)
            print(f"PASS cache-loss seed={seed}", flush=True)

            stage = "backup/restore"
            stop(process)
            process = None
            result = admin(env, "backup", backup)
            assert result["objects_copied"] > 0, result
            restored_env = environment(restored, cache, port)
            result = admin(restored_env, "restore", backup)
            assert result["sequence"] >= len(stable_ids), result
            process = start(restored_env, log)
            verify(port, stable_ids)
            stop(process)
            process = None
            print(f"PASS backup/restore seed={seed}", flush=True)

            stage = "convert"
            result = admin(restored_env, "convert", 2)
            assert result["summary"]["epoch"] == 1, result
            assert result["summary"]["rows"] >= len(stable_ids), result
            assert admin(restored_env, "status")["clustered_epoch"] == 1
            process = start(restored_env, log)
            verify(port, stable_ids)
            hits = request(port, "POST", "/v1/query", {"vector": [0, 0, 0], "k": 64})
            assert set(stable_ids) <= {item["id"] for item in hits["results"]}, hits
            stop(process)
            process = None
            print(f"PASS convert seed={seed}", flush=True)

            stage = "auto-convert"
            # The unconverted namespace converts itself as idle maintenance
            # once a seal (at 32 log objects) leaves enough live rows.
            auto_env = {**env, "GLIDER_AUTO_CLUSTER_ROWS": "16"}
            process = start(auto_env, log)
            status = request(port, "GET", "/v1/status")["clustering"]
            assert status["state"] == "none" and status["auto_cluster_rows"] == 16, status
            auto_ids = []
            for point_id in range(100, 140):
                answer = request(port, "POST", "/v1/write",
                                 write_body(point_id, request_id(port)))
                assert "conflict" not in answer, answer
                auto_ids.append(point_id)
            deadline = time.monotonic() + 30
            while request(port, "GET", "/v1/status")["clustering"]["state"] != "clustered":
                if time.monotonic() > deadline:
                    raise RuntimeError("automatic conversion did not publish")
                time.sleep(0.05)
            def verify_auto():
                verify(port, stable_ids)
                for point_id in auto_ids:
                    point = request(port, "GET", f"/v1/points/{point_id}")
                    assert point["vector"] == [float(point_id), 0.0, 0.0], point
                hits = request(port, "POST", "/v1/query", {"vector": [0, 0, 0], "k": 64})
                found = {item["id"] for item in hits["results"]}
                assert set(stable_ids + auto_ids) <= found, hits
            verify_auto()
            stop(process)
            process = None
            process = start(env, log)
            status = request(port, "GET", "/v1/status")["clustering"]
            assert status["state"] == "clustered" and status["epoch"] == 1, status
            assert status["centroids"] == 1 and status["auto_recluster_factor"] == 4, status
            verify_auto()
            stop(process)
            process = None
            print(f"PASS auto-convert seed={seed}", flush=True)
        except Exception as exc:
            log.flush()
            log.seek(0)
            print(f"FAIL {stage} seed={seed}: {exc}\nserver log:\n{log.read()}", file=sys.stderr)
            raise
        finally:
            for running in (process, paused):
                if running is not None and running.poll() is None:
                    running.send_signal(signal.SIGCONT)
                    running.kill()
                    running.wait(timeout=10)
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
