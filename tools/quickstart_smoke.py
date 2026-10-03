#!/usr/bin/env python3
"""Exercise the Docker Compose demo in docs/INSTALL.md and offline admin status command."""

import json
from pathlib import Path
import subprocess
import sys
import time
import urllib.error
import urllib.request


ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "-p", "glider-smoke"]
BASE = "http://localhost:8080"


def compose(*args):
    return subprocess.run([*COMPOSE, *args], cwd=ROOT, check=True)


def request(method, path, body=None):
    payload = json.dumps(body).encode() if body is not None else None
    with urllib.request.urlopen(
        urllib.request.Request(
            BASE + path, data=payload, method=method,
            headers={"content-type": "application/json"},
        ), timeout=5,
    ) as response:
        return json.load(response)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def wait_for_health():
    deadline = time.monotonic() + 120
    while True:
        try:
            with urllib.request.urlopen(BASE + "/healthz", timeout=2) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.HTTPError):
            pass
        if time.monotonic() >= deadline:
            raise RuntimeError("server did not become healthy within 120 seconds")
        time.sleep(1)


def smoke():
    compose("up", "--build", "-d")
    wait_for_health()
    write = request("POST", "/v1/write", {
        "upsert": [
            {"id": 1, "vector": [0, 0, 0], "metadata": {"color": "red"}},
            {"id": 2, "vector": [1, 1, 1]},
        ],
    })
    require("sequence" in write and "request_id" in write,
            f"write response lacks sequence or request_id: {write!r}")
    hits = request("POST", "/v1/query", {
        "vector": [1, 1, 0.9], "k": 2, "include_metadata": True,
    })["results"]
    require([hit["id"] for hit in hits] == [2, 1],
            f"quickstart query returned unexpected IDs: {hits!r}")
    require(hits[1].get("metadata") == {"color": "red"},
            f"id 1 query metadata is wrong: {hits[1]!r}")
    filtered = request("POST", "/v1/query", {
        "vector": [1, 1, 1], "k": 2, "filter": {"color": "red"},
    })["results"]
    require([hit["id"] for hit in filtered] == [1],
            f"filtered query returned unexpected IDs: {filtered!r}")
    point = request("GET", "/v1/points/1")
    require(point.get("id") == 1 and point.get("vector") == [0, 0, 0]
            and point.get("metadata") == {"color": "red"},
            f"GET /v1/points/1 returned unexpected point: {point!r}")
    compose("stop", "glider")
    compose("run", "--rm", "--no-deps", "--entrypoint", "glider-admin", "glider", "status")


def main():
    failed = False
    try:
        smoke()
        print("PASS Docker Compose quickstart and offline admin status")
    except Exception as exc:
        failed = True
        print(f"FAIL Docker Compose quickstart: {exc}", file=sys.stderr)
        try:
            compose("logs")
        except Exception as log_exc:
            print(f"Could not collect Compose logs: {log_exc}", file=sys.stderr)
    finally:
        try:
            compose("down", "-v")
        except Exception as cleanup_exc:
            failed = True
            print(f"FAIL Compose cleanup: {cleanup_exc}", file=sys.stderr)
            try:
                compose("logs")
            except Exception as log_exc:
                print(f"Could not collect Compose logs: {log_exc}", file=sys.stderr)
    return int(failed)


if __name__ == "__main__":
    sys.exit(main())
