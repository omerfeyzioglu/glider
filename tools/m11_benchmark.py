#!/usr/bin/env python3
"""Run the focused M8 filtered streaming probe on disposable loopback MinIO."""
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys

from test_s3 import IMAGE, ready, run


def main():
    if len(sys.argv) not in (2, 3) or (len(sys.argv) == 3 and sys.argv[2] != "--full"):
        raise SystemExit("usage: python3 tools/m11_benchmark.py NEW_REPORT.json [--full]")
    full = len(sys.argv) == 3
    output = Path(sys.argv[1])
    if output.exists():
        raise SystemExit("report path already exists")
    name = "glider-m11-" + secrets.token_hex(6)
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("AWS_", "GLIDER_S3_", "MINIO_"))}
    env.update(MINIO_ROOT_USER="glider-" + secrets.token_hex(8),
               MINIO_ROOT_PASSWORD=secrets.token_hex(24))
    env.pop("GLIDER_M11_FULL", None)
    if full:
        env["GLIDER_M11_FULL"] = "1"
    run("cargo", "build", "--release", "--locked", "--features", "s3",
        "--example", "m11_filter_probe", env=env)
    try:
        run("docker", "run", "-d", "--name", name, "-p", "127.0.0.1::9000",
            "-e", "MINIO_ROOT_USER", "-e", "MINIO_ROOT_PASSWORD", IMAGE,
            "server", "/data", env=env, capture=True)
        port = run("docker", "port", name, "9000/tcp", capture=True).strip().split(":")[-1]
        endpoint = "http://127.0.0.1:" + port
        ready(endpoint, name)
        run("docker", "exec", name, "mc", "mb", "test/glider-test", capture=True)
        env.update(AWS_ACCESS_KEY_ID=env["MINIO_ROOT_USER"],
                   AWS_SECRET_ACCESS_KEY=env["MINIO_ROOT_PASSWORD"],
                   GLIDER_S3_ENDPOINT=endpoint, GLIDER_S3_BUCKET="glider-test",
                   GLIDER_S3_REGION="us-east-1", GLIDER_S3_NAMESPACE="m11-filter",
                   GLIDER_S3_SERVICE_LABEL=IMAGE + "; loopback Docker " +
                   run("docker", "version", "--format", "{{.Server.Version}}", capture=True).strip())
        raw = run("target/release/examples/m11_filter_probe", "s3", env=env, capture=True)
        report = json.loads(raw)
        assert report["backend"] == "s3"
        assert report["after_filtered"]["get_calls"] == 0
        assert report["after_filtered"]["logical_bytes_read"] == 0
        baseline_queries = 1000 if full else 24
        assert report["before_filtered"]["get_calls"] == 12 * baseline_queries
        assert report["before_filtered"]["sample_count"] == baseline_queries
        assert report["after_filtered"]["sample_count"] == 1000
        assert report["before_unfiltered"]["get_calls"] == report["after_unfiltered"]["get_calls"]
        assert not any(secret in raw for secret in (env["AWS_ACCESS_KEY_ID"],
                                                   env["AWS_SECRET_ACCESS_KEY"]))
        output.write_text(raw)
        for phase in ("before_filtered", "after_filtered", "before_unfiltered", "after_unfiltered"):
            item = report[phase]
            print(phase, "p95_ms=", item["p95_ns"] / 1e6,
                  "GETs=", item["get_calls"], flush=True)
    except subprocess.CalledProcessError:
        print(run("docker", "inspect", "--format", "{{json .State}}", name, capture=True),
              file=sys.stderr)
        raise
    finally:
        subprocess.run(["docker", "rm", "-fv", name], check=False,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
