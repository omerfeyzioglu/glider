#!/usr/bin/env python3
"""Measure the M8 long-tail recovery input with individual and batched writes."""
import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess

from test_s3 import IMAGE, ready, run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="new directory for raw JSON reports")
    parser.add_argument("--compaction-only", action="store_true")
    parser.add_argument("--chunked-only", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    name = "glider-m10-" + secrets.token_hex(6)
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("AWS_", "GLIDER_S3_", "MINIO_"))}
    env.update(MINIO_ROOT_USER="glider-" + secrets.token_hex(8),
               MINIO_ROOT_PASSWORD=secrets.token_hex(24))
    run("cargo", "bench", "--locked", "--features", "s3", "--bench", "baseline", "--no-run", env=env)
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
                   GLIDER_S3_REGION="us-east-1", GLIDER_S3_NAMESPACE="m10-recovery",
                   GLIDER_S3_SERVICE_LABEL=IMAGE + "; Docker " +
                   run("docker", "version", "--format", "{{.Server.Version}}", capture=True).strip())
        cases = ((100, 2000, True, "live-2000-chunked"),) if args.chunked_only else (
            (100, 200, True, "compacted"),
            (100, 2000, True, "live-2000-compacted"),
            (100, 2000, True, "live-2000-chunked"))
        if not (args.compaction_only or args.chunked_only):
            cases = ((1, 200, False, "before"),
                     (100, 200, False, "after")) + cases
        for batch_size, rows, compact, label in cases:
            phase = "before" if label == "before" else "after"
            command = ("cargo", "bench", "--locked", "--features", "s3", "--bench", "baseline",
                      "--", "--backend", "s3", "--scenario", "recovery", "--rows", str(rows),
                      "--dimensions", "64", "--mutations", "2000", "--batch-size", str(batch_size),
                      "--samples", "3", "--seed", "42", "--feature", "m10",
                      "--phase", phase, "--comparison-group", "m10-batch-100",
                      "--root", "target", "--label", "M10 loopback MinIO targeted recovery")
            if compact:
                command += ("--compact-at", "2000")
            if label == "live-2000-chunked":
                command += ("--chunk-bytes", "131072")
            raw = run(*command, env=env, capture=True)
            report = json.loads(raw)
            result = report["results"][0]
            assert result["dataset_sha256"]
            assert result["durable_mutation_objects"] == 2000 // batch_size
            expected_gets = None if label == "live-2000-chunked" else (
                2 if compact else 1 + 2000 // batch_size)
            assert all(counts["get_calls"] <= 64 if expected_gets is None
                       else counts["get_calls"] == expected_gets
                       for counts in result["measured_store_calls_per_sample"])
            assert not any(secret in raw for secret in (env["AWS_ACCESS_KEY_ID"],
                                                       env["AWS_SECRET_ACCESS_KEY"]))
            (args.output / (label + ".json")).write_text(raw)
            print(label, "p95_ms=", result["total_open"]["p95_sample_ns"] / 1e6,
                  "get=", result["measured_store_calls_per_sample"][0]["get_calls"], flush=True)
    finally:
        subprocess.run(["docker", "rm", "-fv", name], check=False,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
