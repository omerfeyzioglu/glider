#!/usr/bin/env python3
"""Run S3 integration tests in an isolated, disposable MinIO container."""
import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess
import time
import urllib.error
import urllib.request

IMAGE = "quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"


def run(*args, env=None, capture=False):
    return subprocess.run(args, env=env, check=True, text=True,
                          stdout=subprocess.PIPE if capture else None).stdout


def ready(endpoint):
    # Bounded service-start readiness polling, never retries database operations.
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(endpoint + "/minio/health/ready", timeout=1) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            pass
        time.sleep(0.1)
    raise RuntimeError("MinIO did not become ready within 30 seconds")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark-smoke", type=Path, help="also validate and save local/S3 all-scenario smoke JSON in a new directory")
    parser.add_argument("--segment-benchmarks", type=Path, help="save local/S3 recovery measurements with and without a checkpoint in a new directory")
    parser.add_argument("--compaction-benchmarks", type=Path, help="save local/S3 checkpoint recovery measurements before and after compaction in a new directory")
    args = parser.parse_args()
    compaction_output = args.compaction_benchmarks
    if compaction_output:
        compaction_output.mkdir(parents=True, exist_ok=False)
    segment_output = args.segment_benchmarks
    if segment_output:
        segment_output.mkdir(parents=True, exist_ok=False)
    output = args.benchmark_smoke
    if output:
        output.mkdir(parents=True, exist_ok=False)
    name = "glider-m2-" + secrets.token_hex(6)
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("AWS_", "GLIDER_S3_", "MINIO_"))}
    env.update(MINIO_ROOT_USER="glider-" + secrets.token_hex(8), MINIO_ROOT_PASSWORD=secrets.token_hex(24))
    run("cargo", "test", "--locked", "--features", "s3", "--lib", "--no-run", env=env)
    if output or segment_output or compaction_output:
        run("cargo", "bench", "--locked", "--bench", "baseline", "--no-run", env=env)
        run("cargo", "bench", "--locked", "--features", "s3", "--bench", "baseline", "--no-run", env=env)
    try:
        run("docker", "run", "-d", "--name", name, "-p", "127.0.0.1::9000",
            "-e", "MINIO_ROOT_USER", "-e", "MINIO_ROOT_PASSWORD", IMAGE,
            "server", "/data", env=env, capture=True)
        port = run("docker", "port", name, "9000/tcp", capture=True).strip().split(":")[-1]
        endpoint = "http://127.0.0.1:" + port
        ready(endpoint)
        run("docker", "exec", name, "/bin/sh", "-c",
            'mc alias set test http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc mb test/glider-test',
            capture=True)
        env.update(AWS_ACCESS_KEY_ID=env["MINIO_ROOT_USER"],
                   AWS_SECRET_ACCESS_KEY=env["MINIO_ROOT_PASSWORD"],
                   GLIDER_S3_ENDPOINT=endpoint, GLIDER_S3_BUCKET="glider-test")
        args = ("cargo", "test", "--locked", "--features", "s3", "--lib")
        run(*args, "store::s3::tests::minio_", "--", "--ignored", "--nocapture", env=env)
        run(*args, "store::s3::tests::server_restart_prepare", "--", "--ignored", env=env)
        run("docker", "kill", "--signal", "KILL", name, capture=True)
        run("docker", "start", name, capture=True)
        # Docker may assign another ephemeral host port when starting again.
        port = run("docker", "port", name, "9000/tcp", capture=True).strip().split(":")[-1]
        env["GLIDER_S3_ENDPOINT"] = "http://127.0.0.1:" + port
        ready(env["GLIDER_S3_ENDPOINT"])
        run(*args, "store::s3::tests::server_restart_verify", "--", "--ignored", env=env)
        if output:
            from benchmark_smoke import validate
            env.update(GLIDER_S3_REGION="us-east-1", GLIDER_S3_NAMESPACE="benchmark-smoke",
                       GLIDER_S3_SERVICE_LABEL=IMAGE + "; Docker " + run("docker", "version", "--format", "{{.Server.Version}}", capture=True).strip())
            for backend in ("local", "s3"):
                command = ["cargo", "bench", "--locked"]
                if backend == "s3":
                    command += ["--features", "s3"]
                command += ["--bench", "baseline", "--", "--backend", backend,
                            "--rows", "10", "--dimensions", "4", "--mutations", "30",
                            "--operations", "5", "--queries", "5", "--samples", "2", "--k", "3",
                            "--seed", "42", "--feature", "s3-benchmarks", "--phase", "baseline",
                            "--comparison-group", "backend-smoke", "--root", "target",
                            "--label", "smoke validation; desktop session; power and competing load uncontrolled"]
                raw = run(*command, env=env, capture=True)
                validate(json.loads(raw), backend, [env["AWS_ACCESS_KEY_ID"], env["AWS_SECRET_ACCESS_KEY"]])
                with (output / (backend + ".json")).open("x") as file:
                    file.write(raw)
            print("Local and S3 benchmark smoke JSON validated.", flush=True)
        if segment_output:
            env.update(GLIDER_S3_REGION="us-east-1", GLIDER_S3_NAMESPACE="segment-benchmark",
                       GLIDER_S3_SERVICE_LABEL=IMAGE + "; Docker " + run("docker", "version", "--format", "{{.Server.Version}}", capture=True).strip())
            for backend in ("local", "s3"):
                for checkpoint in (0, 270):
                    command = ["cargo", "bench", "--locked"]
                    if backend == "s3":
                        command += ["--features", "s3"]
                    phase = "before" if checkpoint == 0 else "after"
                    command += ["--bench", "baseline", "--", "--scenario", "recovery", "--backend", backend,
                                "--rows", "30", "--dimensions", "32", "--mutations", "300", "--samples", "5",
                                "--checkpoint-at", str(checkpoint), "--feature", "segments", "--phase", phase,
                                "--comparison-group", "m3-recovery-300", "--root", "target",
                                "--label", "M3 recovery experiment; desktop load and power uncontrolled"]
                    raw = run(*command, env=env, capture=True)
                    document = json.loads(raw)
                    result = document["results"][0]
                    expected_gets = 301 if checkpoint == 0 else 32
                    for counts in result["measured_store_calls_per_sample"]:
                        assert counts["get_calls"] == expected_gets
                        assert counts["list_calls"] == 1
                        assert counts["create_calls"] == 0
                    if checkpoint:
                        assert result["checkpoint"]["sequence"] == checkpoint
                        assert result["checkpoint"]["creates"] == 1
                        assert result["checkpoint"]["logical_bytes_written"] > 0
                    if backend == "s3":
                        for counts in result["http_requests_per_sample"]:
                            assert counts["get"] == expected_gets and counts["list"] == 1
                            assert counts["put"] == counts["http_errors"] == counts["transport_errors"] == 0
                        if checkpoint:
                            assert result["checkpoint"]["http_requests"]["put"] == 1
                    for secret in (env["AWS_ACCESS_KEY_ID"], env["AWS_SECRET_ACCESS_KEY"]):
                        assert secret not in raw
                    with (segment_output / f"{backend}-{phase}.json").open("x") as file:
                        file.write(raw)
            print("Segment and full-replay recovery measurements validated.", flush=True)
        if compaction_output:
            env.update(GLIDER_S3_REGION="us-east-1", GLIDER_S3_NAMESPACE="compaction-benchmark",
                       GLIDER_S3_SERVICE_LABEL=IMAGE + "; Docker " + run("docker", "version", "--format", "{{.Server.Version}}", capture=True).strip())
            from compaction_benchmark import measure
            for backend in ("local", "s3"):
                hashes = []
                for compact in (False, True):
                    raw = measure(backend, compact, env=env,
                                  secrets=(env["AWS_ACCESS_KEY_ID"], env["AWS_SECRET_ACCESS_KEY"]))
                    hashes.append(json.loads(raw)["results"][0]["dataset_sha256"])
                    phase = "after" if compact else "before"
                    with (compaction_output / f"{backend}-{phase}.json").open("x") as file:
                        file.write(raw)
                assert hashes[0] == hashes[1]
            print("Compaction footprint, amplification and recovery measurements validated.", flush=True)
        print("S3 integration and abrupt MinIO restart checks passed.", flush=True)
    finally:
        subprocess.run(["docker", "rm", "-fv", name], check=False, stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
