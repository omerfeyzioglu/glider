#!/usr/bin/env python3
"""Run S3 integration tests in an isolated, disposable MinIO container."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import secrets
import tempfile

# The legacy Quay repository no longer permits anonymous pulls. This digest
# contains MinIO RELEASE.2025-10-15T17-29-55Z and mc for isolated CI tests.
IMAGE = "ghcr.io/coollabsio/minio@sha256:69b55a1c1c5dc285ce04db96689f5b2102317fc77a50680a1874ca6efd1c87f9"

from minio_harness import container_scope, ready, run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark-smoke", type=Path, help="also validate and save local/S3 all-scenario smoke JSON in a new directory")
    parser.add_argument("--segment-benchmarks", type=Path, help="save local/S3 recovery measurements with and without a checkpoint in a new directory")
    parser.add_argument("--compaction-benchmarks", type=Path, help="save local/S3 checkpoint recovery measurements before and after compaction in a new directory")
    parser.add_argument("--search-smoke", type=Path, help="validate the M5 search matrix against the disposable MinIO service")
    parser.add_argument("--range-only", action="store_true", help="run only the M22 addressable-range integration test")
    parser.add_argument("--segmented-only", action="store_true", help="run only the M22 segmented publication recovery test")
    parser.add_argument("--segmented-capacity", type=Path, help="save one experimental segmented SIFT1M load/recovery probe")
    parser.add_argument("--segmented-cache", type=Path, help="save a targeted 250k-row segmented block-cache probe")
    parser.add_argument("--data", type=Path, help="directory containing verified SIFT1M prefix and query files")
    parser.add_argument("--consolidate-runs", action="store_true", help="enable bounded run-index consolidation in the capacity probe")
    parser.add_argument("--overwrite-half", action="store_true", help="overwrite even IDs once after the capacity load")
    parser.add_argument("--overwrite-prefix-half", action="store_true", help="overwrite the first half of IDs once after the capacity load")
    parser.add_argument("--prune-dead", action="store_true", help="prune fully dead segmented block references")
    parser.add_argument("--reclaim-packs", action="store_true", help="reclaim stale physical packs during the overwrite probe")
    args = parser.parse_args()
    range_only = args.range_only
    segmented_only = args.segmented_only
    capacity = args.segmented_capacity
    cache_probe = args.segmented_cache
    targeted = sum((range_only, segmented_only, capacity is not None,
                    cache_probe is not None))
    if targeted > 1 or (targeted and any((
            args.benchmark_smoke, args.segment_benchmarks,
            args.compaction_benchmarks, args.search_smoke))):
        parser.error("targeted segmented tests cannot be combined with other modes")
    if (capacity or cache_probe) and not args.data:
        parser.error("segmented probes require --data")
    if args.data and not (capacity or cache_probe):
        parser.error("--data requires a segmented probe")
    if args.consolidate_runs and not capacity:
        parser.error("--consolidate-runs requires --segmented-capacity")
    if args.overwrite_half and args.overwrite_prefix_half:
        parser.error("choose one overwrite pattern")
    if (args.overwrite_half or args.overwrite_prefix_half) and not args.consolidate_runs:
        parser.error("overwrite requires --consolidate-runs")
    if args.prune_dead and not args.overwrite_prefix_half:
        parser.error("--prune-dead requires --overwrite-prefix-half")
    if args.reclaim_packs and not (args.overwrite_half or args.overwrite_prefix_half):
        parser.error("--reclaim-packs requires an overwrite pattern")
    search_output = args.search_smoke
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
    if capacity or cache_probe:
        expected = {
            "sift1m_base_250000.fvecs": "fab6b3f6c68d8bca09c72b0ee84a8126b80aebc635765fb52c0ab3efbda51960",
            "sift1m_query.fvecs": "f7fc9be140accdfd64116c2fa2365ecdb69b8f084970c6b0532db5ff79ac8fdc",
        }
        for filename, digest in expected.items():
            hasher = hashlib.sha256()
            with (args.data / filename).open("rb") as source:
                while chunk := source.read(1024 * 1024):
                    hasher.update(chunk)
            if hasher.hexdigest() != digest:
                raise ValueError(f"unexpected SIFT1M digest: {filename}")
        run("cargo", "build", "--locked", "--release", "--features", "s3",
            "--example", "m22_capacity", env=env)
        if cache_probe:
            run("cargo", "build", "--locked", "--release", "--features", "s3",
                "--example", "m23_cache", env=env)
        (capacity or cache_probe).mkdir(parents=True, exist_ok=False)
    else:
        run("cargo", "test", "--locked", "--features", "s3", "--lib", "--no-run", env=env)
    if not (range_only or segmented_only or capacity or cache_probe):
        run("cargo", "test", "--locked", "--features", "s3", "--test", "failure_matrix", "--no-run", env=env)
        run("cargo", "test", "--locked", "--release", "--features", "s3",
            "--test", "segmented_crash", "--no-run", env=env)
        run("cargo", "test", "--locked", "--release", "--features", "s3",
            "--test", "takeover", "--no-run", env=env)
    if output or segment_output or compaction_output or search_output:
        run("cargo", "bench", "--locked", "--bench", "baseline", "--no-run", env=env)
        run("cargo", "bench", "--locked", "--features", "s3", "--bench", "baseline", "--no-run", env=env)
    with container_scope(name):
        run("docker", "run", "-d", "--name", name, "-p", "127.0.0.1::9000",
            "-e", "MINIO_ROOT_USER", "-e", "MINIO_ROOT_PASSWORD", IMAGE,
            "server", "/data", env=env, capture=True)
        port = run("docker", "port", name, "9000/tcp", capture=True).strip().split(":")[-1]
        endpoint = "http://127.0.0.1:" + port
        ready(endpoint, name)
        run("docker", "exec", name, "mc", "mb", "test/glider-test", capture=True)
        env.update(AWS_ACCESS_KEY_ID=env["MINIO_ROOT_USER"],
                   AWS_SECRET_ACCESS_KEY=env["MINIO_ROOT_PASSWORD"],
                   GLIDER_S3_ENDPOINT=endpoint, GLIDER_S3_BUCKET="glider-test")
        test_args = ("cargo", "test", "--locked", "--features", "s3", "--lib")
        if range_only:
            run(*test_args, "store::s3::tests::minio_addressable_payload_range_checks_length_and_bounds",
                "--", "--ignored", "--nocapture", env=env)
            print("M22 addressable-range MinIO test passed.", flush=True)
            return
        if segmented_only:
            run(*test_args, "segmented::tests::minio_segmented_publication_recovers_before_and_after_root_create",
                "--", "--ignored", "--nocapture", env=env)
            print("M22 segmented publication MinIO recovery test passed.", flush=True)
            return
        if capacity or cache_probe:
            namespace = "segmented-capacity-" + secrets.token_hex(6)
            command = ["target/release/examples/m22_capacity",
                       str(args.data / "sift1m_base_250000.fvecs"),
                       str(args.data / "sift1m_query.fvecs"), "250000", namespace]
            if args.consolidate_runs or cache_probe:
                command.append("--consolidate")
            if args.overwrite_half:
                command.append("--overwrite-half")
            if args.overwrite_prefix_half:
                command.append("--overwrite-prefix-half")
            if args.prune_dead:
                command.append("--prune-dead")
            if args.reclaim_packs:
                command.append("--reclaim-packs")
            raw = run(*command, env=env, capture=True, timeout=1800)
            result = json.loads(raw)
            if result["rows"] != 250000 or not result["exact_oracle_passed"]:
                raise ValueError("segmented capacity probe did not verify")
            if cache_probe:
                with tempfile.TemporaryDirectory(prefix="glider-m23-cache-") as cache_dir:
                    cache = json.loads(run("target/release/examples/m23_cache",
                                           str(args.data / "sift1m_query.fvecs"), namespace,
                                           cache_dir, env=env, capture=True, timeout=1800))
                if not cache["exact_results_equal"] or cache["exact_top10_ids"] != result["exact_top10_ids"]:
                    raise ValueError("segmented cache probe disagrees with exact oracle")
                result = {"load": result, "cache": cache}
            result["dataset_sha256"] = expected
            result["minio_image"] = IMAGE
            result["environment"] = platform.platform()
            result["hardware"] = (run("sysctl", "-n", "machdep.cpu.brand_string", capture=True).strip()
                                  if platform.system() == "Darwin" else platform.processor())
            result["rustc"] = run("rustc", "--version", capture=True).strip()
            result["docker_server_version"] = run("docker", "version", "--format", "{{.Server.Version}}", capture=True).strip()
            output_path = (capacity or cache_probe) / "run.json"
            with output_path.open("x") as output_file:
                json.dump(result, output_file, indent=2, sort_keys=True)
                output_file.write("\n")
            print(f"Experimental segmented probe saved to {output_path}.", flush=True)
            return
        run(*test_args, "segmented::tests::minio_segmented_publication_recovers_before_and_after_root_create",
            "--", "--ignored", "--nocapture", env=env)
        run("cargo", "test", "--locked", "--release", "--features", "s3",
            "--test", "segmented_crash", "minio_", "--", "--ignored", env=env)
        run("cargo", "test", "--locked", "--release", "--features", "s3",
            "--test", "takeover", "minio_", "--", "--ignored", env=env)
        run("cargo", "test", "--locked", "--release", "--features", "server",
            "--test", "collections", "minio_", "--", "--ignored", env=env)
        run(*test_args, "store::s3::tests::minio_", "--", "--ignored", "--nocapture", env=env)
        run(*test_args, "store::s3::tests::server_restart_prepare", "--", "--ignored", env=env)
        run("docker", "kill", "--signal", "KILL", name, capture=True)
        run("docker", "start", name, capture=True)
        # Docker may assign another ephemeral host port when starting again.
        port = run("docker", "port", name, "9000/tcp", capture=True).strip().split(":")[-1]
        env["GLIDER_S3_ENDPOINT"] = "http://127.0.0.1:" + port
        ready(env["GLIDER_S3_ENDPOINT"], name)
        run(*test_args, "store::s3::tests::server_restart_verify", "--", "--ignored", env=env)
        env.update(GLIDER_S3_REGION="us-east-1", GLIDER_S3_NAMESPACE="failure-matrix")
        run("cargo", "test", "--locked", "--features", "s3", "--test", "failure_matrix",
            "s3_process_crash_matrix", "--", "--ignored", "--nocapture", env=env)
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
        if search_output:
            env.update(GLIDER_S3_REGION="us-east-1", GLIDER_S3_NAMESPACE="search-smoke",
                       GLIDER_S3_SERVICE_LABEL=IMAGE + "; Docker " + run("docker", "version", "--format", "{{.Server.Version}}", capture=True).strip())
            # Child runner inherits only this isolated service's credentials.
            run("python3", "tools/search_benchmark.py", "--backend", "s3", "--smoke",
                "--output", str(search_output), "--label", "MinIO search smoke; desktop load uncontrolled", env=env)
        print("S3 integration and abrupt MinIO restart checks passed.", flush=True)


if __name__ == "__main__":
    main()
