#!/usr/bin/env python3
"""Run the M22-M24 single-machine acceptance on disposable loopback MinIO.

Loads the verified 250,000-row SIFT1M prefix, measures fresh readiness,
static quality and independent read/write traffic in a new process, then
verifies restart state, update-wave quality, cache loss and backup in another
process. Gates are the M21 declarations; nothing here relaxes them.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import secrets
import tempfile
import time

from minio_harness import container_scope, ready, run
from test_s3 import IMAGE

DATA = {
    "sift1m_base_250000.fvecs": "fab6b3f6c68d8bca09c72b0ee84a8126b80aebc635765fb52c0ab3efbda51960",
    "sift1m_query.fvecs": "f7fc9be140accdfd64116c2fa2365ecdb69b8f084970c6b0532db5ff79ac8fdc",
}
ORACLE = "benchmarks/m24/layout-vector-local-seal/run.json"
MIB = 1024 * 1024
# US East (N. Virginia) S3 Standard list prices used for the physical-work
# cost model; see benchmarks/M24.md for the retrieval date and source.
PRICES = {"put_per_1000": 0.005, "get_per_1000": 0.0004, "list_per_1000": 0.005,
          "delete_per_1000": 0.0, "storage_gb_month": 0.023}


def digest(path):
    hasher = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1 << 20), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def gates(load, serve, verify, visible_bytes):
    seconds = serve["elapsed_seconds"]
    http = serve["http"]
    static = {entry["pass"]: entry for entry in serve["static_quality"]}
    first = static["first_empty_cache"]
    quality = [first["unfiltered"], first["filtered"],
               verify["update_wave_quality"]["unfiltered"], verify["update_wave_quality"]["filtered"]]
    checks = {
        "peak_engine_rss_le_64MiB": serve["peak_rss_bytes"] <= 64 * MIB,
        "cache_occupancy_le_256MiB": serve["cache"]["nvme_bytes"] <= 256 * MIB,
        "zero_lost_acknowledged_writes": verify["value_mismatches"] == 0
        and verify["live_documents"] == verify["expected_documents"],
        "zero_overload": serve["overloaded"]["writes"] == 0 and serve["overloaded"]["queries"] == 0,
        "zero_late_skips": serve["late_slots_skipped"]["writes"] == 0
        and serve["late_slots_skipped"]["queries"] == 0,
        "write_p95_le_150ms": serve["write_ms"]["p95"] <= 150,
        "warm_query_p95_le_50ms": serve["unfiltered_warm_query_ms"].get("p95", 0) <= 50
        and serve["filtered_query_ms"]["p95"] <= 50,
        "cold_query_p95_le_200ms": serve["unfiltered_cold_query_ms"].get("p95", 0) <= 200,
        "fresh_open_le_1s": serve["open"]["ms"] <= 1000 and verify["reopen"]["ms"] <= 1000,
        "mean_recall_ge_0_90": all(q["mean_recall_at_10"] >= 0.90 for q in quality),
        "fifth_percentile_recall_ge_0_80": all(q["fifth_percentile_recall_at_10"] >= 0.80 for q in quality),
        "short_results_lt_1_percent": all(q["short_results"] < 0.01 * q.get("queries", 200) for q in quality)
        and serve["short_results"] < 0.01 * serve["acknowledged"]["queries"],
        "put_per_second_le_6": http["put"] / seconds <= 6,
        "delete_per_second_le_6": http["delete"] / seconds <= 6,
        "upload_le_2MiB_per_second": http["request_body_bytes"] / seconds <= 2 * MIB,
        "cold_query_le_8_get_1MiB": serve["query_remote_reads"]["max"] <= 8
        and serve["query_remote_payload_bytes"]["max"] <= MIB,
        "list_le_1_per_minute": http["list"] <= max(1, seconds / 60),
        "visible_payload_le_1GiB": visible_bytes <= 1024 * MIB,
        "cache_loss_preserves_results": verify["cache_loss_results_equal"],
        "backup_restores_committed_view": verify["backup"]["backup_restored_equal"],
        "no_maintenance_errors": serve["maintenance_errors"] == 0,
    }
    return checks


def cost(serve, visible_bytes):
    seconds = serve["elapsed_seconds"]
    month = 30 * 24 * 3600 / seconds
    http = serve["http"]
    counts = {kind: http[kind] * month for kind in ("put", "get", "list", "delete")}
    dollars = {
        "put": counts["put"] / 1000 * PRICES["put_per_1000"],
        "get": counts["get"] / 1000 * PRICES["get_per_1000"],
        "list": counts["list"] / 1000 * PRICES["list_per_1000"],
        "delete": counts["delete"] / 1000 * PRICES["delete_per_1000"],
        "storage": visible_bytes / 1e9 * PRICES["storage_gb_month"],
    }
    return {"prices_usd": PRICES, "thirty_day_requests": counts,
            "thirty_day_upload_bytes": http["request_body_bytes"] * month,
            "thirty_day_get_payload_bytes": serve["get_payload_bytes"] * month,
            "thirty_day_usd": dollars, "thirty_day_usd_total": sum(dollars.values()),
            "note": "same-region transfer assumed; excludes compute, NVMe and egress"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="new directory for run.json")
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=300, help="seconds of offered load")
    args = parser.parse_args()
    for name, expected in DATA.items():
        if digest(args.data / name) != expected:
            raise ValueError(f"unexpected SIFT1M digest: {name}")
    args.output.mkdir(parents=True, exist_ok=False)
    # Identify the measured code before building it.
    sources = {path: digest(Path(path)) for path in (
        "examples/m24_acceptance.rs", "src/segmented.rs", "src/segmented/sketch.rs",
        "src/segmented/serving.rs", "src/segmented/cache.rs", "src/segmented/directory.rs",
        "src/admission.rs", "src/store.rs", "src/store/s3.rs")}
    revision = run("git", "rev-parse", "HEAD", capture=True).strip()
    dirty = bool(run("git", "status", "--porcelain", capture=True).strip())
    base, query = str(args.data / "sift1m_base_250000.fvecs"), str(args.data / "sift1m_query.fvecs")
    env = {k: v for k, v in os.environ.items() if not k.startswith(("AWS_", "GLIDER_S3_", "MINIO_"))}
    env.update(MINIO_ROOT_USER="glider-" + secrets.token_hex(8), MINIO_ROOT_PASSWORD=secrets.token_hex(24))
    run("cargo", "build", "--locked", "--release", "--features", "s3,experimental-segmented",
        "--example", "m24_acceptance", env=env)
    binary = "target/release/examples/m24_acceptance"
    name = "glider-m24-" + secrets.token_hex(6)
    namespace = "m24-acceptance-" + secrets.token_hex(6)
    with container_scope(name):
        run("docker", "run", "-d", "--name", name, "-p", "127.0.0.1::9000",
            "-e", "MINIO_ROOT_USER", "-e", "MINIO_ROOT_PASSWORD", IMAGE, "server", "/data",
            env=env, capture=True)
        port = run("docker", "port", name, "9000/tcp", capture=True).strip().split(":")[-1]
        endpoint = "http://127.0.0.1:" + port
        ready(endpoint, name)
        # S3 signing rejects requests when the Docker VM clock lags the host,
        # which happens briefly after the host sleeps; wait for it to resync.
        for _ in range(60):
            server = int(run("docker", "exec", name, "date", "-u", "+%s", capture=True).strip())
            if abs(server - int(time.time())) <= 5:
                break
            time.sleep(5)
        else:
            raise RuntimeError("Docker VM clock is not synchronized with the host")
        run("docker", "exec", name, "mc", "mb", "test/glider-test", capture=True)
        env.update(AWS_ACCESS_KEY_ID=env["MINIO_ROOT_USER"], AWS_SECRET_ACCESS_KEY=env["MINIO_ROOT_PASSWORD"],
                   GLIDER_S3_ENDPOINT=endpoint, GLIDER_S3_BUCKET="glider-test")
        load = json.loads(run(binary, "load", base, namespace, env=env, capture=True, timeout=1800))
        with tempfile.TemporaryDirectory(prefix="glider-m24-cache-") as cache:
            raw = run(binary, "serve", query, base, namespace, cache, ORACLE, str(args.rounds),
                      env=env, capture=True, timeout=args.rounds + 1800)
            serve = json.loads(raw)
            serve_path = Path(cache) / "serve.json"
            serve_path.write_text(raw)
            usage = json.loads(run("docker", "exec", name, "mc", "du", "--json",
                                   f"test/glider-test/{namespace}", capture=True))
            verify = json.loads(run(binary, "verify", query, base, namespace, cache, str(serve_path),
                                    namespace + "-backup", env=env, capture=True, timeout=3600))
    visible = usage["size"]
    batches = serve.pop("acknowledged_batches")
    serve["acknowledged_batch_count"] = len(batches)
    checks = gates(load, serve, verify, visible)
    result = {
        "version": 1, "dataset": "SIFT1M-250000-prefix-rotate137-v1", "rows": 250000, "dimensions": 128,
        "metric": "squared_euclidean", "k": 10, "filter": "cohort=one-percent (id % 100 == 0)",
        "backend": "loopback-minio", "minio_image": IMAGE, "dataset_sha256": DATA,
        "oracle": ORACLE, "oracle_sha256": digest(Path(ORACLE)),
        "source_sha256": sources, "git_revision": revision, "working_tree_dirty": dirty,
        "load": load, "serve": serve, "verify": verify,
        "visible_payload_bytes_after_serve": visible,
        "gates": checks, "accepted": all(checks.values()), "cost_model": cost(serve, visible),
        "environment": platform.platform(),
        "hardware": (run("sysctl", "-n", "machdep.cpu.brand_string", capture=True).strip()
                     if platform.system() == "Darwin" else platform.processor()),
        "rustc": run("rustc", "--version", capture=True).strip(),
        "docker_server_version": run("docker", "version", "--format", "{{.Server.Version}}", capture=True).strip(),
    }
    output = args.output / "run.json"
    with output.open("x") as file:
        json.dump(result, file, indent=2, sort_keys=True)
        file.write("\n")
    failed = [name for name, passed in checks.items() if not passed]
    print(f"Saved {output}; accepted={result['accepted']}; failed gates: {failed}", flush=True)


if __name__ == "__main__":
    main()
