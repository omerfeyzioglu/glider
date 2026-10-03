#!/usr/bin/env python3
"""One-process, many-collection glider-server benchmark (stdlib client only).

The runner owns the server but never deletes its storage namespace. Use a fresh
GLIDER_DATA_DIR or GLIDER_S3_NAMESPACE for each run.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import json
import math
import mmap
import os
from pathlib import Path
import platform
import random
import re
import resource
import secrets
import shlex
import signal
import struct
import subprocess
import sys
import threading
import time
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "clients" / "python"))
from glider_client import Client, GliderError  # noqa: E402

DIMENSIONS = 128
BATCH_SIZE = 100
WARM_QUERIES = 5000
RECALL_QUERIES = 500
FILTERED_QUERIES = 1000


def percentiles(values, ranks=(50, 95, 99)):
    """Nearest-rank percentiles; empty samples have null values."""
    ordered = sorted(values)
    return {f"p{rank}": ordered[max(0, math.ceil(rank * len(ordered) / 100) - 1)]
            if ordered else None for rank in ranks}


def recall_at_k(approx, exact, k=10):
    """Fraction of the exact top-k IDs found in the approximate top-k."""
    oracle = set(exact[:k])
    return len(set(approx[:k]) & oracle) / len(oracle) if oracle else 1.0


def tenant_rows(tenant, per_tenant, batch_size=BATCH_SIZE):
    """(local ID, global base row) batches for one tenant."""
    return [[(local, tenant * per_tenant + local)
             for local in range(start, min(start + batch_size, per_tenant))]
            for start in range(0, per_tenant, batch_size)]


def assemble_report(config, phases, verification, storage, **metadata):
    """Assemble the durable report and derive its correctness gate."""
    return {
        "version": 1, "measurement_protocol": "tenants-v1-http-client-wall-clock",
        "config": config, "phases": phases, "verification": verification,
        "storage": storage,
        "passed": verification["mismatch_count"] == 0 and
                  verification["verified_tenants"] == config["tenants"],
        **metadata,
    }


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class Vectors:
    def __init__(self, path, count, seed, synthetic_offset=0):
        self.path = path
        self.count = count
        self.seed = seed
        self.synthetic_offset = synthetic_offset
        self.file = None
        self.data = None
        if path:
            self.file = open(path, "rb")
            self.data = mmap.mmap(self.file.fileno(), 0, access=mmap.ACCESS_READ)
            stride = 4 + DIMENSIONS * 4
            if len(self.data) % stride:
                raise ValueError(f"{path}: invalid fvecs byte length")
            self.count = len(self.data) // stride
            if self.count < count:
                raise ValueError(f"{path}: need {count} vectors, found {self.count}")

    def vector(self, row):
        if self.data is None:
            generator = random.Random(self.seed + self.synthetic_offset + row)
            return [generator.uniform(-1, 1) for _ in range(DIMENSIONS)]
        stride = 4 + DIMENSIONS * 4
        start = row * stride
        if struct.unpack_from("<i", self.data, start)[0] != DIMENSIONS:
            raise ValueError(f"{self.path}: row {row} is not {DIMENSIONS}-dimensional")
        return list(struct.unpack_from(f"<{DIMENSIONS}f", self.data, start + 4))

    def fingerprint(self):
        if self.path:
            return sha256_file(self.path)
        digest = hashlib.sha256()
        for row in range(self.count):
            digest.update(struct.pack(f"<{DIMENSIONS}f", *self.vector(row)))
        return digest.hexdigest()

    def close(self):
        if self.data is not None:
            self.data.close()
            self.file.close()


def timed_parallel(workers, jobs, work):
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(work, jobs))
    return time.monotonic() - started, results


def local_storage(path):
    objects = 0
    size = 0
    for root, _, files in os.walk(path):
        for name in files:
            target = Path(root) / name
            if target.is_file() and not target.is_symlink():
                objects += 1
                size += target.stat().st_size
    return objects, size


def storage_inventory(args):
    if args.storage_bytes_cmd:
        result = subprocess.run(shlex.split(args.storage_bytes_cmd), capture_output=True,
                                text=True, check=True)
        objects = re.search(r"Total Objects:\s*(\d+)", result.stdout)
        size = re.search(r"Total Size:\s*(\d+)", result.stdout)
        if not objects or not size:
            raise ValueError("storage command must print AWS 'Total Objects' and 'Total Size'")
        source = "storage-bytes-cmd"
        count, byte_count = int(objects[1]), int(size[1])
    elif os.environ.get("GLIDER_DATA_DIR"):
        source = "local-directory"
        count, byte_count = local_storage(os.environ["GLIDER_DATA_DIR"])
    else:
        return {"objects": None, "bytes": None, "source": "unavailable",
                "usd_per_month_at_0_023_per_gb": None}
    return {"objects": count, "bytes": byte_count, "source": source,
            "usd_per_month_at_0_023_per_gb": byte_count / 1_000_000_000 * 0.023}


def s3_counters(metrics):
    """Read exposed cumulative S3 request counters, if this binary has them."""
    found = {}
    for line in metrics.splitlines():
        if line.startswith("#") or "s3" not in line.lower():
            continue
        match = re.match(r"([^\s{}]+)(?:\{[^}]*\})?\s+([0-9.eE+-]+)$", line)
        if match and ("request" in match[1] or "http" in match[1]):
            found[match[1]] = found.get(match[1], 0.0) + float(match[2])
    return found or None


class Server:
    def __init__(self, args):
        self.args = args
        self.process = None
        self.peak_rss = None
        self.launches = 0

    def sample_rss(self):
        if self.process is None:
            return
        try:
            status = Path(f"/proc/{self.process.pid}/status").read_text()
            match = re.search(r"^VmHWM:\s*(\d+)\s+kB", status, re.MULTILINE)
            if match:
                value = int(match[1]) * 1024
                self.peak_rss = max(self.peak_rss or 0, value)
        except OSError:
            pass

    def start(self):
        env = os.environ.copy()
        env.pop("GLIDER_DIMENSIONS", None)
        env["GLIDER_LISTEN"] = self.args.listen
        env["GLIDER_MAX_OPEN_COLLECTIONS"] = str(self.args.max_open_collections)
        env["GLIDER_COLLECTION_IDLE_SECONDS"] = str(self.args.idle_seconds)
        self.process = subprocess.Popen([str(self.args.server_bin)], env=env)
        self.launches += 1
        deadline = time.monotonic() + 300
        client = Client("http://" + self.args.listen, token=env.get("GLIDER_API_TOKEN"),
                        timeout=2, max_retries=0)
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"server exited during startup: {self.process.returncode}")
            if client.health():
                self.sample_rss()
                return client
            time.sleep(0.5)
        raise TimeoutError("server did not become healthy within 300 seconds")

    def stop(self, kill=False):
        if self.process is None:
            return
        self.sample_rss()
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGKILL if kill else signal.SIGTERM)
            try:
                self.process.wait(timeout=60)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
                raise TimeoutError("server did not exit after SIGTERM")
        if self.peak_rss is None:
            rss = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
            self.peak_rss = rss * 1024 if platform.system() == "Linux" else rss
        self.process = None


def metrics_text(listen):
    with urllib.request.urlopen(f"http://{listen}/metrics", timeout=10) as response:
        return response.read().decode()


def run(args):
    if args.smoke:
        args.tenants, args.per_tenant = 8, 200
    if args.tenants < 1 or args.per_tenant < 1 or args.workers < 1:
        raise ValueError("tenants, per-tenant and workers must be positive")
    if bool(args.base) != bool(args.query):
        raise ValueError("base and query must both be provided")
    if not args.smoke and not args.base:
        raise ValueError("full runs require --base and --query fvecs")
    base = Vectors(args.base, args.tenants * args.per_tenant, args.seed)
    query = Vectors(args.query, 100 if not args.query else 1, args.seed, 10**9)
    server = Server(args)
    rng = random.Random(args.seed)
    crash_enabled = args.crash if args.crash is not None else not args.smoke
    dataset = {"base": {"path": str(args.base) if args.base else None,
                        "sha256": base.fingerprint(), "rows_used": args.tenants * args.per_tenant},
               "query": {"path": str(args.query) if args.query else None,
                         "sha256": query.fingerprint(), "rows_available": query.count},
               "format": "fvecs" if args.base else "synthetic-random-uniform-f32",
               "dimensions": DIMENSIONS}
    config = {"tenants": args.tenants, "per_tenant": args.per_tenant,
              "workers": args.workers, "seed": args.seed, "batch_size": BATCH_SIZE,
              "warm_queries": WARM_QUERIES, "recall_queries": RECALL_QUERIES,
              "filtered_queries": FILTERED_QUERIES, "max_open_collections": args.max_open_collections,
              "idle_seconds": args.idle_seconds, "crash": crash_enabled,
              "listen": args.listen, "server_bin": str(args.server_bin),
              "storage_bytes_cmd": args.storage_bytes_cmd}
    backend = ("local" if os.environ.get("GLIDER_DATA_DIR") else "s3")
    environment = {"backend": backend, "data_dir": os.environ.get("GLIDER_DATA_DIR"),
                   "s3_bucket": os.environ.get("GLIDER_S3_BUCKET"),
                   "s3_namespace": os.environ.get("GLIDER_S3_NAMESPACE"),
                   "s3_region": os.environ.get("GLIDER_S3_REGION") or os.environ.get("AWS_REGION"),
                   "s3_endpoint": os.environ.get("GLIDER_S3_ENDPOINT"),
                   "cache_dir": os.environ.get("GLIDER_CACHE_DIR"),
                   "cache_bytes": os.environ.get("GLIDER_CACHE_BYTES"),
                   "local_blocks": os.environ.get("GLIDER_LOCAL_BLOCKS"),
                   "auto_cluster_rows": os.environ.get("GLIDER_AUTO_CLUSTER_ROWS"),
                   "auto_recluster_factor": os.environ.get("GLIDER_AUTO_RECLUSTER_FACTOR"),
                   "instance_type": os.environ.get("GLIDER_BENCH_INSTANCE_TYPE"),
                   "platform": platform.platform(), "python": platform.python_version()}
    revision = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True,
                              text=True, check=True).stdout.strip()
    server_binary_sha256 = sha256_file(args.server_bin)
    dirty = bool(subprocess.run(["git", "status", "--porcelain"], capture_output=True,
                                text=True, check=True).stdout.strip())
    phases = {}
    verification = {"verified_tenants": 0, "mismatch_count": 0, "mismatches": []}
    storage = {"objects": None, "bytes": None, "source": "unavailable",
               "usd_per_month_at_0_023_per_gb": None}
    error = None
    started_at = datetime.now(timezone.utc).isoformat()
    try:
        client = server.start()
        if client.list_collections():
            raise ValueError("benchmark namespace already contains collections; use a fresh namespace")
        names = [f"t-{i:05d}" for i in range(args.tenants)]
        elapsed, _ = timed_parallel(args.workers, names,
                                    lambda name: client.create_collection(name, DIMENSIONS))
        phases["create"] = {"seconds": elapsed, "collections_per_second": args.tenants / elapsed}
        server.sample_rss()

        # Round-robin submission spreads active writers over distinct collections.
        rows_by_tenant = [tenant_rows(tenant, args.per_tenant) for tenant in range(args.tenants)]
        batch_count_per_tenant = math.ceil(args.per_tenant / BATCH_SIZE)
        batches = [((tenant, batch), rows_by_tenant[tenant][batch])
                   for batch in range(batch_count_per_tenant)
                   for tenant in range(args.tenants)]
        ready = threading.Event()
        ready.set()
        crashing = threading.Event()

        def write_batch(job):
            (tenant, _), rows = job
            ready.wait()
            bound = client.collection(names[tenant])
            # Boundary zero is valid for the whole default tenant history and
            # avoids a status/open before every batch. Larger tenants use the
            # current sequence to stay inside the 128-commit retention window.
            if batch_count_per_tenant <= 128:
                boundary = 0
            else:
                while True:
                    try:
                        boundary = bound.status()["sequence"]
                        break
                    except GliderError:
                        if not crashing.is_set():
                            raise
                        ready.wait()
            rid = {"boundary": boundary, "nonce": secrets.token_hex(16)}
            points = [{"id": local, "vector": base.vector(global_row),
                       "metadata": {"tenant": names[tenant], "bucket": str(global_row % 10)}}
                      for local, global_row in rows]
            began = time.monotonic()
            try:
                sequence = bound.write(upsert=points, request_id=rid)
            except GliderError:
                if not crashing.is_set():
                    raise
                ready.wait()
                sequence = bound.write(upsert=points, request_id=rid)
            return tenant, sequence, len(rows), time.monotonic() - began

        ingest_start = time.monotonic()
        latencies = []
        written = 0
        ack_sequences = [[] for _ in names]
        crash_record = None
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = [pool.submit(write_batch, job) for job in batches]
            try:
                for future in as_completed(futures):
                    tenant, sequence, count, latency = future.result()
                    written += count
                    latencies.append(latency)
                    ack_sequences[tenant].append(sequence)
                    if crash_enabled and crash_record is None and len(latencies) >= len(batches) // 2:
                        crashing.set()
                        ready.clear()
                        crash_at = time.monotonic()
                        killed_at_utc = datetime.now(timezone.utc).isoformat()
                        try:
                            server.stop(kill=True)
                            client = server.start()
                        finally:
                            ready.set()
                        restart_at = time.monotonic()
                        crash_record = {"at_acknowledged_batches": len(latencies),
                                        "killed_at_utc": killed_at_utc,
                                        "kill_seconds_from_ingest_start": crash_at - ingest_start,
                                        "restart_to_healthy_seconds": restart_at - crash_at}
            except Exception:
                ready.set()
                for future in futures:
                    future.cancel()
                raise
        elapsed = time.monotonic() - ingest_start
        phases["ingest"] = {"seconds": elapsed, "vectors": written,
                            "batches": len(latencies), "vectors_per_second": written / elapsed,
                            "write_latency_seconds": percentiles(latencies), "crash": crash_record}
        server.sample_rss()

        def verify(name):
            bound = client.collection(name)
            count = bound.count()
            ids = list(bound.scan())
            expected = list(range(args.per_tenant))
            sequence = bound.status()["sequence"]
            return name, count, ids == expected, len(ids), len(set(ids)), sequence

        elapsed, checks = timed_parallel(args.workers, names, verify)
        for tenant, (name, count, ids_ok, scan_count, distinct, sequence) in enumerate(checks):
            unique_acks = len(set(ack_sequences[tenant]))
            if (count != args.per_tenant or not ids_ok or
                    sequence != batch_count_per_tenant or unique_acks != batch_count_per_tenant):
                verification["mismatch_count"] += 1
                verification["mismatches"].append({"tenant": name, "count": count,
                    "scan_count": scan_count, "distinct_ids": distinct,
                    "sequence": sequence, "unique_ack_sequences": unique_acks,
                    "expected_count": args.per_tenant,
                    "expected_sequence": batch_count_per_tenant})
            else:
                verification["verified_tenants"] += 1
        phases["verify"] = {"seconds": elapsed, **verification}
        server.sample_rss()

        server.stop()
        restart_start = time.monotonic()
        client = server.start()
        clean_restart = time.monotonic() - restart_start
        cold_names = rng.sample(names, min(200, len(names)))
        cold_jobs = [(name, query.vector(rng.randrange(query.count))) for name in cold_names]

        def query_once(job):
            name, vector = job
            began = time.monotonic()
            hits = client.collection(name).query(vector, k=10)
            return time.monotonic() - began, [hit.id for hit in hits]

        # One request per name; concurrency is the configured worker count.
        elapsed, cold_results = timed_parallel(args.workers, cold_jobs, query_once)
        phases["cold_queries"] = {"seconds": elapsed, "clean_restart_to_healthy_seconds": clean_restart,
                                  "sample_tenants": cold_names, "queries": len(cold_results),
                                  "first_query_latency_seconds": percentiles([r[0] for r in cold_results])}
        server.sample_rss()

        warm_names = rng.sample(names, min(64, args.max_open_collections, len(names)))
        # Open these before the timed approximate window.
        timed_parallel(args.workers, warm_names,
                       lambda name: client.collection(name).query(query.vector(0), k=10))
        warm_jobs = [(warm_names[i % len(warm_names)], query.vector(rng.randrange(query.count)))
                     for i in range(WARM_QUERIES)]
        elapsed, warm_results = timed_parallel(args.workers, warm_jobs, query_once)
        recall_positions = set(rng.sample(range(WARM_QUERIES), RECALL_QUERIES))
        phases["warm_queries"] = {"seconds": elapsed, "queries": WARM_QUERIES,
            "sample_tenants": warm_names, "qps": WARM_QUERIES / elapsed,
            "latency_seconds": percentiles([r[0] for r in warm_results])}

        def exact_for(position):
            name, vector = warm_jobs[position]
            exact = client.collection(name).query(vector, k=10, exact=True)
            return recall_at_k(warm_results[position][1], [hit.id for hit in exact])

        elapsed, recalls = timed_parallel(args.workers, sorted(recall_positions), exact_for)
        phases["recall"] = {"seconds": elapsed, "queries": len(recalls),
                            "mean_at_10": sum(recalls) / len(recalls),
                            "p5_at_10": percentiles(recalls, (5,))["p5"]}
        server.sample_rss()

        filtered_jobs = [(warm_names[rng.randrange(len(warm_names))],
                          query.vector(rng.randrange(query.count)), str(rng.randrange(10)))
                         for _ in range(FILTERED_QUERIES)]

        def filtered(job):
            name, vector, bucket = job
            began = time.monotonic()
            client.collection(name).query(vector, k=10,
                                          filter={"bucket": bucket}, exact=True)
            return time.monotonic() - began

        elapsed, filtered_latencies = timed_parallel(args.workers, filtered_jobs, filtered)
        phases["filtered_queries"] = {"seconds": elapsed, "queries": FILTERED_QUERIES,
            "latency_seconds": percentiles(filtered_latencies, (50, 95))}
        server.sample_rss()

        idle_start = time.monotonic()
        time.sleep(args.idle_seconds + 15)
        descriptions = client.list_collections()
        open_count = sum(bool(item["open"]) for item in descriptions)
        before = s3_counters(metrics_text(args.listen))
        window = time.monotonic()
        time.sleep(60)
        window_seconds = time.monotonic() - window
        after = s3_counters(metrics_text(args.listen))
        request_rates = ({key: (after[key] - before[key]) / window_seconds for key in before.keys() & after.keys()}
                         if before and after else None)
        phases["idle"] = {"seconds": time.monotonic() - idle_start,
                          "collections_open_after_wait": open_count,
                          "expected_open_with_idle_close": 0,
                          "s3_requests_per_second": request_rates,
                          "s3_request_counters_before": before,
                          "s3_request_counters_after": after,
                          "counter_note": None if request_rates is not None else
                          "S3 request counters are not exposed by this server"}
        inventory_start = time.monotonic()
        storage = storage_inventory(args)
        phases["storage_inventory"] = {"seconds": time.monotonic() - inventory_start,
                                       "objects": storage["objects"], "bytes": storage["bytes"]}
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        print(error, file=sys.stderr, flush=True)
    finally:
        try:
            server.stop()
        except Exception as exc:
            error = error or f"server shutdown: {exc}"
        base.close()
        query.close()
    report = assemble_report(config, phases, verification, storage,
        dataset=dataset, environment=environment, git_revision=revision,
        server_revision=revision,
        server_binary_sha256=server_binary_sha256,
        working_tree_dirty=dirty, started_at_utc=started_at,
        completed_at_utc=datetime.now(timezone.utc).isoformat(),
        server_peak_rss_bytes=server.peak_rss, server_launches=server.launches,
        error=error)
    report["passed"] = report["passed"] and error is None and all(
        phase in phases for phase in ("create", "ingest", "verify", "cold_queries",
                                "warm_queries", "recall", "filtered_queries", "idle",
                                "storage_inventory"))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return 0 if report["passed"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-bin", type=Path, required=True)
    parser.add_argument("--listen", default="127.0.0.1:8080")
    parser.add_argument("--base", type=Path)
    parser.add_argument("--query", type=Path)
    parser.add_argument("--tenants", type=int, default=1000)
    parser.add_argument("--per-tenant", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--crash", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--max-open-collections", type=int, default=64)
    parser.add_argument("--idle-seconds", type=int, default=60)
    parser.add_argument("--storage-bytes-cmd")
    args = parser.parse_args()
    if args.max_open_collections < 1 or args.idle_seconds < 1:
        parser.error("max-open-collections and idle-seconds must be positive")
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
