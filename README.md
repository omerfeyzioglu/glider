![Glider](glider.png)

# Glider

[![CI](https://github.com/omerfeyzioglu/glider/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/omerfeyzioglu/glider/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

Glider is a single-node vector database that keeps all of its data in S3 (or
any S3-compatible object store) and uses local RAM and SSD only as
disposable caches. One `glider-server` process serves one collection over a
small HTTP/JSON API: writes are acknowledged only once they are durable in
object storage, a crashed or replaced server is taken over automatically
without losing acknowledged writes, and approximate nearest-neighbor queries
read a bounded amount of data per query. It is written in Rust and is meant
for applications that want vector search with object-storage durability
and cost on one machine, without operating a replicated cluster.

## Key features

- **Durable on S3.** Every acknowledged write is part of an immutable log
  object published with a conditional create; nothing is overwritten in
  place, and local disks never hold the only copy.
- **Crash-safe takeover.** A restarted server waits out the old writer's
  lease, fences it at the object store and replays the log tail; a paused
  former owner can never commit again. No operator step after a crash.
- **Clustered ANN with automatic clustering.** Vectors are grouped into a
  global clustered view (centroids and cluster-contiguous postings) that the
  server builds by itself at 250,000 rows and rebuilds as the collection
  grows. Unfiltered queries read at most 8 remote ranges and 1 MiB.
- **SSD cache.** A local block cache is filled in the background so warm
  queries need no remote reads. Losing it does not lose acknowledged writes;
  it can affect query latency and approximate-search recall until warm again.
- **Filters.** Equality filters on string metadata; one declared predicate
  is answered exactly, and up to four declared keys steer routing.
- **Metadata in results.** Queries can return each hit's metadata and
  vector.
- **Safe retries.** Every write carries a request ID; resending it returns
  the original outcome instead of applying the write twice.
- **Operations.** HTTP API, Docker image and Compose quickstart, Prometheus
  metrics, `glider-admin` for status, backup, restore and clustering.

## Status

Glider 1.0 is a single-node, single-writer database. Its scope:

- one collection per server process, stored under one object-store prefix
  (run several processes on separate prefixes for several collections);
- upsert, delete, get and k-NN query (squared Euclidean, Manhattan or
  cosine) with equality filters, over HTTP or as a Rust library;
- validated up to 1,000,000 128-dimensional vectors on MinIO and on AWS S3
  ([performance](#performance)).

Known limitations:

- No replication, sharding, read replicas or standby; availability during a
  restart depends on the lease (default 10 s) and the open time.
- Filters are equality conjunctions only. Only the declared resident
  predicate is exact; other filters are applied to the routed blocks and
  may return fewer than `k` results.
- Peak memory at 1,000,000 vectors (235.6 MiB on MinIO, 256.7 MiB on AWS)
  is above the 192 MiB target.
- Opening a large collection on S3 takes seconds (4.87 s, and 13.36 s for
  a reopen, at 1,000,000 vectors), and S3 write p95 follows conditional-PUT
  latency (173 ms at 1,000,000 vectors).
- The dimension, metric, resident filter and routed keys are fixed when a
  collection is created.
- Plain HTTP with an optional static bearer token; terminate TLS in a
  reverse proxy.
- A point must fit in one 120 KiB storage block (vector plus metadata, about
  122,000 bytes); larger points are rejected with `400`
  ([limits](docs/API.md#post-v1write)). A namespace written by a binary that
  accepted such a point cannot seal until that point is deleted or replaced,
  and once 64 unsealed log objects accumulate it needs manual repair.
- No built-in scheduled backups; use S3 Versioning and `glider-admin backup`.

## Quickstart

The Docker Compose path needs a Git checkout and Docker with Compose, but **does
not require Rust on your computer**: the Docker build uses Rust inside its
builder image. There is currently no published binary installer or prebuilt
container image. The Compose setup is a local demo with example MinIO
credentials and an HTTP port; it is not an internet-facing deployment.
An AWS deployment requires you to provision the S3 bucket, compute host,
networking, credentials and TLS edge. See the
[deployment architecture](docs/ARCHITECTURE.md#aws-deployment-pattern).

### Docker Compose (with MinIO)

```sh
docker compose up --build
```

This starts MinIO, creates the `glider` bucket and serves collection `demo`
(3 dimensions, resident filter `color=red`) on `localhost:8080`.
`docker compose down -v` removes the containers and data. The image
(`Dockerfile`) runs `glider-server` as a non-root user and is configured
with the [environment variables](#configuration) below.

### From source

Requires Rust 1.98.1 (the version CI uses).

```sh
cargo build --release --features server --bin glider-server
GLIDER_DATA_DIR=./data GLIDER_DIMENSIONS=3 GLIDER_RESIDENT_FILTER=color=red \
  target/release/glider-server
```

`GLIDER_DATA_DIR` stores the collection in a local directory, which is
convenient for development. For S3, set `GLIDER_S3_BUCKET`,
`GLIDER_S3_NAMESPACE` and AWS credentials instead:

```sh
GLIDER_S3_BUCKET=my-bucket GLIDER_S3_NAMESPACE=collections/demo \
GLIDER_S3_REGION=eu-central-1 GLIDER_DIMENSIONS=3 \
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
  target/release/glider-server
```

### First requests

```sh
# Insert two points (one atomic batch).
curl -XPOST localhost:8080/v1/write -H 'content-type: application/json' \
  -d '{"upsert":[{"id":1,"vector":[0,0,0],"metadata":{"color":"red"}},{"id":2,"vector":[1,1,1]}]}'
# {"conflict":null,"request_id":{"boundary":1,"nonce":"..."},"sequence":2}

# Two nearest neighbors, with metadata.
curl -XPOST localhost:8080/v1/query -H 'content-type: application/json' \
  -d '{"vector":[1,1,0.9],"k":2,"include_metadata":true}'
# {"results":[{"distance":0.01000000476837215,"id":2,"metadata":{}},
#             {"distance":2.8099999570846563,"id":1,"metadata":{"color":"red"}}],"sequence":2}

# Exact filtered query on the declared resident filter.
curl -XPOST localhost:8080/v1/query -H 'content-type: application/json' \
  -d '{"vector":[0,0,0],"k":5,"filter":{"color":"red"}}'
# {"results":[{"distance":0.0,"id":1}],"sequence":2}

# Read one point; then check status.
curl localhost:8080/v1/points/1
curl localhost:8080/v1/status
```

## Configuration

`glider-server` and `glider-admin` read these environment variables (see
`ServerConfig::from_env` in `src/server/config.rs`). Empty values count as
unset; an invalid value stops the server with an error.

| Variable | Default | Meaning |
|---|---|---|
| `GLIDER_DIMENSIONS` | required | Vector dimension. Fixed at creation. |
| `GLIDER_METRIC` | `squared_euclidean` | `squared_euclidean`, `manhattan` or `cosine`. Fixed at creation. |
| `GLIDER_RESIDENT_FILTER` | unset | One `key=value` equality predicate answered exactly from vectors kept in memory. Fixed at creation. |
| `GLIDER_ROUTED_KEYS` | unset | Up to four comma-separated metadata keys whose values restrict query routing (sorted and deduplicated). Fixed at creation. |
| `GLIDER_DATA_DIR` | unset | Store the collection in this local directory; when set, the S3 variables are ignored. |
| `GLIDER_S3_BUCKET` | required without `GLIDER_DATA_DIR` | Bucket name. |
| `GLIDER_S3_NAMESPACE` | required without `GLIDER_DATA_DIR` | Key prefix owned by this collection. Use one prefix per collection and never overlap prefixes. |
| `GLIDER_S3_REGION` | `us-east-1` | Bucket region. |
| `GLIDER_S3_ENDPOINT` | unset | Endpoint for S3-compatible stores such as MinIO; an `http://` endpoint enables plain HTTP. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | unset | Credentials, read by the `object_store` S3 client (`AmazonS3Builder::from_env`). |
| `GLIDER_LISTEN` | `127.0.0.1:8080` | Listen address (`0.0.0.0:8080` in the Docker image). |
| `GLIDER_API_TOKEN` | unset | If set, required as `Authorization: Bearer <token>` on every endpoint except `/healthz` and `/metrics`. |
| `GLIDER_LEASE_SECONDS` | `10` | Writer lease duration (fractions allowed). After a crash, the next start waits at most this long before taking over. |
| `GLIDER_CACHE_DIR` | `glider-cache` | Local block cache directory, relative to the working directory (`/var/lib/glider/cache` in the Docker image). Any local disk works: instance-store NVMe, EBS or a container volume. |
| `GLIDER_CACHE_BYTES` | `268435456` (256 MiB) | Local cache limit. While idle the server copies the collection into the cache up to this limit; set it above `cache.namespace_bytes` from `/v1/status` to keep everything local. |
| `GLIDER_LOCAL_BLOCKS` | `24` | Cached blocks a query may rerank in addition to its remote budget. `0` makes results independent of the cache contents. |
| `GLIDER_AUTO_CLUSTER_ROWS` | `250000` | Live sealed rows at which a collection without a clustered view is converted to one in the background. `0` disables. |
| `GLIDER_AUTO_RECLUSTER_FACTOR` | `4` | Rebuild the clustered view with more clusters once the collection holds more than this factor times the rows it was sized for (about 4,000 per cluster). `0` disables. |

The server uses the 1,000,000-row serving profile
(`SegmentedServingOptions::m31`): per query 12 candidate blocks, 8 remote
range requests and 1 MiB, 32 cluster probes, 8 routing threads; at most 4
concurrent queries; an admission queue of 8 commands and 1 MiB. These are
not configurable through the environment.

## HTTP API

| Endpoint | Purpose |
|---|---|
| [`POST /v1/write`](docs/API.md#post-v1write) | Atomic batch `{"upsert":[...],"delete":[...],"request_id":{...}}` (up to 100 operations); returns `sequence` and `request_id` |
| [`POST /v1/query`](docs/API.md#post-v1query) | `{"vector":[...],"k":10,"filter":{...},"include_metadata":false,"include_vector":false}` |
| [`GET /v1/points/{id}`](docs/API.md#get-v1pointsid) | Current vector and metadata, or `404` |
| [`GET /v1/requests/{boundary}/{nonce}`](docs/API.md#get-v1requestsboundarynonce) | Resolve a write whose response was lost |
| [`GET /v1/status`](docs/API.md#get-v1status) | Sequence, queue, cache warm-up and clustering state |
| [`GET /healthz`](docs/API.md#get-healthz) | Liveness (no auth) |
| [`GET /metrics`](docs/API.md#get-metrics) | Prometheus metrics (no auth) |

Errors are JSON `{"error": "..."}` with `400` for invalid input, `401`,
`404`, `409` for request-ID misuse, `429` when the queue is full (retry),
`500` for corruption and `503` when storage or the worker is unavailable.
See the [API reference](docs/API.md) for schemas, limits and retry rules.

A Python client needs nothing beyond `requests`:

```python
import requests, secrets

base = "http://localhost:8080"
# A client-chosen request ID makes the write safe to resend after a timeout.
boundary = requests.get(f"{base}/v1/status").json()["sequence"]
write = {
    "upsert": [{"id": 10, "vector": [0.5, 0.5, 0.5], "metadata": {"color": "red"}}],
    "request_id": {"boundary": boundary, "nonce": secrets.token_hex(16)},
}
print(requests.post(f"{base}/v1/write", json=write).json())

hits = requests.post(f"{base}/v1/query", json={
    "vector": [0.4, 0.5, 0.5], "k": 3, "filter": {"color": "red"},
    "include_metadata": True,
}).json()["results"]
for hit in hits:
    print(hit["id"], hit["distance"], hit["metadata"])
```

## Operations

- **Status and health.** `GET /v1/status` reports the committed sequence,
  queue state, cache warm-up (`cache.state`) and clustering
  (`clustering.state`: `none`, `converting` with progress, or `clustered`).
  `GET /metrics` exposes request counts and latency histograms per
  endpoint, queue depth, maintenance, cache and conversion metrics
  ([list](docs/API.md#get-metrics)).
- **Shutdown and recovery.** SIGINT/SIGTERM drains queued work and releases
  the lease. After a crash or kill, start the server again on the same
  prefix with the same collection settings: it waits at most
  `GLIDER_LEASE_SECONDS`, fences the old writer and serves every
  acknowledged write. Resolve writes whose response was lost by request ID.
  See [RECOVERY.md](docs/RECOVERY.md).
- **Clustering.** The server converts a collection to the clustered view at
  `GLIDER_AUTO_CLUSTER_ROWS` and rebuilds it after
  `GLIDER_AUTO_RECLUSTER_FACTOR`-fold growth, in the background; writes
  and queries continue, queries use the previous layout until the new one
  is published, and a crash during a conversion loses nothing (the next
  start begins a new one and removes the interrupted one's objects). New
  writes are assigned to the view's clusters as they are sealed.
- **`glider-admin`** uses the same environment variables. Stop the server
  first: each command acquires the writer lease like a server start, and
  fails with a lease error while a server holds it. Each prints one JSON
  object.

  ```sh
  cargo build --release --features server --bin glider-admin
  export GLIDER_DATA_DIR=./data GLIDER_DIMENSIONS=3 GLIDER_RESIDENT_FILTER=color=red
  target/release/glider-admin status
  target/release/glider-admin backup ./backup          # or s3://bucket/prefix
  GLIDER_DATA_DIR=./restored target/release/glider-admin restore ./backup
  target/release/glider-admin convert                  # build or rebuild the clustered view now
  ```

  `backup` writes a consistent, validated copy to an empty location that
  does not overlap the source; `s3://bucket/prefix` locations use the
  configured region, endpoint and credentials. `restore` copies a backup into an empty
  destination and validates it; the first server start there takes it over.
  Never reuse a failed destination. A crash needs no restore.
  `convert [CENTROIDS]` seals the log tail and builds the clustered view now
  (about 4,000 rows per cluster by default); it also repairs a missing or
  corrupt view.
- **Protecting against mistakes.** Enable S3 Versioning on the bucket with
  a lifecycle rule that expires noncurrent versions after your retention
  window and aborts incomplete multipart uploads. Glider never overwrites
  an object, so versioning only retains the objects that cleanup deletes.
  To recover, copy the object versions current at the chosen time into a
  fresh prefix and check it with `glider-admin status` before serving it.
- **Drills.** `python3 tools/drills.py --seed 29` builds release binaries and
  checks kill-and-restart, paused-writer fencing, cache loss, backup and
  restore, and conversion on a local directory, reporting PASS/FAIL.

The [serving guide](docs/SERVING.md) has further procedures.

## Performance

Measured at 1,000,000 vectors of SIFT1M (128 dimensions, squared Euclidean,
k=10, 200 queries) under the M31 workload: load in 100-row batches, then
300 s of four writers each overwriting 100 rows per second and four readers
each querying every 100 ms, then restart, cache-loss and backup checks. Both
runs use the clustered view (explicit conversion, 32 probes); they predate
automatic clustering and root manifests.

| Measure | MinIO, Apple M4 ([M37](benchmarks/M37.md#1m-minio-acceptance-on-the-clustered-view), `a636045`) | AWS S3 Standard, c7g.2xlarge ([M39](benchmarks/M39.md#clustered-view-on-an-8-vcpu-instance), `af7fb0e`) | Target |
|---|---:|---:|---:|
| Static recall@10 (mean / p5) | 0.998 / 1.0 | 0.998 / 1.0 | >=0.90 / >=0.80 |
| Recall@10 after updates, cold (mean / p5) | 0.963 / 0.8 | 0.966 / 0.8 | >=0.90 / >=0.80 |
| Warm unfiltered query p95 | 36.6 ms | 29.4 ms | <=50 ms |
| Cold unfiltered query p95 | 33.7 ms | 57.1 ms | <=200 ms |
| Write p95 | 33.0 ms | 173.0 ms | <=150 ms |
| Open / reopen | 1.43 / 1.73 s | 4.87 / 13.36 s | <=2 s |
| Peak engine RSS | 235.6 MiB | 256.7 MiB | <=192 MiB |

MinIO ran on loopback on an Apple M4 (10 cores, 16 GiB). The AWS run used a
c7g.2xlarge (8 Graviton3 vCPU, 16 GiB) in eu-central-1 against S3 Standard;
its filtered (resident 1%) query p95 was 9.8 ms, and it lost no
acknowledged writes and returned equal results after cache loss and backup
restore. Datasets, seeds, raw
results and the remaining measurements are in [BENCHMARKS.md](BENCHMARKS.md)
and [benchmarks/](benchmarks/SUMMARY.md).

## Architecture

![Glider runtime architecture](docs/architecture/runtime.svg)

- A write batch becomes one immutable log object, created conditionally,
  before it is acknowledged.
- Idle maintenance seals the log tail into immutable packs of vector-local
  blocks with compact five-bit sketches, and publishes each new state as a
  root generation that references per-run manifests.
- The clustered view assigns rows to centroids so a query reads only the
  nearest clusters' blocks; new seals keep it clustered and small extents
  are merged in the background.
- Takeover uses a renewed lease to pace restarts and permanent fence objects
  so that a deposed writer's next publication fails at the store.

The [architecture guide](docs/ARCHITECTURE.md) includes an AWS deployment
diagram. [DESIGN.md](DESIGN.md) states the formats, invariants, and crash and
recovery semantics in full.

## Library

The crate can also be used directly from Rust, including the resident
in-memory `Database` engine and the segmented engine behind the server. See
the [library guide](docs/LIBRARY.md).

## Development

Checks that CI runs:

```sh
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --locked
cargo test --all-features --locked
python3 -m unittest discover -s tests -p 'test_*.py'
python3 tools/benchmarks.py summary --check
```

Object-store integration tests run against a disposable, pinned MinIO
container (requires Docker and Python 3):

```sh
python3 tools/test_s3.py
```

Local failure drills: `python3 tools/drills.py --seed 29`. Benchmarks and
acceptance runs (`tools/m24_acceptance.py` on MinIO, `tools/aws_acceptance.py`
on EC2 and S3) are described in [BENCHMARKS.md](BENCHMARKS.md). See
[CONTRIBUTING.md](CONTRIBUTING.md) for the workflow.

## Documentation

| Document | Contents |
|---|---|
| [docs/API.md](docs/API.md) | HTTP API reference |
| [docs/LIBRARY.md](docs/LIBRARY.md) | Rust library guide |
| [docs/RECOVERY.md](docs/RECOVERY.md) | Crash, takeover and restore procedures |
| [docs/SERVING.md](docs/SERVING.md) | Serving envelopes and operating procedures |
| [DESIGN.md](DESIGN.md) | Architecture, formats and guarantees |
| [docs/M37_CLUSTERED_INDEX.md](docs/M37_CLUSTERED_INDEX.md) | Clustered index design |
| [ROADMAP.md](ROADMAP.md) | Milestones and next work |
| [BENCHMARKS.md](BENCHMARKS.md), [benchmarks/SUMMARY.md](benchmarks/SUMMARY.md) | Measurements and how to reproduce them |
| [CHANGELOG.md](CHANGELOG.md) | Release notes |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Development workflow |
| [docs/EVOLUTION.md](docs/EVOLUTION.md) | History of major decisions |

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Unless you explicitly state
otherwise, any contribution intentionally submitted for inclusion in this
project, as defined in the Apache-2.0 license, shall be dual licensed as
above, without any additional terms or conditions.
