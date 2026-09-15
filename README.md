![Glider](glider.png)

# Glider

Glider is a Rust-based, object-storage-native vector database and search engine
for durable similarity search at scale.

The project is designed around a simple principle: search performance must grow
without weakening correctness, durability, or recovery guarantees.

## What Glider Is Building

Glider separates persistent truth from the structures used to serve queries:

```text
Client/API
  -> validation and query planning
  -> exact or approximate candidate generation
  -> metadata filtering and exact reranking
  -> deterministic top-k results

Mutation path
  -> durable commit protocol
  -> immutable logs and persistent segments
  -> published authoritative state
  -> rebuildable indexes and caches
```

Authoritative data is versioned, immutable where appropriate, and published only
when complete. In-memory state, search indexes, caches, and ANN structures are
derived from that data and can be rebuilt after restart or failure.

## Design Priorities

- **Correctness first:** exact search remains the reference for validating every
  approximate search optimization.
- **Object-storage-native durability:** the engine is designed around immutable
  objects and explicit publication semantics rather than filesystem-specific
  behavior.
- **Predictable recovery:** acknowledgement, crash behavior, recovery, and
  reader visibility are defined for every durable write path.
- **Measured performance:** changes are evaluated with reproducible workloads,
  fixed seeds, retained raw measurements, and before/after comparisons.
- **Composable evolution:** storage, persistence, query execution, filtering,
  and indexing remain separate so each can evolve without weakening the others.

## Evolution Path

Glider evolves from a durable exact-search foundation toward a complete vector
search engine:

1. Durable authoritative state and deterministic exact search.
2. S3-compatible object storage with the same correctness contract.
3. Immutable segments and compaction for efficient persistence and recovery.
4. Metadata filtering and query planning.
5. Rebuildable ANN indexes, evaluated against exact search using recall, latency,
   throughput, and resource measurements.
6. Further scaling mechanisms such as concurrency, sharding, and replication
   when workload measurements justify them.

Every stage preserves logical results and makes architectural trade-offs
measurable.

## Engineering Model

Glider treats persisted formats, manifests, segments, and indexes as explicit
versioned contracts. Derived indexes are never the only source of truth, and a
partial publication must never become authoritative.

Performance results are retained as reproducible benchmark artifacts so changes
can be compared against the same workload, environment, and implementation
revision over time.

See [DESIGN.md](DESIGN.md) for the architecture and durability model, and
[ROADMAP.md](ROADMAP.md) for the project development path.


## S3-compatible backend (M2)

Enable the `s3` Cargo feature. Provision a bucket and give one database exclusive
ownership of a namespace prefix. Use nonoverlapping prefixes. Configure credentials through the client builder
or standard AWS environment variables; do not place credentials in source files.
The backend requires strongly consistent GET/LIST and conditional PUT support.

```rust,no_run
use glider::{Config, Database, Metric, store::s3::{AmazonS3Builder, S3Store}};

let builder = AmazonS3Builder::from_env()
    .with_bucket_name("my-glider-bucket")
    .with_region("us-east-1");
// For MinIO, additionally set .with_endpoint("http://127.0.0.1:9000")
// and .with_allow_http(true). Use HTTPS for remote deployments.
let store = S3Store::open(builder, "vectors/example")?;
let metrics = store.metrics();
let mut db = Database::open(store, Config {
    dimensions: 2, metric: Metric::SquaredEuclidean,
})?;
db.put(42, vec![1.0, 2.0])?;
println!("{:?}", metrics.snapshot());
# Ok::<(), glider::Error>(())
```

This API blocks. In a Tokio application, construct and use the database inside
`tokio::task::spawn_blocking`; do not call it directly from an async task.
After a mutation error, discard the database and open a fresh store/database to
resolve the uncertain outcome. Creates use conditional native publication with
no automatic retries. The local body/seal protocol is not used on S3.

Run offline unit tests and real integration tests:

```sh
cargo test --locked --features s3
python3 tools/test_s3.py
```

The integration runner requires Docker and Python 3. It starts a pinned MinIO
image on an ephemeral loopback port, creates a disposable bucket with generated
test credentials, tests pagination and failure recovery, kills/restarts MinIO,
and removes its container/data afterward. No existing buckets or credentials are
used. Service-dependent Rust tests are explicitly ignored in ordinary test runs;
the runner executes them, and CI invokes the runner.

`S3Store::metrics()` returns a cloneable observer that remains available after the
store is moved into `Database`. Snapshot differences count actual HTTP client
attempts, including every list page, request-body bytes (with envelope overhead),
HTTP error responses and HTTP client call errors (later response-body consumption
errors are excluded from that counter). Credential requests using that client
are included. These counters do not measure physical I/O; exact queries generate
no object requests. MinIO recovery measurements are archived in
[benchmarks/SUMMARY.md](benchmarks/SUMMARY.md); a cloud-provider latency baseline
has not been established.

## Recovery checkpoints (M3)

Call `db.checkpoint()?` to persist the current live state as one immutable segment.
Reopen loads the latest checkpoint plus newer mutations. Checkpoint errors require
reopening before further writes, just like uncertain mutations. Logs and older
checkpoints are retained until explicitly compacted. See [DESIGN.md](DESIGN.md) for
publication semantics and [BENCHMARKS.md](BENCHMARKS.md) for recovery measurements.


## Compaction (M4)

Call `db.compact()?` to publish a full snapshot and reclaim covered mutations and
older snapshots. It runs synchronously and preserves live values, deletes and
sequence numbers. Reopen after a compaction error before writing again; calling
compaction again finishes interrupted cleanup. Compaction is explicit, so choose
its frequency based on measured maintenance and recovery costs.

A compacted namespace requires an M4-capable binary. Compaction reclaims logical
objects; S3 bucket versioning may retain historical versions and delete markers.
See [DESIGN.md](DESIGN.md) for recovery semantics and [BENCHMARKS.md](BENCHMARKS.md)
for footprint and amplification measurements.
