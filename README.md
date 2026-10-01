![Glider](glider.png)

# Glider

Glider is an object-storage-native vector database and search engine written in Rust.
It provides durable single-writer storage, exact search and metadata filtering,
bounded concurrent admission, recovery, and backup/restore. IVF-Flat is an
optional experimental search path; supported serving defaults to exact search.

Authoritative data uses versioned logs and snapshots. In-memory state and derived
indexes are rebuildable; performance changes must preserve durability and recovery.
Collections larger than RAM are served from S3 through bounded RAM and NVMe
caches; the 250,000-vector envelope is accepted (see `benchmarks/M24.md`).

## Quickstart: HTTP server

`glider-server` serves one collection. Try it on a local directory:

```sh
cargo build --release --features server --bin glider-server
GLIDER_DATA_DIR=./data GLIDER_DIMENSIONS=3 GLIDER_RESIDENT_FILTER=color=red \
  target/release/glider-server
```

```sh
curl -XPOST localhost:8080/v1/write -H 'content-type: application/json' \
  -d '{"upsert":[{"id":1,"vector":[0,0,0],"metadata":{"color":"red"}},{"id":2,"vector":[1,1,1]}]}'
curl -XPOST localhost:8080/v1/query -H 'content-type: application/json' \
  -d '{"vector":[1,1,0.9],"k":2}'
curl -XPOST localhost:8080/v1/query -H 'content-type: application/json' \
  -d '{"vector":[0,0,0],"k":5,"filter":{"color":"red"}}'
curl localhost:8080/v1/points/1
```

Or run the server with a local MinIO in containers:

```sh
docker compose up --build
```

This starts MinIO, creates the `glider` bucket and serves collection `demo`
(3 dimensions, resident filter `color=red`) on `localhost:8080`; the curl
commands above work unchanged. `docker compose down -v` removes the data.
The image (`Dockerfile`) runs `glider-server` as a non-root user and reads
the same environment variables.

For S3 or MinIO, replace `GLIDER_DATA_DIR` with `GLIDER_S3_BUCKET`,
`GLIDER_S3_NAMESPACE`, optional `GLIDER_S3_REGION`/`GLIDER_S3_ENDPOINT` and the
`AWS_*` credentials. Other settings: `GLIDER_LISTEN` (default
`127.0.0.1:8080`), `GLIDER_METRIC`, `GLIDER_API_TOKEN` (bearer auth),
`GLIDER_CACHE_DIR`, `GLIDER_CACHE_BYTES`.

API (JSON):

| Endpoint | Purpose |
|---|---|
| `POST /v1/write` | Atomic batch `{"upsert":[…],"delete":[ids],"request_id":{…}?}`; returns `sequence` and the `request_id` to retry with |
| `POST /v1/query` | `{"vector":[…],"k":10,"filter":{…}?}`; unfiltered queries are approximate within a fixed read budget, the declared `GLIDER_RESIDENT_FILTER` is exact, other filters are rejected |
| `GET /v1/points/{id}` | Current vector and metadata, or 404 |
| `GET /v1/requests/{boundary}/{nonce}` | Resolve an uncertain write by its request ID |
| `GET /v1/status`, `GET /healthz` | Sequence and queue state; liveness |

A write is acknowledged only after durable publication; resend an uncertain
write with the same `request_id` to get its original outcome. SIGINT/SIGTERM
drains queued work and releases the collection's ownership claim; after a
crash, follow [the recovery procedure](docs/RECOVERY.md) before restarting.

- [Design](DESIGN.md): current architecture, guarantees and target direction.
- [Roadmap](ROADMAP.md): milestone status, acceptance criteria and next work.
- [Serving guide](docs/SERVING.md): validated workloads and operating procedures.
- [Benchmarks](BENCHMARKS.md): reproducible measurements and their limits.

## S3-compatible backend (M2)

Enable the `s3` Cargo feature. Provision a bucket and give one database exclusive
ownership of a namespace prefix. Use nonoverlapping prefixes. Configure credentials through the client builder
or standard AWS environment variables; do not place credentials in source files.
The backend requires strongly consistent GET/LIST and conditional PUT support.

For a deployed single writer, use `ownership::OwnedDatabase::open` in place of
`Database::open`, then call `close()` on graceful shutdown. A process exit leaves
an owner claim; the next writer receives a busy error. Inspect it with
`ownership::claims(&store)`. Clear an exact stale key only after proving the old
process is stopped and its outstanding requests have quiesced. An enrolled
namespace rejects raw `Database::open`; stop all legacy writers before first
enrolling an existing namespace. After a crash or uncertain S3 write, use a
fresh prefix as described in [the recovery procedure](docs/RECOVERY.md); a
timed-out old request may still arrive after process exit. See `DESIGN.md` for
the full guarantees.

```rust,no_run
use glider::{Config, Metric, ownership::OwnedDatabase, store::s3::{AmazonS3Builder, S3Store}};

let builder = AmazonS3Builder::from_env()
    .with_bucket_name("my-glider-bucket")
    .with_region("us-east-1");
// For MinIO, additionally set .with_endpoint("http://127.0.0.1:9000")
// and .with_allow_http(true). Use HTTPS for remote deployments.
let store = S3Store::open(builder, "vectors/example")?;
let metrics = store.metrics();
let mut db = OwnedDatabase::open(store, Config {
    dimensions: 2, metric: Metric::SquaredEuclidean,
})?;
db.put(42, vec![1.0, 2.0])?;
println!("{:?}", metrics.snapshot());
db.close()?;
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

MinIO runners bound commands to 10 minutes (Docker commands to 2 minutes),
authenticated startup to 30 seconds, each failure diagnostic to 5 seconds and
cleanup to 10 seconds. Timed-out commands and their child processes are killed;
database requests are not retried. Stage durations and sanitized failure output
are saved to `target/minio-diagnostics/events.jsonl` and retained by CI, whose
outer job limit is 20 minutes. Cleanup failure cannot replace the original test
failure; if Docker is unavailable, removal may require later manual cleanup of
the named disposable container. These limits bound the test harness, not engine
request latency or a provider's durability guarantee.

`S3Store::metrics()` returns a cloneable observer that remains available after the
store is moved into `Database`. Snapshot differences count actual HTTP client
attempts, including every list page, request-body bytes (with envelope overhead),
HTTP error responses and HTTP client call errors (later response-body consumption
errors are excluded from that counter). Credential requests using that client
are included. These counters do not measure physical I/O; exact queries generate
no object requests. MinIO recovery measurements are archived in
[benchmarks/SUMMARY.md](benchmarks/SUMMARY.md); a cloud-provider latency baseline
has not been established.

For the bounded real-provider handoff, see [the S3 pilot](docs/S3_PILOT.md).
It has a disposable MinIO mode and an explicit AWS mode that checks an active
Free account plan before writing. Routine CI uses only MinIO.

## Atomic batch writes

Use a batch to publish several ordered operations with one object-store PUT:

```rust,ignore
use glider::Mutation;
use std::collections::BTreeMap;
db.apply_batch(vec![
    Mutation::Put {
        id: 1,
        vector: vec![1.0, 2.0],
        metadata: BTreeMap::from([("team".into(), "red".into())]),
    },
    Mutation::Delete { id: 2 },
])?;
```

A successful call acknowledges every operation together. Operations on the same
ID run in order. Empty batches and invalid vectors are rejected before writing.
If publication fails or its result is uncertain, reopen before writing again;
recovery finds either the complete batch or none of it. The caller chooses a
batch size that fits one request and memory. Single `put` and `delete` calls keep
their existing behavior and log format.

For safe retries after a lost acknowledgement, use `apply_request` with an
unchanged request ID and optional document revision conditions. Its durable
outcome survives restart, compaction and isolated takeover within a bounded
128-commit window. See [the retry contract and example](docs/RETRIES.md).

For concurrent callers, [bounded admission](docs/ADMISSION.md) provides one
commit worker, count/byte limits, cancellation and explicit shutdown.

## Recovery checkpoints (M3)

Call `db.checkpoint()?` to persist the current live state as one immutable segment.
Reopen loads the latest checkpoint plus newer mutations. Checkpoint errors require
reopening before further writes, just like uncertain mutations. Logs and older
checkpoints are retained until explicitly compacted. See [DESIGN.md](DESIGN.md) for
publication semantics and [BENCHMARKS.md](BENCHMARKS.md) for recovery measurements.

For datasets where one full snapshot object is too large, call
`db.checkpoint_chunked(8 * 1024 * 1024)?` instead. The argument caps each encoded
data chunk in bytes; choose a limit that fits at least one document and your
object-store request budget. Recovery reads the versioned manifest and all its
chunks. The full live map still resides in memory.
New single-object snapshots use version 4 and chunked manifests use version 5
to preserve retry metadata. Older binaries that do not support these versions
refuse them; existing snapshot versions 1–3 remain readable.

For read-only exact queries without retaining all base vectors in RAM, open a
streaming reader after publishing a chunked snapshot:

```rust,ignore
use glider::{store::LocalStore, streaming::StreamingDatabase};
let reader = StreamingDatabase::open(LocalStore::open(&path)?, config)?;
let nearest = reader.search(&query, 10)?;
```

It includes newer mutations, supports exact metadata filtering and reads each
base chunk on every search. This mode requires version 3 roots, keeps the
mutation tail and manifest in memory, and is subject to the same exclusive
namespace ownership rule. It is useful when base-vector RAM matters more than
remote read latency.

For a known selective equality, retain its matching rows during the validated
open. The row budget prevents an unexpectedly broad filter from filling RAM:

```rust,ignore
let reader = StreamingDatabase::open_with_filter(
    LocalStore::open(&path)?, config, "selected", "true", 64,
)?;
let nearest = reader.search_filtered(&query, 10, &[("selected", "true")])?;
```

That predicate uses resident matching rows plus newer mutations; other queries
still scan chunks. The posting is rebuilt on every open and makes no durable
writes. Exceeding the row budget returns an error; use ordinary streaming open
for a broader filter.

## Compaction (M4)

Call `db.compact()?` to publish a full snapshot and reclaim covered mutations and
older snapshots. It runs synchronously and preserves live values, deletes and
sequence numbers. Reopen after a compaction error before writing again; calling
compaction again finishes interrupted cleanup. Compaction is explicit, so choose
its frequency based on measured maintenance and recovery costs.
`db.compact_chunked(8 * 1024 * 1024)?` uses the same bounded-chunk format and
reclaims obsolete chunks after publishing the new manifest. The byte limit is
an example, not a measured default for every deployment.

A compacted namespace requires an M4-capable binary. Compaction reclaims logical
objects; S3 bucket versioning may retain historical versions and delete markers.
See [DESIGN.md](DESIGN.md) for recovery semantics and [BENCHMARKS.md](BENCHMARKS.md)
for footprint and amplification measurements.

## Rebuildable IVF-Flat search

Exact `db.search(query, k)` remains available. To trade recall for fewer distance
calculations, build the derived in-memory index after loading your data:

```rust,ignore
use glider::ivf::IvfConfig;
db.build_ivf(IvfConfig { partitions: 16, iterations: 8, seed: 42 })?;
let result = db.search_ivf(&query, 10, 4)?; // probe four nearest partitions
println!("{:?}", result.neighbors);
```

These parameters are examples, not recommended settings for every dataset.
Probing all partitions matches exact search; fewer probes can miss neighbors and
return fewer than k results. Every successful put/delete invalidates the index;
rebuild before the next IVF query. To save retraining across restarts, call
`db.load_or_build_ivf(options)?` instead of `build_ivf`: the first call publishes
an immutable index object, and subsequent opens load it for the same data version
and options. Reopening alone does not load an index. Cache publication errors
require reopening before further durable writes; exact search remains available.
See [DESIGN.md](DESIGN.md) for training, validation and lifecycle semantics.

Run the short comparison with `python3 tools/ann_benchmark.py --output target/ann`.
See [BENCHMARKS.md](BENCHMARKS.md) and [the latest ANN comparison](benchmarks/ANN.md).

## Metadata equality filtering

Attach a complete string-to-string metadata map to each document. A normal
`put` replaces any previous metadata with an empty map. Filter pairs are combined
with AND; a missing key does not match. Empty filters behave like ordinary search.

```rust,ignore
use glider::ivf::IvfConfig;
use std::collections::BTreeMap;
let metadata = BTreeMap::from([("team".to_string(), "red".to_string())]);
db.put_with_metadata(42, vec![1.0, 2.0], metadata)?;
let exact = db.search_filtered(&[1.0, 2.0], 10, &[("team", "red")])?;
db.build_ivf(IvfConfig { partitions: 16, iterations: 8, seed: 42 })?;
let approximate = db.search_ivf_filtered(&[1.0, 2.0], 10, 4, &[("team", "red")])?;
let filled = db.search_ivf_filtered_adaptive(&[1.0, 2.0], 10, 4, &[("team", "red")])?;
```

Full IVF probing matches filtered exact search. Partial probing can return fewer
than k matches. Adaptive probing scans at least the requested four partitions,
then expands until it finds k matches or exhausts the index. It reports the
number of partitions probed. A full scan is exact; an early stop can still miss
nearer matches. `search_filtered` always takes the exact path, even with a
built IVF index. Choose an IVF method only when approximate answers are acceptable;
there is no automatic planner. The [filtered quality results](benchmarks/FILTERING.md)
show why sparse filters can make partial probing both incomplete and inaccurate.
Metadata and vectors share the mutation and snapshot durability
boundary; existing version 1 databases open with empty metadata.

## Bounded single-machine serving

For the initial 2,000-row, 64-dimension deployment, `SingleMachine` owns the
namespace, enforces capacity, and completes due maintenance before a batch.
Queries use exact mode; M12 did not justify enabling approximate serving.

```rust
use glider::{Config, Metric, Mutation, store::LocalStore};
use glider::serving::{SingleMachine, ServingOptions, SearchMode};
let config = Config { dimensions: 64, metric: Metric::SquaredEuclidean };
let mut service = SingleMachine::open(
    LocalStore::open("vectors")?, config, ServingOptions::m8())?;
service.apply_batch(vec![Mutation::Put {
    id: 1, vector: vec![0.; 64], metadata: Default::default(),
}])?;
let hits = service.query(&vec![0.; 64], 10, &[], SearchMode::Exact)?;
let status = service.status();
service.close()?;
# Ok::<(), glider::Error>(())
```

The [M20 SIFT envelope](benchmarks/M20.md) additionally validates 5,000×128
descriptors with four callers on local MinIO; configuration and scope are in
[serving operations](docs/SERVING.md#larger-sift-descriptor-envelope).

Use 100-operation batches for the measured M8 maintenance envelope. This
serial library API has no HTTP listener or background scheduler. See
[serving operations](docs/SERVING.md) for status, backup/restore, failure handling,
and the 30-minute soak command. After uncertainty, follow the
[fresh-prefix recovery procedure](docs/RECOVERY.md).

## Larger-than-RAM segmented serving (experimental)

With the `experimental-segmented` feature (plus `s3` for object storage),
`SegmentedServing` keeps only a compact ID directory, persisted per-pack
five-bit routing sketches and the unsealed log tail in memory. Vectors stay in
immutable object-storage packs read through a bounded RAM/NVMe block cache.
Unfiltered queries are approximate: they read a fixed number of routed blocks
and rerank them exactly. The one equality predicate declared at namespace
creation is answered exactly from full-precision vectors kept in the sketches;
other filters are rejected, and `search_exact` remains the oracle. Run it
behind `admission::Service`, which executes seal, consolidation, reclamation
and cleanup in bounded units while no command is queued.

```rust
use glider::{admission::{Limits, Service, Shutdown}, Config, Metric};
use glider::segmented::{SegmentedOptions, SegmentedServing, SegmentedServingOptions};
let config = Config { dimensions: 128, metric: Metric::SquaredEuclidean };
let declared = SegmentedOptions {
    resident_filter: Some(("cohort".into(), "one-percent".into())),
};
let engine = SegmentedServing::open(
    store, config, declared, SegmentedServingOptions::m21("block-cache".into()))?;
let service = Service::start(engine, Limits {
    read_priority: Some(std::time::Duration::from_millis(50)),
    ..Limits::default()
})?;
let client = service.client();
let hits = client.query(vec![0.; 128], 10, vec![])?.wait()?;
service.shutdown(Shutdown::Drain)?;
```

The measured 250,000-row envelope, its gates and the reproduction command are
in [benchmarks/M24.md](benchmarks/M24.md#single-machine-acceptance-protocol-declared-before-the-run).
