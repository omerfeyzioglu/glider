# Library guide

Glider is also a Rust library. This guide covers using it from Rust: the S3
backend, the resident `Database` engine (milestones M2–M13) and the
segmented engine that `glider-server` is built on. For the server, see the [README](../README.md) and the
[HTTP API reference](API.md).

The crate has two engines with disjoint namespace formats
([DESIGN.md](../DESIGN.md#engines-and-namespace-compatibility)):

- the **segmented engine** (`segmented::SegmentedDatabase`, served by
  `SegmentedServing`), which keeps vectors in immutable object-storage packs
  and only a compact directory, routing sketches and the unsealed log tail in
  RAM; it is the serving path, described [last](#segmented-collections-library-api);
- the **resident engine** (`Database`, `OwnedDatabase`, `SingleMachine`),
  which keeps every document in RAM; it suits small collections and is the
  exact reference.

Opening a namespace with the other engine fails with an explicit error.
Cargo features: `s3` enables the S3-compatible store; `server` adds the
HTTP server and admin binaries.

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
fresh prefix as described in [the recovery procedure](RECOVERY.md); a
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
[benchmarks/SUMMARY.md](../benchmarks/SUMMARY.md); a cloud-provider latency baseline
has not been established.

For the bounded real-provider handoff, see [the S3 pilot](S3_PILOT.md).
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
128-commit window. See [the retry contract and example](RETRIES.md).

For concurrent callers, [bounded admission](ADMISSION.md) provides one
commit worker, count/byte limits, cancellation and explicit shutdown.

## Recovery checkpoints (M3)

Call `db.checkpoint()?` to persist the current live state as one immutable segment.
Reopen loads the latest checkpoint plus newer mutations. Checkpoint errors require
reopening before further writes, just like uncertain mutations. Logs and older
checkpoints are retained until explicitly compacted. See [DESIGN.md](../DESIGN.md) for
publication semantics and [BENCHMARKS.md](../BENCHMARKS.md) for recovery measurements.

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
See [DESIGN.md](../DESIGN.md) for recovery semantics and [BENCHMARKS.md](../BENCHMARKS.md)
for footprint and amplification measurements.

## Rebuildable IVF-Flat search

`Config.metric` accepts `SquaredEuclidean`, `Manhattan`, or `Cosine`. Cosine
requires nonzero vectors and queries; `get` returns a normalized vector for a
cosine collection. Distance is `1 - dot(q, v)` after normalization.

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
See [DESIGN.md](../DESIGN.md) for training, validation and lifecycle semantics.

Run the short comparison with `python3 tools/ann_benchmark.py --output target/ann`.
See [BENCHMARKS.md](../BENCHMARKS.md) and [the latest ANN comparison](../benchmarks/ANN.md).

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
there is no automatic planner. The [filtered quality results](../benchmarks/FILTERING.md)
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

The [M20 SIFT envelope](../benchmarks/M20.md) additionally validates 5,000×128
descriptors with four callers on local MinIO; configuration and scope are in
[serving operations](SERVING.md#larger-sift-descriptor-envelope).

Use 100-operation batches for the measured M8 maintenance envelope. This
serial library API has no HTTP listener or background scheduler. See
[resident serving operations](SERVING.md#resident-library-mode) for status,
backup/restore, failure handling and the 30-minute soak command. After
uncertainty, follow the
[fresh-prefix recovery procedure](RECOVERY.md).

## Segmented collections (library API)

The HTTP server is built on this engine; the sections above describe the
resident `Database` engine, which keeps every vector in RAM and suits small
collections. The two use different namespace formats, and opening one with the
other fails with an explicit error. With the `s3` feature for object storage,
`SegmentedServing` keeps only a compact ID directory, persisted per-pack
five-bit routing sketches and the unsealed log tail in memory. Vectors stay in
immutable object-storage packs read through a bounded RAM/NVMe block cache.
Unfiltered queries are approximate: they read a fixed number of routed blocks
and rerank them exactly. The one equality predicate declared at namespace
creation is answered exactly from full-precision vectors kept in the sketches;
declared routed keys restrict candidate rows before block ranking; all filters are checked during reranking (approximate, possibly fewer than k results), and `search_exact` remains the oracle. Run it
behind `admission::Service`, which executes seal, consolidation, reclamation
and cleanup in bounded units while no command is queued, and runs up to
`Limits::queries` queries in parallel on the latest acknowledged state.
`SegmentedServing::open` takes the namespace over, fencing every earlier
writer at the object store; hold a `lease::Lease` (as `glider-server` does)
so a live writer is not deposed.

```rust
use glider::{admission::{Limits, Service, Shutdown}, Config, Metric};
use glider::segmented::{SegmentedOptions, SegmentedServing, SegmentedServingOptions};
let config = Config { dimensions: 128, metric: Metric::SquaredEuclidean };
let declared = SegmentedOptions {
    resident_filter: Some(("cohort".into(), "one-percent".into())),
    routed_keys: vec!["half".into(), "pct".into()],
};
let engine = SegmentedServing::open(
    store, config, declared, SegmentedServingOptions::m21("block-cache".into()))?;
// Up to four queries run at once on reader threads beside the committer.
let service = Service::start(engine, Limits { queries: 4, ..Limits::default() })?;
let client = service.client();
let hits = client.query(vec![0.; 128], 10, vec![])?.wait()?;
service.shutdown(Shutdown::Drain)?;
```

The measured 250,000-row envelope, its gates and the reproduction command are
in [benchmarks/M24.md](../benchmarks/M24.md#single-machine-acceptance-protocol-declared-before-the-run).
