# Benchmarks

Run from the repository root with Rust/Cargo installed. `benches/baseline.rs` is a
custom `cargo bench` harness using `Instant`, the public database API, and the real
`LocalStore`. It reuses existing dependencies; there is no Criterion dependency.
Normal correctness tests do not run benchmarks. CI's existing Clippy command
compiles the harness; there are no latency thresholds.

```sh
mkdir -p target/baselines
cargo bench --locked --bench baseline -- --help
cargo bench --locked --bench baseline -- --root target > target/baselines/all.json
```

The default runs all scenarios at dimension 32, seed 42: search over 1,000 rows
with 100 queries, k=10 and five timed batches; 200 commits per phase; and five
warm recoveries of 5,000 mutations over 1,000 live IDs. Progress goes to stderr;
stdout is one JSON document. Errors abort the run. Each scenario owns a fresh
randomly named temporary namespace beneath `--root`, removed after measurement.
Use an existing directory on the filesystem you intend to measure, with enough
space. Cleanup and setup can take much longer than measured operations.

## Measurement boundaries

- **Search:** generate and hash documents and independent queries, insert through
  the public API, then run one untimed query pass. Time repeated batches of those
  queries, including result allocation/sorting and result destruction. Samples
  are batch durations; amortized latency is batch time divided by query count.
  Batch p95 is **not** per-query tail latency. Save exact ordered neighbor IDs from
  the warmup as an oracle for future ANN recall comparisons.
- **Commit:** on a fresh initialized database, time each acknowledged insert,
  then overwrite the same IDs, then delete them. Generate vectors and transfer
  them from the input iterator outside the timer. Serialization, hashing, file
  publication/synchronization and map updates stay inside it. Metadata creation
  is excluded; no mutation warmup is performed. Phases run sequentially on the
  same history. Throughput uses summed operation times, excluding harness gaps.
- **Recovery:** create `--mutations` round-robin puts over `--rows` IDs (must be
  at least rows), then close the database. Perform one untimed warm recovery.
  Each sample times a new `LocalStore::open` followed by `Database::open`;
  report the two phases and total independently. Validate all live values and
  destroy the handle outside timing. Increasing mutations with fixed rows
  isolates history growth from live size. This is warm OS-cache recovery in one
  process, not process launch or cold-cache recovery.
- **Storage:** a benchmark-only wrapper counts engine-to-store get/list/create
  calls and payload bytes. It misses backend-internal reads, directory operations,
  syncs, and physical device traffic. Inventory, after closing the database and
  outside timers, measures logical objects, physical files, and summed file
  lengths. File lengths are **not** allocated disk space or bytes written to the
  device. The current immutable successful-write workload makes them a useful
  persisted-footprint baseline; do not infer future compaction write volume from
  final footprint. The wrapper can later wrap another object backend.

All vectors use the specified SplitMix64/high-24-bit generator, independent
uniform components in [-1, 1), and squared Euclidean distance. Dataset and query
SHA-256 values cover f32 bits in little-endian order. Query seed is the dataset
seed XOR `0xd1b54a32d192ed03`, independent of row count. Overwrite phase uses seed+1
(wrapping); IDs are ascending, starting at zero. Recovery history contains puts
only; commit phases separately cover deletes. These are synthetic workloads, not
claims about real embedding distributions.

## Comparable runs

Vary one axis at a time. For example, the initial baseline matrix is:

```sh
mkdir -p target/baselines
for d in 32 128; do
  for n in 1000 10000; do
    cargo bench --locked --bench baseline -- --scenario search --rows "$n" --dimensions "$d" --seed 42 --queries 100 --samples 5 --root target --label "record filesystem/device/power/load" > "target/baselines/search-$n-$d.json"
  done
  cargo bench --locked --bench baseline -- --scenario commit --dimensions "$d" --operations 200 --seed 42 --root target --label "record filesystem/device/power/load" > "target/baselines/commit-$d.json"
done
for h in 100 1000 5000; do
  cargo bench --locked --bench baseline -- --scenario recovery --rows 100 --dimensions 32 --mutations "$h" --samples 5 --seed 42 --root target --label "record filesystem/device/power/load" > "target/baselines/recovery-$h.json"
done
```

Run serially on an otherwise idle machine; repeat full invocations to assess
noise. Keep compiler, build settings, filesystem, power mode and workload fixed.
JSON records configuration, raw nanosecond samples, nearest-rank p50/p95, CPU,
memory, OS, mount information, compiler, build environment, revision, dirty status,
and a content fingerprint of Cargo files and Rust source under `src/` and
`benches/`. Always invoke via Cargo so the binary matches those source files.
`--label` records additional device/filesystem, power, and competing-load context.
Unavailable machine metadata is null. Keep nonstandard Cargo configuration with
results; runtime metadata cannot reconstruct every compiler or OS setting.

Do not compare five samples as statistically established tail behavior. Timers,
counter instrumentation, allocator state, filesystem caches, background work,
thermal/power scheduling, and the fixed scenario order contribute noise. No cache
eviction, CPU pinning, power-loss modeling, network, concurrency or ANN measurement
is performed. Exact search currently uses the in-memory map and should show zero
store calls during queries. Local recovery's internal validation reads are not
fully represented by the wrapper's logical read counts.

Ad hoc output belongs under ignored `target/baselines/`. Deliberately retained
baselines live under `benchmarks/baselines/`; the initial machine run is
[2026-09-13-local.json](benchmarks/baselines/2026-09-13-local.json). It contains the
nine reports from the matrix, including raw samples and workload/source context.
No implementation tuning was performed from these measurements.
