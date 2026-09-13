# Benchmarks

Run from the repository root with Rust/Cargo installed. `benches/baseline.rs` is a
custom `cargo bench` harness using `Instant`, the public database API, and the real
`LocalStore`. A benchmark-only `libc` dependency supplies process resource counters
on Linux/macOS; there is no Criterion dependency. Archive tooling uses Python 3
and its standard library. Database implementation and durability are unchanged.
Normal correctness tests do not run performance workloads. CI compiles the
harness, tests metric/archive logic, and checks generated summaries; there are no
latency thresholds.

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
  queries, including result allocation/sorting and result destruction. Version 2
  records individual query durations as well as batch durations; batch throughput
  includes the extra query-timer bookkeeping. Latency percentiles in the new
  summary use individual query samples. Legacy batch p95 is **not** per-query tail
  latency and remains labeled as a batch. Save exact ordered neighbor IDs from
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
JSON records configuration, raw nanosecond samples, nearest-rank p50/p95/p99/max, CPU,
memory, OS, mount information, compiler, build environment, revision, dirty status,
and a content fingerprint of Cargo files and Rust source under `src/` and
`benches/`. Each new report also records `feature`, `phase` (baseline/before/after),
`comparison_group`, Git revision, the measurement protocol, explicit dataset/query
seeds and input fingerprints. Queries not used by a scenario have null metadata. Always invoke via Cargo so the binary matches those source files.
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

## Process metrics and compatibility

`benches/support/metrics.rs` owns tested metric calculations. Percentiles use
nearest rank, without interpolation. Throughput is completed operations divided
by timed seconds. Zero measured latency stays zero; undefined throughput and
unavailable measurements are null.

`getrusage(RUSAGE_SELF)` captures process user/system CPU time around measured
loops; recovery sums open windows and excludes validation/destruction. Query and
commit CPU windows include timers and loop bookkeeping. Resource probes sit
outside latency timers. CPU does not include child processes. Maximum RSS is the
**absolute process-lifetime high-water mark**, including setup and any earlier
scenarios; it is neither current RSS nor a per-operation allocation measurement.
Linux's KiB values are converted to bytes; macOS already reports bytes. Unsupported
platforms/probe failures produce null. Keep scenario order fixed or run scenarios
in separate invocations when comparing RSS.

Version 2 retains the old fields and adds `query_latency`, p99, resource samples,
and comparison metadata. Version 1 reports and the original aggregate archive
remain supported without modification. Missing legacy CPU/RSS/p99/feature/phase
metadata stays null; installed machine RAM is never substituted for process RSS.
Change `measurement_protocol` whenever instrumentation or timing boundaries change.
The changed query timing protocol prevents automatic version-1/version-2 latency
comparisons. Do not claim a regression or improvement across that boundary.

Normalized storage counters mean logical payload bytes and engine-to-store calls,
not backend-internal I/O. Recovery counters sum the recorded open samples. The
replay and total rows overlap; do not add them together. Component CPU is null
because only whole-open CPU was measured. Commit phase footprints are null because
the harness inventories after the complete insert/overwrite/delete workload;
`commit/final-footprint` contains the measured final object/file counts and bytes.

## Archive and deterministic summaries

```sh
python3 tools/benchmarks.py archive target/baselines/all.json
python3 tools/benchmarks.py summary
python3 tools/benchmarks.py summary --check
```

The default archive is `benchmarks/` (override with `--archive DIRECTORY`). Raw
single reports are preserved byte-for-byte as `runs/<sha256>.json`; reimporting the
same bytes is idempotent and a corrupted existing object is rejected. Original
`baselines/*.json` archives are read in place, without duplication or rewriting.
Keep these raw reports in version control. Archive imports and summary rebuilds
should run serially; derived index/summary publication uses atomic local file
replacement. This tooling is independent of the database storage protocol.

[SUMMARY.md](benchmarks/SUMMARY.md) is generated for human review;
[index.json](benchmarks/index.json) contains normalized metrics, complete workload
and environment context, source references, and comparison deltas. Both can be
deleted and rebuilt from raw JSON. Ordering is deterministic, with no generation
timestamp or machine-dependent state added during rendering. `--check` reads only
and fails if either derived file is stale. Raw JSON is the source of truth.

For a future feature comparison, use identical workload arguments and environment
notes on both revisions, and archive the before run before changing code. For separate Git worktrees, use
the same absolute `--root` directory in both runs (run serially):

```sh
cargo bench --locked --bench baseline -- --scenario recovery --rows 100 --mutations 1000 --feature segments --phase before --comparison-group segments-100 --root target --label "same device and power mode" > target/baselines/before.json
python3 tools/benchmarks.py archive target/baselines/before.json
# After implementing and validating the feature, on its new Git revision:
cargo bench --locked --bench baseline -- --scenario recovery --rows 100 --mutations 1000 --feature segments --phase after --comparison-group segments-100 --root target --label "same device and power mode" > target/baselines/after.json
python3 tools/benchmarks.py archive target/baselines/after.json
```

A pair requires exactly one before and one after with the same feature, group,
workload parameters, input seeds/hashes, scenario order, timing protocol, backend,
cache/warmup policy, compiler/build configuration, CPU/memory/OS, filesystem device,
root and environment label. Git revision, timestamp, source hash, free disk space,
and measured output may differ. Unknown required identity fields, duplicate phases,
or mismatches leave runs unpaired; the summary does not pick a convenient latest
run. Use a new comparison group for another controlled pair. Baseline rows are
standalone references, never silently treated as before runs. Labels select no
database functionality: `--feature segments` only describes the experiment.

Changes are `(after - before) / before * 100`; missing values and zero denominators
produce null. Higher throughput generally helps; higher latency/CPU/RSS/bytes/counts
generally costs more. Equal-revision pairs are labeled repeatability comparisons,
not evidence of a feature effect. Environment matching is a guard, not proof of
identical thermal state or absence of background load.

Focused checks (no performance thresholds):

```sh
cargo test --locked --test benchmark_metrics
python3 -m unittest discover -s tests -p 'test_benchmarks.py'
python3 tools/benchmarks.py summary --check
```

Ad hoc output belongs under ignored `target/baselines/`. Deliberately retained
baselines live under `benchmarks/baselines/`; the initial machine run is
[2026-09-13-local.json](benchmarks/baselines/2026-09-13-local.json). It contains the
nine reports from the matrix, including raw samples and workload/source context.
No implementation tuning was performed from these measurements.
