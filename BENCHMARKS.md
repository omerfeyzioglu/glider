# Benchmarks

Run from the repository root with Rust/Cargo installed. `benches/baseline.rs` is a
custom `cargo bench` harness using `Instant`, the public database API, and `LocalStore` (default) or feature-gated `S3Store`. A benchmark-only `libc` dependency supplies process resource counters
on Linux/macOS; there is no Criterion dependency. Archive tooling uses Python 3
and its standard library. Database implementation and durability are unchanged.
Normal correctness tests do not run performance workloads. CI compiles the
harness, tests metric/archive logic, and checks generated summaries. A dedicated compaction workload checks results and
deterministic counter repeatability; CI retains its raw reports as artifacts.
There are no latency thresholds.

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

[SUMMARY.md](benchmarks/SUMMARY.md) is the compact starting point;
[latest.json](benchmarks/latest.json) contains raw-run pointers. They show the latest
explicit baseline and latest observation per backend/result scope, chosen by
recorded timestamp (ties: content hash/run index). Experiments are not silently
promoted to baselines; missing timestamps are excluded. Different workloads can
appear in these inspection views and are never implicitly compared.

Detailed views remain reproducible with `python3 tools/benchmarks.py summary --full`:
`benchmarks/index.json` preserves normalized metrics and complete metadata, and
`benchmarks/HISTORY.md` renders every historical run and compatible pair. These
large derived files are ignored by Git; all immutable raw reports remain tracked.
Default `summary --check` verifies the compact tracked views and validates every
raw archive digest. Add `--full --check` to check regenerated details locally.
Raw JSON remains the source of truth; no measurements or metadata are discarded.

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

Explicit comparison also works without importing reports or changing their phase:

```sh
python3 tools/benchmarks.py compare target/baselines/before.json target/baselines/after.json
# Optional deterministic layout/counter check; any change requires review:
python3 tools/benchmarks.py compare target/baselines/before.json target/baselines/after.json --check-counters
```

Comparison requires matching complete workload, input, backend, protocol, environment,
feature and group identities; ambiguous/incompatible reports fail rather than
producing misleading percentages. Output includes revisions, raw hashes, signed
metric deltas and separately labeled maintenance observations. It does not impose
a latency/CPU/RSS threshold or infer significance. The optional counter check fails
on any changed measured byte/count/footprint/amplification value, including newly
missing values; it is a layout regression check, not a universal optimization rule.
CI runs identical compaction workloads twice and checks deterministic repeatability,
not performance improvement against an unrelated desktop baseline.

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


## S3 benchmarks

Select `--backend local|s3`; omitting it preserves the existing LocalStore defaults,
workloads, timing boundaries, version-2 JSON shape and measurement protocol.
The workloads are statically specialized for each backend, with the same vector
and query generation, search, commit phases, and replay validation.

For S3, enable the existing `s3` feature and supply configuration through the
environment before running. The harness has no endpoint or credential defaults.

| Variable | Meaning |
|---|---|
| `GLIDER_S3_ENDPOINT` | Required HTTP(S) service endpoint; no userinfo, query, or fragment |
| `GLIDER_S3_BUCKET` | Required pre-provisioned bucket |
| `GLIDER_S3_REGION` | Required signing region |
| `GLIDER_S3_NAMESPACE` | Required parent prefix reserved for benchmark runs |
| `AWS_ACCESS_KEY_ID` | Required access key; never included in reports |
| `AWS_SECRET_ACCESS_KEY` | Required secret key; never included in reports |
| `AWS_SESSION_TOKEN` | Optional session token; never included in reports |
| `GLIDER_S3_SERVICE_LABEL` | Optional server/version/topology description for reproducibility |

```sh
cargo bench --locked --features s3 --bench baseline -- --backend s3 --scenario all --rows 100 --mutations 300 --operations 30 --queries 100 --samples 5 --dimensions 32 --seed 42 --feature m2 --phase baseline --comparison-group s3-baseline > target/baselines/s3.json
```

Each scenario creates a fresh random child of the configured prefix and rejects a
nonempty namespace before setup. The exact child prefix is recorded per result;
the stable parent prefix is part of the comparison environment. The harness does
not delete remote objects. Retain or remove only the recorded benchmark children
as appropriate for the test bucket. Local temporary namespaces still clean up as
before. `--root` remains local client context; it is not S3 storage.

S3 reports use schema version 3 and protocol
`s3-v1-individual-query-timers-rusage`. Without explicit overrides their feature
is `m2`, phase is `baseline`, and comparison group is `s3-v1`. Existing version-1
and version-2 raw archives are read unchanged; unavailable historical fields
remain null. The generated index now uses schema version 2.

S3 endpoint, bucket, region, parent prefix, service label, addressing mode, retry
policy and default request timeout are recorded under `environment.s3`. Credentials
are excluded. All ordinary workload, Git, compiler, machine, CPU, RSS, percentile,
and throughput metadata remains available. CPU and RSS describe the benchmark
client process, not the remote service.

Transport metrics are separate from the existing successful engine-to-store
logical counters:

- `build_http_requests` covers setup for search/recovery.
- `measured_http_requests` covers search or each commit phase.
- `http_requests_per_sample` covers each timed recovery; archive totals sum all
  samples. Recovery total/replay rows overlap and must not be added together.
- HTTP counters include GET, each LIST page, PUT, DELETE, other attempts, attempted request
  body bytes including envelopes, HTTP error responses, and HTTP client call
  errors. Later response-body consumption errors are excluded from that last
  counter. Any workload error aborts the run; these are not failure-rate benchmarks.
- `store_open` times S3 client construction; `database_replay` performs the remote
  listing and reads; `total_open` includes both. One warmup reopen is performed,
  but server caches are uncontrolled. Local recovery keeps `local_store_open`
  and its warm OS-cache description.
- Untimed native listing records actual object count and summed object lengths.
  S3 physical-file counts and file footprint are null. Inventory/preflight requests
  are excluded from workload counters. Exact queries use no object-store requests.

LocalStore and S3Store are separate backend results. The archive requires matching
backend and S3 environment, as well as workload and timing protocol, for automatic
before/after feature comparisons. It never interprets a local-to-S3 switch as a
feature improvement or regression. Original LocalStore raw reports are not rewritten.

For isolated MinIO integration checks and small all-scenario smoke runs:

```sh
python3 tools/test_s3.py --benchmark-smoke target/backend-smoke
python3 tools/benchmarks.py archive target/backend-smoke/local.json
python3 tools/benchmarks.py archive target/backend-smoke/s3.json
python3 tools/benchmarks.py summary --check
```

The output directory must be new. The runner generates credentials, starts MinIO
on a fresh loopback port, runs integration and server-restart tests, validates
both smoke JSON reports and removes the container/data. Smoke runs use 10 rows,
4 dimensions, 30 mutations, 5 operations per commit phase, 5 queries, 2 samples,
k=3 and seed 42. They validate reporting and correctness; their tiny samples and
uncontrolled desktop load are not evidence of comparative backend performance.


## Checkpoint recovery (M3)

Recovery still defaults to full log replay. `--checkpoint-at N` publishes one
complete snapshot after put N during setup, then appends the remaining puts.
N must be positive and no greater than `--mutations` (0 disables checkpointing).
The same generated history, expected vectors, warmup and timers serve both modes.
Raw recovery JSON records checkpoint sequence, publication latency, logical bytes
written and create/HTTP counters separately; recovery timing excludes checkpoint
creation. Default output and historical reports are unchanged.

```sh
cargo bench --locked --bench baseline -- --scenario recovery --rows 100 --mutations 1000 --checkpoint-at 900 --feature segments --phase after --comparison-group m3-recovery-1000 --root target
python3 tools/test_s3.py --segment-benchmarks target/m3-recovery
```

The isolated MinIO runner measures both backends with 300 mutations, 30 live
vectors of dimension 32 and five warm reopens, both without a checkpoint and with
one at mutation 270. It verifies 301 versus 32 engine GETs per reopen (metadata,
snapshot and 30 tail mutations), and the matching S3 HTTP counts. This is a storage
layout experiment: `checkpoint_at` remains part of workload identity, so the
archive does not automatically pair different checkpoint settings as a feature
improvement. M3 demonstrates reduced GET and payload-replay work, with lower
warm-reopen latency observed in these runs; five warm samples under uncontrolled
load do not prove a definitive performance improvement. The 300-mutation runs
compare checkpoint settings in the same executable. Checkpoint creation cost is
measured separately and excluded from reopen latency, so these measurements do
not establish an amortized total-cost improvement or a cross-backend speedup.

Exact GET-count assertions are M3 layout regression checks, not permanent
correctness invariants. They encode the current metadata/snapshot/tail layout and
may change with M4 compaction. State preservation, sequence correctness and safe
recovery remain the correctness requirements regardless of request counts.

`--segment-benchmarks` is an explicit M3 recovery experiment, suitable for
manual or scheduled validation rather than every PR. It runs smoke/regression
checks for checkpoint setup, recovered results, reporting and request counts.
These are not performance acceptance tests: no latency or throughput threshold
gates a PR. The recorded timings do not establish performance on CI hardware.

LocalStore still reads/validates and syncs all retained files before engine
recovery. Its engine GET savings are not physical-I/O savings. S3 avoids GETs for
covered payloads but still lists the complete retained namespace. Checkpoint
creation adds one object, and M3 removes none. Desktop/MinIO measurements do not
establish cloud-provider latency or production durability.


## Compaction (M4)

`--compact-at N` compacts after put N in recovery setup (0 disables it). If a
checkpoint is scheduled at the same N, it runs first. Recovery timing excludes
both maintenance operations. Raw `compaction` results record latency, create/list/
remove counts, logical payload bytes, and S3 HTTP attempts including DELETE.
Zero removal counts are omitted to preserve default LocalStore report shape.

Additional read/write amplification divides compaction's engine payload bytes by
all mutation payload bytes through N; metadata and prior checkpoint bytes are
excluded from the denominator. This is maintenance overhead, not total workload
amplification or device I/O. The live map supplies the snapshot, so engine GET
bytes are zero; LocalStore's internal listing still reads and validates envelopes.
No physical read/write amplification is measured.

```sh
python3 tools/compaction_benchmark.py --output target/compaction.json
# Same workload without compaction: add --checkpoint-only. S3: add --backend s3.
python3 tools/test_s3.py --compaction-benchmarks target/m4-recovery
cargo bench --locked --bench baseline -- --scenario recovery --rows 100 --dimensions 32 --mutations 300 --checkpoint-at 300 --compact-at 300 --samples 3 --seed 42 --root target
```

The standalone runner and MinIO runner share the same validation and use 300 round-robin puts over 100 live vectors, dimension 32, seed 42,
a checkpoint at 300 and three warm reopens. It validates identical inputs and
recovered vectors, two engine GETs per reopen, and 302 objects before versus two
after compaction on both backends. Compaction adds one 36,413-byte payload and
removes 301 objects: 0 additional engine read amplification and 0.2831 additional
write amplification relative to 128,625 mutation payload bytes. MinIO records
one PUT, one LIST and 301 DELETE attempts, with no GET or retry.

| Backend / raw measurements | Warm reopen p50 before → after | Compaction latency |
|---|---:|---:|
| LocalStore ([before](benchmarks/runs/e2df5ef8d12496d57db96a9ccbc69f0922aa0e8349cc714bc64d0485c07003a8.json), [after](benchmarks/runs/8e922662f8e08c4a87fc61937389763456c862b684c416ba6bc743e56f908a58.json)) | 870.677 → 37.140 ms | 3537.080 ms |
| MinIO ([before](benchmarks/runs/a6a9341d47616b20e89e3ddbf778401fbf991028e7ef952ede0f5a130f8b4658.json), [after](benchmarks/runs/78f2dbf2f5dc871683a6c51e0a6f3337031afe7b75e709a4f721d73169e6be2e.json)) | 7.114 → 2.627 ms | 150.213 ms |

These compare layouts in the same executable. `compact_at` is part of workload
identity, so the archive does not automatically pair them. A separate
[pre-change LocalStore baseline](benchmarks/runs/65aaa5bb71e71382d95104711b3c5f2a01e02a234e83032ecf7b23ff1fbeb4b1.json)
measured 22.580 ms for the uncompact layout. That variability, uncontrolled desktop
load/cache and three samples preclude a stable latency or amortized-cost claim.
The observed object-count reduction is the demonstrated result. Retained bytes
exclude S3 historical versions/delete markers. Raw reports retain environment,
source hashes, all samples and exact amplification values.


## Search characterization (M5)

```sh
python3 tools/search_benchmark.py --output target/m5-search
python3 tools/search_benchmark.py --smoke --output target/search-smoke
python3 tools/test_s3.py --search-smoke target/search-s3-smoke
```

The full matrix varies one factor around 1,000 rows, 128 dimensions and k=10:
10,000 rows; 32 or 768 dimensions; k=100. Each case runs twice in a fresh process
with seed 42, 100 queries and five timed batches (500 individual query samples).
This is a provisional synthetic workload, not a production capacity target.
`--backend s3` uses the existing explicit S3 environment; the MinIO smoke command
provisions an isolated service. CI runs small local/S3 correctness matrices and
retains their reports, without latency thresholds.

The runner preserves the existing Cargo harness, generator, timings and raw JSON.
It independently regenerates input fingerprints and checks the first three query
answers with a scalar f64 oracle outside the measured process. All saved answers
must match across repeats and form consistent prefixes across k. Every query must
issue zero engine storage calls/bytes and, on S3, zero HTTP requests. The matrix
inventory records raw-file hashes and the independently checked query indices.

These are warm in-memory queries after ingestion and one full warmup pass. Cold
CPU/OS caches and cold startup are not measured; opening a process alone would
not evict those caches. The current query path holds all vectors in memory, so
cold object-store query latency is not represented. RSS is the Rust process's
lifetime high-water mark, including setup; it excludes the separate Python oracle.

Recorded on Apple M4/macOS, with desktop load and power uncontrolled. Ranges below
span the two invocations; throughput uses batch time. Raw reports retain all
samples, CPU counters, RSS, exact neighbor IDs, source/commit and environment data.

| Rows × dimensions / k (raw runs) | p50 µs | p95 µs | Queries/s | Peak RSS MiB |
|---|---:|---:|---:|---:|
| 1,000 × 128 / 10 ([r1](benchmarks/runs/7aeb9adc7cdcd3fd24cf0001532858c9a8c5c9c223b0d9d72c61871a2731eedf.json), [r2](benchmarks/runs/c56ec61b5deaae2a68f70bb8bb64fe72d8424acb6cf5f5e17be64587df344c50.json)) | 87.75–87.96 | 153.12–153.83 | 10304–10907 | 4.05–4.67 |
| 10,000 × 128 / 10 ([r1](benchmarks/runs/1720fc520c7f20c1627205d592465bfc79d24ce7653d08b4f1102ec76b24a2fd.json), [r2](benchmarks/runs/ce2c0a14454d31f911852fff79f87e2c441a48567d259587159f71e811dfc8e7.json)) | 551.38–562.67 | 583.42–610.00 | 1777–1782 | 23.42–23.50 |
| 1,000 × 32 / 10 ([r1](benchmarks/runs/8849c1b2907eb1cc1a5aeede19097d68c0dba2e6f91d4492301ed5645b2bcdfa.json), [r2](benchmarks/runs/927d6eefc65ee50511d0c0f32f90776dd6720ea1117d14921f003504a1ba0862.json)) | 48.79–49.17 | 57.50–57.54 | 20139–20163 | 4.14–4.23 |
| 1,000 × 768 / 10 ([r1](benchmarks/runs/483ffca854fca02169725b44fb22f4a94c87d888046198f9f62b5f5948035a91.json), [r2](benchmarks/runs/43d319d1f08b47c4c09a223e320c719e822733ccaf333253817a0566903cc1d5.json)) | 360.96–361.33 | 425.58–425.79 | 2698–2701 | 7.55–7.56 |
| 1,000 × 128 / 100 ([r1](benchmarks/runs/9a6eedf68c6f6b03830d3e8a58a1c12ae9b956f0a9dc22e411c4360901c608e1.json), [r2](benchmarks/runs/9ba388d3a60cb66dc96226c06518739f9ad7e495cc7e1eb3fb6963fbd533f7b6.json)) | 87.88–87.88 | 153.75–154.08 | 10290–10400 | 4.62–4.62 |

Larger row counts and dimensions increased observed query cost. Raising k from
10 to 100 left the 1,000 × 128 median near 88 µs, consistent with the engine
sorting every scored vector before truncation. This does not isolate scoring
versus sorting time: profile those stages before selecting an optimization.
Two invocations and synthetic uniform vectors do not establish tail guarantees,
production throughput, or an ANN requirement. M5 changes measurement coverage,
not engine performance; the next design decision still needs a workload/recall
budget and profiling evidence.

## Native search profiling and exact top-k

```sh
python3 tools/profile_search.py --output target/search-profile
```

On macOS this builds the normal harness with bench debug information, waits for
`PROFILE_READY <pid>` after ingestion and oracle warmup, and samples five seconds
of an eight-second diagnostic search loop. `--profile-seconds N` is opt-in and
search-only; its default is zero and omitted from ordinary reports. Other native
profilers can attach at the same marker. Profiled JSON is diagnostic, not an
unprofiled latency baseline; its configuration and build environment distinguish it.

For 10,000 vectors × 128 dimensions, k=10, seed 42, the
[compressed native sample](benchmarks/profiles/ea12da7154ec2145c8a9b9d28ba86bbfc787c0904ceb96faa7a3f5471f19c00e.sample.txt.gz)
and [diagnostic metadata](benchmarks/profiles/08fde9b3852be8f80cd3da950b429364dd678b6305f48d9b40484e70866263c1.json)
record 4,169 main-thread samples: 3,039 in `Metric::score` (about 73%) and
1,036 at the full-sort call (about 25%). These are sampled stack shares, not exact
stage timers or hardware counters. Nested sort frames must not be added together.
Profiles stay outside `runs/`, so they do not become latest latency baselines.

Scoring is the larger cost, but sorting is material. Search now partitions to the
best k and sorts only that prefix, removing unnecessary ordering work with the
existing candidate vector. A bounded heap could instead reduce candidate memory
to O(k), but adds per-candidate heap bookkeeping and comparator machinery; memory
pressure has not been demonstrated here. Partitioning still scores every vector
and retains O(N) candidate storage, so it does not remove the eventual need for
ANN if the workload requires fewer distance calculations.

Fresh unprofiled before/after runs use 1,000 rows, seed 42, 100 queries and five
batches, with two process invocations per case. All six pairs preserve every
ordered neighbor ID, build counters, query counters and storage footprint.

| Dimensions / k | Before p50 µs | After p50 µs | Paired p50 reduction |
|---|---:|---:|---:|
| 128 / 10 | 77.29–88.42 | 63.38–77.54 | 12.3–18.0% |
| 768 / 10 | 361.13–361.63 | 351.46–351.63 | 2.7–2.8% |
| 128 / 900 | 87.79–88.79 | 74.88–75.17 | 14.4–15.7% |

These desktop measurements do not establish a universal speedup: load and power
were uncontrolled, and 768-dimensional p99 increased from 452.54–455.25 µs to
519.29–523.88 µs. The modest high-dimensional median gain is consistent with
scoring dominating. No timing threshold or candidate-memory improvement is claimed.
Both phases report the same dirty parent commit; their recorded source hashes
distinguish the implementations. Raw runs remain in the archive under feature
`exact-top-k`, phases `before`/`after` and groups `topk-r1`/`topk-r2`; inspect
`benchmarks/SUMMARY.md`, or generate the complete index with
`python3 tools/benchmarks.py summary --full`.

Reproduce each case at its respective source version, substituting phase,
dimensions, k and repetition (1 or 2):

```sh
cargo bench --locked --bench baseline -- --scenario search --backend local \
  --rows 1000 --dimensions 128 --k 10 --queries 100 --samples 5 --seed 42 \
  --feature exact-top-k --phase after --comparison-group topk-r1 \
  --root target --label 'top-k experiment; desktop load and power uncontrolled' > target/topk.json
python3 tools/benchmarks.py archive target/topk.json
python3 tools/benchmarks.py compare BEFORE.json AFTER.json --check-counters
```

## Short algorithm comparison (IVF-Flat)

```sh
python3 tools/ann_benchmark.py --output target/ann
# Tiny correctness-only CI run:
python3 tools/ann_benchmark.py --smoke --output target/ann-smoke
# Retain raw runs and regenerate benchmarks/ANN.md:
python3 tools/ann_benchmark.py --output target/ann-recorded --archive
```

The quick workload uses 512 rows × 64 dimensions, k=10, seed 42, 24 independent
queries and three timed batches. It compares exact search with 16 IVF partitions,
eight training iterations, and 1/4/16 probes, twice per setting with reversed
order on the second pass. Uniform and clustered datasets expose different recall
trade-offs. Clustered inputs use 16 fixed centers plus uniform noise scaled by
0.1; this deliberately favorable distribution is not representative embeddings.
The smoke workload is 48 × 8 with 4 partitions, 8 queries and two batches.
Existing search, commit, recovery and compaction defaults/history are unchanged.

Each invocation uses the ordinary LocalStore harness and a fresh durable setup.
Only warm queries are timed; explicit index build time is reported separately.
Every saved exact answer is checked by an independent Python scalar oracle.
Full-probe answers must equal exact; repeats must preserve answers, candidate
counts and storage counters. No arbitrary latency or partial-probe recall gates
are imposed. `tools/benchmarks.py compare A.json B.json --check-counters` also
reports ANN build time, recall and distance counts; changes in deterministic
quality/work counts require review, while build/query timings are informational.
Different probe counts are different workloads, not automatic regression pairs.

Raw reports retain commit/source/environment, seed/input hashes, exact and ANN
IDs, per-query recall and centroid/vector evaluations, all timing samples, CPU,
process-lifetime peak RSS and backend metrics. RSS includes setup and the index;
it is not isolated index memory. Dataset setup dominates wall-clock benchmark
runtime and is excluded from query timings. The small quick workload is for
seeing trade-offs; use the harness flags for larger datasets before choosing
production parameters. Latest compact results: [ANN.md](benchmarks/ANN.md).
