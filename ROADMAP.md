# ROADMAP.md

This roadmap describes the current development direction, not a fixed feature
commitment. Milestones may change when measurements or correctness findings
justify it.

## M1 — Durable exact vector store

Status: complete

Goal:
Establish correct single-writer durability, recovery, and exact vector search.

Done when:
- put / get / delete work
- exact top-k search is correct
- acknowledged writes survive restart
- uncertain writes require safe recovery
- corruption and interrupted writes are tested
- canonical Rust checks pass

## M2 — Real object-store backend

Status: complete (S3-compatible backend and MinIO integration tests)

Goal:
Validate that the engine's storage contract works against S3-compatible object
storage rather than only the local filesystem implementation.

Done when:
- an S3-compatible backend implements the existing ObjectStore contract
- integration tests run against MinIO or equivalent
- restart/recovery semantics match the local backend
- object-store request behavior is measurable
- engine code does not depend on backend-specific filesystem behavior

Not in scope:
- distributed writers
- sharding
- ANN

## M3 — Persistent immutable segments

Status: complete (single-object and optional bounded-chunk checkpoints, safe tail
replay, LocalStore/MinIO failure tests and archived legacy recovery measurements)

Problem:
Individual writes create one object each; atomic batches reduce that cost for
grouped writes, but object count and full-replay recovery still grow with the
number of published write groups.

Goal:
Introduce immutable persistent state that reduces dependence on replaying the
entire mutation history.

Done when:
- segment semantics and publication are explicit
- recovery can start from persisted compact state plus newer mutations
- logical results remain identical to full replay
- restart cost is benchmarked before and after
- failure during segment publication cannot corrupt authoritative state

## M4 — Compaction

Status: complete (single-object and optional bounded-chunk consolidation, safe
history reclamation, LocalStore/MinIO failure tests and archived legacy
amplification measurements)

Goal:
Bound accumulation of immutable persistent state without changing logical
results.

Done when:
- multiple segments can be merged safely
- newest-value and delete semantics are preserved
- interrupted compaction is recoverable
- compaction write/read amplification is measured

## M5 — Search baseline and performance characterization

Status: complete (archived warm synthetic matrix, independent exact-oracle checks,
local/S3 smoke validation; cold caches and production capacity remain unmeasured)

Goal:
Establish reproducible performance baselines before approximate indexing.

Done when:
- reproducible vector datasets and query workloads exist
- exact search latency and throughput are measured
- cold/warm behavior is distinguished where relevant
- bytes read and object-store requests can be observed
- benchmark results are reproducible

## M6 — Approximate nearest-neighbor index

Status: complete (rebuildable IVF-Flat, exact-oracle recall comparison and
archived synthetic measurements; optional persisted derived cache, no recall target)

Goal:
Reduce vector-search cost while explicitly measuring quality loss.

Done when:
- one ANN design is selected from justified alternatives
- ANN state is derived/rebuildable
- recall@k is measured against exact search
- latency / throughput / resource trade-offs are measured
- persistence and restart behavior are defined

## M7 — Filtering and query execution

Status: complete (durable string metadata and exact/IVF equality filtering;
reproducible filtered ANN quality evaluation; adaptive probe expansion fills
available matches; read-only streaming exact search over chunked snapshots;
exact default and explicit approximate execution policy)

Goal:
Support metadata filtering without silently destroying search correctness.

Done when:
- filter semantics are correct independently of ANN
- filtered exact search provides a correctness baseline
- ANN + filtering behavior is measured against that baseline
- query execution decisions are justified by measurements

The current policy leaves automatic exact-versus-IVF planning for later work:
measured sparse-filter recall and result counts do not justify a hidden
approximate choice. See `DESIGN.md` and `benchmarks/FILTERING.md`.

## Next: dependable single-machine object-storage search

The next milestones target one machine with one authoritative writer and an
object store. They do not assume shared disk, POSIX locks, or multiple writers.
The goal is a usable capacity envelope with explicit durability, recovery,
accuracy, latency and resource limits, not feature parity with another engine.

For each milestone, reproduce the relevant bottleneck first, retain a small
baseline, make the change, and rerun only the affected workload and correctness
checks. Record backend, data/query fingerprints, seed, configuration, code
revision, requests/bytes, latency, CPU and peak memory where relevant. Keep CI
focused on deterministic correctness and failure cases; do not turn every change
into a full benchmark run. Insert or reorder a small milestone when a measured
bottleneck or correctness failure changes the priority. Persist architectural
decisions and changed guarantees in `DESIGN.md`.

## M8 — Define the single-machine operating envelope

Status: complete for the initial 2,000-vector deployment envelope.

The workload, numeric budgets, raw local/MinIO baselines and bottleneck list are
in [benchmarks/M8.md](benchmarks/M8.md). The long mutation tail exceeds the
restart budget, and sparse-filter IVF misses the stated quality budget; both
are explicit inputs to M10–M12. Larger or real embedding collections require a
new envelope rather than extrapolation from these synthetic measurements.

Goal:
Set measurable acceptance limits using workloads that resemble intended use,
so later optimizations have a target rather than an assumed benefit.

Steps:
- Define dataset size, dimensions, metadata cardinality/selectivity, update and
  delete rate, query mix, k, and the memory and recovery budgets for an initial
  deployment. Include a sparse-filter and a long mutation-tail case.
- Capture targeted local and MinIO baselines for ingest, restart, exact and IVF
  search. Separate warm and cold starts where the distinction is measurable;
  include an actual S3-compatible service before claiming deployment behavior.
- Record p50/p95/p99 latency from enough independent queries to interpret tails,
  throughput, peak RSS, object count, GET/PUT/DELETE and bytes, and ANN recall@k
  against the independent filtered exact oracle.

Done when:
- The workload, reproducible inputs and numeric acceptance budgets are recorded.
- A bottleneck list identifies which cost is CPU, memory, listing/recovery,
  remote requests/bytes, write amplification or ANN quality; no optimization is
  selected from synthetic timing alone.

## M9 — Harden ownership, failures and recovery

Status: complete for the M8 single-machine envelope.

The ownership boundary is in PR #30. LocalStore and MinIO process-exit tests
cover batch, chunk, manifest, derived-index and compaction-cleanup boundaries;
MinIO tests also cover deterministic request timeouts and selected-object damage.
The authoritative-root and uncertain-write procedure is in
[docs/RECOVERY.md](docs/RECOVERY.md). Same-prefix takeover after a crash is
unsafe without proof that old requests have quiesced, so M9a stages a fresh
prefix. Arbitrary external loss still requires backup or a separate witness.

Goal:
Make the single-writer operating model and every uncertain publication safe to
run and diagnose on a real object store.

Steps:
- Exercise process termination, timeout, lost acknowledgement and restart at
  batch, chunk, manifest, derived-index and compaction-cleanup boundaries. Check
  that acknowledged state survives, incomplete state is invisible, and recovered
  exact results match the logical oracle.
- Test missing/corrupt selected objects, sequence gaps and poisoned handles on
  both backends. State precisely which external object loss is detectable and
  which requires a separate witness or backup; do not claim protection the
  current object contract cannot provide.
- Define and enforce the deployment's exclusive-writer ownership boundary.
  Evaluate storage-safe fencing or a verifiable single-owner process contract
  before allowing concurrent opens; filesystem locking cannot be the engine's
  correctness mechanism.

Done when:
- Automated failure and reopen tests cover the chosen crash model, including
  accidental double ownership, without partial results or silent divergence.
- A written recovery procedure identifies the authoritative root and the
  required action for uncertain writes and unrecoverable corruption.

### M9a — Isolate late requests during takeover

Status: complete for the M8 single-machine envelope.

A timed-out S3 PUT can publish after the old process exits. The owner claim
prevents a second live writer but cannot fence a request already in flight.
The recovery path must stage a frozen validated state in a fresh prefix, publish
metadata last, and switch clients only after validation and a new owner claim.
Test the stale-view counterexample, late old-prefix publication, interrupted
staging and restart on LocalStore and MinIO. Do not reuse a failed destination.

## M10 — Bound memory, recovery and write-side amplification

Status: complete for the M8 initial envelope; larger workloads require a new envelope

The 2,000-mutation stress input now opens from 20 version-3 batch objects in
27.3 ms p95 on loopback MinIO, with 21 GETs, one LIST page and 11.7 MiB peak
client RSS. At 2,000 live rows, 128 KiB chunked compaction opens in 28.0 ms
with 14 GETs and 13.2 MiB peak RSS; maintenance writes 0.980 times the input
mutation payload and leaves 14 objects. The runtime M8 policy signals
compaction before its 24-object hard tail, rejects writes at that bound without
publication, and reconstructs counters after restart. Interrupted compaction
resumes from its committed root. Existing persisted versions remain readable.
See `benchmarks/M10.md` for the narrow measurements and layout assumptions.

Goal:
Keep opening, ingesting, checkpointing and compacting within the M8 resource
budgets as live data, manifest entries and mutation history grow.

Steps:
- Measure each component separately: full-map loading, mutation-tail replay,
  whole-namespace listing, manifest decoding, checkpoint serialization and
  compaction's temporary coexistence of old and new objects.
- Choose versioned, object-store-safe state/catalog and maintenance changes only
  for measured limits. Bound or page manifest and tail state where needed; avoid
  full-map clones in maintenance. Preserve the ability to detect invalid
  selected roots and replay gaps.
- Define explicit maintenance triggers and backpressure for long tails or too
  many objects; an interrupted maintenance operation must remain resumable.

Done when:
- Peak memory, open time and object/request growth stay within the M8 budgets
  at the target live size and update history, including after crash/restart.
- New formats have versioned compatibility and crash tests; old namespaces open
  or fail with an explicit migration path.

## M11 — Make filtered exact queries selective on object storage

Status: planned

Goal:
Avoid reading every vector chunk for a selective equality filter while keeping
filtered exact search a no-false-negative correctness oracle.

Steps:
- Compare chunk summaries and a derived metadata posting layout on the M8
  filter distributions. Select a layout by GET count, bytes, memory, write cost
  and recovery behavior, then version and validate it.
- Apply newer puts/deletes over the selected base consistently. A missing or
  stale derived structure must never silently omit an eligible document;
  fall back to a validated exact scan or return an explicit error.
- Measure selective and nonselective predicates against the existing streaming
  full scan. Arbitrary unfiltered exact kNN may still require every vector;
  do not promise selective reads without a sound pruning rule.

Done when:
- Results and distance/ID ties match the independent filtered exact oracle
  across updates, compaction, restart and injected failures.
- Selective workloads meet their M8 request, byte, latency and memory budgets;
  the nonselective path has no unjustified regression.

## M12 — Search persisted ANN partitions without loading all vectors

Status: planned

Goal:
Turn IVF from an in-memory candidate baseline into a useful object-store query
path, while authoritative vectors remain recoverable without the derived index.

Steps:
- Test a versioned partition layout that fetches only probed candidates and
  supports exact reranking and the M11 filter path. Compare alternatives before
  fixing the layout; include index build, update and remote GET costs.
- Tie each index generation to a committed mutation boundary. Define rebuild,
  publication, invalidation and garbage collection so stale or partial indexes
  cannot silently answer a newer query.
- Evaluate recall@k and short-result rate by filter selectivity, alongside
  p95 latency, requests/bytes, index-build time and peak RSS. Keep exact search
  available and require an explicit approximate-quality policy before any
  automatic planner.

Done when:
- Restart and failure tests prove indexes are derived and safe to discard or
  rebuild; exact results remain correct without them.
- The selected deployment workload meets its M8 ANN quality and resource
  budgets, or the result is recorded and the exact path remains the supported
  serving mode.

## M13 — Single-machine serving and recovery operations

Status: planned

Goal:
Run the selected workload continuously on one machine with predictable reads,
maintenance, restart and restore behavior.

Steps:
- If concurrent readers and a writer are needed, add pinned committed views
  and safe reclamation under the M9 ownership contract. Test visibility and
  garbage collection with overlapping reads, writes and crashes; keep one
  authoritative writer.
- Provide bounded maintenance scheduling, observability for sequence, tail,
  index freshness, storage errors and resource use, and actionable error paths.
- Define and rehearse backup/restore of a committed root plus all referenced
  objects on the chosen service. Test migration from supported old formats and
  interruption during restore without claiming recovery from arbitrary loss.

Done when:
- A repeatable soak with reads, updates, maintenance and restarts stays inside
  the M8 budgets and passes exact-oracle and durability checks.
- Operators can identify the committed state, restore it, and explain the
  system's documented failure and capacity limits.

## Beyond the single-machine target

Sharding, replication, consensus, multi-writer execution, GPU work,
quantization and additional index families need separate requirements and
measurements. They are not prerequisites for this roadmap.
