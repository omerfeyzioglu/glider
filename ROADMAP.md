# ROADMAP.md

This roadmap describes the current development direction, not a fixed feature
commitment. Milestones may change when measurements or correctness findings
justify it.

## Working method

- Start from a concrete failure, measured bottleneck or explicit scaling requirement.
  Declare acceptance limits before implementation; separate queue, storage and
  compute time. Correctness work requires failure tests, not an artificial benchmark.
- Reuse valid baselines. Run one targeted comparison per decision with enough
  samples for the claimed statistic; stop at the first useful boundary and proceed
  when evidence is sufficient. Avoid parameter sweeps,
  repeated soaks and unrelated measurements. Retain dataset/query fingerprints,
  seed where applicable, configuration, revision, backend/environment, latency,
  requests/bytes, CPU and peak memory where relevant; see [BENCHMARKS.md](BENCHMARKS.md).
- Keep deterministic correctness and failure coverage in CI. Bundle related
  measurement, implementation, tests and concise evidence into one logical PR;
  follow [AGENTS.md](AGENTS.md) for branch, reading and documentation rules, and
  verify CI before merging. Avoid documentation-only follow-up churn.
- A demonstrated blocker may justify a small intermediate milestone with its own
  evidence and acceptance criteria; resolve it before resuming dependent work.
  Record the changed plan; never mark unmet criteria complete.
  Update [DESIGN.md](DESIGN.md) for architectural decisions
  and changed guarantees.
- Measure PUT/GET/LIST/DELETE counts and bytes separately from client/network,
  queue, compute and maintenance time. Separate physical work from any price model;
  MinIO timing is not AWS latency or billing evidence. Use AWS only for a named
  provider-dependent question through the [guarded pilot](docs/S3_PILOT.md), with
  Free-plan verification and cleanup. Keep fault matrices on MinIO; unstable
  mobile-network observations cannot establish provider or engine regressions.

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

### M10a — Keep idle object-store transport tasks running

During the M11 comparison, a second S3 reader left idle behind a long scan
failed its next GET with an incomplete HTTP response while MinIO stayed alive.
The S3 adapter's current-thread runtime stopped driving connection tasks between
calls. Keep one I/O worker active per S3 handle; verify background progress
without retries and retain the existing publication/failure tests. This small
transport correction precedes the full M11 comparison.

## M11 — Make filtered exact queries selective on object storage

Status: complete for the M8 hot equality predicate; see [benchmarks/M11.md](benchmarks/M11.md).

Goal:
Avoid reading every vector chunk for a selective equality filter while keeping
filtered exact search a no-false-negative correctness oracle.

Steps:
- Compare chunk summaries and a derived metadata posting layout on the M8
  filter distributions. Select a layout by GET count, bytes, memory, write cost
  and recovery behavior. Version any persisted layout; validate a transient
  layout against the selected authoritative snapshot on each open.
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

### M11a — Reuse the mandatory validation scan for one hot equality predicate

The M8 `selected=true` predicate occurs in 11 of 12 sorted 128 KiB chunks, so
chunk summaries would still fetch 11 chunks and exceed the four-GET budget. The
streaming reader already validates all selected chunks at open. Retaining the
20 matching documents during that scan avoids a separate persisted posting
object, write amplification and a new recovery dependency. This intermediate
choice has no new persisted format; the posting is rebuilt after restart.
The 1,000-query MinIO comparison meets the selective and nonselective budgets.
Update/delete, ties, compaction, restart, missing chunks and posting-cap tests
verify the exact path. Broader predicates use the validated full scan.

## M12 — Search persisted ANN partitions without loading all vectors

Status: complete by the exact-serving acceptance alternative for M8.
The tested IVF layouts fail the sparse-filter budgets; production persisted ANN
is deferred. See [benchmarks/M12.md](benchmarks/M12.md).

Goal:
Turn IVF from an in-memory candidate baseline into a useful object-store query
path, while authoritative vectors remain recoverable without the derived index.

Steps:
- First gate candidate layouts on quality and serialized request/byte costs.
  If no tested layout meets M8, retain exact serving and defer publication work.
- For an accepted candidate, test a versioned partition layout that fetches only probed candidates and
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

### M12a — Reject unsuitable layouts before adding a publication protocol

The version-1 full-vector partition candidate and four-partition bundle were
serialized and round-tripped against the existing deterministic IVF cache.
On 1,000 queries per distribution, eight probes miss sparse-filter recall;
higher probing exceeds eight GETs. Bundles exceed 512 KiB even at the minimum
observed transfer. M11's 20-row exact posting needs no query GETs. Filter-only
partition copies would add publication and invalidation work to duplicate that
already-exact path. No persisted ANN generation is enabled for this envelope;
there is no new format to recover or reclaim. Existing derived-cache loss,
corruption, restart and uncertain-publication tests still verify exact recovery.
Reopen this decision for a workload that justifies ANN, without claiming that
all possible layouts have been ruled out.

## M13 — Single-machine serving and recovery operations

Status: complete for the M8 envelope. Serial serving, failure/restore tests
and the six-process 30-minute MinIO soak meet the acceptance criteria.
See [benchmarks/M13.md](benchmarks/M13.md); PR merge requires green CI.

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

### M13a — Budget synchronous batch admission explicitly

The smoke found about 56 ms p95 for 100-operation batches including due
compaction. M10's write-amplification result relies on batching; the original
single-write latency budget does not describe this admission pause. Add a
separate 100 ms batch p95 and at least 100 logical mutations/s serving budget.
The first paced segment exceeded the batch budget at 112 ms p95 with chunked
compaction. Use the M10-measured single-object snapshot for resident serving to
reduce publication and reclamation requests; keep optional chunks for streaming.
Retain the single-write baseline unchanged, and verify the full soak against
both the batch and existing query/recovery/resource bounds. This is an explicit
serial contract; no concurrent request queue latency is claimed.

## Next: safe concurrent use and a measured capacity boundary

M8–M13 establish a serial contract for 2,000 synthetic vectors, not a general
production capacity claim. M14–M20 record the next stages; conditional work stays
deferred when its entry condition is absent. One owner continues to publish
authoritative state unless a later measured decision explicitly changes that.

## M14 — Bound and diagnose MinIO CI failures

Status: complete. [PR #38](https://github.com/omerfeyzioglu/glider/pull/38)
passed CI, including normal MinIO integration and serving recovery. Nine harness
failure tests cover deadlines, descendants, readiness, cleanup and redaction.

Baseline before M14: M10a had fixed idle S3 transport progress. The then-current main
[CI run](https://github.com/omerfeyzioglu/glider/actions/runs/36237507054)
passed in about four minutes; its MinIO integration step took 98 seconds and
serving smoke 12 seconds. This is one observation, not a duration guarantee.
`tools/test_s3.py` had unbounded child commands, including the authenticated
probe inside its nominal 30-second readiness deadline and container cleanup.
The workflow had no explicit job timeout. M14 closed these harness gaps without
changing the engine transport protocol.

Steps:
- Attribute time to build, image/startup, readiness, tests and cleanup; inspect
  existing CI logs before adding measurements or changing coverage.
- Bound child processes and cleanup in the shared MinIO harness and its callers;
  enforce the remaining readiness deadline and an explicit outer job limit.
  On failure retain the failed stage and sanitized diagnostics without secrets.
- Keep startup polling separate from database operations. Fix reproduced causes;
  do not hide failures with blind retries or larger timeouts. Preserve short
  object-store, crash/restart and serving checks; long performance runs stay
  outside routine CI. Remove duplicate setup only when timing justifies it.

Done when:
- Injected stuck probe, failed startup/test and stuck cleanup terminate within
  declared bounds, report the original cause and attempt bounded cleanup.
- A normal MinIO CI run passes with stage timing and useful failure artifacts;
  no recovery coverage is silently removed to obtain a green result.

### M14a — Bounded real-S3 correctness pilot before further expansion

Status: complete. Tooling and MinIO CI passed in
[PR #39](https://github.com/omerfeyzioglu/glider/pull/39). The bounded AWS run
at `29df242` passed conditional publication, separate-process recovery, lost
mutation acknowledgement, isolated takeover, exact filtered results and
backup/restore; cleanup verified all five generated namespaces empty.
[Evidence](benchmarks/M14a.md): 177 HTTP attempts and 21,699,343 payload bytes.
This is provider correctness acceptance, not a latency or capacity claim.
See [docs/S3_PILOT.md](docs/S3_PILOT.md) for the guarded procedure.

Later remote diagnostic over operator-confirmed unstable mobile data exposed a
snapshot GET body timeout; cleanup passed. [Evidence](benchmarks/AWS_DIAGNOSTICS.md).
It does not establish an AWS service or internet-independent engine bottleneck,
and does not justify reopening M17/M18. Remote performance acceptance still needs
a representative stable deployment path; local capacity results remain local.

Run the first AWS validation after M14, before treating MinIO behavior as provider
evidence. Daily development, failure matrices and PR CI continue on MinIO.
Use a dedicated S3 Standard bucket from the existing machine; EC2 and broad
benchmarks are not prerequisites. Subsequent AWS checks should follow relevant
storage changes, not every PR. M19 still needs its own capacity evidence.

The AWS runner must verify the same credentials belong to an active Free plan,
with remaining credit/time and an owned, unversioned test bucket. Refuse paid,
expired or unverifiable plans. Bound requests, payload and elapsed time; retain
only the current run's reports and clean only its fresh prefixes. A failing or
timed-out run must preserve its original error and identify possible leftovers.

Done when:
- Local tests prove the plan/bucket refusal paths, budget enforcement and safe
  cleanup; the MinIO pilot and CI pass.
- One bounded AWS run passes the same correctness/recovery checks, records its
  backend, revision and request/payload counts, and verifies cleanup. Until then,
  mark provider acceptance pending and make no AWS latency/capacity claim.

## M15 — Safe client retries and conditional updates

Status: complete. [PR #42](https://github.com/omerfeyzioglu/glider/pull/42)
passed [CI](https://github.com/omerfeyzioglu/glider/actions/runs/36323811350),
including the MinIO uncertain-PUT/takeover/backup test, existing crash matrix,
serving recovery and pilot rehearsal. Twelve local retry tests cover process
exits, conflicts, expiration, corruption, concurrent duplicates and retention
bounds. See [the client contract](docs/RETRIES.md) and `tests/retry.rs`.

Problem: a lost acknowledgement leaves clients unsure whether a batch committed;
repeating an old request can overwrite newer data even when replay is idempotent.

Steps:
- Define request IDs, payload identity and durable result lookup. Persist the
  deduplication decision atomically with the mutation; checkpoint and recovery
  must preserve it. Compare bounded retention alternatives before choosing a
  versioned format, including explicit behavior for expired/unknown IDs.
- Add document revision preconditions checked by the owner at commit time,
  including delete/reinsert cases. Specify batch conflict atomicity and read
  visibility after acknowledgement. Do not confuse revisions with request IDs.
- Define retry guarantees across isolated takeover, backup and restore; restoring
  an older backup must not imply knowledge of requests beyond its boundary.

Done when:
- Lost acknowledgements, restart, compaction and isolated takeover tests show
  that a retained request cannot apply twice; mismatched payload reuse and stale
  revisions return explicit conflicts. Concurrent duplicates share one outcome.
- Retention memory/storage is bounded, expiration is explicit, and failure tests
  cannot produce a durable mutation without its required retry metadata.
- A client can resolve an uncertain result within the documented retention and
  recovery scope; no claim of unlimited exactly-once delivery is made.

## M16 — Bounded concurrent admission with one committer

Status: complete. [PR #43](https://github.com/omerfeyzioglu/glider/pull/43)
passed [CI](https://github.com/omerfeyzioglu/glider/actions/runs/36325701688). The
bounded FIFO admits by count and encoded bytes, with one owner worker and
explicit cancellation/shutdown semantics. Six deterministic failure tests pass.
The paired MinIO run checked 4,000 exact queries across two processes; the worker
met all latency, throughput, RSS and recovery budgets.
[Workload and raw evidence](benchmarks/M16.md).

Problem: the serial API has no queue limits or latency contract for many callers.

Steps:
- Define a small concurrent workload, arrival rate and numeric queue, memory and
  end-to-end latency budgets. Measure queue wait, commit time and maintenance
  separately; the M13 batch timing is not an end-to-end concurrent baseline.
- Introduce bounded admission by both request count and bytes, explicit overload
  responses, and one commit worker. Compare caller batching with bounded group
  commit only if small requests make PUT cost or throughput the limiting factor.
- Specify fairness, maximum batching delay, shutdown and cancellation before
  versus after publication. Preserve each request's atomicity and M15 identity.

Done when:
- Controlled load within the declared envelope meets its budgets; overload has
  bounded memory and explicit rejection instead of indefinite queue growth.
- Tests cover full queues, cancellation, shutdown, worker failure and uncertain
  PUT: acknowledged requests survive recovery; other outcomes remain resolvable
  under M15. No request receives success before durable publication.

## M17 — Consistent concurrent read views

Status: pinned views deferred for the M16 envelope. FIFO exact queries bind to
one committed boundary; deterministic admission tests prove whole-batch visibility
and cancellation/shutdown ordering. The controlled run measured filtered/unfiltered
query p95 of 16.695/1.077 ms against the 50 ms budget. No overlapping-read
requirement is established. Revisit only when a larger workload requires it;
this is a serialization decision, not an implemented pinned-view protocol.
[Evidence and limits](benchmarks/M16.md).

Steps:
- Compare a simple lock with immutable pinned views on the affected workload.
  Choose the least complex design meeting the budget; full transactional MVCC
  is not a prerequisite. Avoid copying the entire document map per small batch
  unless the measured memory and update costs justify it.
- Bind vectors, metadata, tombstones and any derived index to one committed
  boundary. Define read-your-writes and the lifetime of an old view. Retain
  referenced memory/objects until safe reclamation; bound slow-reader retention.

Done when:
- Deterministic interleaving tests prove readers never see partial batches,
  mixed revisions or reclaimed data during updates, compaction and view release.
- Acknowledged writes are visible according to the documented view contract;
  long readers and failures obey explicit memory/storage retention limits.
- The targeted overlap measurement meets the declared latency/resource budgets.
  If serialization suffices, record the evidence and defer pinned-view work.

## M18 — Maintenance without unbounded foreground stalls

Status: background maintenance deferred for the M16 envelope. Twelve synchronous
maintenance events had p95 79.281 ms against the 100 ms budget, with read/write
budgets also satisfied. Existing crash tests cover publication and cleanup; the
worker closes admission on uncertain failures. No concurrent reader holds old
objects. Write queue p95 (73.110 ms versus 75 ms) leaves little margin; revisit
if M19 exposes a stall. [Evidence and limits](benchmarks/M16.md).

Steps:
- Attribute admission/read tail latency to snapshot construction, publication
  and cleanup. Compare smaller bounded synchronous work with a background worker
  before adding scheduling and another publication participant.
- If background work is justified, budget CPU, memory, I/O and backlog. Build
  from a fixed committed boundary; coordinate publication with the owner and
  preserve newer writes. Reclamation must respect M17 views when enabled.

Done when:
- The identified stall fits the predeclared budget without unbounded backlog,
  memory or write amplification; hard limits still apply backpressure.
- Crash tests before publication, after publication and during cleanup preserve
  acknowledged writes and resume safely. Reader-held objects survive cleanup.
- Maintenance/storage failure has an explicit serving/recovery outcome; a worker
  cannot silently die while admission continues beyond the resource limits.

### M19a — Bound S3 reads before capacity exploration

Status: complete. [PR #44](https://github.com/omerfeyzioglu/glider/pull/44)
passed [CI](https://github.com/omerfeyzioglu/glider/actions/runs/36326086242).
Scripted tests cover declared
and actual body size, inventory count/bytes and early pagination cutoff. The
MinIO test rejects an oversized snapshot before any GET, then verifies all rows
with adequate limits. The full MinIO crash/restart suite and cleanup passed.

Demonstrated blocker: serving checks row capacity after recovery, while the S3
backend previously collected an unrestricted listing and complete object bodies.
A large namespace could consume resources before explicit rejection. Add opt-in
inventory count/byte and streamed object byte limits before M19 exploration.
Limits must fail closed, preserve authoritative state, and retain existing
uncertain-publication/ownership recovery semantics. Verify header/body oversize,
pagination cutoff and successful recovery with an adequate budget on MinIO.

## M19 — Establish a representative larger operating envelope

Status: complete for the selected local MinIO workload.
[PR #46](https://github.com/omerfeyzioglu/glider/pull/46) passed
[CI](https://github.com/omerfeyzioglu/glider/actions/runs/36327039489). The supported
SIFT/MinIO envelope is 2,000×128 with four clients and 400 offered mutations/s.
The first larger row step (5,000) fails write-queue p95; the final 50-second
workload, full state/oracle checks, backup/restore and failure recovery pass.
Remote capacity acceptance remains open. [Evidence](benchmarks/M19.md).
Does not assume that M17/M18 need implementation.

Steps:
- Select one intended deployment workload: representative dimensions, dataset,
  filter selectivity, update rate, concurrency and storage service. Record numeric
  latency, throughput, RSS, recovery, request/byte and backup/restore budgets.
- Increase the suspected limiting axis in a few steps and stop at the first
  budget breach. Profile that boundary rather than running a broad matrix.
  Separate cold recovery, steady reads/writes and maintenance costs.
- Validate remote storage on the chosen service before claiming remote behavior.
  If unavailable, report only local/MinIO evidence and leave remote acceptance
  open. Include bounded admission/open behavior for oversized namespaces.

Done when:
- Reproducible evidence identifies a supported envelope and its first limiting
  resource, with exact-oracle checks and failure/recovery tests at that size.
- One final workload-specific soak and backup/restore rehearsal meet its stated
  budgets after targeted fixes; the small M8 result is not extrapolated.

### M19b — Bound foreground cleanup latency

Status: complete; PR #46 and its CI passed. The same 2,000-row probe now
passes (write-queue p95 58.017 ms; maintenance p95/max 49.718 ms), with identical
HTTP counts/bytes and unchanged budgets. The S3 backend issues at most four native DELETEs concurrently;
other backends default to serial cleanup. Partial failures retain a poisoned
handle and recovery rebuilds counters.

The slowest maintenance event spent 44.703 ms deleting 17 independent obsolete
keys serially, after publishing a complete snapshot. Compare bounded concurrent
native deletes against serial cleanup, preserving all-or-uncertain acknowledgement,
poisoning on partial failure, and fresh recovery counts. Avoid another publication
participant or new provider-specific bulk-delete operation. Reuse the failed
baseline, measure the same boundary once after the change, and continue the
bounded capacity steps only if it passes.

### M20a — Release spare candidate capacity in completed exact results

Status: complete. [PR #47](https://github.com/omerfeyzioglu/glider/pull/47)
passed [CI](https://github.com/omerfeyzioglu/glider/actions/runs/36327642974).
The regression fails before the change and passes after it.

The 5,000-row investigation found that truncating scored candidates to k retained
the full Vec capacity in each completed result. A deterministic regression test
observed capacity 8,192 for one returned neighbor. Return a compact allocation
while preserving exact ordering, ties and temporary scoring behavior. Tests cover
both metrics, filters with all/some/no matches and k from zero through above the
row count. Each result must retain only its returned neighbor allocation.
This is separate from the snapshot encoding change because it addresses an
independent memory source; no new soak is needed for the allocation proof.

## M20 — Remove the demonstrated capacity bottleneck

Status: complete for the selected local MinIO envelope.
[PR #48](https://github.com/omerfeyzioglu/glider/pull/48) passed
[CI](https://github.com/omerfeyzioglu/glider/actions/runs/36327978017), including
the 5,000-row correctness/recovery smoke. Lossless integer snapshot
encoding plus the separately reviewed M20a result allocation fix meet the
5,000×128 SIFT/MinIO envelope: queue p95 66.390 ms, RSS 35.469 MiB and 400.751
mutations/s in the final 50-second workload. Exact state, backup/restore and
failure recovery pass. [Before/after evidence](benchmarks/M20.md).

The next scoped local probe at 10,000×128 fails write-queue p95 (109.093 ms
against 75 ms). Synchronous full-snapshot maintenance dominates the affected
rounds; search, commit, throughput, memory and recovery budgets pass. This is
the next demonstrated boundary, not an accepted larger envelope or a selected
background-publication design. [Current attribution](benchmarks/CURRENT_BOUNDARY.md).

Steps:
- Choose one cause and compare the smallest relevant alternatives: selective
  reads/cache layout for GET/byte cost, compact representation for RAM, indexing
  for exact-scan CPU, or batching for commit overhead. Split independent changes
  into separately reviewed steps.
- Reopen persisted ANN only when exact search misses the chosen envelope.
  Require exact-oracle recall/short-result, latency, memory and update/rebuild
  budgets, with generation publication, corruption and recovery tests.
- Consider multiple committers only after evidence shows the single committer
  is saturated after batching and avoidable work is removed. Compare continued
  serialization, conflict validation and partition ownership; specify fencing
  and object-store publication before changing the ownership contract.

Done when:
- A targeted before/after result meets the selected envelope without weakening
  durability, recovery or query quality, and relevant failure tests plus CI pass.
- The chosen mechanism and rejected alternatives are documented. Unjustified
  ANN, multi-writer or other branches remain deferred, not marked implemented.

## Next: large-data economics on one machine

The target is a durable, low-cost, fast object-storage-native vector engine,
with bounded RAM and NVMe caching before multiple machines. Small tests verify
invariants and isolate causes; fitting a small resident corpus is not the goal.
M21 has a merged measurement and layout decision; M22–M24 are
planned, not implemented. Preserve M1–M20 evidence and scope limits.
Do not import another engine or copy a reference architecture without a separate
justified decision. Independent read/write arrivals, working-set size and object
costs must drive prioritization rather than feature count.

## M21 — Establish the large-data workload and cost decision

Status: accepted after [PR #54 CI](https://github.com/omerfeyzioglu/glider/actions/runs/36412910918)
passed. Measurement and layout decision are recorded in
[benchmarks/M21.md](benchmarks/M21.md). The independent-arrival
10,000-row run breached the 75 ms write-queue p95 gate at 99.921 ms.

Steps:
- Specify one representative vector dataset, dimensionality, filter/update mix,
  independent read/write arrivals and numeric latency/quality/resource budgets
  before new acceptance measurements. Select a corpus exceeding a fixed engine
  RAM budget, without requiring a large paid deployment to expose that condition.
- Separate durable write acknowledgement from query-visible/indexed state;
  retain existing guarantees unless an explicit architectural decision changes them.
- Add a bounded independent-query diagnostic: the current clients wait for their
  own write before issuing queries, which can hide read arrivals during maintenance.
  Record overloads and arrival lateness instead of allowing an unbounded generator.
- Attribute write amplification, full-materialization memory/recovery, query
  GET amplification and cold/warm behavior. Compare at most the relevant small
  alternatives for snapshot transfer versus changed-data publication. A compressed
  full snapshot can be a baseline, not proof of scalable maintenance.

Done when: the workload, numeric gates, measured/structural limits and next
layout decision are recorded. Distinguish demonstrated bottlenecks from untested
scaling risks. No ANN/cache/background implementation is required just to close
this measurement milestone; equally, resident-query speed cannot close the
larger-than-RAM requirement.

## M22 — Bound segment publication and maintenance work

Status: in progress; M21 selected addressable base blocks and bounded sorted
delta runs with vector-partition summaries, jointly with M23's selective-read
and cache requirements. The range-read contract and packed-block design are in
[benchmarks/M22.md](benchmarks/M22.md); segmented publication and acceptance
remain pending.

Steps:
- Compare incrementally reusable segments with bounded sorted delta segments;
  select the smallest layout that supports changed-data writes and selective
  reads. Splitting a full rewrite into chunks alone is insufficient.
- Version segment and manifest formats; state the authoritative publication
  boundary, log/retry retention, delete semantics, compatibility and reclamation.
- Bound rewrite tasks, object/manifest growth, replay and temporary coexistence.
  If foreground budgets require overlapping maintenance, define a bounded worker
  and fixed generation protocol with backpressure; do not introduce another
  uncoordinated publisher or silently weaken acknowledgement.

Done when: a targeted larger-data workload meets M21's declared write, request,
byte and recovery gates, with publication/cleanup crash tests and exact results.
At fixed changed data, increasing the base must not require routine full-base
rewrites. Report unavoidable compaction amplification rather than claiming none.

## M23 — Serve through bounded NVMe and RAM caches

Status: planned; segment identity and read granularity coordinate with M22.

Steps:
- Open/read without loading every vector into RAM. Fetch addressable data on
  demand through bounded RAM and disposable NVMe caches, with object storage
  remaining authoritative. Bound metadata, prefetch and concurrent read buffers.
- Test cache misses, eviction, restart, missing/corrupt cached files and total
  cache loss. Cache errors may refetch within explicit request/deadline budgets;
  authoritative corruption must still fail closed. Local files cannot acknowledge
  writes or become an implicit recovery dependency.
- Measure cold, NVMe-warm, RAM-hot and cache-pressure cases with one fixed budget;
  record hit rates, S3 requests/bytes, occupancy and end-to-end latency. Label OS
  page-cache effects; do not call a fresh process a guaranteed cold disk cache.

Done when: a corpus exceeding the configured RAM budget meets the declared
correctness, latency and resource gates; locality reduces remote requests and
losing either cache changes performance only. No multi-node work is required.

## M24 — Selective retrieval and single-machine cost acceptance

Status: planned; reuse validated M21–M23 evidence.

Steps:
- Evaluate selective filtering/ANN on addressable segments when required by the
  larger-data GET/byte/latency gates. The small M12 exact-serving alternative does
  not close this workload. Use a stated recall/short-result target and exact oracle.
- Bound index metadata/build/update work and validate generation visibility,
  missing/corrupt derived indexes and recovery. Keep unsupported query modes explicit.
- Run one selected-envelope read/write/recovery/backup acceptance with cold/warm
  cache behavior and a reproducible request/byte cost model. Use one bounded AWS
  check only for the unresolved provider-dependent question, not a cloud sweep.

Done when: the chosen larger-than-RAM single-machine envelope has measured
quality, latency, resource and storage-work acceptance plus failure/recovery
evidence. Remote acceptance remains explicitly pending if that evidence is absent.
Then identify the next actual limit before considering more machines.

## Beyond the single-machine target

Distributed sharding, replication, consensus and GPU work need separate
requirements and measurements. Quantization, additional index families and
multi-writer execution are conditional options under M20, not prerequisites.
