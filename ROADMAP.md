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

Status: complete (explicit checkpoint segments, safe tail replay, LocalStore/MinIO
failure tests and archived recovery measurements)

Problem:
One object per mutation causes object count and full-replay recovery cost to grow
with mutation history.

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

Status: complete (full-state consolidation, safe history reclamation, LocalStore/MinIO
failure tests and archived amplification measurements)

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
archived synthetic measurements; no persisted index or recall target)

Goal:
Reduce vector-search cost while explicitly measuring quality loss.

Done when:
- one ANN design is selected from justified alternatives
- ANN state is derived/rebuildable
- recall@k is measured against exact search
- latency / throughput / resource trade-offs are measured
- persistence and restart behavior are defined

## M7 — Filtering and query execution

Status: in progress (durable string metadata and exact/IVF equality filtering;
reproducible filtered ANN quality evaluation; adaptive probe expansion fills
available matches; exact-versus-IVF planning remains)

Goal:
Support metadata filtering without silently destroying search correctness.

Done when:
- filter semantics are correct independently of ANN
- filtered exact search provides a correctness baseline
- ANN + filtering behavior is measured against that baseline
- query execution decisions are justified by measurements

## Later

Only pursue when justified by requirements or measurements:

- caching
- query planning
- quantization
- concurrency
- sharding
- replication
- GPU execution
- additional index families
