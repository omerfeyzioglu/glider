# DESIGN.md

## Goal

Build `glider`, an object-storage-native vector database / search engine with
explicit correctness, durability, recovery, and performance characteristics.
Architecture evolves from requirements and measurements, not anticipated scale.

## Architecture

The Rust library implements this path:

```text
Database API (put / get / delete / exact search)
    -> in-memory document map + durable mutation log / recovery
    -> ObjectStore abstraction
    -> local development backend
```

One mutable `Database` handle exclusively owns a storage namespace. The caller
must prevent concurrent owners, including across processes; no locking or
multi-writer protocol is provided. An owned object-store handle isolates the
engine from filesystem operations. The complete durable log is authoritative;
the in-memory map is derived. Reads observe that map, and successful writes
become visible after durable storage.

Document IDs are u64. Vectors have one positive, persisted dimension and finite
f32 components. The persisted metric enum supports squared Euclidean and Manhattan
distance, accumulated in f64. Exact search scans all live documents, sorts by
ascending distance then ID, and returns at most k results.

## Future / target architecture

This section describes the direction of the project, not functionality that is
already implemented. The target is a durable, object-storage-native vector
search engine with a clear separation between authoritative state, derived
indexes, and query execution.

```text
Client/API
    -> query validation and planning
    -> candidate generation (exact scan or ANN index)
    -> filtering and exact reranking
    -> deterministic top-k results

Mutation path
   -> single-writer commit protocol
   -> immutable log/segment objects
   -> explicit publication of authoritative persistent state
   -> rebuildable in-memory and ANN indexes
```

The planned evolution is incremental:

1. Keep the current exact search as the correctness baseline and benchmark it
   with reproducible datasets, queries, seeds, metrics, and storage backends.
2. Add and validate an S3-compatible object-storage backend. The local backend
   remains the reference implementation of the object contract.
3. Add immutable segments and compaction. Compaction may reorganize physical
   objects, but must preserve logical results and must never make a partial
   segment authoritative.
4. Add filtering and an ANN candidate index. ANN is an optimization only: tests
   and benchmarks compare it with exact search using recall@k, latency, and
   resource/bytes-read measurements.
5. Consider concurrent writers, sharding, and replication only after the
   single-writer durability and recovery model has measured limits and explicit
   coordination semantics.

### Target invariants

- Authoritative state is versioned durable data with explicit publication
 semantics; indexes, caches, and ANN structures are derived and rebuildable.
- Every publication has explicit acknowledgement, crash, recovery, and reader
  visibility semantics. A reader never observes a partially published segment.
- Exact search remains available as the correctness oracle for every ANN or
  query-planning optimization.
- Persisted formats, manifests, segments, and indexes have independent explicit
  versions and compatibility rules.
- Benchmark claims include the dataset, dimensions, metric, query count,
  distribution, seed, backend, hardware, configuration, and measured output.

The project should record durable architectural decisions here and record
benchmark results separately as reproducible artifacts. This document is not a
feature checklist or a promise to implement every referenced mechanism.

## Design principles

- Database semantics remain independent of the storage provider; engine
  correctness uses no append, atomic rename, in-place mutation, filesystem locks,
  or shared local disk.
- Define acknowledgement, authoritative state, crash behavior, and recovery before
  optimizing. Prefer immutable data when justified by storage or concurrency needs.
- Keep derived indexes and caches distinguishable from authoritative state and
  rebuildable from it unless an explicit later decision changes that property.
- Evaluate bytes transferred, access patterns, and remote round trips alongside
  algorithmic complexity. Compare meaningful alternatives for durable decisions.

## Current invariants

1. An acknowledged durable operation survives process restart under the storage
   contract below. An unacknowledged operation may be present or absent after a
   crash, but recovery must produce a valid logical state.
2. The latest committed mutation in sequence order determines a document's value.
   A committed delete prevents that document from appearing in logical results
   unless a later put replaces it; deletion of an absent ID is also logged.
3. Recovery reconstructs the same logical state from authoritative durable data;
   repeated replay is idempotent.
4. Exact kNN returns the correct top-k for the configured metric with deterministic
   ties. It remains the correctness oracle for future ANN quality evaluation.
5. Future compaction or physical reorganization must preserve logical results.
6. Changing storage backends must preserve database-level correctness semantics.

## Object storage and persisted formats

Successful object creation means complete, immutable bytes are durable and visible
to get/list. Reads and listings must expose only durable complete objects; after
an uncertain create, a backend must stabilize them or reject access until reopened.
Complete keys cannot be replaced. Listings must be complete and
strongly consistent, though ordering is not required. A future S3-compatible
backend must collect all listing pages and provide this same object contract
using native complete-object publication.

`metadata` is the first durable object: UTF-8 JSON containing format version 1
and the configuration. Each mutation is one immutable UTF-8 JSON object containing
format version 1, its sequence, and a put or delete operation. Its key is
`mutation-` followed by a contiguous 20-digit decimal sequence starting at 1.
Schema changes require explicit format-version handling.

Immutable numbered objects avoid append operations unavailable in object stores.
Immutable segments with a conditional mutable head could coordinate writers but
add a publication protocol unnecessary for exclusive ownership. The chosen layout
costs one object per mutation and full replay at startup. JSON favors straightforward
validation over a custom binary format.

## Acknowledgement and recovery

A storage error can mean a write committed. After any mutation storage error, the
handle rejects further mutations until reopened; reads continue to show its last
acknowledged state. Initialization has the same uncertain outcome: reopen after
an error to discover whether metadata was published. Acknowledgement never depends
on destructors.

Recovery replays every complete log object in sequence, including completed writes
that were not acknowledged. Requested configuration must exactly match metadata.
Unknown fields, invalid records or vectors, unsupported versions, unexpected keys,
internal sequence gaps, and mutation objects without metadata fail recovery.

## Local backend

Each logical object has a separate body and seal file. The body contains, in order:

- `VTOBJ001` (the versioned envelope identifier)
- unsigned 64-bit little-endian payload length
- payload
- SHA-256 of all preceding body bytes

The complete seal is exactly `VTSEALED`. These persisted identifiers are part of
the storage format. Separate sealing avoids mistaking interrupted payload bytes
for a completion marker and implements publication without atomic rename or locks.

The write protocol is:

1. Write and sync the body, then sync the namespace directory.
2. Write and sync the seal, then sync the directory before acknowledging creation.

A missing or partial seal denotes an unpublished attempt, excluded from get/list
and reclaimable by a subsequent create. A complete seal requires a valid body and
checksum; invalid full-length seals or sealed bodies fail recovery. Published objects are
never modified. A local handle rejects get/list/create after an interrupted or
failed publication; passing that handle to database recovery also fails. On reopen,
complete objects are validated and synced before new writes, stabilizing any full
seal left by failure before the final sync.

The namespace's parent directory must already exist. Initialization syncs both
namespace and parent. Local durability requires exclusive access and a
filesystem/device honoring file and directory synchronization. These operations
implement the object contract locally; they are not engine-level storage APIs.

## Limits and current non-goals

The durability contract covers process termination and interrupted writes, not
arbitrary media loss. Checksums detect sealed body corruption, but deletion of
the final log object or loss/truncation of its seal is indistinguishable from an
operation that never published. Detecting such external loss requires an
additional integrity protocol. Memory use and recovery time grow with the dataset
and mutation history; there is no bounded-resource guarantee.

ANN, filtering, compaction, an S3 backend, sharding, replication, distributed
consensus, multi-node execution, quantization, networking, SQL compatibility,
authentication/authorization, production hardening, and GPU execution are outside
the current implementation. These are not permanent restrictions; additions
require justified design decisions and must preserve the invariants above.
