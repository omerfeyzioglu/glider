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
to get/list. Complete keys cannot be replaced. Listings must be complete and
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
checksum; invalid seals or sealed bodies fail recovery. Published objects are
never modified. On reopen, complete objects are validated and synced before new
writes, stabilizing any full seal left by failure before the final sync.

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
