# DESIGN.md

## Goal

Build `glider`, an object-storage-native vector database / search engine with
explicit correctness, durability, recovery, and performance characteristics.
Architecture evolves from requirements and measurements, not anticipated scale.

## Architecture

The Rust library implements this path:

```text
Database API (put / get / delete / exact and filtered search)
    -> in-memory document map + immutable checkpoints + mutation log / recovery
    -> ObjectStore abstraction
    -> local development backend or S3-compatible backend
```

One mutable `Database` handle exclusively owns a storage namespace. The caller
must prevent concurrent owners, including across processes; no locking or
multi-writer protocol is provided. An owned object-store handle isolates the
engine from filesystem operations. The newest complete checkpoint and newer
mutations are authoritative; without a checkpoint, recovery uses the complete
durable log. The in-memory map is derived. Reads observe that map, and successful
writes become visible after durable storage.

Document IDs are u64. Vectors have one positive, persisted dimension and finite
f32 components. Each live document also has a map of UTF-8 string keys to string
values. The persisted metric enum supports squared Euclidean and Manhattan
distance, accumulated in f64. Exact search scans all live documents, sorts by
ascending distance then ID, and returns at most k results. Filtered exact search
requires every specified key/value equality to hold; missing keys do not match.
An empty filter includes every document. A put replaces both the vector and the
complete metadata map; a delete removes both.

## Derived IVF-Flat candidate index

`build_ivf(IvfConfig)` explicitly builds an in-memory index from the acknowledged
map. Seeded farthest-point initialization and a fixed number of assignment/update
iterations train means for squared Euclidean distance and coordinate medians for
Manhattan distance. Each ID belongs to one final partition; full-precision vectors
remain in the document map. Empty partitions retain their centers. Ties are
deterministic; partitions are capped at the live row count.

`search_ivf(query, k, probes)` scores every center, scans the nearest partitions,
and orders candidates by exact distance then ID. It can miss true neighbors and
return fewer than k results; probing all partitions equals exact search. Results
include centroid and vector distance counts. The exact `search` API is unchanged.
`search_ivf_filtered` applies the same metadata predicate to probed candidates
before exact scoring. Full probing equals filtered exact search; partial probing
may return fewer than k filtered matches. It counts only scored matching vectors.
`search_ivf_filtered_adaptive(query, k, min_probes, filter)` scans at least the
nearest `min_probes` partitions, then expands in center-distance order until it
has k filtered matches or has scanned every partition. It returns `min(k, matches)`
results; if fewer than k matches exist, full probing makes the result exact.
Stopping after k candidates remains approximate and has no recall guarantee.
Results also report the number of partitions actually probed. Expansion scores
each center once and does no additional storage I/O.
The index groups IDs by vector only; metadata is read from the acknowledged map.
There is no metadata index or automatic exact-versus-IVF planner.

Every successful put/delete or batch invalidates the index; queries then return
an explicit error until rebuilt. Failed mutation publication leaves reads and the
index at the last acknowledged state, including on a poisoned handle. Recovery
starts without an in-memory index. Checkpoint/compaction preserve it because they
do not change live state. `build_ivf` and IVF queries perform no storage I/O.

`load_or_build_ivf(options)` explicitly reads a derived cache for the recovered
mutation sequence and requested dimensions, metric, partitions, iterations and
seed. On a hit, it validates the version, identity, finite centers and strictly
ordered postings containing every live ID exactly once, then installs the index.
On a miss, it trains the same index as `build_ivf` and publishes one immutable
`ivf-{sequence:020}-{sha256(identity)}` object. Identity is the JSON encoding of
`(cache version 1, database configuration, IVF options)`; the object is version 1
JSON containing that identity, the sequence, centers and ID posting arrays. Store
envelopes protect complete bytes. A structurally invalid cache returns an error;
exact search and explicit in-memory rebuilding remain available. Database
recovery only validates cache key syntax and its sequence boundary; malformed
cache JSON cannot prevent recovery of authoritative vectors. Backend envelope
corruption still fails storage recovery. No index is loaded without an explicit
options choice.

Successful cache publication acknowledges only the derived index, not a mutation.
The authoritative document map and mutation sequence never change. A publication
error or panic leaves the trained index readable on that handle but poisons writes
and maintenance until reopen, since the cache object may have committed. Recovery
can load a complete published cache or rebuild if publication did not complete.
Later mutations make older caches stale; compaction removes caches from older
sequences and retains caches at its current sequence. Distinct option sets can
coexist. An immutable option-keyed object avoids a mutable index head and keeps
the exclusive-owner publication model. The cache saves retraining; recovery still
loads authoritative documents into memory, and queries still use them for exact
candidate scoring and filtering.

IVF is the first partitioned baseline, chosen for a small implementation and a
layout suitable for later object-storage evaluation. HNSW would instead add graph
memory and maintenance; SPFresh-style local rebalancing is deferred until update
workloads demonstrate that rebuilding is too costly. This prototype rebuilds
synchronously in O(iterations × rows × partitions × dimensions) assignment work;
Manhattan median updates add selection work. Training uses O(rows + partitions ×
dimensions) auxiliary state. It does not provide online index maintenance,
independently searchable persisted partitions, or a guaranteed recall/latency target.

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
5. Checkpoints and compaction must preserve logical results.
6. Changing storage backends must preserve database-level correctness semantics.

## Object storage and persisted formats

Successful object creation means complete, immutable bytes are durable and visible
to get/list. Reads and listings must expose only durable complete objects; after
an uncertain create, a backend must stabilize them or reject access until reopened.
Complete keys cannot be replaced. Successful removal means durable absence;
removing an absent key succeeds. Removal errors are uncertain and require the
same stabilization or rejection. The engine never reuses reclaimed keys, so a
delayed remote DELETE cannot remove newer authoritative data. Listings must be complete and
strongly consistent, though ordering is not required. The S3-compatible
backend collects all listing pages and provides this object contract using native
complete-object publication.

`metadata` is the first durable object: UTF-8 JSON containing format version 1
and the configuration. Each mutation-log object is immutable UTF-8 JSON with a
format version and sequence. Versions 1 and 2 contain one put or delete; version
1 puts contain an ID and vector, while version 2 puts also require a
string-to-string `metadata` map. Ordinary single writes still use version 2.
Version 3 contains a nonempty ordered `mutations` array of the version 2 operation
schema. `apply_batch` publishes one version 3 object for all its operations.
Recovery accepts all three versions and treats version 1 metadata as empty. The
mutation key is `mutation-` followed by a contiguous 20-digit decimal sequence
starting at 1. A sequence counts a published log object, not the number of
operations inside a batch.
Schema changes require explicit format-version handling.

Immutable numbered objects avoid append operations unavailable in object stores.
Immutable segments with a conditional mutable head could coordinate writers but
add a publication protocol unnecessary for exclusive ownership. The chosen layout
costs one object per single write; a batch trades one larger object and group
acknowledgement for fewer conditional PUTs and keys. Checkpoints reduce payload
replay, while explicit compaction reclaims covered history. JSON favors
straightforward validation over a custom binary format.

## Acknowledgement and recovery

A storage error can mean a write committed. After any mutation storage error, the
handle rejects further mutations until reopened; reads continue to show its last
acknowledged state. Initialization has the same uncertain outcome: reopen after
an error to discover whether metadata was published. Acknowledgement never depends
on destructors.

`apply_batch` rejects an empty batch or any invalid vector before storage access.
Operations are applied in input order, so the last operation for an ID wins.
One successful conditional create acknowledges the entire batch and makes its
final state visible. A create error or panic poisons the handle while reads keep
the previous acknowledged state. After a crash or lost acknowledgement, recovery
sees either no batch object or one complete object and replays all operations in
order. It never exposes a partial batch. Checkpoint and compaction use the batch's
single sequence boundary. Batch size is caller controlled; there is no automatic
splitting or second publication step. The caller must choose a size supported by
the configured object store and available memory.

Recovery loads the newest complete checkpoint, if present, and replays newer
complete log objects in sequence, including completed writes that were not
acknowledged. Requested configuration must exactly match metadata.
Unknown fields, invalid records or vectors, unsupported versions, unexpected keys,
internal sequence gaps, and mutation objects without metadata fail recovery.

## Immutable checkpoint segments (M3)

`Database::checkpoint()` publishes the complete live state at the current mutation
sequence as one immutable `segment-` object with a 20-digit sequence suffix.
Version 1 JSON contains version, sequence, configuration and a strictly ID-sorted
array of `(id, vector)` entries. Version 2 contains `(id, {vector, metadata})`
entries, with metadata a required string-to-string map. New snapshots use version
2 for the single-object API, and recovery accepts version 1 snapshots with empty metadata. The existing
backend envelope protects its bytes. Deletes are represented by absence; the
sequence boundary prevents older puts from resurrecting deleted IDs. An empty
sequence-zero checkpoint is valid.

A complete single-object snapshot uses native object publication. A separate
manifest or mutable head would add another uncertain publication boundary without
benefit while snapshots fit in one object and ownership is exclusive. Recovery
selects the highest complete segment key from the strongly consistent listing;
there is no rename, in-place update or filesystem-specific engine operation.

Success acknowledges a durable snapshot, without advancing the mutation sequence.
Errors or panics during publication poison mutations and checkpoints until reopen;
reads retain the acknowledged state. An incomplete snapshot is invisible under the
backend contract. A complete snapshot with a lost acknowledgement can be selected
on reopen. A delayed S3 publication remains a valid snapshot of its fixed prefix,
including after newer mutations commit. Repeating a checkpoint at the recovered
checkpoint sequence is a no-op; no object is overwritten.

Recovery validates the selected segment's version, configuration, sequence,
strict ID ordering, dimensions and finite vectors, then decodes only newer mutation
payloads. An invalid or missing selected segment fails recovery, never silently
falls back. Without a compaction snapshot, retained mutation keys must be contiguous from 1;
with one, continuity is required only above its sequence. An ordinary segment
cannot extend beyond that boundary plus the retained contiguous tail. Covered mutation payloads and older
segments are not decoded by the engine; LocalStore still validates all envelopes
and stabilizes all files on open. Older binaries reject segment keys rather than
misread the namespace. Existing log-only databases remain readable.

Checkpointing is explicit and synchronous; the single-object form serializes the
full live map in memory. Checkpointing alone retains all logs and snapshots and
requires listing all keys. It does not change mutation acknowledgement or detect
external loss of an unwitnessed tail.

### Chunked snapshots (version 3)

`checkpoint_chunked(max_chunk_bytes)` and `compact_chunked(max_chunk_bytes)`
publish the same logical state as the single-object methods. They split the
strictly ID-ordered document map into version 1 JSON chunks, each at most the
caller-selected encoded payload limit. A single document that cannot fit is an
input error detected before storage writes. Chunk keys are
`segmentchunk-{sequence:020}-{limit:020}-{ordinal:010}` or
`compactedchunk-{sequence:020}-{limit:020}-{ordinal:010}`. The limit in the key
allows a failed attempt to be retried with a different layout without reusing a
published key. Chunk objects contain version, sequence, configuration and rows.

After every chunk is durably created, the writer publishes a version 3 manifest
at the existing `segment-` or `compacted-` key. It contains version, sequence,
configuration, byte limit and ordered chunk references with ID bounds, row counts
and SHA-256 payload digests. The manifest is the sole publication boundary:
chunks without it are orphaned derived bytes, not authoritative state. A
successful manifest create acknowledges the checkpoint or compaction root. An
error or panic during any chunk or manifest operation poisons writes and
maintenance until reopen; reads retain the last acknowledged map. A complete
manifest with a lost acknowledgement is selected on recovery. Retrying the same
layout may reuse an already published chunk only after comparing its complete
bytes; a conflicting chunk is an error. A late conditional PUT cannot replace a
complete chunk or manifest.

Recovery accepts legacy snapshot versions 1 and 2 and the version 3 manifest.
For a selected manifest it requires every referenced chunk, checks digest,
version, sequence, configuration, byte limit, row count and ID bounds, and
validates all vectors and globally increasing IDs. It never falls back to an
older snapshot when a selected chunk is missing or invalid. Unreferenced chunks
from incomplete publications are ignored. Compaction retains only chunks named
by the current compacted manifest and reclaims older and orphaned chunks after
the new root is durable; interrupted cleanup resumes on a repeated call. Older
binaries reject the new chunk keys and version 3 manifests.

The single-object format uses fewer requests but makes one payload grow with the
dataset. The manifest format bounds each data payload and temporary chunk
serialization memory at the cost of more PUTs and recovery GETs. A mutable head
or content-addressed keys would add coordination or key-reuse risks to cleanup
without helping the exclusive owner. These APIs remain explicit; the database
still materializes the full map in memory, scans the complete object listing on
open, and stores one unbounded manifest. They do not yet support lazy partition
reads or a bounded-memory database.

## Compaction (M4)

`Database::compact()` publishes the complete current live state as
`compacted-` plus its 20-digit mutation sequence, using the same versioned snapshot
payload as an ordinary segment. `compact_chunked` instead publishes the version 3
chunked form described above. Only this distinct snapshot kind authorizes
reclamation. After durable publication, compaction lists and validates the cleanup
plan, then removes covered mutations, covered ordinary segments and older
compaction snapshots. Metadata and the new compaction snapshot remain.

Existing segments already contain full live state; consolidation serializes the
recovered map rather than merging overlapping full snapshots. The single-object
form combines state and reclamation boundary; the chunked form uses one final
manifest as that boundary. A mutable head would introduce a coordination
requirement unnecessary for the exclusive owner. There are no background workers
or automatic compaction policy.

Success acknowledges the snapshot and all listed removals, without consuming a
mutation sequence. Any storage error or panic poisons writes and maintenance until
reopen. Interrupted publication leaves the prior history intact; interrupted
cleanup leaves a complete replacement plus an arbitrary subset of obsolete keys.
Repeating compaction at the recovered sequence resumes cleanup without rewriting
the snapshot. A late older PUT may recreate obsolete garbage; recovery ignores it
and a later compaction reclaims it. Successful cleanup covers the listing, not
requests still in flight from a discarded handle.

Recovery validates the highest compaction snapshot, even when a newer ordinary
segment supplies the live state. It allows missing keys at or below that boundary,
requires a contiguous mutation tail above it, and never falls back from a corrupt
selected snapshot. Old log/segment namespaces remain readable; older binaries
reject the new key kind and cannot open a compacted namespace.

After cleanup, storage contains metadata plus one snapshot root and its referenced
chunks, growing again with new writes and checkpoints until the next explicit
compaction. Legacy full-map cloning and serialization require temporary memory;
either replacement coexists with old objects until cleanup. This bounds retained
logical history between explicit compactions, not live dataset size, total
memory, or provider-retained versions.

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
and reclaimed on reopen or by a subsequent create. A complete seal requires a valid body and
checksum; invalid full-length seals or sealed bodies fail recovery. Published objects are
never modified. A local handle rejects get/list/create after an interrupted or
failed publication or removal; passing that handle to database recovery also fails. On reopen,
complete objects are validated and synced before new writes, stabilizing any full
seal left by failure before the final sync.

Removal deletes the seal and syncs the directory before deleting the body and
syncing the directory again. This prevents a crash from exposing a complete seal
with a deleted body. Reopen reclaims unsealed debris; failure during cleanup or
synchronization fails open. Filesystem synchronization stays inside LocalStore.

The namespace's parent directory must already exist. Initialization syncs both
namespace and parent. Local durability requires exclusive access and a
filesystem/device honoring file and directory synchronization. These operations
implement the object contract locally; they are not engine-level storage APIs.

## S3-compatible backend

The optional `s3` feature implements the same synchronous ObjectStore contract.
The `object_store` client handles signing, HTTP and pagination; glider retains
ownership of the log, validation, recovery and search. Using this client avoids
handwritten signing/HTTP machinery. A private Tokio runtime bridges its async I/O
without changing the engine API; callers use blocking threads.

One bucket plus a nonempty, nonoverlapping namespace prefix identifies a database. The caller
provisions the bucket, credentials and exclusive namespace ownership. The service
must provide strongly consistent get/list and atomic conditional PUT. Each create
sends one `If-None-Match: *` PUT with no preflight existence check, multipart upload,
unconditional fallback or automatic retry. Success acknowledges durable native
publication. Any create error or panic poisons the store until a fresh handle is
opened; the database retains its existing acknowledged-state read semantics.
A timed-out request may still complete remotely. Conditional creation prevents a
late request from replacing a newly committed object at the same sequence key.

Removal sends one native DELETE, with no automatic retry; absence is success.
Errors or panics poison the store. Never reusing reclaimed keys makes delayed
DELETE requests safe across reopen and later compactions. Versioned buckets may
retain old versions and delete markers: glider reclaims the visible namespace,
not those provider-managed historical bytes.

Each S3 object contains the existing `VTOBJ001` length/payload/SHA-256 envelope;
there are no seal objects. Native publication replaces local sealing. GET consumes
and validates the entire envelope before returning payload bytes. Recovery gets
all listing pages before replay; page errors fail open rather than returning a
partial prefix. Namespace keys and persisted records remain strictly validated.
Complete visible objects are already durable under the required service contract,
so recovery needs no local synchronization barrier. External tail-object deletion
remains undetectable, as with the local backend.

Cloneable request metrics count transport-level GET, listing-page, PUT, DELETE and other
attempts, request body bytes, HTTP error responses and transport errors. These
are not device I/O or latency measurements. MinIO integration tests exercise
conditional creation, pagination, namespace isolation, response-loss uncertainty,
late conditional requests, corruption, client process exit and abrupt server restart. They do not prove
provider hardware durability or substitute for validation against a deployment's
chosen S3-compatible service.

## Limits and current non-goals

The durability contract covers process termination and interrupted writes, not
arbitrary media loss. Checksums detect sealed body corruption, but deletion of
the final log object or loss/truncation of its seal is indistinguishable from an
operation that never published. Loss of an unwitnessed compaction snapshot can
also erase the only remaining state without a detectable gap. Detecting such external loss requires an
additional integrity protocol. Memory use and recovery time grow with the dataset
and mutation history; there is no bounded-resource guarantee.

Independently searchable persisted ANN partitions, metadata indexes, automatic
exact-versus-IVF planning, sharding, replication, distributed consensus,
multi-node execution, quantization, networking, SQL compatibility,
authentication/authorization, production hardening, and GPU execution are outside
the current implementation. These are not permanent restrictions; additions
require justified design decisions and must preserve the invariants above.
