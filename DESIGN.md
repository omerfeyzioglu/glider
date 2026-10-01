# DESIGN.md

## Goal

Build `glider`, an object-storage-native vector database / search engine with
explicit correctness, durability, recovery, and performance characteristics.
Architecture evolves from requirements and measurements, not anticipated scale.

## Architecture

### Engines and namespace compatibility

Two engines share the object contract, configuration and metrics but use
disjoint namespace formats:

- The segmented engine (`segmented::SegmentedDatabase`, served by
  `SegmentedServing` and `glider-server`) is the serving path. RAM holds the
  latest-ID directory, pack sketches and the unsealed log tail; vectors stay in
  immutable packs read through bounded caches.
- The resident engine (`Database`, `SingleMachine`) keeps every document in
  RAM. It is the small-collection library mode and the exact reference.

The `metadata` object's version names the engine: 1 is resident, 2, 3 and 4 are
segmented. Opening a namespace with the other engine, or with a configuration
or options different from the stored ones, is an `Invalid` error before any
write; an unknown version is `Corrupt`. Each engine reads every version its
format sections below list and writes the current one. Nothing migrates a
namespace between engines in place; data moves by reading one and writing the
other.

The Rust library implements this path:

```text
Database API (put / get / delete / exact and filtered search)
    -> in-memory document map + immutable checkpoints + mutation log / recovery
    -> ObjectStore abstraction
    -> local development backend or S3-compatible backend
```

For version 3/5 chunked snapshots, a separate read-only `StreamingDatabase` path
keeps the selected manifest and newer mutations in memory and scans validated
data chunks directly from the object store for exact queries.

One mutable database handle exclusively owns a storage namespace. For a
single-writer deployment, `OwnedDatabase` establishes an object-store claim
before opening the database. Legacy `Database::open` on an unclaimed namespace
still requires the caller to ensure exclusive ownership; a namespace enrolled
in owned mode rejects raw opens. No multi-writer protocol is provided. An owned
object-store handle isolates the engine from filesystem operations. The newest complete checkpoint and newer
mutations are authoritative; without a checkpoint, recovery uses the complete
durable log. The in-memory map is derived. Reads observe that map, and successful
writes become visible after durable storage.

Document IDs are u64. Vectors have one positive, persisted dimension and finite
f32 components. Each live document also has a map of UTF-8 string keys to string
values. The persisted metric enum supports squared Euclidean, Manhattan and
cosine distance, accumulated in f64. Cosine puts and queries require nonzero
L2 norm. Cosine vectors are normalized in f64 and stored as f32 when applied
to the resident map or segmented tail, including replay; durable requests and
retry digests retain the submitted vector. `get` returns the stored unit vector.
Cosine distance is `1 - dot(q, v)` on normalized vectors. Exact search scans all
live documents, sorts by ascending distance then ID, and returns at most k
results. Exact query results retain allocation only for the returned neighbors;
temporary scoring still uses O(matching rows) memory. Holding many completed
results remains a caller
resource choice. Filtered exact search
requires every specified key/value equality to hold; missing keys do not match.
An empty filter includes every document. A put replaces both the vector and the
complete metadata map; a delete removes both.

## Derived IVF-Flat candidate index

`build_ivf(IvfConfig)` explicitly builds an in-memory index from the acknowledged
map. Seeded farthest-point initialization and a fixed number of assignment/update
iterations train means for squared Euclidean and cosine distance and coordinate
medians for Manhattan distance. Each ID belongs to one final partition;
full-precision vectors remain in the document map. Empty partitions retain
their centers. Ties are deterministic; partitions are capped at the live row
count.

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
Query execution is explicit: `search_filtered` is always exact, including when
an IVF index is present; callers choose fixed or adaptive IVF methods when they
accept approximate results. There is no metadata index or automatic
exact-versus-IVF planner. The reproducible 512-row, 64-dimension, seed-42
filtered workload in `benchmarks/FILTERING.md` supports this decision: with
16 partitions and one match per 32 documents, exact search scores 16 eligible
vectors, whereas four IVF probes perform 20.96 mean distance evaluations
(including centers), achieve 37.08% mean recall@10 and return too few results
for all 24 queries. Even with every document eligible, four probes achieve
57.50% mean recall@10. Adaptive probing fills available result slots but does
not bound recall loss. Those short synthetic runs do not establish a reliable
latency threshold, selectivity model or acceptable recall target for silently
selecting ANN. A future automatic planner needs a stated quality policy and
representative measurements before it can change query behavior.

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

For the M8 deployment, exact search is the supported serving policy. The M12
quality gate tested serialized full-vector partitions and four-partition bundles:
eight probes fail sparse-filter recall, while higher probing or bundling exceeds
the selected request/byte budgets. A filter-specific persistent partition copy
would duplicate the small M11 resident exact posting. No production persisted
partition protocol is introduced for this envelope. Explicit resident IVF APIs
remain experimental; approximate requests in the serving wrapper are rejected
until a workload-specific quality policy is justified. See `benchmarks/M12.md`.

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

The next target is a large collection on one machine, exceeding its configured
RAM budget, with object storage as the durable source and bounded NVMe/RAM caches.
The M8–M20 resident workloads are correctness/performance baselines, not the
architectural capacity target. Cache loss must not lose acknowledged writes;
local storage is disposable acceleration, never an acknowledgement substitute
for durable object publication. NVMe/RAM tiering precedes multi-node execution.

The target read path selects immutable segments/blocks through bounded metadata
and derived indexes, then consults RAM, NVMe and object storage as needed. Opening
must not require materializing all vectors. Immutable identity and integrity
checks bind cached bytes to the selected committed generation. Cache budgets must
include metadata and in-flight reads, and eviction/cold starts must preserve
logical results. The existing streaming reader is a useful baseline, not this
completed serving/cache implementation.

The original physical ID-sorted blocks were suitable for bounded changed-data
writes but insufficient for the M21 one-block-per-GET quality/request envelope:
an oracle selecting the best eight committed blocks
reaches only 0.855 unfiltered and 0.800 filtered mean recall@10 on 200 SIFT1M
queries. Selective serving therefore needs a measured vector-aware read layout
or another block grouping that passes both byte and request limits. Any derived
candidate structure must bind to a committed generation, tolerate missing or
corrupt derived data through explicit rebuild/failure semantics, and leave the
root and logs authoritative. See `benchmarks/M24.md` for the bound and limits.

Experimental sealing now uses balanced vector-local groups of at most 170 put
records, splitting again if the encoded block exceeds 128 KiB. Deletes occupy
separate ID-sorted blocks. Records within a block and the authoritative run
index remain ID-sorted; physical blocks need not be ordered by ID. The root,
block and index format versions and log acknowledgement boundary are unchanged.
On the M21 corpus this grouping improves the optimistic eight-block unfiltered
recall ceiling to 0.9455 with at most 913,347 encoded block bytes across those
eight, while the filtered ceiling is only 0.841. See `benchmarks/M24.md`.

### Persisted pack sketches and selective reads

Every pack written by a seal or reclamation begins with a sketch frame: the
versioned magic `GLPKSK01`, the sketch length (u64 little-endian), the
SHA-256 of the sketch bytes, then the sketch; block offsets follow the frame
and remain covered by the root's block digests. The sketch payload starts
with its own versioned magic `GLSKT001`, then dimensions, metric, five-bit
width, the SHA-256 of the namespace's derived-index options, the pack key and,
per block, the block's SHA-256 digest and put-row count. The body holds one
per-pack affine codebook (f32 minimum and step per dimension), row IDs in
block order, packed five-bit codes (little-endian bit fields) and full f32
vectors of rows matching the declared resident predicate. Cosine uses squared
Euclidean sketch distances and lower bounds on stored unit vectors, preserving
routing order and the nonnegative-prefix pruning proof. Tombstones have no
row. When up to four sorted, unique, nonempty routed keys are declared, metadata
version 4 stores them and packs use `GLSKT002`: the `GLSKT001` layout with
the new magic followed,
for each declared key in order, by a u32 dictionary length (at most 254),
u32-length-prefixed sorted unique UTF-8 values, and one u8 code per put row.
Codes 0 through 253 index the dictionary, 254 means absent, and 255 means a
present value beyond the dictionary. Empty routed keys retain metadata versions
2/3 and byte-identical `GLSKT001` sketches. A routed namespace rebuilds any
legacy or invalid sketch from authenticated blocks. These fields are derived;
the root and block records remain authoritative. Seal and reclamation publish
the frame before the root; interruption leaves the prior root authoritative.
A per-pack codebook is between the measured run-local and block-local
granularities. Embedding the sketch gives it exactly its pack's lifetime and
costs no extra PUT or DELETE: consolidation and pruning reuse packs and their
sketches unchanged, and reclamation writes a new pack with a new sketch. The
root format is unchanged. Packs are at most 1 MiB of blocks plus a sketch of
at most 1 MiB; a sealed pack holds at most 12 blocks, which bounds the region
its codebook covers.

New blocks use block format version 2: the magic `GLB2`, the raw length as
u32, then a zstd frame of a binary layout (dimensions, metric, partition,
record count; per record ID, sequence, kind, little-endian f32 components and
length-prefixed UTF-8 metadata in key order). Raw layouts are at most 120 KiB
so a compressed block stays within 128 KiB. Readers accept version 1 JSON
blocks and version 2 by magic; both are authenticated by the root's block
digest before decoding, and version 2 decoding validates configuration, ID
order, sequences, finite components, metadata encoding and length. SIFT
blocks compress about threefold, so the per-query byte budget covers more
candidate rows, uploads and cache footprint shrink, and reranking scans
records from a reused buffer without per-record allocation instead of parsing
JSON floats. Logs, roots and indexes keep their formats.
Block v2 and sketch metric bytes are 0 for squared Euclidean, 1 for Manhattan,
and 2 for cosine; existing bytes retain their meaning.

The sketch is derived, never authoritative. Opening reads a bounded prefix
of each referenced pack through batched range reads (8 at a time), re-reads
a longer prefix if the frame needs it, verifies the frame digest (range reads
bypass the whole-object envelope) and decodes it, then binds each root block
reference to a sketch block with the same digest. A pack without a valid
frame, a store-reported corrupt range, or a sketch failing decoding or
identity checks has its sketch rebuilt in memory from that pack's
authenticated referenced blocks and counted (`sketch_rebuilds`). An
interrupted pack publication leaves an unreferenced pack that cleanup removes;
the prior root remains selected. Liveness is a per-row bit derived from the
latest-ID directory and log tail: an acknowledged tail write clears its prior
row, a published seal or reclamation activates its new pack's current rows,
and consolidation/pruning only rebind locations. A cleared row never becomes
live again, so an in-memory compaction (at open and as an idle maintenance
unit) drops shadowed rows and keeps resident routing state proportional to
the live set; reopening reloads the full persisted sketches. Root
publications do not invalidate the reader, and recovery reconstructs
identical bits. A failure to bind after a root publication poisons the
handle. The latest-ID directory is a sorted vector of 24-byte slots, sized
once at open and merged in place. Sketch row IDs are stored as u32 offsets
from the pack's smallest ID when its span allows, else as u64.

`search_selective_within(query, k, budget, filter)` supports two modes. With
no filter it scores live codes, visiting packs in order of a per-pack lower
bound and abandoning a row once its prefix sum exceeds both its block's best
and the current last kept candidate; sums of nonnegative terms never
decrease, so the ranked blocks equal those of a full scan. It ranks
`budget.blocks` candidates by minimum approximate row distance. In rank order
each candidate widens its pack's span, one byte range from the first to the
last chosen block of that pack, while all spans total at most `budget.bytes`
and number at most `budget.requests`; every live block inside a span is
reranked because its bytes are read anyway. The choice never depends on cache
contents, so each query issues at most that many range GETs and bytes, and
losing a cache changes latency only. Blocks are looked up in the cache; a span
containing any miss is fetched whole in one batched read and its blocks share
that buffer. Blocks are authenticated, checked against the sketch's live rows
and exactly reranked with the live tail on scoped threads. The result is
approximate. With exactly the declared resident predicate the query scans the
resident full-precision vectors and matching tail rows and is exact, with no
block reads by default. Requested metadata/vector travel with the same version
scored during block reranking or from the in-memory tail. Only resident-filter
queries with requested fields fetch documents for their final k hits through
the block cache (and charge any remote reads); the fetched vector is checked
against the scored distance before returning its metadata. For other
conjunctions, routing considers only live rows whose
codes match every predicate on a declared routed key or have overflow code
255; a block with no candidate row is omitted. Unrouted predicates do not
affect routing. Reranking still applies the full filter to authenticated
blocks and the live tail, so results match but remain approximate at a bounded
read budget. `search_exact` remains the oracle. Segmented metadata version 3
declares the resident predicate, and version 4 additionally declares routed
keys. Version 2 has neither declaration; opening with different options fails.
A resident predicate is justified only when its matching rows fit the
index budget: at M21's 1% cohort it adds about 1.3 MB. Filter-specific
grouped block copies were rejected because each overwrite would publish a
second copy and the exact resident posting already meets the query gates.

Reclamation freezes, together, the mostly dead packs with the most garbage
whose estimated live bytes fit 7/8 of one pack (the margin covers estimation
error) and whose live rows fit 12 blocks (decoded records are held until the
new pack is written), rewrites their live records into one new pack block for
block, and publishes one root; this bounds reclamation PUTs, DELETEs, roots
and memory per dead byte. A pack is reclaimed only if it frees at least the
configured minimum garbage (default 64 KiB).

A seal plans block membership by ID from borrowed tail vectors, then each
step materializes only its own pack's records. If a newer write replaces a
tail version the seal still has to publish, that version moves into the
seal's side map, so the seal publishes exactly its frozen prefix. Run
consolidation merges two run indexes straight into the output entries only
while their encoded sizes total at most 2 MiB, bounding its memory and time.
With 24-byte entries and size-tiered pairing, runs settle between about 44,000
and 87,000 IDs, so the 64-run root limit leaves room beyond 1,000,000 rows (a
512 KiB cap exceeded it near 800,000). `ObjectStore::get_many` and `get_ranges` batch independent reads; the
S3 backend issues up to 16 concurrently, and other backends default to serial
reads. Opening uses them for run indexes, the log tail and sketch frames.

### Group commit

`SegmentedDatabase::apply_requests` publishes several independent requests in
one conditional log create. Each accepted request gets its own consecutive
sequence, retry receipt and conditional decision, made in order against the
state left by the earlier requests, on a copy of the retry state; nothing is
applied or visible before the shared create succeeds. A single request keeps
log version 1; a group uses log version 2 (`first_sequence` plus ordered
request/outcome entries) at the key of its first sequence, and recovery
replays it as consecutive sequences. Invalid requests and retained duplicates
are answered individually without publication. If the create fails, every
accepted request in the group is uncertain and the handle is poisoned, as for
a single write. The admission worker groups consecutive queued writes (at most
16 and 1 MiB) without any added delay, so concurrent writers share one PUT;
log-object, tail and backup bookkeeping count objects, not sequences.

### Segmented serving

`SegmentedServing` claims the namespace with the owned-store protocol, opens
it with a sketch byte budget and an optional block cache, and implements the
`admission::Engine` trait, so `admission::Service` runs it on the single
committer thread. Queries use `search_selective_within` with a fixed read
budget (M21: 12 ranked candidates, 8 range requests, 1 MiB);
unfiltered results are approximate under the M21 quality policy (mean
recall@10 >=0.90, fifth percentile >=0.80, <1% short results), and the
resident predicate is exact. Maintenance never runs concurrently with a
command: while the queue is empty the worker executes one bounded unit (one
seal/prune/reclaim step, an in-memory sketch compaction of one pack, a seal
plan, one run consolidation, a prune/reclaim plan or a four-object cleanup
batch) and then rechecks the queue. A seal starts
at 32 unsealed log objects. If the tail reaches the hard bound of 64 before
idle time allows the seal, the next write finishes any staged prune/reclaim
and a full seal synchronously, reported as that command's maintenance time.
A failed idle read leaves state unchanged, is counted and retried after the
next command; an uncertain write poisons the engine and fails the service.

`backup_to` copies root zero, the selected root, its indexes, packs (each
verified against the root's block digests before its PUT; packs carry their
sketches) and the acknowledged log tail into an empty destination, writes `metadata`
last, then opens the destination and compares sequence, root generation and
live counts. Destination failure does not poison the source; a failed or
partial destination must not be promoted.

The target write path retains explicit durable acknowledgement and recovery.
Routine maintenance should rewrite affected bounded data, rather than the full
collection at a fixed mutation count. M21 selects addressable immutable base
blocks and bounded ID-sorted delta runs with per-run vector-partition summaries
as its initial layout direction. The current experimental seal clusters physical
blocks while retaining ID-sorted run indexes; the vector summaries are the
per-pack sketches above rather than per-run summaries. A versioned root names block identities,
sequence and retry state; immutable mutation logs remain authoritative until a
complete root covers them. Readers need a bounded latest-ID directory to hide
stale base candidates and an exact path for quality checks. Block identity and
digest bind disposable RAM/NVMe cache entries to a pinned root in the
segmented namespace; `SegmentedServing` serves it, and the declared M21
250,000-row envelope is accepted on local MinIO (`benchmarks/M24.md`). The
measured 10,000-row independent-arrival boundary and alternatives are in
`benchmarks/M21.md`. The segmented API defines metadata v2/v3/v4,
root/index/log v1 and block v1/v2 in a fresh namespace. It acknowledges
immutable logs, publishes a fixed sequence through an immutable root generation
after its packs/index, and replays newer contiguous logs; uncertain publication
requires reopen. It can coalesce adjacent small ID indexes through another root
generation, reusing immutable blocks and dropping fully unreferenced packs.
For mixed live/stale packs, one reclamation plan freezes the latest IDs for a
<=1 MiB pack, reads authenticated blocks one at a time, then writes a new pack
and publishes a root in separate steps. Each referenced block must retain at
least one record so run indexes remain valid; shadowed index entries need not
still exist in physical blocks. New acknowledged logs may shadow retained
records during staging, while seal and run consolidation wait for the root.
The old pack remains authoritative until root publication; an interrupted
attempt leaves an unreferenced pack for cleanup on reopen. For a run index no
larger than 2 MiB, a separate staged pruning plan removes references to fully
dead blocks, remaps surviving block ordinals in a new index, then publishes one
root. A completely dead run disappears. Logs may grow during this plan, but
seal, run consolidation and repacking wait for its root; uncertain index or
root creation requires reopen. Indexes above this size are not pruned yet.
The opt-in block cache reads through bounded RAM and a versioned local NVMe
directory. Its key is a SHA-256 of object, range, complete payload length and
block digest, kept as 32 bytes in memory with tick-ordered LRU; every hit is
checked against the selected root before decoding.
Missing or corrupt cache bytes trigger an authoritative range fetch. Opening
rejects a missing selected pack, and corrupt bytes fetched from object storage
fail closed. Cache bytes never acknowledge mutations or participate in root
recovery. The cache mutex covers lookup, remote fetch and admission; a
selective query fetches its blocks serially under it, then authenticates,
decodes and scores them on scoped threads before settling hits or rejecting
corrupt cached bytes. Maintenance never overlaps a command: the segmented
serving engine runs bounded units only while the admission queue is empty.

Read latency, write acknowledgement latency, index visibility and object-store
cost are separate targets. One experiment should answer a specific decision,
using retained evidence and relevant failure tests; do not require every mechanism
to fail a tiny resident workload before evaluating its large-data necessity.
Exact search remains the quality oracle even if production serving needs selective
ANN. Concurrent writers, sharding and replication require a demonstrated remaining
single-machine limit and explicit coordination semantics.

[Turbopuffer](https://turbopuffer.com/docs/architecture) and
[OpenData](https://github.com/opendata-oss/opendata) are references for economics
and operating behavior. OpenData's SlateDB foundation is not an adoption decision
for glider's owned storage engine. The next measurable stages are M21–M24 in
`ROADMAP.md`; the segmented engine's acceptance evidence is in
`benchmarks/M24.md`.

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

`ObjectStore::get_range` reads one bounded slice of an immutable payload by
key, offset, length and expected complete payload length. The S3 backend checks
the complete envelope size and response length but does not read the envelope's
whole-object checksum for each slice. A caller must compare returned bytes with
the SHA-256 digest committed for that logical block before decoding or caching
them. Full `get` retains whole-envelope validation; the local backend's range
default uses it. This read API creates no new acknowledgement or durable format,
and the segmented reader uses it for block reads. The M22 physical segment bundles
small addressable blocks in one larger PUT; M23's cache key includes object key,
range and committed block digest.

`metadata` is the first durable object: UTF-8 JSON containing format version 1
and the configuration. Each mutation-log object is immutable UTF-8 JSON with a
format version and sequence. Versions 1 and 2 contain one put or delete; version
1 puts contain an ID and vector, while version 2 puts also require a
string-to-string `metadata` map. Ordinary single writes still use version 2.
Version 3 contains a nonempty ordered `mutations` array of the version 2 operation
schema. `apply_batch` publishes one version 3 object for all its operations.
Version 4 stores a bounded retry request and its durable conditional outcome in
the same object; successful outcomes apply its operations, conflicts apply none.
Recovery accepts all four versions and treats version 1 metadata as empty. The
cosine log vector remains exactly as submitted; applying or replaying it derives
the unit vector held in memory and later snapshots or sealed blocks. The log
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

### Single-writer ownership boundary

`OwnedDatabase::open` is the deployment writer entry point. It creates the
immutable `owner-root-v1` control object if absent, then creates a unique
`owner-v1-{32 lowercase hex digits}` claim using OS randomness and conditional
create. The root payload is exactly version 1 JSON `{"version":1}`. A claim
payload is version 1 JSON containing its token. The opener lists the complete
namespace after publication and becomes active only if its claim is the sole
listed claim. Concurrent openers may all fail, but cannot both become active:
the opener publishing second sees the first claim, unless the first has already
ended and durably removed it. These objects are control state, not vector state;
the document checkpoint and mutation tail remain authoritative.

The root's successful create acknowledges permanent enrollment in owned mode.
An uncertain root create requires a fresh store. Successful claim publication
alone does not grant ownership; the confirming strongly consistent listing does.
A claim create/list error or panic may leave a claim but never acknowledges a
writer. A competing opener removes only its own claim before returning busy;
an uncertain removal requires fresh inspection. A crash or dropped handle leaves
its claim in place. `OwnedDatabase::close` stops using the database and removes
that handle's claim; successful removal acknowledges release. A release error
may have removed the claim, so inspect on a new store before takeover. Unique
claim keys are never reused, preventing a delayed DELETE from removing a later
owner's claim. The immutable root is never removed by normal maintenance.

After a crash, an operator must first prove the former process cannot issue new
requests. Its already-issued timed-out PUT may still finish later. Clearing the
claim and reopening the same prefix then permits a stale in-memory view: a late
mutation can become durable after the new owner's initial listing. Therefore an
uncertain write, forced exit or crash uses a fresh, unadvertised destination
prefix and `stage_isolated_namespace` for takeover unless the chosen service can
prove every old request has quiesced. The old prefix remains quarantined. A
graceful successful `close` permits same-prefix reopen. Claims may be inspected
and explicitly cleared for a verified stopped process, but a claim alone does
not fence its earlier in-flight requests. There is no timed lease or automatic
takeover. Existing namespaces can enroll only after all legacy writer processes
are stopped: an already-open older binary cannot be fenced retroactively. Older
binaries reject the new key kinds. The wrapper hides ownership keys from
database recovery and compaction; raw `Database::open` rejects an enrolled
namespace. An external deletion of an active claim violates the object-store
contract and can permit another owner; backup and operator controls are required
for arbitrary object loss.

`stage_isolated_namespace` freezes one strongly consistent source listing and
copies its complete objects, except ownership control objects, to a fresh empty
prefix. It creates `metadata` last. Before metadata, nonempty interrupted copies
cannot open as a database; after metadata, the full selected set has been
published. It then opens and validates the destination's selected roots, chunks
and contiguous tail. A successful return acknowledges a validated staged copy,
not a mutation in the old namespace. Errors or crashes leave an unpromoted
destination that must not be reused; the operator repeats into another prefix.
The new prefix is exposed to clients only after validation and an owned claim.
Late old-prefix PUTs or DELETEs cannot change it because prefixes do not overlap.
An unacknowledged old mutation may be included or excluded according to the
frozen listing. This does not repair loss of an acknowledged source object and
does not replace a backup. The exact operating procedure is in
`docs/RECOVERY.md`.

This chooses immutable claims over a mutable lease: lease expiry cannot safely
fence a delayed writer with the current object-store operations. A process-only
singleton without a storage witness cannot detect a second opener on another
host. The claim protocol uses conditional create, strongly consistent list and
durable remove, without filesystem locking or in-place mutation.

### Bounded retries and conditional batches (M15)

`apply_request(Request)` adds a bounded retry contract to `Database`,
`OwnedDatabase` and `SingleMachine`. An ID is the complete pair of an observed
commit boundary and a 128-bit nonce; `request_id` uses OS randomness without I/O.
The client keeps both fields unchanged. SHA-256 of the canonical serde JSON
request (ID, ordered conditions and operations, ordered metadata maps) identifies
its payload. At most 100 operations, 100 strictly ID-ordered conditions and 1 MiB
of encoded request bytes are admitted. Legacy mutation APIs have no retry IDs.

A new ID is admitted while `current_sequence - boundary < 128`; future boundaries
are rejected. A version 4 mutation object contains the entire request and its
outcome. The owner's one conditional create atomically publishes the decision
and all successful mutations. Conditional conflicts publish a decision with no
state changes and still consume a sequence. A retained duplicate returns the
original sequence/conflict without I/O, regardless of later revisions or write
capacity; different payload reuse returns `RequestConflict`. Input/capacity
rejections before publication are not durable decisions. Preconditions compare
against the state before the whole batch; operations then apply in input order.
Acknowledgement exposes the complete resulting map. Uncertain errors/panics
poison writes and result lookup, retaining the prior acknowledged read view.

Receipts remain through `boundary + 128` and expire after that boundary. There
are at most 128 receipts containing fixed-size IDs, digests and outcomes; full
request payloads are not retained in memory or snapshots. Unknown IDs at the
admission cutoff return `Expired`, even if no prior decision is known, and
submission cannot reinterpret an expired ID as new. `lookup_request` distinguishes
`Retained`, `Unknown` in this history, `Expired` and `Ahead`. Recovery revalidates
and replays decisions; checkpoints and compaction retain the same receipt state.
A clock-based TTL would need a durable trusted time rule across restore; an LRU
of arbitrary UUIDs would mistake evicted IDs for new requests. The chosen fixed
commit window needs neither clocks nor a permanent retired-ID ledger. It is an
explicit capacity bound, not a promised wall-clock retry duration.

`revision(id)` observes either presence or absence at the current sequence.
Conditions reject a change to that ID after the observation, including a delete,
reinsert, same-value replacement or deletion of an absent ID. Unrelated writes
do not conflict. Recent ID/change-sequence pairs include tombstones and are
retained above `revision_floor`, normally `sequence - 128`. At most 12,800 pairs
are retained. A larger legacy batch advances the floor to its own sequence and
clears older observations instead of keeping unbounded tombstones. Conditions
below the floor return a durable `ExpiredRevision` conflict. This chooses bounded
observation validity over indefinite per-ID tombstones or rejecting conditions
on every unrelated write. Request identity and document observation are separate.
These tokens are a trusted library concurrency contract, not authentication.

Single-object snapshots version 4 and chunked manifests version 5 require the
bounded receipt/change state. Chunk data stays version 1. Legacy snapshots 1–3
are readable; their boundary is the initial revision floor, because they do not
preserve earlier ID changes or receipts. New snapshots preserve observations
across maintenance without changing their sequence. Older binaries reject these
versions instead of dropping retry metadata. Streaming readers validate and
replay the same decisions while returning only document/query results.

Isolated takeover copies the selected history and its receipts; later writes
in the quarantined source cannot affect it. `Unknown` in that destination is
safe to submit there, but cannot prove absence in the quarantined source. Backup
and restore preserve only the backup boundary: no request beyond it is promised
known, and its revisions/IDs cannot be treated as a continuation of discarded
future history. Clients must reconcile across an explicitly announced rollback.
Concurrent callers serialize through the owner using bounded admission,
exclusive borrowing or a caller mutex; concurrent duplicates share one outcome.
No unlimited exactly-once or cross-rollback delivery guarantee is provided.

## Immutable checkpoint segments (M3)

`Database::checkpoint()` publishes the complete live state at the current mutation
sequence as one immutable `segment-` object with a 20-digit sequence suffix.
Version 1 JSON contains version, sequence, configuration and a strictly ID-sorted
array of `(id, vector)` entries. Version 2 contains `(id, {vector, metadata})`
entries, with metadata a required string-to-string map. New single-object
snapshots use version 4 with the same documents and required retry/revision state.
Single-object snapshot writers emit exactly represented integral f32 components
in [-2^24, 2^24] as JSON integers, preserving negative zero and the standard
floating representation of other values. Existing version-4 readers already
decode both JSON number forms into the same f32 bits; the schema and version
remain unchanged. Mutation/retry identity encoding and chunk byte-reuse encoding
retain their existing representation. This reduces serialization and PUT bytes
for integral descriptor workloads without rounding vector values.
Recovery accepts version 1 snapshots with empty metadata and version 2 snapshots
without retry state. The backend envelope protects its bytes. Deletes are represented by absence; the
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

### Chunked snapshots (versions 3 and 5)

`checkpoint_chunked(max_chunk_bytes)` and `compact_chunked(max_chunk_bytes)`
publish the same logical and retry/revision state as the single-object methods.
They split the strictly ID-ordered document map into version 1 JSON chunks, each at most the
caller-selected encoded payload limit. A single document that cannot fit is an
input error detected before storage writes. Chunk keys are
`segmentchunk-{sequence:020}-{limit:020}-{ordinal:010}` or
`compactedchunk-{sequence:020}-{limit:020}-{ordinal:010}`. The limit in the key
allows a failed attempt to be retried with a different layout without reusing a
published key. Chunk objects contain version, sequence, configuration and rows.

After every chunk is durably created, the writer publishes a version 5 manifest
(version 3 plus required retry/revision state) at the existing `segment-` or
`compacted-` key. It contains version, sequence,
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

Recovery accepts single-object versions 1, 2 and 4 and manifest versions 3 and 5.
For a selected manifest it requires every referenced chunk, checks digest,
version, sequence, configuration, byte limit, row count and ID bounds, and
validates all vectors and globally increasing IDs. It never falls back to an
older snapshot when a selected chunk is missing or invalid. Unreferenced chunks
from incomplete publications are ignored. Compaction retains only chunks named
by the current compacted manifest and reclaims older and orphaned chunks after
the new root is durable; interrupted cleanup resumes on a repeated call. Older
binaries reject unsupported chunk keys or manifest versions.

The single-object format uses fewer requests but makes one payload grow with the
dataset. The manifest format bounds each data payload and temporary chunk
serialization memory at the cost of more PUTs and recovery GETs. A mutable head
or content-addressed keys would add coordination or key-reuse risks to cleanup
without helping the exclusive owner. These APIs remain explicit; the database
still materializes the full map in memory, scans the complete object listing on
open, and stores one unbounded manifest. They do not yet support lazy partition
reads or a bounded-memory database.

### Streaming exact reads

`StreamingDatabase::open(store, config)` requires an existing namespace whose
selected compaction boundary and selected newer checkpoint, if any, use version
3/5 chunked manifests. It performs the same key, metadata and contiguous-log
checks as `Database::open`, validates each selected root and all referenced
chunks, then replays newer version 1/2/3/4 mutations into an ID-keyed overlay.
Legacy single-object roots are rejected for this mode; `compact_chunked` can
migrate them. The view freezes at the recovered mutation sequence and uses the
same exclusive namespace ownership precondition as the mutable database.

Ordinary `search` and `search_filtered` read and revalidate each base chunk in order,
skip IDs replaced or deleted in the overlay, score matching base rows and live
overlay rows, then return the exact distance/ID top-k. A bounded top-k heap
avoids retaining all candidates. `get_with_metadata` first checks the overlay,
then uses manifest ID ranges to read at most one base chunk and returns owned
values. A missing or invalid referenced chunk makes the operation fail rather
than return partial results. The reader makes no durable writes and cannot
observe later mutations without reopening.

This separates base-vector working memory from dataset size: ordinary searches
retain a manifest, the latest uncheckpointed IDs, one decoded chunk and up to k
neighbors, plus transient response/decoder allocations. The manifest and mutation
tail are not bounded, nor is a request for k equal to the whole dataset. Opening
validates all chunks and ordinary exact searches issue one GET per chunk, so
this path trades RAM for remote reads and is not an ANN latency optimization.

`StreamingDatabase::open_with_filter` may retain one equality posting during
the mandatory selected-chunk validation scan. The posting contains only matching
base documents and is derived entirely in memory. A query whose conjunction
contains that exact key/value scores the posting after excluding IDs changed in
the overlay, then scores matching live overlay rows. Other predicates and
unfiltered queries use the ordinary validated full scan. The posting adds no
persisted format, publication boundary or recovery dependency; every reopen
rebuilds it from the selected authoritative snapshot. A missing or invalid
selected chunk fails open. A later external loss is detected on reopen; the
resident posting still represents the previously validated frozen view. At the
M8 size the chosen filter matches 20 of 2,000 rows. The caller supplies a maximum
posting row count; exceeding it fails open explicitly before retaining more
rows. A broader filter or larger dataset needs its own memory budget or the
ordinary streaming path.

## Compaction (M4)

`Database::compact()` publishes the complete current live state as
`compacted-` plus its 20-digit mutation sequence, using the same versioned snapshot
payload as an ordinary segment. `compact_chunked` instead publishes the version 5
chunked form described above. Only this distinct snapshot kind authorizes
reclamation. After durable publication, compaction lists and validates the cleanup
plan, then removes covered mutations, covered ordinary segments and older
compaction snapshots. The validated obsolete set may be removed in bounded
parallel groups through `ObjectStore::remove_many`; default backends retain
serial removal. Success acknowledges every removal. Errors/panics may leave an
arbitrary subset absent, poison the handle and require recovery to rebuild
object counts before new writes. Metadata and the new compaction snapshot remain.

Existing segments already contain full live state; consolidation serializes the
recovered map rather than merging overlapping full snapshots. The single-object
form combines state and reclamation boundary; the chunked form uses one final
manifest as that boundary. A mutable head would introduce a coordination
requirement unnecessary for the exclusive owner. There are no background workers. The core database requires explicit maintenance;
the serial serving wrapper schedules it before admitting a batch at soft limits.

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

### Bounded single-machine maintenance (M10)

`Database::set_maintenance_limits` enables a runtime policy after recovery;
callers must reapply it on every open. The policy does not change the persisted
format or mutation acknowledgement. Successful object creation/removal updates a
visible engine-object counter; recovery reconstructs it from the strongly
consistent listing. An uncertain operation poisons the handle, so stale counters
cannot authorize another write. `maintenance_status` exposes the sequence, tail
object count, visible object count, soft compaction signal and hard write block.

The M8 policy signals compaction at 16 tail or 64 visible engine objects and
blocks a new mutation at 24 tail or 96 visible objects. It caps each atomic batch
at 100 mutations. Backpressure is checked before mutation publication, so a
rejected write creates no object and does not poison the handle. Checkpointing
can reset the replay tail while retaining old objects; the visible-object bound
still drives compaction. Maintenance is explicit, not part of a mutation's
acknowledgement. `compact` and `compact_chunked` publish a new root before
cleanup, and a repeated call after reopening resumes interrupted cleanup.

At the M8 size, 100-operation batches reduce 2,000 writes to 20 mutation
objects. A 128 KiB chunked compaction of 2,000 live rows yields 12 chunks and
one manifest; with 24 subsequent tail objects, recovery needs at most 38 GETs
including metadata. This is a measured layout choice for the initial envelope,
not a bound for arbitrary datasets or chunk sizes. `benchmarks/M10.md` records
the targeted counts, timings, memory and write amplification. That layout needed
no new catalog; M15 added versioned retry metadata to snapshots and request records. Old namespaces remain readable.

## Serial single-machine serving (M13)

`serving::SingleMachine` holds one `OwnedDatabase` and exposes serial exact
queries, bounded atomic batches, status, maintenance and backup. Exclusive
borrowing prevents overlapping publication, reads and reclamation; this contract
needs no pinned concurrent views or background workers. It uses the resident
map because the M8 set fits the measured memory budget. M11 remains available
for separately opened frozen streaming views under its ownership precondition.
An approximate request returns an explicit policy error; there is no silent
quality downgrade or automatic ANN planner.

`ServingOptions::m8` limits the final live set to 2,000 documents, each serialized
vector/metadata payload to 4,096 bytes, and batches to 100 mutations. It uses
single-object compaction and the M10 tail/object limits. The M8 resident set
fits the measured memory/recovery limits; one snapshot PUT and one old-root
DELETE avoid per-chunk publication/reclamation overhead. Callers needing
streaming snapshots can select `chunk_bytes: Some(131_072)` and establish their
own admission budget. Batches validate capacity,
vectors and document bytes before any storage access. At a soft limit the
wrapper completes compaction before publishing the next batch; a maintenance
failure means that submitted batch was never published. A batch publication
failure retains the ordinary uncertain-outcome semantics. Neither path retries
an uncertain request. Reads retain the last acknowledged map; status exposes
sequence, tail, visible engine objects, row capacity, exact policy, returned
storage errors, backup errors, maintenance runs and the recovery-required flag.
Counters are per handle and reset on reopen; the sequence is durable. Operators
measure process RSS separately. Bounds are reapplied and recovered rows checked
on every open; recovery itself still validates/loads the authoritative state
before checking serving capacity, so this is not protection from opening an
arbitrarily oversized namespace.

`backup_to` runs synchronously on a clean handle, compacts and completes cleanup,
then stages that committed root and all referenced objects into a fresh empty
nonoverlapping prefix. Metadata publishes last and the complete destination is
validated before success. It acknowledges a backup of the current sequence,
not a new mutation. Destination errors leave an unpromoted copy and do not
poison the source; source maintenance errors do. Backup restoration stages into
another fresh prefix, validates exact state and claims ownership before client
switch. A poisoned serving handle refuses graceful close and leaves its claim;
use isolated takeover, never same-prefix reopening after uncertainty. Legacy
v1/v2 records migrate through version-4 single-object snapshots or version-5
chunked manifests, preserving M15 retry/revision state for later requests.

This is a synchronous library serving contract, not a network server or a
concurrent request latency guarantee. The M8 workload uses full 100-operation
batches to amortize maintenance; small batches, larger rows or other arrival
rates require new capacity and write-amplification measurements. Operator steps
are in `docs/SERVING.md` and `docs/RECOVERY.md`.

### HTTP service

`glider-server` (feature `server`) exposes one segmented collection over
HTTP/JSON through an axum router in front of `admission::Service`; blocking
ticket waits run on Tokio's blocking pool. Every write carries a request ID:
`/v1/query` optionally includes metadata and/or vector on each hit; omitted
flags preserve the ID-and-distance response.
a client-supplied ID makes the request safely retryable, otherwise the server
issues one at the current sequence and returns it, so an uncertain response
can be resolved through the request lookup. HTTP adds no durability step:
the response is sent only after the admission worker reports durable
publication. Overload maps to 429, invalid input to 400, request-ID misuse to
409, corruption to 500 and an unavailable or poisoned engine to 503.
SIGINT/SIGTERM stops accepting connections, drains queued commands and
releases the ownership claim; a failed worker keeps the claim for the
documented recovery procedure. An optional static bearer token guards every
endpoint except `/healthz` and `/metrics`. The latter serves Prometheus 0.0.4
text: server atomics record per-endpoint response classes and latency buckets,
while a read-only admission command samples sequence, segmented maintenance,
cache and sketch values on the owning worker. Queue state is sampled separately;
metrics are observational and do not change publication or recovery semantics.

## Bounded concurrent admission (M16)

`admission::Service` moves one engine (`SingleMachine` or `SegmentedServing`,
through the `admission::Engine` trait) to a single blocking worker.
Cloneable clients share a FIFO of writes, exact queries, revision observations
and result lookups. At most eight commands and 320 KiB of encoded payload are
admitted by default, including active work. Count and byte exhaustion returns
`Overloaded` before retaining a normalized payload; inputs are validated and
caller-controlled spare capacities are discarded. The queue lock covers bounded
normalization and bookkeeping, never storage or search. Encoded bytes are an
admission measure, not allocator RSS. Count, operation/filter limits and payload
limits also bound container overhead; caller-owned inputs and completed results
are outside the service's retention budget.

One owner executes commands in enqueue order by default, with no group commit
or batching delay. `Limits::read_priority` optionally lets a queued query run
before queued writes, lookups and observations younger than the configured
age; queued commands are concurrent, so either order is linearizable, and a
query submitted after an acknowledgement still observes that write. When the
queue is empty the worker may run one bounded engine maintenance unit, then
rechecks the queue; a unit is never preempted, so its duration adds to the
wait of commands that arrive during it. Clients choose their own atomic M15 batches. Due
maintenance stays synchronous. Each result reports queue wait, execution and
maintenance separately; client end-to-end latency additionally includes admission
and response delivery. Admission capacity is released before delivery. Numeric
workload budgets and the mutex comparison are in `benchmarks/M16.md`; a serial
batch measurement alone is not concurrent latency evidence.

Ticket cancellation succeeds only while queued, proving that execution and
publication will not start. Once the worker takes a command, cancellation cannot
undo it; dropping the response does not stop a PUT. Resolve uncertainty with
its M15 ID after recovery. A dropped queued ticket is cancelled, but retains its
charge until the worker removes it. Queries bind results and reported sequence
to one committed state; there is no simultaneous reader touching the map.

`begin_shutdown` closes admission synchronously and either drains accepted work
or cancels queued work. Active execution finishes in both modes. `shutdown` joins
the worker and acknowledges successful ownership release; its time bound depends
on the configured backend operation deadlines. Dropping the service closes
admission and cancels queued work, but does not acknowledge shutdown. A worker
panic or uncertain storage error fails pending work, closes admission, releases
queue charges and leaves the ownership claim for isolated recovery. Input errors
and conditional conflicts do not stop a healthy worker. Thread-spawn failure
can leave the already acquired claim; inspect/recover it as a stopped owner.
No successful write result is delivered before durable publication. Backup is
performed after graceful shutdown using the serial serving API.

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
without changing the engine API; callers use blocking threads. Each S3 handle
keeps one runtime worker active between synchronous calls so HTTP connection
tasks process peer closure and pool expiration while the caller is idle. A
current-thread runtime would suspend those tasks outside `block_on`, permitting
stale pooled connections to survive a long idle period. This does not add
automatic request retries or alter write acknowledgement and recovery.

One bucket plus a nonempty, nonoverlapping namespace prefix identifies a database. The caller
provisions the bucket, credentials and exclusive namespace ownership. The service
must provide strongly consistent get/list and atomic conditional PUT. Each create
sends one `If-None-Match: *` PUT with no preflight existence check, multipart upload,
unconditional fallback or automatic retry. Success acknowledges durable native
publication. Any create error or panic poisons the store until a fresh handle is
opened; the database retains its existing acknowledged-state read semantics.
A timed-out request may still complete remotely. Conditional creation prevents a
late request from replacing a newly committed object at the same sequence key.

Removal sends one native DELETE per key, with no automatic retry; absence is success.
Compaction cleanup executes at most four such requests simultaneously and waits
for the entire validated set before acknowledging success or returning a storage
error. Partial failure poisons the store. This is synchronous bounded parallel
I/O after snapshot publication, not a background publication participant or an
S3 bulk-delete request. Obsolete keys cannot be referenced by the serial reader
or reused by later writes, so reordered or delayed DELETEs remain safe.
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

`S3Store::with_read_limits` optionally bounds the visible object count, total
listed envelope bytes and each object envelope. Listing consumes the SDK stream
incrementally and rejects excess before exposing any inventory to recovery. GET
checks metadata before body collection and checks each streamed chunk before
appending it. A read-limit error performs no deletion, does not poison the store
and never returns partial data. Owned opens can still leave a claim when a
subsequent listing/recovery check fails; inspect it using the existing stopped
owner procedure. Limits include ownership keys, obsolete objects and envelopes.
They must cover the temporary coexistence of old/new snapshots. They bound
retained input, not exact RSS: SDK page/transport buffers, decoder allocations
and resident state also consume memory. Defaults preserve the unbounded legacy
API; deployments must opt in and validate their serving/RSS budgets. This changes
no persisted format, write acknowledgement or publication protocol.

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
