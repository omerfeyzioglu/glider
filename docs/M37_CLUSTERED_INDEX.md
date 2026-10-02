# M37: global clustered index for the segmented engine

Status: stages 1-5 implemented: formats, the offline probe, explicit
conversion (`convert_clustered`, `glider-admin convert`), clustered queries,
clustered seals and bounded posting merges. Static recall on real packs
passes the gates at 250,000 and 1,000,000 rows, and so does recall after
an in-memory replay of the M31 update wave (`benchmarks/M37.md`, stages 3
and 4-5); the MinIO/S3 acceptance (stage 7) runs with
`tools/m24_acceptance.py --clustered`. Split/reassign (stage 6) is not
implemented: the wave measurements did not require it. `DESIGN.md`
("Clustered view") states the implemented invariants.
The first implementation target is the M31 single-writer, 128-dimensional SIFT1M
envelope. Exact search over the committed runs and log tail remains the oracle.

## Problem and target

M31's 1,000,000-row MinIO run has 20 runs, 5,928 blocks, 115.1 MB of charged
sketches, 155.4 MiB peak engine RSS and 0.894 static unfiltered mean recall@10.
Eight range requests use at most 0.47 MB, yet the best eight blocks contain only
about 0.885 of the true top-10 in `benchmarks/M31.md` (the roadmap reports
0.888 for its later summary).
More per-seal blocks or better sketch ranking cannot recover neighbors absent
from those requests. Ten and twelve uniformly selected requests raise the
measured ceiling to 0.962 and 0.990, respectively, but violate the eight-request
gate. The independent seal is the wrong locality boundary.

The design goal is a namespace-wide partition of **current** vectors so one
cluster's rows from many seals can be read in one or a few pack ranges. A cold
query must stay within eight remote range GETs and 1 MiB of payload, including
all posting ranges; the existing 12-block candidate limit is a starting profile,
not a reason to spend requests on unrelated seal fragments. Static and update
wave recall must meet the M31 mean >=0.90, fifth percentile >=0.80 and short
result fraction <1% gates at both 250,000 and 1,000,000 rows. A clustered
layout is a hypothesis until the measured curves establish those gates.

The design borrows SPANN's RAM centroids and disk postings, with optional
boundary copies, to address the measured cross-pack fan-out. SPFresh/LIRE
motivates local split, merge and reassignment instead of routine full rebuilds.
Turbopuffer's whole-namespace SSD cache motivates the separate M35 warm path;
the cold gate here cannot depend on it. OpenData Vector's rejection of
per-segment centroid indexes matches the fan-out problem here, but its SlateDB
storage protocol is not adopted. These are design inputs, not guarantees for
glider's workload.

## Committed state and persisted objects

Keep `sglog-*`, the ID-sorted run indexes, canonical block records and the
24-byte-per-ID latest-ID directory. Their latest `(ID, sequence, deleted)`
decision, plus newer log entries, remains authoritative for `get`, exact search,
retry state and recovery. A serving copy never acknowledges a mutation. The
directory still has exactly one canonical location per ID; duplicate posting
rows never enter an ID run index.

A **root v4** keeps v1's generation, sequence, configuration, retry state and
runs, and adds a required clustered-view reference: `(epoch, centroid key,
centroid length and SHA-256, catalog key, catalog length and SHA-256)`. The
old root v1 remains selected during conversion and serves through the existing
per-seal route under its existing quality policy. Root v2 already carries M36
takeover fences, and v3 is the M36 fence marker, so v4 is the next free root
version. The v4 JSON requires `clustered`; v1 and v2 forbid it. A v4 root may
also carry takeover fences. Opening a selected v4 root loads its view (a
missing or corrupt view object makes the view unavailable, below); every
later root of a converted namespace is v4. Older binaries reject v4 rather than discard its
reference. Root keys remain
`sgroot-{generation:020}`; their never-reused generations are the only index
visibility switch. Root zero and metadata keep their existing meanings.

The immutable **centroid object v1** has a new key kind `sgcentroid-{attempt}`
and a length/digest in the root. It begins with `GLCENT01` and contains config identity
(dimension and metric), source root generation/sequence, deterministic training
seed, sample definition and digest, epoch, centroid count, f32 coordinates,
and stable u32 cluster IDs. It does
not contain postings. Validate finite centers, metric-compatible normalization,
unique IDs, count and exact byte length. An epoch is immutable: moving a center
or changing cluster count creates a new epoch rather than reinterpreting old
posting labels. The root binds one epoch and its catalog together.

The immutable **catalog v1**, `sgcluster-{attempt}` with magic `GLCLCAT1`, lists clusters in ID order
and each cluster's ordered posting extents. An extent binds `(pack key, total
payload length, offset, length, row count, epoch, cluster ID)` and an ordered
list of the constituent block offsets, lengths and SHA-256 digests; one extent
is one cluster-contiguous block or adjacent run of blocks. It also marks
whether the extent is a canonical seal block or a derived serving copy.
No row may be omitted: for every latest live sealed `(ID, sequence)` not
shadowed by the unsealed tail, at least one current posting copy is reachable.
Validate sorted/nonoverlapping ranges within a pack, digests, cluster IDs,
bounded counts and referenced-object presence before serving. This invariant
is checked by a full exact-vs-catalog audit in tests and migration; later
maintenance preserves it by construction. Routine open validates structure
and authenticated sketches, not a full vector scan.
The catalog is a whole, versioned object so one root generation selects a
complete view; the 1M starting estimate is thousands of extents and roughly
1-3 MiB of resident descriptors, to be measured. Cap its encoded size (for
example 16 MiB) and fail maintenance before publication if exceeded; do not
silently omit partitions.

Stage 1 binary layouts use little-endian numbers and exact lengths. Centroid
v1 is `GLCENT01`, dimension `u32`, metric `u8` (0 squared Euclidean, 1
Manhattan, 2 cosine), source generation and sequence `u64`, seed `u64`, sample
rule `u8` (1 = seeded hash priority over live IDs), sample row count `u32`,
training iterations `u32`, sample SHA-256 (32 raw bytes), epoch `u64`, center
count `u32`, then each center's ID `u32` and `dimension` f32 coordinates.
There are at most 4,096 unique centers and 16,384 sampled rows; a centroid
object is at most 2 MiB. Cosine centers have squared norm within `1e-4` of 1.
Catalog v1 is `GLCLCAT1`, epoch `u64`, cluster count `u32`, then ID-ordered
clusters: ID `u32`, extent count `u32`, then each extent's pack-key byte length
`u16`, UTF-8 key, payload length, offset, length and row count `u32`, epoch
`u64`, cluster ID `u32`, kind `u8` (0 canonical, 1 derived), posting role
`u8` (0 primary, 1 secondary), block count `u32`,
then each block's offset and length `u32` and SHA-256 (32 raw bytes). The
catalog lists every center, including empty clusters; extents are ordered by
`(pack key, offset)` within a cluster, and it has at most 65,536
extents and 16 MiB of bytes. Blocks cover each extent exactly; extents do not
overlap within a pack. Root references authenticate complete centroid and
catalog bytes by length and SHA-256 before their decoders run.

Packs retain the existing <=1 MiB block-data and <=12-block limits, <=128 KiB
encoded blocks, `GLB2` full-precision records, and authenticated sketch frame.
New clustered packs use `GLSKT003` sketches carrying per-block cluster ID and
center fingerprint (metric, coordinates and ID), plus per-row sequence: the
`GLSKT001`/`GLSKT002` body (routed sections iff routed keys are declared)
followed by, per block, the cluster ID (u32) and fingerprint (SHA-256 of the
metric byte, dimension u32, center ID u32 and f32 coordinates), then per row
the copied version's sequence (u64), little-endian. Blocks stay `GLB2`; their
partition field is the cluster ID. Each block contains
one cluster's rows, sorted by ID, and adjacent blocks of the same cluster are
contiguous in a pack. Several small clusters may share a pack, but one cluster
extent must be a contiguous byte range. The center fingerprint must match the
root-selected centroid for that cluster, including when an unchanged posting
pack is reused in a later epoch. The root authenticates the catalog, the
catalog authenticates each block, and the sketch binds to those block digests.
Legacy `GLSKT001/002` packs remain readable.
Range reads still authenticate each block against its committed digest.
The M31 visible payload of 262.8 MiB implies roughly 270 KiB for 1,024 rows
before layout differences, so a consolidated cluster should often cost one
range and leave room for about three such clusters inside 1 MiB. This is an
estimate to verify against the actual new packs, not a quality assumption.

Start with **one primary posting per sealed live version**. Boundary duplication is
an optional measured second step: assign a row to its closest center and, only
when the second center is within a calibrated distance ratio and the duplicate
budget allows it, to that neighboring cluster too. Limit extra live copies to
5% of rows and at most one secondary copy per row; record primary/secondary in
the catalog/sketch format. Both copies carry the same `(ID, sequence)` and
complete vector and metadata. At query time the tail wins; otherwise compare
each candidate's sequence with the latest-ID directory and accept only that
version, then deduplicate by ID before the exact `(distance, ID)` top-k heap.
Deletes and overwrites clear **all** older copies through the same directory
comparison. Reassignment publishes replacement copies before removing old
ones. Duplication is disabled if it fails the memory, upload, or quality gates.

For 128 dimensions, current five-bit codes cost 80 B/physical row, narrow IDs
4 B, liveness about 0.125 B, and four declared routed keys up to 4 B. A new
u64 sequence adds 8 B/physical row so derived copies can be tested against
the latest-ID directory without relying on canonical block location. The
resident 1% predicate adds about 5.12 B/live row of vectors, plus indexes and
per-pack codebooks. The latest-ID directory adds 24 B/logical ID. Thus the
first-order RAM charge is about 97 B/physical posting row plus 24 B/logical
row, with codebook, catalog, allocator and tail overhead measured separately.
At 1M rows, the sequence and 5% duplication add about 13 MB of sketches to
the measured 115.1 MB base against a 128 MiB sketch limit. Keep exactly one
active routing sketch per primary physical row: canonical source rows replaced
by derived postings are not also retained as active routing codes. Centroids
at 1,024 x 128 x 4 B consume 0.5 MiB. Do not assume the 192 MiB RSS gate from
these estimates; measure peak during catalog switches and merges.

## Centroid training and count

Stage 2 measurements set the profile: about 4,000 live primary rows per
cluster, `2^round(log2(rows / 4,000))` centers (64 at 250k, 256 at 1M),
capped by the sample and 4,096 (an explicit count may be given); no boundary
duplication; two Lloyd iterations on a 16,384-row sample. Train from a
bounded, deterministic sample of at most 16,384 live IDs selected by a seeded
hash priority while streaming authenticated canonical rows; record seed,
sample rule, row count and training iterations in the object. Reuse `src/ivf.rs`
metric routing, seeded deterministic tie behavior, final-center assignment,
means for squared Euclidean/cosine and coordinate medians for Manhattan, but
extract a bounded trainer: `build_ivf` currently gathers all resident points
and builds full ID postings, so calling it on the segmented namespace would
violate the memory target. For cosine, train on the engine's stored normalized vectors and validate centers
for routing. Empty clusters retain centers but have no extents. Training cost
is a bounded offline maintenance cost; it must not run in a foreground PUT.

## Write and maintenance path

An acknowledged request still publishes **one immutable log object** (or one
group log for a group) and updates the in-memory tail only after that create
succeeds. No centroid, catalog or posting PUT participates in its
acknowledgement. Newer tail writes shadow every older posting copy immediately
through the directory/tail liveness rule. Failed log creation retains the
previous acknowledged read view and requires reopen, as today.

At seal, freeze the log boundary and retry state exactly as the current
`SealState` does. Assign each frozen put to the epoch's closest center, with
deterministic `(score, cluster ID)` ties; deletes remain ID-sorted canonical
blocks and have no vector posting. Materialize one <=1 MiB pack per step,
grouping cluster fragments contiguously and splitting at 170 rows or the
120 KiB raw block limit. Preserve displaced frozen tail versions while later
acknowledged writes arrive. Stage packs and the ordinary ID run index, then a
new catalog with these fragments, then root v4 at the seal boundary. Only the
root PUT makes the seal and its cluster fragments visible; later logs replay
over it. A missing fragment prevents clustered serving; the canonical run and
logs still define exact results.

Independent seals create small cluster fragments. Use a per-cluster,
size-tiered posting merge: when a cluster has more than two extents at a tier,
stream authenticated live rows from those extents and write larger contiguous
blocks, keeping immutable old extents reachable until publication. Prefer
merging a base extent with accumulated delta only when the measured query
range count or dead-byte threshold requires it; rewriting a ~1,000-row base
for every three-row seal fragment would exceed write gates. A unit reads at
most one old <=1 MiB pack at a time and emits at most one <=1 MiB new pack;
stage up to four clusters, one catalog and one root per publication. Its
decoded-row bound is 12 x 170 = 2,040 records per output pack; a single large
cluster is split across units. Planner memory, old/new sketches, catalog clone
and in-flight buffers count toward the M31 192 MiB RSS gate. If the estimate
would exceed it, reduce the unit or defer maintenance; the existing 64-log
hard tail bound still forces a seal, never an unbounded merge. Measure the
steady-state extent count and merge write amplification under the 300 s wave.
Target one base plus at most three delta extents for a cluster after maintenance
catches up; measure the backlog during offered writes. The query cap still
applies if it has more extents. Admission cannot force unbounded rewriting to
maintain this target: sustained failure to keep it within the M31 write gates
is a failed design experiment, not permission to exceed eight GETs.

For **merge, split, reassign and centroid change**, use the same staged rule:
freeze a selected root/catalog and the directory's live `(ID, sequence)` set;
read and authenticate bounded source extents; stage replacement packs/sketches;
stage a complete catalog; publish one next-generation root; then rebind
liveness and schedule obsolete keys. New log writes may continue to shadow
frozen versions, but another root-changing maintenance plan waits. A merge
only changes physical locality. A split trains two local centers from one
overfull cluster, gives them new stable IDs in a new centroid epoch, and
reassigns that cluster's rows; a small/mostly dead cluster may merge into a
neighbor. A LIRE-style local reassign moves boundary rows between adjacent
postings after sampling their exact distances; it is triggered by measured
imbalance or recall loss, not every update. No in-place edit or POSIX rename
is used. An epoch change requires a catalog whose **every** extent is labeled
for the new epoch; unaffected clusters can reuse old packs only if their
cluster IDs and centroid coordinates retain the same meaning. Otherwise build
the new epoch's complete postings in bounded units, keep serving the old
epoch, and atomically switch both pointers in one root. This may require
temporary old/new payload coexistence and is refused before exceeding the
1 GiB visible-payload gate.

| Operation | Staged replacement before the root | State selected after the root |
|---|---|---|
| Posting merge | Live extents to <=1 MiB output pack, new catalog | Same center and canonical versions; fewer ranges |
| Cluster split/merge | New centroid object, changed clusters' packs, complete catalog | New epoch; unchanged-center packs may be reused by fingerprint |
| Boundary reassign | Moved rows in replacement packs, new catalog | Same epoch; one primary per live version |
| Center retrain/count change | Bounded full-view build, centroid object, catalog | New epoch selected atomically; old view eligible for cleanup |

Publication success acknowledges only index maintenance, not a mutation.
Before the root create, the prior root, catalog and packs are authoritative for
serving; staged objects are orphans. After it, the new root selects the new
view even if the response was lost. Any uncertain create or panic poisons
writes and maintenance on that handle; reopen selects the highest complete
root, validates its references and replays contiguous newer logs. A selected
root with missing/corrupt canonical data fails recovery; missing/corrupt
derived centroid, catalog or posting data fails **clustered serving** and
requires explicit repair from canonical runs, without silently serving lower
recall under the same policy. Invalid sketch frames may be rebuilt from
authenticated posting blocks as today. Cleanup derives reachability from the
selected root's canonical runs, centroid, catalog and posting packs, plus the
log tail and root zero; it removes obsolete/orphan objects in bounded batches
only after publication. Removal errors poison the handle; unique attempt keys
are never reused, so late DELETEs cannot erase a later generation. Prune,
canonical reclaim and run consolidation must update or preserve the catalog
in the **same** new root generation; none may reclaim a canonical pack still
referenced as a posting source. Backup copies and validates both canonical
objects and the selected clustered view, then publishes metadata last.

## Query path and filters

Score every resident centroid with the configured metric, order by
`(routing score, cluster ID)`, and probe the 16 nearest clusters (a database
setting; stage 2 and 3 measured 1-32). Every block of a probed posting with a
current row is a candidate, ranked with the per-seal candidates of versions
sealed after the view. Rank live blocks using their five-bit
codes and existing nonnegative-prefix pruning. Choose byte-contiguous spans
in score order, coalescing adjacent extents in one pack, with **both** <=8 remote range GETs and <=1 MiB
remote payload. Charge the logical cold plan independently of cache hits for
the cold gate; separately report actual remote calls when M35 serves hits from
SSD. Stop rather than exceed either cap, and report probes, selected blocks,
bytes, ranges and whether a posting was skipped. Full-precision rerank reads
authenticated blocks, checks latest `(ID, sequence)` and the full predicate,
deduplicates IDs, includes live tail rows, and sorts by exact `(distance, ID)`.
This remains ANN; probing all centroids under an insufficient byte cap is not
an exact guarantee. `search_exact` continues to scan canonical runs and tail.

The M35 SSD namespace cache may warm posting and canonical blocks and make a
warm query read more blocks locally under its separate policy. Cache bytes
are disposable, keyed by immutable object/range/digest and checked against the
pinned root. Cold-query quality is evaluated with the cache empty and is never
credited with free remote reads. A query whose conjunction is exactly the
declared M30 resident predicate still uses its full-precision RAM posting and
tail and remains exact with zero block GETs. For declared routed keys, sketches
keep the current per-row value codes (including overflow 255), restrict block
routing conservatively, and the exact metadata predicate is checked on rerank.
Unrouted predicates remain post-filtered and have no promised M31 quality until
measured against the filtered exact oracle; no silent exact fallback spends an
unbounded remote budget. M38 can add metadata indexes only with a separate
format, memory and query-quality decision.

## Compatibility and conversion

Existing metadata v2/v3/v4 and root v1 namespaces open on the present per-seal
path without rewriting their data. New code must not infer a clustered view
from a stray centroid or catalog object. Conversion is explicit and offline
under exclusive ownership with writes quiesced: select and validate the
current canonical root plus tail, train the bounded sample, stream its current
live rows into clustered packs, stage centroid and catalog, then publish root
v4 pointing to the complete view. Retain the existing canonical runs and
log tail; the selected root sequence and retry state do not change.
Interrupted conversion leaves root v1 selected and its serving behavior intact;
on reopen, cleanup removes staged orphans or a new attempt uses new keys.
Stage 3 freezes the selected root and directory rather than quiescing
writes: root-changing maintenance waits, while acknowledged tail writes may
continue and shadow converted rows by the directory/tail rule. The view
covers every live version sealed at or below the source root sequence (its
boundary). Stage 3 binaries sealed later writes in the per-seal layout and
routed them through canonical sketches beside the postings; stage 4 seals
assign them to clusters instead (below), and opening derives which live
versions a posting covers, so a stage 3 namespace keeps routing its
uncovered per-seal versions canonically until a new epoch. Converting again
builds the next epoch, which is also the explicit repair of an unavailable
view.

Stage 4 and 5 decisions. A clustered seal's put packs are the canonical
packs of its run and `Canonical` catalog extents at once, so a seal uploads
its rows once; the seal adds one catalog create before its root. A merge
writes `Derived` copies and leaves canonical packs in their runs, so merged
seal rows are stored twice until reclamation or pruning frees the canonical
copy. A merge round may stage up to 32 output packs (each at most 12
blocks and 2,040 rows, several clusters per pack) under one catalog and one
root, rather than four clusters per publication: each root create uploads
every block reference (about 2.3 MB at 1,000,000 rows), so publications,
not packs, dominated upload in the wave replay. An extent is small below
three full blocks (510 current rows); a cluster with more than three small
extents is due. A six-block threshold measured 0.9615 / 0.8 recall after
the 1,000,000-row wave but 5.81 PUT/s and 2.00 MiB/s upload; three blocks
measured 0.9585 / 0.8 at 4.90 PUT/s and 1.68 MiB/s. A catalog may list
only some blocks of a posting pack; the others are not routed. Coverage is
derived at open from the posting rows themselves (no persisted boundary
field), which keeps catalog v1 and root v4 unchanged.
During offline conversion, stage new sketches as bytes without retaining a
second full resident set; release v1 routing sketches before loading the v4
view. Query views keep the state they hold across the switch. After
successful root publication the handle loads the view as an open does. There is no mixed partial index
visible to queries. An unconverted namespace can remain v1 indefinitely;
changing metadata or silently starting
a full rebuild on open is not required. Restore into an empty prefix carries
the selected version, catalog and epoch and validates them before promotion.

## Alternatives and risks

- **Keep per-seal packs and increase budgets:** the M31 eight-block ceiling is
  below the mean recall gate; 10-12 requests help on the measured corpus but
  break the fixed remote gate and still scale with independent seals.
- **Per-segment centroid indexes:** they avoid cross-seal rewrite, but every
  query must probe an increasing number of segment-local lists. Their IDs and
  training are segment-scoped, so merges require rebuilding or complex ID
  translation. They do not solve the measured cross-pack fan-out.
- **HNSW or DiskANN-style graphs:** neighbor edges and entry points need an
  update/recovery protocol and may cause random remote reads. An SSD-resident
  graph is a plausible future alternative, but first needs a measured memory,
  cache-loss and cold-range budget advantage over contiguous postings.

Risks are centroid imbalance, too many delta extents during updates, boundary
misses, duplicate/sketch RAM growth, catalog/root PUT amplification, expensive
epoch rebuilds, and serving corruption when a derived object is missing.
The proposal deliberately leaves the probe count, split thresholds, duplication
ratio and merge tiers as benchmark-selected parameters. If no setting passes
both quality and cost gates, the design is rejected or revised; M35 warm-cache
quality must not conceal a cold-layout failure.

## Evaluation and reviewable implementation stages

Use the unchanged M31 SIFT1M base/query files, digest, 200 queries, 128-d
squared Euclidean, k=10, exact f64 oracle with ID ties, 100-row loads,
four-writer/four-reader 300 s wave, MinIO backend and M31 cache/RAM limits.
Record seed, selected centroids, cluster occupancy, duplication fraction,
extent count per cluster, range/byte histograms, PUT/DELETE/upload rates,
visible payload, charged sketch/catalog bytes, peak RSS, open time and p95
latency. At 250k and 1M, measure static and update-wave mean/p5 recall@10 and
short results for budgets 1/2/4/8/10/12 requests at byte caps 256 KiB,
512 KiB and 1 MiB, with exact top-10 and an oracle best-eight-postings ceiling.
Run cache-empty cold and SSD-warm cases separately. Reuse all M31 gates:
<=192 MiB peak engine RSS, <=2 s open/reopen, <=50 ms warm and <=200 ms cold
query p95, <=150 ms write p95, <=8 GET and <=1 MiB per cold query,
<=256 MiB NVMe, <=1 GiB visible payload, <=6 PUT/s, <=6 DELETE/s,
<=2 MiB/s upload, <=1 LIST/minute, and zero lost acknowledged writes,
overloads, late slots or maintenance errors. The 250k comparison also uses
M21's tighter 64 MiB RSS and 1 s open gates when evaluating that envelope.

1. Add format decoders/validators and tests for root v4, centroid v1 and
   catalog v1; keep v1 opens and exact results unchanged. Include malformed
   length/digest/epoch/extent and old-reader rejection tests.
2. Add deterministic bounded training and an offline clustering probe only;
   produce occupancy and recall-versus-budget curves before enabling writes.
3. (Done.) Add explicit v1-to-v4 conversion and clustered read-only queries. Test
   crashes after each staged pack, centroid, catalog and root create, plus
   restart, cache loss, missing/corrupt derived objects and exact equality.
4. (Done.) Add clustered seal-time assignment and root/catalog publication. Test
   overwrite/delete shadowing, displaced tail versions, group retries,
   uncertain log/root PUTs and replay at every seal step.
5. (Done.) Add one bounded physical posting-merge unit and make consolidation, prune,
   reclaim, cleanup and backup account for catalog reachability. Test every
   publication and deletion crash point, delayed DELETE and restore.
6. Add local split/reassign and whole-epoch replacement only if occupancy or
   update-wave evidence requires them; test mid-build restart and atomic
   epoch switch. Trial boundary duplication separately and retain it only if
   it improves recall within RSS and write gates.
7. Run the full M31 acceptance and 250k comparison on a clean revision;
   record raw results and update `DESIGN.md` with the final accepted formats
   and invariants when implementation establishes them.
