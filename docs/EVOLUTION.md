# Engineering evolution

This is a selective evidence index for retrospectives. [DESIGN.md](../DESIGN.md)
defines current architecture and guarantees; raw files under
[`benchmarks/`](../benchmarks/) remain measurement sources of truth. Entries
describe observed results, not general guarantees, and omit routine changes.

## 2026-09-13 — Recovery-safe LocalStore publication

**Commit/PR:** [`f95be69`](https://github.com/omerfeyzioglu/glider/commit/f95be692829d17a21c9358498d55db0ed8f03b5a), [PR #1](https://github.com/omerfeyzioglu/glider/pull/1).
The local body-plus-seal protocol already represented complete immutable objects,
but a failed or panicked publication could leave a handle whose durability state
was uncertain. The fix poisoned that handle before filesystem mutation, rejected
further operations, and made a fresh `LocalStore::open` validate and synchronize
visible objects before permitting new writes. Deterministic hooks cover body/seal
creation, partial writes, file syncs, directory syncs, restart, sequence reuse and
subsequent writes. This was a correctness fix, not a measured speedup. It remains
local-filesystem-specific and depends on honored file/directory sync; loss of an
unwitnessed tail seal remains indistinguishable from a never-published write.
[Code](../src/store.rs), [fault tests](../src/store/tests.rs), [process tests](../tests/local.rs).

## 2026-09-14 — S3-compatible object-store backend

**Commit/PR:** [`d71a5eb`](https://github.com/omerfeyzioglu/glider/commit/d71a5ebc1ac6c1c629ed330361dc7a8ca6f9e25d), [PR #6](https://github.com/omerfeyzioglu/glider/pull/6).
Previously only LocalStore exercised the object contract. The optional S3 backend
uses the SDK for signing, pagination and transport, publishes the existing
checksummed envelope with one conditional `PUT`, and relies on native complete
object visibility instead of local body/seal files. This kept POSIX synchronization
inside LocalStore and preserved engine-level acknowledgement and uncertain-write
rules. Request counters made remote GET/LIST/PUT and bytes observable. MinIO tests
cover pagination, response-loss ambiguity, late requests, corruption, process exit
and abrupt restart; they establish protocol behavior, not cloud-provider hardware
durability or comparative latency. The backend remains synchronous through a
private runtime and requires exclusive namespace ownership and strongly consistent
LIST/GET plus conditional creation. [Code](../src/store/s3.rs), [tests](../src/store/s3/tests.rs).

## 2026-09-15 — Immutable checkpoints plus tail replay (M3)

**Commit/PR:** [`0a1e02d`](https://github.com/omerfeyzioglu/glider/commit/0a1e02d9261aaa2ddb75ed445bd905eb4bc616b8), [PR #8](https://github.com/omerfeyzioglu/glider/pull/8).
One immutable object per mutation required full payload replay. An explicit,
versioned full-state checkpoint at sequence N lets recovery load that snapshot and
only newer mutations; that checkpoint plus tail defines recovered state, while
exact search behavior remains unchanged.
For 300 mutations over 30 live 32-D vectors, five warm reopens observed engine
GETs fall 301→32; p50 was 23.296→18.704 ms locally and 130.518→19.564 ms on MinIO.
Checkpoint creation was separate (16.553 ms local, 1.134 ms MinIO). These same-
executable runs recorded parent `47a2900`, a dirty tree, and source fingerprint
`a56c9c…`; they demonstrate reduced replay work, not a general speedup. M3 retained
history and added one object; LocalStore still validated/synced retained files.
[Results](../BENCHMARKS.md#checkpoint-recovery-m3), [raw local](../benchmarks/runs/2c457dc87323c82c4d5b779c2aca2b902f8d603267bd1eca05a64f6ddcf2caf2.json), [raw S3](../benchmarks/runs/edc3eed7d19266c18f95077c2ed9fd7c70f289eabc0054f83e05dbce8fc75596.json), [tests](../tests/segments.rs).
[Code](../src/lib.rs).

## 2026-09-15 — Explicit snapshot compaction (M4)

**Commit/PR:** [`94b7bf0`](https://github.com/omerfeyzioglu/glider/commit/94b7bf0109f5c1f2bc2404c24be5327ff747289a), [PR #12](https://github.com/omerfeyzioglu/glider/pull/12).
M3 reduced replay payloads but retained every mutation and checkpoint. Explicit
compaction now publishes a full-state reclamation root, then removes covered
history; interrupted cleanup is resumable and cannot authorize a partial snapshot.
With 300 mutations, 100 live 32-D vectors and three warm reopens, both backends
reduced 302 objects to two. One compaction wrote 36,413 logical bytes, removed 301
objects and added 0.283 logical write amplification; observed maintenance latency
was 3.537 s local and 150.213 ms MinIO. Reopen timings varied sharply—an earlier
uncompacted local report was 22.580 ms versus 870.677 ms in the matched layout
run—so object-count reduction, not latency, is the robust result. Reports used a
dirty tree at parent `2d2064a`, source `72936a…`; S3 versions/delete markers are
excluded. [Summary/raw links](../BENCHMARKS.md#compaction-m4), [tests](../tests/compaction.rs).
[Code](../src/lib.rs).

## 2026-09-15 — Exact-search characterization (M5)

**Commit/PR:** [`a44e2a8`](https://github.com/omerfeyzioglu/glider/commit/a44e2a8beafd50d26ea58b9d6293f96b0748357f), [PR #15](https://github.com/omerfeyzioglu/glider/pull/15).
Before selecting an ANN design, the unchanged exhaustive search path was measured
with reproducible warm synthetic squared-Euclidean workloads and an independent
exact oracle. On Apple M4/macOS, seed 42, k=10, 100 queries and five batches,
1,000×128-D p50 was 87.75–87.96 µs; 10,000×128-D was 551.38–562.67 µs. At
1,000×128-D, raising k to 100 left p50 near 87.88 µs, consistent with scoring and
full sorting all rows. Two invocations, uniform synthetic vectors, uncontrolled
desktop load and warm caches do not establish production tails or capacity.
Reports identify parent `6903472`, a dirty tree, and source `72936a…`; M5 changed
measurement coverage, not search behavior. [Method/results](../BENCHMARKS.md#search-characterization-m5), [1k raw](../benchmarks/runs/7aeb9adc7cdcd3fd24cf0001532858c9a8c5c9c223b0d9d72c61871a2731eedf.json), [10k raw](../benchmarks/runs/1720fc520c7f20c1627205d592465bfc79d24ce7653d08b4f1102ec76b24a2fd.json), [oracle](../tools/search_benchmark.py).

## 2026-09-15 — Partial selection for exact top-k

**Commit/PR:** [`8d2a6b6`](https://github.com/omerfeyzioglu/glider/commit/8d2a6b6242d1a538579dac10a8194999b2b141cd), [PR #16](https://github.com/omerfeyzioglu/glider/pull/16).
Profiling 10,000×128-D, k=10 recorded 4,169 main-thread samples: about 73% in
distance scoring and 25% at full sort. Exact search therefore replaced sorting all
N candidates with `select_nth_unstable_by(k)` plus sorting the selected prefix;
distance/ID ordering and exact results were preserved. Across two paired process
runs per case (1,000 rows, seed 42, 100 queries, five batches), p50 fell 12.3–18.0%
at 128-D/k=10, 2.7–2.8% at 768-D/k=10, and 14.4–15.7% at 128-D/k=900. The
768-D p99 worsened from 452.54–455.25 to 519.29–523.88 µs. Both phases record
dirty parent `98fecab`; source hashes `4b55ec…`/`c21440…` distinguish code. Memory
remains O(N), and every vector is still scored. [Evidence](../BENCHMARKS.md#native-search-profiling-and-exact-top-k), [code](../src/lib.rs), [oracle test](../tests/behavior.rs).

## 2026-09-16 — Rebuildable IVF-Flat alongside exact search

**Commit:** [`aad2006`](https://github.com/omerfeyzioglu/glider/commit/aad2006bdaaf10e3f8b9644ae9522419d35ec8e2).
IVF-Flat was added as an optional candidate path; it did not replace exact search.
Deterministic farthest-point initialization and eight refinement iterations build
16 full-precision partitions in memory; probing selected partitions reranks by
exact distance, while full probing equals the exact oracle. For 512×64-D,
squared Euclidean, k=10, seed 42, 24 queries and three warm batches, one probe on
uniform data observed 5.75–5.83 µs, 20.42% recall@10, 59.17 distances/query and
6.12–6.86 ms build time. Favorable clustered data observed 3.92–4.96 µs and 100%
recall@10, which is not a general accuracy guarantee or semantic relevance metric.
Full probing restored 100% recall but evaluated 528 distances and could be slower
than exact. Reports record dirty parent `8d2a6b`, source `9eebcc…`; RSS does not
isolate index memory. Writes invalidate the non-persistent index. [Results](../benchmarks/ANN.md), [code](../src/ivf.rs), [tests](../tests/ivf.rs).


## 2026-09-27 — Bounded durable retry decisions (M15)

Client retries previously could overwrite a newer document after a lost
acknowledgement. Requests now publish their payload identity, conditional decision
and mutations together. A 128-commit window bounds receipts and refuses expired
IDs; it avoids TTL clock assumptions and arbitrary UUID eviction ambiguity.
Bounded recent ID changes validate observations across delete/reinsert without
indefinite tombstones. Snapshot versions 4/5 preserve this authoritative state;
legacy inputs remain readable. Restore guarantees stop at the selected backup
boundary. The failure and process-exit tests in `tests/retry.rs` and the MinIO
retry/takeover test exercise this contract. See [DESIGN.md](../DESIGN.md) for
current semantics and alternatives.

## 2026-09-27 — One committer behind bounded admission (M16)

A FIFO limits outstanding commands and encoded bytes, including active work.
The worker owns all publication, maintenance and reads; client batches retain
M15 identity. Cancellation has an explicit queued/executing boundary, and worker
failure closes admission while leaving the claim for isolated recovery. This
adds concurrent callers without another publication participant or a read-view
protocol. The mutex comparison and conditional M17/M18 decisions are recorded
in [the M16 evidence](../benchmarks/M16.md).

## 2026-09-27 — Bounded parallel obsolete-object cleanup (M19b)

The SIFT capacity probe attributed a foreground stall to serial native DELETEs
after snapshot publication. Cleanup now allows four requests in flight, waits
for completion, and poisons the handle on any partial error. The selected root
and newer history remain untouched; recovery rebuilds counts and resumes cleanup.
No new publication participant, retry or persisted format is added. The targeted
2,000×128 probe passes with identical request counts/bytes; 5,000 rows expose the
next full-snapshot/queue boundary. [Evidence](../benchmarks/M19.md).

## 2026-09-27 — AWS timing diagnostic under unstable mobile connectivity

The bounded pilot recorded a snapshot body timeout and a multi-second synchronous
maintenance pause; cleanup verified all test prefixes empty. The operator confirmed
an unreliable mobile-data connection. Transport occupied over 99% of write/recovery
time while local queries stayed below 0.4 ms. This does not establish a provider or
internet-independent engine bottleneck, so it does not justify architectural
optimization or changing local M17/M18 decisions. Cloud performance conclusions
need a representative stable connection. [Evidence](../benchmarks/AWS_DIAGNOSTICS.md).

## 2026-09-29 — Staged physical reclamation for segmented packs

The experimental M22 layout originally coalesced ID indexes but retained
mixed live/stale physical packs. On 250,000 SIFT1M rows followed by 125,000
distinct overwrites, a synchronous repack saved 63.3 MB of visible payload but
took up to 135 ms in one call. Reclamation now fixes a pack's live-ID snapshot,
reads one block per step, then creates the replacement pack and root in separate
steps. Later acknowledged logs can shadow that snapshot; seal and index
consolidation wait for root publication. The same probe saved the same bytes
with a 38.2 ms maximum step, at 970 extra GET and 137.3 MB extra upload versus
no reclamation. Packs containing wholly dead blocks still need a separate
index-remapping path. [Evidence](../benchmarks/M22.md).

## 2026-09-29 — Prune fully dead segmented blocks

A fully shadowed block kept its pack ineligible for the mixed-pack repacker.
The experimental engine now reconstructs one bounded run index from the latest
ID directory, omits dead block references, and publishes index then root in
separate steps. A run with no live entries is removed; interrupted index/root
publication recovers through the selected root and orphan cleanup. On a
250,000-row prefix-overwrite probe, this cut visible payload by 86.3 MB for
18.6 MB extra upload and a 17.1 ms maximum planner or publication call. The
probe does not establish independent-load latency. [Evidence](../benchmarks/M22.md).

## 2026-09-29 — Disposable authenticated block cache

The experimental segmented reader now caches only root-authenticated immutable
block slices under byte-bounded RAM and NVMe budgets. A versioned identity
includes the object key, range, full payload length and committed digest;
missing or corrupt cache bytes refetch from the authoritative object store.
Writes and recovery never depend on cache files. On the 250,000-row exact
probe, a fresh process used 172.99 MB of NVMe cache and removed all 1,292
query GETs, but still needed 1,005 ms to scan and decode every block. This
measured limit directs the next work to selective retrieval rather than a
larger cache. [Evidence](../benchmarks/M23.md).

## 2026-09-29 — ID-sorted block layout cannot meet the selective-read target

On the 250,000-row SIFT1M root, an exact oracle given the best eight actual
blocks reaches only 0.855 unfiltered and 0.800 filtered mean recall@10 over
200 queries. This is a ceiling for the current one-block-per-GET read pattern,
not measured ANN recall or a general impossibility result for regrouped ranges.
It directs selective-serving work toward a measured vector-aware or regrouped
read layout while keeping root/log publication authoritative.
A following offline probe found that 170-row vector-local groups and a scanned
4-bit routing sketch can meet the static 200-query quality threshold, while
block centroids alone cannot. It does not establish encoded byte, update,
memory or latency acceptance. [Evidence](../benchmarks/M24.md).
