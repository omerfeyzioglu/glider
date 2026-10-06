# Heavy single-node hardening validation

The HTTP crash soak and SIFT acceptance runs exercise the hardening stack after
[the initial measurements](HARDENING.md). The new pressure scheduling regression
keeps a successor queued at every write boundary: it fails before the fix and
admits a put after bounded sealing progress. A separate check preserves retained
retry outcomes without scheduling pressure maintenance. The final production
code at `b34a735` passes 297 release/server Rust tests (35 ignored), 32 MinIO
checks in eight suites, release/server all-target Clippy and formatting. Root
Python tests: 66 passed; client: 81 passed, 17 integration checks skipped.
Production-stack CI additionally passed checks, audit, MinIO, drills and Docker.

## 100,000 rows at 768 dimensions

[Raw result](heavy-hardening-2026-10-06/http-100k-768.json), runner `687250c`:
a single HTTP collection on disposable loopback MinIO, 100-row batches, 128 MiB
index watermark, 256 MiB NVMe cache, no RAM block cache, automatic conversion
and local extra candidate blocks disabled. Synthetic integer f32 vectors in
[-125, 125], Python `random.Random`, per-row seed
`42 + id * 0x9e3779b1 + generation * 0xd1b54a32`. Complete stored vectors,
metadata, deleted-ID absence, total count and ordered IDs are compared with
acknowledged state. Query hits must have the vector their metadata generation
identifies and the exact squared distance to the submitted query.

| Check or measurement | Result |
|---|---:|
| Ingest batches acknowledged | 1,000 |
| Mixed traffic duration | 120.14 s |
| Mixed upsert/delete batches acknowledged | 1,673 |
| Concurrent queries completed | 4,145 |
| HTTP 429 in mixed traffic | 0 |
| SIGKILL followed by complete cache removal | 2 |
| IDs checked after first / second restart | 50,000 / 100,000 |
| Final live IDs (after deletes) | 92,014 |
| Lost acknowledged values / metadata / absence mismatches | 0 |
| Restart to healthy, first / second | 1.66 / 2.00 s |
| Exact top-10 query results preserved through final restart | 5 / 5 |
| Mixed 100-op write p95, per writer | 170.01 / 168.19 ms |
| Mixed query p95, per reader | 90.61 / 89.74 ms |
| Sampled server RSS peak | 159.13 MiB |

Two writers use disjoint ID halves and randomly upsert or delete 100 unique IDs
per batch; two readers query continuously. Both SIGKILLs happen after preceding
write responses have arrived; maintenance may be in flight. Smaller existing
crash matrices cover uncertain in-flight writes. Every acknowledged vector in the first 50,000 rows is
checked after ingest restart, and every possible ID is checked after mixed
traffic restart. The final exact-query check uses the exhaustive public engine
path; its implementation is independently covered by repository oracle tests.

Latency includes Python input encoding, HTTP and response decoding; client
vector generation is outside individual timers. Load wall time includes the
mid-load crash/restart/full verification. RSS is sampled once per second using
`ps`, excludes the client and MinIO, and can miss short peaks. Local compilation
and MinIO regression checks overlapped this correctness stress run; these
latencies are not an isolated before/after performance comparison. Five uniform
synthetic ANN queries have recall@10 0.7/0.8/0.8/0.8/0.9; that sample does not
establish the SIFT quality envelope. No ANN algorithm changed.

Reproduce with a new output directory (the runner owns the MinIO container,
server and temporary cache, clears inherited AWS/Glider settings, and removes
its container and cache even after failure):

```sh
cargo build --release --locked --features server --bin glider-server
python3 tools/hardening_soak.py --output target/heavy-soak-new \
  --rows 100000 --dimensions 768 --seconds 120
```

The SIFT acceptance runner now counts capacity rejections separately and checks
only acknowledged batches on restart; its zero-capacity-rejection gate still
fails when such responses occur. An explicit `--index-bytes` records a changed
watermark without changing the historical memory, quality or latency gates.

## 1,000,000 SIFT rows: default 128 MiB watermark

The clustered M31 profile uses 128-dimensional SIFT1M, squared Euclidean
distance, 256 centroids, 32 probes, eight scoring threads, a 256 MiB NVMe cache
and no RAM block cache. Four writers offer one 100-operation batch each second;
40 queries per second alternate unfiltered ANN and the exact resident predicate
`cohort=one-percent`. Writes rotate the vectors of known IDs by 137 positions.
The raw files record the oracle hash, revision `687250c`, compiler, configuration
and latency samples. The local runner also records input hashes; the AWS bootstrap
checks the same dataset hashes before running. Source fingerprints in the local
report identify production code despite unrelated untracked files.

| Measurement | [MinIO / Apple M4](heavy-hardening-2026-10-06/minio-1m-index128.json) | [S3 / c7g.xlarge](heavy-hardening-2026-10-06/aws-1m-index128.json) |
|---|---:|---:|
| Mixed traffic window | 300 s | 120 s |
| Offered / acknowledged write batches | 1,200 / 1,165 | 480 / 455 |
| Capacity-rejected batches | 35 (2.92%) | 25 (5.21%) |
| Queries completed | 12,000 | 4,800 |
| Acknowledged write p95 | 32.74 ms | 136.11 ms |
| Warm unfiltered query p95 | 36.85 ms | 47.24 ms |
| Cold unfiltered query p95 | 37.19 ms | 75.26 ms |
| Filtered query p95 | 15.12 ms | 15.25 ms |
| Serving peak engine RSS | 230.38 MiB | 227.67 MiB |
| Initial serving open | 1.72 s | 3.29 s |
| Post-traffic reopen | 2.57 s | 3.51 s |
| Open after cache removal | 1.94 s | 3.37 s |
| Full-state value mismatches | 0 / 1,000,000 IDs | 0 / 1,000,000 IDs |
| Cache-loss results / backup restore equal | yes / yes | yes / yes |
| Post-update ANN mean / p5 recall@10 (100 queries) | 0.963 / 0.80 | 0.980 / 0.90 |
| Filtered recall / short results | 1.0 / 0 | 1.0 / 0 |
| Maintenance errors / overload / skipped arrival slots | 0 / 0 / 0 | 0 / 0 / 0 |
| Acceptance | **false** | **false** |

Both runs fail the unchanged 192 MiB serving RSS and 2 s open/reopen gates,
and the new explicit zero-capacity-rejection gate. Every other recorded gate
passes, including durability, quality, latency and object-request limits.
Write latency percentiles cover acknowledged batches, not rejected responses;
rejection counts must therefore accompany latency claims. A typed capacity
rejection is a pre-publication decision, not a lost acknowledged write, but it
still misses the offered-load acceptance target. The pressure progress fix
prevents permanent queue starvation; it does not promise that every put fits.

RSS is the serving process high-water mark, including setup. It excludes MinIO,
clients and the separate loader/converter/verification processes. The 128 MiB
watermark counts routing sketches plus conservative pending-put reservations;
it is not a total RSS limit. Raising it cannot by itself satisfy a 192 MiB RSS
target. Existing [M39 S3 measurements](M39.md) also miss memory/open gates, but
use a different instance and duration, so they do not establish a before/after
speedup or regression. The initial paired 10k measurements remain the limited
evidence for normal-path overhead.

The AWS run used one c7g.xlarge (four Graviton3 vCPUs, 8 GiB RAM) in
eu-central-1 against S3 Standard, with a 30 GiB gp3 root disk configured for
deletion on termination and a 45-minute hard shutdown cap. The bootstrap builds
the pinned revision, and its exit trap cleans its run prefixes and shuts down;
the parent also explicitly terminates and waits, downloads results, and removes
the isolated dataset/result prefix. [The final independent check](heavy-hardening-2026-10-06/aws-cleanup.json)
confirms the instance is terminated and the entire owned S3 prefix is empty.
The identity cannot list EBS volumes, so disk deletion is recorded as configured,
not independently verified. The account uses a paid plan with credits; the
reported balance was $115.04 before and after. Billing can lag: neither the
unchanged balance nor the runner's hypothetical 30-day S3 cost model measures
this run's actual bill.

Reproduce locally with fresh output/cache/container ownership:

```sh
python3 tools/m24_acceptance.py target/heavy-sift-new \
  --data target/hardening-data --rows 1000000 --rounds 300 --clustered
```

For AWS, first upload the hash-verified SIFT files into `datasets/` beneath a
fresh permitted prefix. Keep that prefix isolated from existing data:

```sh
AWS_PROFILE=glider-test python3 tools/aws_acceptance.py target/heavy-s3-new \
  --revision 687250c96364ea09b5403a318f073207ff5c62c6 \
  --prefix glider-pilot/REPLACE-WITH-FRESH-OWNED-PREFIX \
  --rows 1000000 --rounds 120 --clustered \
  --instance-type c7g.xlarge --max-minutes 45 --cleanup-datasets
```

The drivers can exit successfully after collecting a report whose `accepted`
field is false; inspect that field and every gate. Earlier abort-on-capacity
attempts did not complete acceptance and are not successful measurements. A
192 MiB comparison interrupted by Docker clock skew has no valid report;
the rerun keeps the host awake without changing engine retries or acceptance gates.

## 192 MiB admission comparison and merge decision

[The completed MinIO rerun](heavy-hardening-2026-10-06/minio-1m-index192.json)
uses the same revision, dataset, clustered profile and 300-second workload with
`--index-bytes 201326592`. `caffeinate -i` prevents host sleep for the driver's
lifetime; no persistent system setting or engine retry policy changes.

| Measurement | 192 MiB watermark |
|---|---:|
| Offered / acknowledged write batches | 1,200 / 1,200 |
| Capacity rejections | 0 |
| Queries offered / completed | 12,000 / 11,977 |
| Query arrival slots skipped / write slots skipped | 23 / 0 |
| Acknowledged write p95 | 30.94 ms |
| Warm / cold unfiltered query p95 | 36.93 / 31.42 ms |
| Serving peak engine RSS | 234.92 MiB |
| Initial open / reopen / cache-loss open | 2.20 / 2.45 / 2.04 s |
| Full-state value mismatches | 0 / 1,000,000 IDs |
| Cache-loss results / backup restore equal | yes / yes |
| Post-update ANN mean / p5 recall@10 | 0.962 / 0.80 |
| Acceptance | **false** |

Capacity rejections disappear with more reservation headroom in this run, but
RSS remains above the original 192 MiB gate. All acknowledged state is preserved.
The open/reopen and zero-late-slots gates fail as well: the query producer skips
23 arrivals when it is over one interval behind; these are not overload responses.
Do not omit these skips or turn this single changed-watermark run into a
statistically established performance improvement. The server default is unchanged.

The larger tests therefore support the correctness fixes, but do **not** pass
the complete million-row resource/arrival envelope. The correctness fixes and
validation artifacts have been merged separately from that unmet performance
target. The [sustained mutation soak](SOAK.md) measures longer-term serving
stability. Further single-node work should measure total allocations and cold-open reads, then reduce
directory/view/maintenance allocation peaks and recovery I/O. Simply raising the
index watermark masks neither the measured RSS nor the reopen target. The 23
query skips also require controlled repeat/profiling before attributing them to
the engine or changing arrival gates.
