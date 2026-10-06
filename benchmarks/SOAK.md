# Single-node sustained mutation soak

The optional M24 soak profile tests repeated mutation of a fixed ID domain,
maintenance progress and crash recovery separately from the historical 1M
resource envelope. It changes benchmark code only; server defaults and
durability, persisted formats and acceptance gates are unchanged.

## Protocol

Use the hash-verified 1,000,000-row SIFT1M corpus at 128 dimensions with the
clustered M31 profile, 192 MiB index admission watermark, 256 MiB NVMe cache and
no RAM block cache. Four writers offer one 100-operation batch per second;
four readers offer 40 queries per second, half ANN and half the exact resident
predicate. Each writer revisits 25,000 IDs in its own quarter every 250 seconds.
Every batch deletes exactly ten IDs and puts the other ninety. The deleted
cohort rotates by 17 positions each revisit so later rounds reinsert deleted
IDs. Put generation `g` uses SIFT row `(id + 137*g) % 1000000`, with the
original deterministic metadata. There are no newly allocated IDs.

Thirty minutes offers 7,200 batches / 720,000 mutations and 72,000 queries;
rejected or late slots are recorded, never silently retried. The verification
oracle includes only acknowledged batches in writer/round order; disjoint writer
quarters make that order independent of cross-writer commit scheduling.

The sampler records lifetime high-water RSS and current `ps` RSS once per
second. Every ten seconds it submits a metrics command, recording sequence,
tail objects, runs, index/admission charge, maintenance counters, queue charges
and maintenance errors to `progress.jsonl`. An overloaded probe is recorded
separately, not retried or mistaken for a workload rejection. This probe adds
committer work and may publish a view, so these latencies are an instrumented
soak, not a directly comparable uninstrumented speed benchmark.

Memory includes the serving engine and native benchmark process, not MinIO or
the Python orchestrator. The native runner retains timing events and idle samples;
`retained_event_storage_floor_bytes` measures only the live event records,
excluding spare vector capacity, acknowledgements, transport buffers and
allocator retention. Neither this floor nor the 192 MiB index watermark is
total RSS. Compare five-minute windows and the late-run trend, and report this
harness overhead before attributing growth to the engine. A 30-minute plateau
does not prove indefinite bounded memory.

After the final write responses, the live service writes and flushes its complete
acknowledgement oracle, then sends itself SIGKILL without draining or running
destructors. Maintenance may be in flight. The driver requires that exact
signal termination, removes the complete block/routing cache directory, then
opens in a fresh process and checks every live vector/metadata, expected deleted
absence via membership and count, and exact-query quality. It additionally
checks a second cache-loss open and backup/restore through the existing M24
checks. This complements smaller uncertain-in-flight-write crash matrices.

The historical memory, open, latency, quality and request gates still appear in
`run.json`; `accepted=false` must remain visible even if logical correctness and
observed stabilization pass. Distinguish missed query arrival slots, service
overload and pre-publication index-capacity rejections.

## Reproduce

Run on an otherwise idle host, keeping it awake for the driver lifetime. The
runner owns its disposable loopback MinIO container and temporary cache and
cleans both after failure or success. AWS credentials are replaced with unique
local MinIO credentials; this command provisions no AWS resources.

```sh
caffeinate -i python3 tools/m24_acceptance.py target/soak-new \
  --data target/hardening-data --rows 1000000 --rounds 1800 --clustered \
  --index-bytes 201326592 --hot-rows 100000 --delete-percent 10 \
  --soak-metrics --crash-after-serve
```

On hosts without `caffeinate`, prevent host/VM sleep using the host's normal
power controls. Use a fresh output directory. Dataset/oracle and source hashes,
Git revision, compiler, hardware, the full progress trace and all gates are
recorded with the result.

Summarize five-minute trace windows and the descriptive current-RSS trend:

```sh
python3 tools/soak_summary.py target/soak-new/run.json
```

The trend is ordinary least squares of current MiB against elapsed minutes,
using ten-second observations at or after minute 10. Missing probes stay unknown,
window boundaries are half-open, and the summary preserves every failed historical
gate. It introduces no new stability threshold or longer-duration extrapolation.

## Measured 30-minute result

[Raw report](soak-2026-10-07/minio-1m-30min.json) and
[window summary](soak-2026-10-07/summary.json): Apple M4, macOS 26.6.2,
rustc 1.98.1, pinned loopback MinIO, measured revision `300a64a`. Production
sources match the merged hardening stack; later commits add only summary tooling
and CI coverage. Normal foreground applications were present; no CPU pinning or
exclusive-host control was used. This is sustained correctness/maintenance
stress, not an isolated paired speed comparison.

| Measurement/check | Result |
|---|---:|
| Mixed traffic duration | 1,799.93 s |
| Offered / acknowledged 100-op batches | 7,200 / 7,198 |
| Offered / completed queries | 72,000 / 72,000 |
| Index-capacity rejections | 0 |
| Workload queue overload, writes / queries | 2 / 0 |
| Skipped arrival slots, writes / queries | 0 / 0 |
| Acknowledged write p95 / p99 | 29.92 / 41.55 ms |
| Warm unfiltered query p95 / p99 | 36.82 / 41.42 ms |
| Cold unfiltered query p95 | 34.60 ms |
| Filtered query p95 | 15.31 ms |
| Native serving/benchmark peak RSS | 236.09 MiB |
| Live timing-event storage floor | 4.12 MiB |
| Sampled maximum tail objects / runs | 33 / 22 |
| Sampled maximum index admission charge | 138.21 MiB |
| Seal steps by last observation | 2,830 |
| Maintenance errors | 0 |
| Initial serving open / crash-and-cache-loss reopen | 1.78 / 2.04 s |
| Second cache-loss open | 1.92 s |
| Expected / recovered live IDs | 990,000 / 990,000 |
| Expected deleted IDs | 10,000 |
| Full-state vector/metadata/unexpected-live mismatches | 0 |
| Cache-loss query results / backup restored view equal | yes / yes |
| Post-update unfiltered mean / p5 recall@10 | 0.971 / 0.80 |
| Filtered recall / short results | 1.0 / 0 |
| Historical acceptance | **false** |

| Traffic minutes | Current RSS median (MiB) | Sampled max tail objects |
|---|---:|---:|
| 0–5 | 213.56 | 31 |
| 5–10 | 228.57 | 31 |
| 10–15 | 225.98 | 32 |
| 15–20 | 205.64 | 33 |
| 20–25 | 202.13 | 32 |
| 25–30 | 205.70 | 32 |

The recorded high-water mark reaches 236.09 MiB before minute 10 and stays
there through the remaining traffic. Current-RSS OLS after minute 10 is
-0.82 MiB/minute over 119 observations. This window shows no positive memory
trend or continuously accumulating log tail; it does not establish an indefinite
bound, cover growing ID cardinality, automatic conversion/reclustering, or
many concurrently open collections. The absolute 192 MiB RSS target still fails.

The sampler is a ninth producer sharing the eight-command admission limit with
four writers and four readers. One metrics probe and two workload writes are
overloaded; each is retained in the report, not retried or removed from gates.
Given the eight workload producers each wait for their preceding ticket and
admission releases its charge before response delivery, probe contention is a
plausible source of these overloads; rejection times were not logged, so this is
an inference rather than a measured attribution. Do not use these counts as an
uninstrumented engine rejection rate. A future latency/admission comparison
should observe without adding competing admission commands.

Three historical gates fail: peak RSS, zero overload, and fresh open/reopen
(the post-crash reopen is 2,043 ms against 2,000 ms). All other recorded gates
pass, including capacity, arrival completeness, latency, quality, durability,
cache, backup and physical request limits. The default server watermark remains
128 MiB; 192 MiB is this collection's measured configuration, not a global RAM
bound or a new default. No AWS resources were provisioned. The disposable MinIO
container and temporary cache were removed successfully.

Validation of the runner: three native example tests, 70 root Python tests,
release/server all-target Clippy with warnings denied, formatting, generated
summary and documentation-link checks. A 4,000-row clustered smoke checks
12 acknowledged mixed batches and 40 deleted IDs through SIGKILL/cache loss
and backup restore. CI checks, audit, MinIO (including the three native
regressions), recovery drills and Docker passed on `cb1c59e`.
