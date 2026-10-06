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

After the final write responses, the live service prints/flushed its complete
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
