# Single-machine operations

## Supported envelope and scheduling

`serving::SingleMachine` is a serial library API over `OwnedDatabase`. One
process owns one bucket/prefix. The initial workload is 2,000 live documents,
64 dimensions, squared Euclidean distance, k=10, and `selected=true` on every
100th ID. Exact serving is selected by M12; `SearchMode::Approximate` is rejected.
No network listener, authentication layer or concurrent request queue is supplied.

Use `ServingOptions::m8()` on every open. It caps live rows at 2,000, serialized
vector plus metadata at 4,096 bytes/document, and each batch at 100 operations.
The measured workload submits full batches: 200 overwrites, 100 deletes, and
100 new IDs per cycle, keeping 2,000 live rows. Small batches are valid but do
not inherit this workload's maintenance write-amplification result.

The wrapper checks input/capacity before I/O and compacts when the M10 soft
limits are reached, before publishing the next batch. This introduces a visible
maintenance pause in that batch call. Batch latency includes it; it is not a
single-write latency claim. The serving budget is batch p95 <=100 ms including
maintenance and sustained throughput >=100 logical mutations/s. A successful
batch acknowledges all its mutations.
An error during scheduled maintenance occurs before that batch is published;
an error during batch publication may have committed the entire batch. Stop
writes whenever `status().recovery_required` is true.

## Status and limits

Record `status()` alongside process memory metrics:

- `maintenance.sequence`: last committed mutation object; each batch consumes one.
- `tail_objects`, `visible_objects`, `should_compact`, `writes_blocked`: M10
  recovery pressure. Counts exclude the owner root and live claim (two objects).
- `documents` / `max_documents`: live row capacity.
- `search_mode`: exact; no enabled derived ANN generation can be stale.
- `storage_errors`, `backup_errors`, `maintenance_runs`: per-handle counters;
  they reset after restart. `recovery_required` is the actionable error state.

Invalid input and capacity rejection publish nothing and keep the handle usable.
Do not raise limits to hide growing memory or missed maintenance: establish a
new measured envelope first. Recovery loads/validates authoritative data before
checking serving capacity, so the cap does not bound memory for an arbitrarily
oversized existing database. Process RSS and the backing service remain external
operating limits. These results cover loopback MinIO, not remote-cloud latency.

## Backup and restore

1. Quiesce application calls through the serial wrapper. Record the source
   prefix, configuration and `status().maintenance.sequence` in the application's
   backup inventory. Choose a fresh, empty, nonoverlapping backup prefix.
2. Call `service.backup_to(backup_store)`. It compacts, finishes cleanup and
   copies the committed root plus all referenced chunks, publishing metadata
   last. Success includes full destination recovery validation; the sequence
   does not change. Do not permit a writer on the completed backup prefix.
3. If backup fails, keep the destination unadvertised and do not reuse it.
   A destination-copy failure leaves the source usable. A source maintenance
   failure requires the crash procedure. No partial-copy retry is implicit.
4. To restore, call `stage_isolated_namespace(&backup_store, fresh_restore_store,
   config)`, then open `SingleMachine` on that new prefix. Verify the recorded
   sequence, known rows/metadata and exact results against application records
   before switching clients. Keep the previous namespace quarantined until
   verification and retention requirements permit its removal.

Graceful successful `close()` permits same-prefix restart. A poisoned serving
handle refuses close and preserves its claim. After a crash or uncertain write,
follow [RECOVERY.md](RECOVERY.md); a stopped process may still have requests in
flight, so use a fresh prefix. A claim is not a fence for those old requests.
Checksums and recovery detect corruption and missing referenced chunks, but
unwitnessed final-object deletion cannot always be detected. Recovery cannot
recreate acknowledged data lost from both the source and its backups.

Legacy v1 vector-only and v2 metadata records remain readable. Default backup
compaction produces the existing v2 single-object snapshot. Set
`chunk_bytes: Some(131_072)` to produce v3 chunks for streaming readers; this
layout needs a separately measured admission budget. Do not open a compacted
namespace with an older binary that does not understand that format.

## Verification

`cargo test --locked --test serving` covers capacity before I/O, scheduled
maintenance failure before a submitted batch, lost mutation acknowledgements,
poisoned close, interrupted backup, exact restoration and legacy migration.
M9's process-crash tests also exercise the underlying publication boundaries.

`python3 tools/m13_soak.py target/m13-run` starts disposable MinIO and runs six
successive processes for five minutes each. Each cycle runs 100 seeded oracle
queries and 400 mutations, paced at one cycle/second. The M12 policy maps the
original 10% planned ANN traffic to explicitly selected exact queries, giving
60% unfiltered and 40% selective exact. The first process injects a lost cleanup
acknowledgement, stages a fresh-prefix takeover, and rehearses backup/restore.
Every subsequent process reconstructs an independent expected state and checks
all vectors, metadata and exact results. The runner checks memory, open latency
and requests, query p95, visible objects, takeover time and maintenance bytes.
Raw epoch samples and environment are saved under the output directory.

`--smoke` performs the same transitions unpaced over two short processes for CI;
it checks correctness and request counts, not machine-specific latency budgets.
Neither the smoke nor a successful short pilot substitutes for the 30-minute run.
