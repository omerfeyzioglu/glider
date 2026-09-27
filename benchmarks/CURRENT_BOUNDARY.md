# Current local capacity boundary after M20

**The first newly tested failing step is 10,000×128: synchronous full-snapshot
maintenance delays queued writes.** This is a loopback MinIO result, independent
of the operator's unstable mobile internet. It is not an exact maximum row count
or an AWS capacity result. The accepted 5,000×128 M20 envelope remains unchanged.

## One bounded diagnostic

On 2026-09-27, run the existing SIFT workload for one 20-round paced step at
10,000 rows, four clients, 400 offered mutations/s and 40 exact queries/s (half
1% filtered). Reuse M20's valid 5,000-row evidence: `src`, Cargo dependencies and
`examples/m19_capacity.rs` are byte-identical to its measured `9601d0b` revision.
Do not repeat smaller steps or run a final acceptance soak after this breach.

Reproduce with:

```sh
python3 tools/m19_benchmark.py NEW_OUTPUT --data target/m19-data --probe-only 10000
```

`--probe-only` selects one existing bounded row size and omits final/verify phases;
default M19/M20 behavior and every performance/resource limit are unchanged.
The runner verifies the existing SIFT file hashes and removes the disposable
MinIO container on exit. The branch's runner-only diff was present during the
measurement; [run metadata](current-boundary/run.json) records parent `b557a00`,
that dirty file, and the exact source fingerprint. Client: Apple M4, macOS 26.6.2
arm64, Rust 1.98.1 release, Docker 29.1.3, pinned MinIO image. OS/container caches,
power and competing load are uncontrolled. This is attribution, not a stable
production tail estimate or a statistical before/after optimization claim.

## Result and attribution

| Measure | Observed | Existing limit |
|---|---:|---:|
| Write queue p95 | **109.093 ms** | 75 ms |
| Maintenance event p95/max, four events | 101.769 ms | 100 ms |
| End-to-end write p95 | 117.596 ms | 150 ms |
| Commit execution p95, excluding maintenance | 12.162 ms | 100 ms |
| Filtered / unfiltered query p95 | 20.831 / 6.592 ms | 50 ms each |
| Query queue p95 | 10.560 ms | 75 ms |
| Acknowledged mutations/s | 401.776 | >=350 |
| Process peak RSS | 46.000 MiB | 64 MiB |
| Fresh-process open | 88.915 ms | 1,000 ms |

[Raw samples](current-boundary/probe-10000/serve.json),
[operation profile](current-boundary/probe-10000/profile.json),
[stop decision](current-boundary/decision.json).

The meaningful breach is write-queue p95, about 45% above budget; do not overread
the maintenance maximum's 1.8% overrun from only four events. All other declared
gates passed. The four 3.76 MB snapshot PUTs took 49.480–69.626 ms. In the slowest
101.769 ms maintenance event, snapshot PUT took 69.626 ms, listing 1.747 ms,
bounded cleanup 6.132 ms and the remaining engine/harness work about 24.265 ms.
Store time includes the S3 client's envelope/checksum, Docker/MinIO I/O and local
transport; this does not identify MinIO disk or JSON formatting alone as the
root cause. The remaining gap is not a CPU sampling profile.

The logical cause is whole-dataset snapshot publication on the sole foreground
worker every 16 new batches. `SingleMachine::apply_request` runs `maintain`
before the triggering mutation; `Database::compact_with` clones/serializes all
live documents, publishes the snapshot, lists and removes obsolete objects.
Following writes wait for that synchronous work. Among 64 writes in ordinary
rounds, queue p95/max were 28.319/35.200 ms. Among the 16 writes in rounds containing
maintenance, they were 117.565/117.565 ms. These groups derive from snapshot
sequence +1 through +4 in the stored write order; they are descriptive subsets,
not independent experiments. Query arrival occurs after each client's write,
so these query timings do not prove unrestricted simultaneous-read latency.

800 query observations matched the independent commit-order oracle, every final
vector/metadata value matched after reopen, and over-capacity admission rejected
before I/O. There were zero HTTP/transport errors and zero serving GETs. Serving
used 84 PUTs, 69 DELETEs, four LISTs and 20,159,131 request-body bytes. Fresh open
used five GETs, two LISTs and 3,752,156 logical GET bytes. Cleanup removed the
container. No new backup/fault rehearsal ran because this step failed performance
acceptance; the existing 5,000-row failure/recovery evidence remains its scope.

## Consequence

For this workload the next target is reducing full-snapshot foreground work or
its frequency with explicit replay/memory bounds. Compare the smallest bounded
change first; moving publication into the background would additionally require
coordination, memory and crash/recovery design. Neither ANN nor more committers
is justified by this result. No engine optimization is implemented here, no gate
is relaxed and 10,000 rows are not marked accepted.
