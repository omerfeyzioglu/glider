# Bounded AWS bottleneck diagnostic

**Result: failed recovery, verified cleanup.** On 2026-09-27, a snapshot GET
received HTTP 200 but timed out while reading its body. The initial write and
fresh-process reopen succeeded; isolated takeover failed validation, so backup
and restore were not reached. The earlier [M14a success](M14a.md) remains historical
evidence, not a pass for this run or current remote capacity acceptance.

## Question and protocol

Does the current small serving workload expose an unexpected remote bottleneck?
One fixed 2,000×64, seed-42 `mod65536-v1`, squared-Euclidean, k=10 run used
`tools/s3_pilot.py --aws` from clean revision
`c8e9d19fc48befd5f53500605561718552d02623`. The runner retained every existing
request/byte/time guard and the 10-second HTTP deadline; there were no retries,
parameter sweeps, timeout increases, new infrastructure or account upgrades.
No production latency gate is inferred from local MinIO budgets.

Client: Apple M4, macOS 26.6.2 arm64, Rust 1.98.1 release, development-machine
network to Frankfurt S3 (`eu-central-1`). Before writes the runner verified FREE,
ACTIVE, USD 100 remaining, expiry 2027-03-27, bucket ownership/region and disabled
versioning. Credentials from `glider-test` stayed in the child environment.
[Run metadata](aws-diagnostics/live/run.json).

Reports use `pilot-client-timing-v1`: HTTP time includes connection setup, client
network, server response and full body consumption. Logical operation times
include library work and synchronous maintenance. Input/oracle generation is
outside batch/query timers. HTTP samples include failures; overlapping DELETE
intervals are merged when measuring wall-time coverage. Raw individual samples
are retained; these small samples do not establish tail percentiles or concurrency
capacity. This is the synthetic fractional-vector pilot, not M20's 5,000×128 SIFT
capacity workload.

## Observations

| Operation | Observation |
|---|---|
| 19 ordinary 100-row insertion batches | Median 308.490 ms; max 395.739 ms |
| Batch triggering maintenance | 5,289.937 ms, including 4,715.609 ms maintenance |
| Snapshot PUT during that maintenance | 3,541.700 ms, 1,179,737 envelope bytes |
| Fresh-process reopen | 13,244.296 ms; 12 HTTP attempts |
| Two completed snapshot GETs | 9,359.944 and 7,147.267 ms; 1,179,737 bytes each |
| Failed snapshot GET | Headers 421.832 ms; total 10,002.283 ms; 1,063,782 bytes received |
| Exact query, eight observations | Median 0.167 ms; max 0.386 ms; zero HTTP attempts |
| Filtered exact query, eight observations | Median 0.202 ms; max 0.286 ms; zero HTTP attempts |

The failed GET was the takeover destination's `compacted-...16` validation read.
The [captured SDK error](aws-diagnostics/live/failure.json) reports a response-body
timeout with zero retries. Successful reads of
the same-size snapshot already spent about 7.0–9.2 seconds after headers; the
failed read consumed only about 90% of its body before the deadline. This locates
the immediate failure in remote body transfer, rather than JSON decoding, exact
search, an S3 rejection or the phase's 300-second budget. No 429/5xx occurred.
Transport intervals cover 16.922 of 16.989 write seconds and 40.695 of 40.756
recovery seconds (over 99% in each). This is not a measurement of AWS internal
service time and cannot separate provider throughput from the client's network
path. The evidence does not establish a specific network fault or engine CPU bug.

The large synchronous snapshot upload also produces a multi-second foreground
pause. With the single worker this would hold queued work; concurrent queue
latency was not measured here. The local M17/M18 deferrals therefore must not be
extrapolated to this remote deployment. Before extending cloud capacity claims,
resolve the deployment's snapshot transfer boundary and declare remote read/write
budgets. A scoped comparison of smaller bounded snapshot objects versus a smaller
maintenance pause is justified only against that requirement; pinned views or a
background publisher are not selected by this diagnostic. Increasing the timeout
alone would hide the observed transfer/foreground problem.

## Failure and cleanup evidence

| Phase | Result | HTTP attempts | Request bytes | Response bytes | Seconds |
|---|---|---:|---:|---:|---:|
| [Write](aws-diagnostics/live/write.json) | Passed | 56 | 2,671,145 | 12,975 | 16.989 |
| [Recovery](aws-diagnostics/live/recover.json) | Failed | 35 | 1,481,711 | 4,036,195 | 40.756 |
| [Cleanup](aws-diagnostics/live/cleanup.json) | Passed | 29 | 0 | 9,967 | 6.855 |

Total: 120 HTTP attempts, 8,211,993 payload bytes, 64.600 measured phase seconds.
The three read-only CLI preflight calls are separate. Expected 404/412 responses
and the deliberately discarded successful mutation response are distinguished
from the unexpected body timeout. Reopen, complete vector/metadata checks,
filtered/unfiltered exact oracles, overwrite/delete and poisoned-handle visibility
passed before takeover failed. The failed destination was never promoted.
Cleanup removed 19 objects and verified all five generated namespaces empty.
The timeout was a GET, and all earlier writes had completed at the server.
There were no extra AWS workload runs after the failure.

The same instrumented workload passed completely on pinned local MinIO, including
lost-acknowledgement recovery, backup/restore and cleanup: 177 HTTP attempts.
[MinIO raw reports](aws-diagnostics/minio/run.json) record the dirty parent revision
because instrumentation was tested before committing; the diff became `c8e9d19`.
Its 19 ordinary insertion batches had median 3.846 ms, maintenance 38.854 ms, and
the snapshot PUT 16.364 ms. These uncontrolled loopback observations support
attribution only, not an AWS speedup comparison. Both runs' HTTP sample counts
and payload sums were reconciled with the independent budget counters, including
the failed body transfer. Five Rust tests cover shared limits and failed-request
telemetry; seven Python tests cover Free-plan/bucket refusal and bounded cleanup.
Routine regression and fault testing remains on MinIO.

Reproduction uses [the guarded procedure](../docs/S3_PILOT.md) with a fresh output
directory; timing adds no remote requests. Retain failure evidence and verify
cleanup rather than rerunning until a pass. Source changes after the measured
revision only strengthen telemetry tests and record these results.
