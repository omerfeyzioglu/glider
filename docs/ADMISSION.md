# Bounded concurrent clients

Move an open `SingleMachine` into one worker and clone its client:

```rust,ignore
use glider::{admission::{Limits, Service, Shutdown}, retry::Request, Mutation};
let service = Service::start(db, Limits::default())?;
let client = service.client();
let observed = client.observe(42)?.wait()?.value;
let request = Request {
    id: observed.request_id,
    conditions: vec![observed.revision],
    mutations: vec![Mutation::Put {
        id: 42, vector: vec![1., 2.], metadata: Default::default(),
    }],
};
let outcome = client.write(request.clone())?.wait()?.value;
// Keep the unchanged request to resolve a lost response.
service.shutdown(Shutdown::Drain)?;
```

Defaults admit at most eight commands and 320 KiB of encoded payload, including
the active command. Count and bytes are independent limits. `Overloaded` means
this submission was not enqueued; it cannot publish. All clients share the same
limits. Payloads are normalized before retention, so caller-reserved capacities
do not inflate queued memory. Caller-owned inputs and completed results are
outside the service budget. `status()` exposes outstanding charges and whether
admission is closed or the worker failed.

The worker executes FIFO, with one authoritative owner. Writes retain the
[bounded retry contract](RETRIES.md); exact queries return a committed sequence
with their results. `Timed` separates queue wait, execution and due maintenance.
Measure client latency around submission and `wait()` to include normalization,
response delivery and caller scheduling. There is no automatic group commit.

An engine that publishes snapshots, such as `SegmentedServing`, runs queries
and document reads on `Limits::queries` reader threads (default 4) beside the
worker, each on the latest acknowledged state; they count toward the same
admission limits. A read submitted after a write's acknowledgement observes
that write. `read_priority` applies only to reads that run on the worker.

`ticket.cancel()` returning true proves execution will not start. Once started,
a write continues even if its ticket is dropped; resolve its result using the
unchanged request ID. Dropping a queued ticket cancels it. A cancelled command
keeps its charge until removed from the queue.

`begin_shutdown` closes admission immediately. `shutdown(Drain)` finishes
accepted work; `shutdown(CancelQueued)` cancels work that has not started. Both
allow active execution to finish and join the worker. Only successful explicit
shutdown acknowledges ownership release. Its completion time depends on the
backend's operation deadlines. Dropping the service requests cancellation of
queued work but does not wait for active publication or acknowledge release.

An uncertain PUT or worker panic closes admission and fails pending commands.
Stop the worker, then use [isolated recovery](RECOVERY.md) and M15 result lookup;
never clear its claim while it may still issue requests. Failed thread creation
can also leave the previously acquired claim. A caller's invalid input or a
conditional conflict leaves a healthy worker running. For backup, shut down
successfully and reopen the serial serving API.

[The M16 workload](../benchmarks/M16.md) defines the measured operating envelope;
these limits do not promise latency under arbitrary load.
