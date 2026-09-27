# Retrying an uncertain conditional batch

Use `apply_request` on the exclusive database owner or `SingleMachine`:

```rust,ignore
use glider::{retry::Request, Mutation};
let observed = db.revision(42); // also valid if the document is absent
let request = Request {
    id: db.request_id()?,
    conditions: vec![observed], // increasing document IDs; all checked before batch
    mutations: vec![Mutation::Put {
        id: 42, vector: vec![1., 2.], metadata: Default::default(),
    }],
};
let outcome = db.apply_request(request.clone())?;
// outcome.conflict == None means every mutation committed atomically.
// Keep request unchanged if the call's acknowledgement is lost.
```

A request ID includes its observed commit boundary and random nonce. Changing
either creates a different ID. The canonical serialized request identifies its
payload; reuse with different operations or conditions returns `RequestConflict`.
A retained duplicate returns the original outcome, including conditional conflicts,
and cannot overwrite later data. Successful conditional decisions and rejected
conditional decisions each consume one durable sequence. Invalid inputs, admission
limits and failed maintenance before publication do not publish a decision.

After a storage error, stop the writer and follow [isolated recovery](RECOVERY.md).
The poisoned handle cannot answer result lookup from its stale view. On the new
owner, `lookup_request(id)` returns:

- `Retained(outcome)`: the original durable sequence and optional conflict.
- `Unknown`: no decision in this recovered history; resubmit the unchanged request.
- `Expired`: outcome cannot be resolved; resubmission is refused. Reconcile state
  before deciding whether to issue a new request with fresh observations.
- `Ahead`: the ID refers to a later boundary than this history, such as a rollback.

New requests require `current_sequence - id.boundary < 128`; retained outcomes
remain available through `id.boundary + 128`. Ordinary writes and conflict
decisions also advance this window; maintenance and duplicates do not. Limits
are 100 operations, 100 conditions and 1 MiB encoded request, with at most 128
retained receipts. Serving capacity limits also apply.

Revision observations detect all later changes to their ID, including deletion
and reinsertion; unrelated updates are allowed. `revision_floor()` exposes the
oldest valid observation. Older ones return `ExpiredRevision` rather than risk
accepting a stale write. The normal floor is at least current sequence minus 128;
an oversized legacy batch can advance it further to bound recent-change metadata.
All conditions see the same state before the batch. Reads see the whole batch
after acknowledgement. Concurrent callers must serialize through one owner;
this API alone does not provide a concurrent admission queue.

Checkpoints, compaction, isolated takeover and backups preserve receipts within
their window. A restored older backup has no knowledge of later requests. Clients
must reconcile an announced rollback before reusing observations or retrying
requests from its discarded future. An isolated destination cannot resolve late
commits in the quarantined source. This is bounded deduplication within one
selected history, not unlimited exactly-once delivery.
