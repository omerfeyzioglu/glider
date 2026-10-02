# Recovery procedures

The object store must satisfy the complete-object, strongly consistent listing
and conditional-create contract in `DESIGN.md`.

## Segmented collections (`glider-server`)

A crash, kill, host loss or uncertain write needs no operator step. Start
`glider-server` (or any `glider-admin` command) again on the same prefix with
the same `GLIDER_DIMENSIONS`, `GLIDER_METRIC`, `GLIDER_RESIDENT_FILTER` and
`GLIDER_ROUTED_KEYS`. It waits at most one lease duration
(`GLIDER_LEASE_SECONDS`, default 10) for the dead process's lease to expire,
then takes the namespace over: it fences the previous writer at the object
store, replays the log tail and serves every acknowledged write. A graceful
SIGINT/SIGTERM releases the lease, so the next start does not wait.

- A process that was only paused or partitioned, not dead, is fenced: its
  later writes fail with a "fenced by a takeover" error and are not
  committed, its next lease renewal reports it deposed and it exits. Until
  then it may still answer reads from its old view, so remove it from client
  routing.
- A second process started while a server renews its lease waits one lease
  duration and fails with a lease (busy) error; it deposes nobody.
- Resolve a write whose outcome was lost with `GET
  /v1/requests/{boundary}/{nonce}` or by resending it with the same
  `request_id` after the restart; an unacknowledged write may be present or
  absent, never partial.
- Upgrading from a binary without takeover fencing: stop every old server
  first (it holds no lease and would be fenced immediately). Its leftover
  `owner-*` claim objects are ignored. After the first takeover, those older
  binaries reject the namespace.

Restore is needed only to move data or after loss or corruption: a missing
or corrupt selected root, index or pack, a log gap or invalid metadata fails
open. Do not delete objects to make it open. A missing or corrupt derived
clustered-view object (centroids, catalog or posting pack) does not need a
restore: the server refuses to start (and queries reading a corrupt posting
block fail), while `glider-admin convert` rebuilds the view from the
canonical runs as a new epoch.
`glider-admin backup <empty-destination>` creates a validated copy of the
collection while no server holds the lease. With `GLIDER_DATA_DIR` (or
`GLIDER_S3_NAMESPACE`) set to a fresh empty destination, `glider-admin restore
<backup-or-prefix>` copies a backup or a stopped namespace, excluding lease and
claim objects, publishes `metadata` last and validates the segmented root and
log tail. The first server start there takes it over. Never reuse a failed
destination.

## Resident `OwnedDatabase` namespaces

Keep the old namespace and its backups intact while diagnosing failure.

1. **Stop the old writer and client traffic.** Verify the process cannot issue
   new requests. A timed-out request already sent to S3 may still finish after
   the process exits. Do not treat a stale owner claim as a fence for those
   requests.
2. **Classify shutdown.** After a successful `OwnedDatabase::close` with no
   uncertain write, reopen the same prefix with a new owned handle. After a
   timeout, lost acknowledgement, storage error, forced exit or crash, reserve
   a fresh, nonoverlapping, empty destination prefix. Do not clear the old claim
   merely to resume writes in the old prefix. Keep that prefix quarantined.
3. **Stage and validate.** Open a fresh source store for the old prefix and a
   fresh destination store for the new prefix. Call
   `recovery::stage_isolated_namespace(&source, destination, config)`. It freezes
   one source listing, copies complete objects except ownership controls, writes
   `metadata` last and returns only after a full `Database::open` validation of
   the destination. Open `OwnedDatabase` on the destination and check its
   known document IDs and exact results against the available acknowledged-write
   log or application oracle. An operation without an acknowledgement may be
   present or absent. Switch client configuration only after those checks pass.
   If the old listing contains only ownership control objects and no `metadata`,
   no database write could have been acknowledged there; initialize a fresh
   empty prefix instead. If any data object exists without `metadata`, treat it
   as corruption, not an empty database.
4. **Handle interruption.** If staging errors or stops before validation, never
   point clients at that destination and never reuse it. Reserve another empty
   prefix for the next attempt. A nonempty partial copy without metadata fails
   database open. If metadata published before the interruption, the candidate
   may be complete, but still requires full validation before promotion.
5. **Handle corruption.** A missing/corrupt selected compaction snapshot,
   selected checkpoint or referenced chunk, a log gap, invalid metadata or
   unsupported version is an error. Do not fall back to an older root or skip
   the object. Restore a known-good backup into another fresh prefix and
   validate before promotion. No backup or witness means the lost state may be
   unrecoverable.

To identify the authoritative data, read `metadata` for format/configuration,
select and validate the highest compaction root, then a newer checkpoint root
if present, then replay the contiguous mutation tail above the selected
checkpoint boundary. A version 3 root names every required chunk. A derived
IVF cache is never authoritative and may be rebuilt. The engine performs these
checks on open; do not infer validity from object names alone.

For ownership diagnostics, `ownership::claims(&store)` lists and validates
claims. `ownership::clear_stale_claim(&mut store, exact_key)` removes one claim
only after an operator proves its process stopped. On an uncertain release,
reopen the store and inspect again. A malformed claim body can be cleared by
exact key after that proof; a malformed key requires direct provider repair.
Never clear another live writer's claim. An already-open legacy writer must be
stopped before enrolling a namespace in owned mode.

The engine detects corrupt complete bytes, missing selected roots/chunks and
internal sequence gaps. It cannot detect arbitrary external deletion of an
unwitnessed final mutation or sole compaction root, nor reconstruct lost
acknowledged bytes without backup. A fresh prefix isolates late writes to the
old prefix; it is not a substitute for a backup or a multi-writer protocol.
