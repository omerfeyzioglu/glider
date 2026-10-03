# HTTP API reference

`glider-server` serves one collection or many collections over HTTP/JSON. This page describes
every route the router (`src/server/http.rs`) registers. Configuration is in
[CONFIGURATION.md](CONFIGURATION.md); durability and recovery semantics
are in [DESIGN.md](../DESIGN.md#http-service) and [RECOVERY.md](RECOVERY.md).

| Method and path | Auth | Purpose |
|---|---|---|
| [`POST /v1/write`](#post-v1write) | bearer | Atomic batch of upserts and deletes |
| [`POST /v1/query`](#post-v1query) | bearer | k-nearest-neighbor query with optional metadata filter |
| [`POST /v1/points/get`](#post-v1pointsget) | bearer | Get up to 1000 points in one consistent read |
| [`POST /v1/scan`](#post-v1scan) | bearer | Count and page through live points matching a filter |
| [`GET /v1/points/{id}`](#get-v1pointsid) | bearer | Current vector and metadata of one point |
| [`GET /v1/requests/{boundary}/{nonce}`](#get-v1requestsboundarynonce) | bearer | Resolve the outcome of an uncertain write |
| [`GET /v1/status`](#get-v1status) | bearer | Sequence, queue, cache and clustering state |
| [`GET /healthz`](#get-healthz) | none | Liveness |
| [`GET /metrics`](#get-metrics) | none | Prometheus text metrics |
| [`GET /console`](#get-console) | none | Built-in browser console; `/` redirects here |

## Collections

When `GLIDER_DIMENSIONS` is unset, the base storage prefix is a collection
catalog. All seven data endpoints above use the prefix
`/v1/collections/{name}` (for example,
`POST /v1/collections/demo/write` and `GET /v1/collections/demo/status`).
Request and response schemas, request IDs and durability are unchanged.
Unprefixed data endpoints return JSON `404` directing clients to the prefix.
`/healthz`, `/metrics`, `/console` and `/` remain global. Names match
`^[a-z0-9][a-z0-9-]{0,62}$`.

| Method and path | Body and result |
|---|---|
| `POST /v1/collections` | JSON `{"name":"demo","dimensions":3,"metric":"squared_euclidean","resident_filter":{"key":"value"},"routed_keys":["key"]}`. `metric` defaults to `squared_euclidean`; filter and routed keys are optional. Returns a description with `open` and `201` on create, `200` for identical configuration, `409` for a conflict. |
| `GET /v1/collections` | `{"collections":[...]}` sorted by name; descriptions include `name`, `dimensions`, `metric`, `resident_filter`, `routed_keys` and `open`. Listing does not open collections. |
| `GET /v1/collections/{name}` | Description plus `status` containing the same body as that collection's `/status`; opens it if needed. `404` if absent. |
| `DELETE /v1/collections/{name}` | Drains and releases the collection, deletes its catalog entry, then cleans its generation. `204` after the catalog deletion, `404` if absent. A later create uses a new generation and starts empty. |

Opening is lazy. At the configured open limit, the least recently used
collection without requests in flight drains and closes; a later request
reopens it transparently. When every open collection is busy, opening another
returns `429`. `glider-admin` currently supports only single-collection mode.

## Conventions

- **Authentication.** When `GLIDER_API_TOKEN` is set, every route except
  `/healthz`, `/metrics`, `/console` and `/` requires `Authorization: Bearer <token>`; a
  missing or different token returns `401`. Without the variable no route
  is authenticated. The server speaks plain HTTP; terminate TLS in a
  reverse proxy.
- **Bodies.** `POST` bodies must be JSON with `Content-Type:
  application/json` and at most 2 MiB (the axum default; larger bodies get
  `413`). Unknown fields are rejected.
- **IDs and values.** Point IDs are unsigned 64-bit integers. Vectors are
  arrays of finite numbers (stored as `f32`) whose length equals
  the collection's dimensions (or `GLIDER_DIMENSIONS` in single mode). With
  cosine distance a vector must not be all
  zeros; it is stored normalized to unit length. Metadata is a flat object
  of string keys to string values.
- **Distances.** `squared_euclidean`, `manhattan`, or `cosine` (`1 - dot`
  of normalized vectors), computed in `f64`. Results are ordered by
  ascending distance, then ID.
- **Sequence.** Every acknowledged write consumes one commit sequence
  number. Responses carry the sequence of the state they reflect.

### Errors

Every error response has a JSON body, including routing and parsing
failures:

```json
{"error": "k must be between 1 and 1000"}
```

| Status | Meaning | Retry? |
|---|---|---|
| `400` | Invalid input: wrong dimension, non-finite or zero cosine vector, `k` out of range, empty write, too many operations, malformed `request_id`, request boundary ahead of the collection's history, a request whose encoded form exceeds 1 MiB, a point too large for one block (see [limits](#post-v1write)), malformed JSON syntax, or a path segment that is not a number | No; fix the request |
| `401` | Missing or invalid bearer token | No |
| `404` | Point not found, or unknown route | No |
| `405` | Wrong method for a route (the `Allow` header is kept) | No |
| `409` | Request ID reused with a different payload, or expired (older than 128 commits) | No; see [request IDs](#request-ids-and-retries) |
| `413` | Body larger than 2 MiB | No; split the batch |
| `415` | Missing `Content-Type: application/json` | No |
| `422` | JSON does not match the schema (unknown field, wrong type, missing required field) | No |
| `429` | Admission queue full (8 queued commands or 1 MiB of queued encoded requests) | Yes, with backoff; resend writes with the same `request_id` |
| `500` | Stored data failed validation (corruption) | No; investigate |
| `503` | Storage error, worker stopped or failed, server shutting down | Yes for reads; for writes resolve the request ID first |

Rejections produced by the HTTP framework before Glider sees the request
(malformed JSON, `413`, `415`, `422`, `405`, `404`) use the same shape; their
message is the framework's diagnostic, or the status reason when it has none.

## `POST /v1/write`

Apply one atomic batch: all upserts, then all deletes, published together
in one durable object. The response is sent only after that publication;
an acknowledged write survives any later crash.

Request:

| Field | Type | Default | Notes |
|---|---|---|---|
| `upsert` | array of points | `[]` | Each point: `id` (u64, required), `vector` (array of numbers, required), `metadata` (object of strings, default `{}`). An upsert replaces the point's vector and its complete metadata. |
| `delete` | array of u64 | `[]` | Deleting an absent ID is allowed. |
| `request_id` | object | issued by the server | `{"boundary": u64, "nonce": "<32 lowercase hex digits>"}`; see [request IDs](#request-ids-and-retries). |

Limits: at least one and at most 100 operations (`upsert` plus `delete`)
per write. Operations on the same ID apply in order, upserts before
deletes. Two byte limits apply:

- A write whose JSON-encoded form exceeds 1 MiB is rejected with `400`. The
  admission queue also holds 1 MiB, so any write that passes this check is
  admitted when the queue has room, and `429` only means "retry later".
- Each point must fit in one storage block: its stored size, which is 17
  bytes plus 4 bytes per vector component plus 4 bytes plus, for every
  metadata entry, 8 bytes plus the UTF-8 length of its key and value, must
  not exceed 122,867 bytes. A larger point rejects the whole write with
  `400` (`document <id> needs <n> raw bytes; at most 122867 fit in one
  block`) before anything is published or acknowledged, so the collection
  keeps accepting and sealing writes.

```sh
curl -XPOST localhost:8080/v1/write -H 'content-type: application/json' \
  -d '{"upsert":[{"id":1,"vector":[0,0,0],"metadata":{"color":"red"}},{"id":2,"vector":[1,1,1]}]}'
```

Response `200`:

```json
{"request_id":{"boundary":1,"nonce":"ecd721367d3720c6b60de32f814c7097"},"sequence":2}
```

| Field | Type | Notes |
|---|---|---|
| `sequence` | u64 | Commit sequence of this write |
| `request_id` | object | The ID this write was recorded under; resend with it to retry |

## `POST /v1/query`

Return the `k` nearest current points to `vector`. See [Filters](#filters).

| Field | Type | Default | Notes |
|---|---|---|---|
| `vector` | array of numbers | required | Same dimension and validity rules as writes |
| `k` | integer | `10` | 1 to 1000 |
| `filter` | object | `{}` | Metadata predicate; see [Filters](#filters) |
| `include_metadata` | bool | `false` | Add each hit's `metadata` |
| `include_vector` | bool | `false` | Add each hit's stored `vector` |
| `exact` | bool | `false` | Exhaustive exact search of the acknowledged view |
| `profile` | bool | `false` | Include query mode, elapsed time, queue wait and remote I/O counters in the response |

How the query is answered depends on the filter:

With `"exact":true`, all live points in the published root and unsealed tail
are scored, and a filtered query returns `min(k, matches)` hits. It reads every
block of the collection: local cache hits are free, but uncached blocks cause
remote GETs. Use it for small collections, filtered queries that must be
complete, or evaluation. The query uses the same admission and reader
concurrency limits as approximate queries.

With `"exact":false` (the default):

- **No filter:** approximate. Blocks are ranked with persisted sketches (or
  clustered-view centroids) and the best ones are read within a fixed
  budget (12 candidate blocks, 8 remote range requests and 1 MiB per query,
  plus up to `GLIDER_LOCAL_BLOCKS` cached blocks), then reranked exactly.
  Unsealed recent writes are always scanned exactly.
- **Only the declared `GLIDER_RESIDENT_FILTER` equality:** exact, from
  full-precision vectors held in memory. Repeating it is equivalent.
- **Any other filter:** approximate post-filtering over the same routed
  blocks; required equality leaves on `GLIDER_ROUTED_KEYS` first restrict
  which rows are routed. May return fewer than `k` results, or none, even
  when matches exist.

```sh
curl -XPOST localhost:8080/v1/query -H 'content-type: application/json' \
  -d '{"vector":[1,1,0.9],"k":2,"include_metadata":true,"include_vector":true}'

curl -XPOST localhost:8080/v1/query -H 'content-type: application/json' \
  -d '{"vector":[1,1,0.9],"k":2,"filter":{"color":"red"},"exact":true}'

curl -XPOST localhost:8080/v1/query -H 'content-type: application/json' \
  -d '{"vector":[1,1,0.9],"k":2,"profile":true}'
```

Response `200`:

```json
{
  "results": [
    {"distance": 0.01000000476837215, "id": 2, "metadata": {}, "vector": [1.0, 1.0, 1.0]},
    {"distance": 2.8099999570846563, "id": 1, "metadata": {"color": "red"}, "vector": [0.0, 0.0, 0.0]}
  ],
  "sequence": 2
}
```

`metadata` and `vector` appear only when requested, and come from the same
version of the point that the distance was computed on. `sequence` is the
acknowledged state the query read.

With `"profile":true`, the response also includes a `profile` object, for
example:

```json
{"profile":{"mode":"approximate","server_ms":1.42,"queue_ms":0.08,"remote_reads":0,"remote_bytes":0}}
```

`mode` is `approximate`, `resident_exact`, or `exact_scan` for the path used.
`server_ms` measures from the query handler start until the engine result is
received, including admission queue wait; `queue_ms` is the admission queue
wait. The remote counters count this query's object reads and payload bytes.
They can be zero when the query is served from the unsealed tail or cache.
Without `profile`, the response shape is unchanged. Collection query routes
accept the same field.

## `GET /v1/points/{id}`

```sh
curl localhost:8080/v1/points/1
```

Response `200`:

```json
{"id":1,"metadata":{"color":"red"},"vector":[0.0,0.0,0.0]}
```

A deleted or never-written ID returns `404` with
`{"error":"no point 1"}`. Cosine collections return the stored unit vector.

## `POST /v1/points/get`

Read 1 to 1000 IDs from one acknowledged view. Duplicate IDs produce duplicate
points; found points retain request order, and missing IDs appear in request
order in `missing`. Both field flags default to `true`; omitted fields are
absent from each point. Cosine vectors are stored unit vectors.

```sh
curl -XPOST localhost:8080/v1/points/get -H 'content-type: application/json' \
  -d '{"ids":[2,99,1,2],"include_vector":false}'
```

Response `200`:

```json
{"points":[{"id":2,"metadata":{}},{"id":1,"metadata":{"color":"red"}},{"id":2,"metadata":{}}],"missing":[99],"sequence":2}
```

## `POST /v1/scan`

Exhaustively scan live points with a [filter](#filters). `filter` defaults
to `{}`; `after` excludes IDs at or below it. `limit` defaults to 1000 and
must be 1 to 10000. `include_metadata` defaults to `false`. Results are in
ascending ID order. `matched` counts all matches before `after` and `limit`;
`next` is the last returned ID when more matches remain, otherwise `null`.
The scan reads every block, including uncached remote blocks. It retains only
bounded IDs and requested metadata, never vectors. Pages read separate
acknowledged views; writes between pages can change the result set.

```sh
curl -XPOST localhost:8080/v1/scan -H 'content-type: application/json' \
  -d '{"filter":{"color":"red"},"after":1,"limit":2,"include_metadata":true}'
```

Response `200`:

```json
{"points":[{"id":3,"metadata":{"color":"red"}}],"next":null,"matched":2,"sequence":2}
```

To delete by filter, scan pages, collect their IDs, then submit ordinary
`/v1/write` delete batches of at most 100 operations. Concurrent writes can
change which IDs match while paging.

## Filters

Metadata remains a map of strings to strings. A filter is a JSON object;
several members in one object are ANDed. `{}` matches every point. A bare
string value means equality, as before. A key can instead contain operators:

```json
{"color":"red","price":{"$gt":1.5,"$lte":10},"tag":{"$in":["a","b"]}}
```

| Operator | Argument | Meaning |
|---|---|---|
| `$eq`, `$ne` | string | Equal or unequal |
| `$in`, `$nin` | array of strings | Member or not a member |
| `$exists` | boolean | Key present or absent |
| `$gt`, `$gte`, `$lt`, `$lte` | JSON number | Numeric comparison |

Several operators for one key are ANDed. Numeric operators parse the stored
string as a finite `f64`; missing, invalid and nonfinite stored values never
match a numeric comparison. `$ne` and `$nin` **do** match a missing key.
`$in` and `$nin` accept empty arrays. For example, `$in:[]` matches nothing
and `$nin:[]` matches every point.

`{"$and":[filter,...]}`, `{"$or":[filter,...]}` and `{"$not":filter}`
work at the top level or inside other logical filters. Empty AND matches all;
empty OR matches none. Keys beginning with `$` are reserved. Unknown operators
and wrong argument types return HTTP 400. Maximum nesting depth is 8,
maximum leaf conditions is 64, and each `$in` or `$nin` array has at most
1024 values; exceeding a limit returns HTTP 400.

`exact:true` queries and `/v1/scan` evaluate the full filter exhaustively.
Default queries are approximate except when the filter consists only of the
declared resident equality. Approximate routing uses only equality conditions
that every match must satisfy; the full filter is checked after reading
candidates. A bounded approximate query may omit matching points.

## `GET /v1/requests/{boundary}/{nonce}`

Resolve a write whose response was lost, by its request ID.

| `state` | Meaning | What to do |
|---|---|---|
| `retained` | The write was decided; `outcome` has the same fields as the write response | Done |
| `unknown` | No write with this ID was committed | Resend the identical write with the same `request_id` |
| `expired` | The ID is more than 128 commits old; the outcome is no longer known | Read the affected points and decide |
| `ahead` | The ID's boundary is later than this collection's history (for example after restoring an older backup) | Reconcile with the restored state |

```json
{"outcome":{"request_id":{"boundary":2,"nonce":"0123456789abcdef0123456789abcdef"},"sequence":3},"state":"retained"}
```

## `GET /v1/status`

```json
{
  "sequence": 133,
  "queued_commands": 0,
  "queued_bytes": 0,
  "closed": false,
  "failed": false,
  "maintenance_errors": 0,
  "cache": {"state": "warm", "nvme_bytes": 16384, "nvme_limit_bytes": 268435456, "namespace_bytes": 8192, "warm_bytes": 8192},
  "clustering": {"state": "none", "epoch": 0, "centroids": 0, "auto_cluster_rows": 250000, "auto_recluster_factor": 4,
                 "reclusters": 0, "progress": null, "conversions": 0, "conversion_failures": 0}
}
```

| Field | Meaning |
|---|---|
| `sequence` | Last acknowledged commit sequence |
| `queued_commands`, `queued_bytes` | Admission queue occupancy (limits 8 and 1 MiB) |
| `closed`, `failed` | The worker is shutting down, or failed after an uncertain write; restart the server |
| `maintenance_errors` | Background maintenance units that failed without affecting acknowledged data (retried later) |
| `cache.state` | `disabled` (no NVMe tier), `cold` (warm-up not started), `warming`, `warm` (every block of the current layout is local) or `partial` (the cache limit is below `namespace_bytes`) |
| `cache.nvme_bytes`, `cache.nvme_limit_bytes` | Local cache use and limit (`GLIDER_CACHE_BYTES`) |
| `cache.namespace_bytes`, `cache.warm_bytes` | Cache charge of the current layout, and how much of it the current warm-up pass found or cached |
| `clustering.state` | `none` (per-seal routing), `converting` (a clustered view is being built; queries use the previous layout) or `clustered` |
| `clustering.epoch`, `clustering.centroids` | The selected clustered view, if any |
| `clustering.auto_cluster_rows`, `clustering.auto_recluster_factor` | The configured thresholds (0 = disabled) |
| `clustering.progress` | While converting: `phase` (`sample`, `assign`, `gather`, `write`, `catalog`, `root`), `sources`, `sources_done`, `pass`, `passes`, `posting_packs`, `rows`, and the `epoch` and `centroids` being built; otherwise `null` |
| `clustering.reclusters`, `clustering.conversions`, `clustering.conversion_failures` | Automatic rebuilds started, conversions published, and automatic conversions abandoned since start |

## `GET /console`

Returns a self-contained HTML console for overview, collection browsing and
vector queries. `GET /` redirects to it. Both routes are public and can be
disabled with `GLIDER_CONSOLE=0`; the page sends the token entered by the user
on API requests and keeps it in browser session storage only. The response
sets a restrictive Content Security Policy and loads no external resources.

## `GET /healthz`

Returns `200` with an empty body while the admission worker accepts work,
and `503` once it is closed or has failed. It does not touch storage. It
needs no token.

## `GET /metrics`

Prometheus text format 0.0.4 (`Content-Type: text/plain; version=0.0.4`),
no token required. Restrict access to it at the network level if needed.
Counters reset when the process restarts.

HTTP metrics, labelled by `endpoint` (`/healthz`, `/metrics`, `/v1/status`,
`/v1/write`, `/v1/query`, `/v1/points/{id}`, `/v1/points/get`, `/v1/scan`,
`/v1/requests/{boundary}/{nonce}`, `unmatched`):

- `glider_http_requests_total{endpoint, status_class="1xx".."5xx"}` (counter)
- `glider_http_request_duration_seconds` (histogram; buckets 5 ms to 10 s)

In multi mode these HTTP metrics are process wide, including collection
routes (counted under `unmatched`), and `glider_open_collections` reports the
number of open collection services. Admission and engine metrics below are
exposed in single-collection mode; use each collection's `/status` in multi
mode for its engine state.

Admission and engine:

| Metric | Type | Meaning |
|---|---|---|
| `glider_admission_commands`, `glider_admission_bytes` | gauge | Queue occupancy |
| `glider_worker_failed`, `glider_worker_closed` | gauge | 1 when the worker failed or is closed |
| `glider_maintenance_errors_total` | counter | Failed background maintenance units |
| `glider_committed_sequence` | gauge | Last acknowledged sequence |
| `glider_writer_epoch` | gauge | Writer epoch from the last takeover |
| `glider_sketch_index_bytes` | gauge | Resident routing state |
| `glider_segmented_{seal,prune,reclaim,merge}_starts_total`, `..._steps_total` | counter | Maintenance plans started and units completed |
| `glider_segmented_consolidations_total`, `glider_segmented_forced_seals_total`, `glider_segmented_sketch_compactions_total`, `glider_segmented_removed_objects_total`, `glider_segmented_warm_steps_total` | counter | Other maintenance units; a forced seal ran inside a write because the log tail was full |
| `glider_cache_ram_hits_total`, `glider_cache_nvme_hits_total`, `glider_cache_remote_fetches_total`, `glider_cache_remote_payload_bytes_total`, `glider_cache_corrupt_entries_total` | counter | Block cache activity of queries |
| `glider_cache_warm_fetches_total`, `glider_cache_warm_payload_bytes_total` | counter | Warm-up reads |
| `glider_cache_nvme_bytes`, `glider_cache_nvme_entries`, `glider_cache_nvme_limit_bytes`, `glider_cache_namespace_bytes`, `glider_cache_warm_bytes`, `glider_cache_warm_complete` | gauge | Cache size and warm-up state |
| `glider_clustered_state` | gauge | 0 none, 1 converting, 2 clustered |
| `glider_clustered_epoch`, `glider_clustered_centroids` | gauge | Selected clustered view |
| `glider_auto_cluster_rows`, `glider_auto_recluster_factor` | gauge | Configured thresholds |
| `glider_conversion_phase` | gauge | 0 idle, 1 sample, 2 assign, 3 gather, 4 write, 5 catalog, 6 root |
| `glider_conversion_sources`, `glider_conversion_sources_done`, `glider_conversion_pass`, `glider_conversion_passes`, `glider_conversion_posting_packs`, `glider_conversion_rows`, `glider_conversion_epoch`, `glider_conversion_centroids` | gauge | Running conversion progress |
| `glider_conversion_starts_total`, `glider_conversion_steps_total`, `glider_conversions_total`, `glider_conversion_failures_total`, `glider_recluster_starts_total` | counter | Conversion activity |

## Request IDs and retries

Every write is recorded under a request ID `{boundary, nonce}`. A network
error or timeout leaves a write's outcome uncertain: it was either
committed completely or not at all, never partially. To make a write safely
retryable, supply the ID yourself:

1. Read a recent `sequence` (from `/v1/status` or any response) and use it as
   `boundary`; generate 16 random bytes as `nonce` (32 lowercase hex digits).
2. Send the write. If the response is lost, resend the identical body with
   the same `request_id`. A committed write returns its original response;
   an uncommitted one is applied once. The same ID with a different body
   returns `409`.
3. Alternatively, resolve it with `GET /v1/requests/{boundary}/{nonce}`.

The boundary must not be ahead of the collection (`400`) and must be within
the last 128 commits (`409` "expired"); outcomes stay resolvable until 128
commits after the boundary. Without a `request_id`, the server issues one
and returns it, which helps only when the response arrives. Retries
survive restarts, crashes and takeovers; a restore from an older backup
does not know later requests (`ahead`).

```python
import requests, secrets

base = "http://localhost:8080"
sequence = requests.get(f"{base}/v1/status").json()["sequence"]
body = {
    "upsert": [{"id": 7, "vector": [0.1, 0.2, 0.3], "metadata": {"color": "blue"}}],
    "request_id": {"boundary": sequence, "nonce": secrets.token_hex(16)},
}
for attempt in range(5):
    try:
        response = requests.post(f"{base}/v1/write", json=body, timeout=10)
    except requests.RequestException:
        continue  # outcome uncertain: resend the same body
    if response.status_code in (429, 503):
        continue  # not committed or uncertain: resend the same body (add backoff)
    response.raise_for_status()
    print(response.json()["sequence"])
    break
```

## Consistency

- One process owns the collection; all writes go through one committer.
- A write is acknowledged only after its batch is durable in object
  storage. A query, point read or status request sent after an
  acknowledgement observes that write (read-your-writes), and each query
  reads one consistent snapshot.
- During a restart or takeover the server is unavailable; the new process
  serves every acknowledged write. A paused former owner may answer reads
  from its old state until it notices it was deposed, but cannot commit;
  remove it from client routing ([RECOVERY.md](RECOVERY.md)).
- Queries with `exact:false` are approximate except for the declared resident
  filter; their measured recall is in [BENCHMARKS.md](../BENCHMARKS.md).
  Queries with `exact:true` use the library's exhaustive oracle.
