# Changelog

All notable user-visible changes are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Close idle multi-mode collections after `GLIDER_COLLECTION_IDLE_SECONDS` to release their writer leases.
- Collections: without `GLIDER_DIMENSIONS`, one server creates, lists,
  deletes and serves many collections over HTTP (`/v1/collections`), each
  in its own namespace under a versioned catalog; collections open lazily
  and the least recently used close beyond `GLIDER_MAX_OPEN_COLLECTIONS`.
- Metadata filters with equality, inequality, set membership, existence,
  numeric ranges and nested logic for queries and scans.
- `exact: true` on `POST /v1/query` for exhaustive exact search, so filtered
  queries return every match; `POST /v1/points/get` for up to 1000 points per
  request; `POST /v1/scan` to count, list and page matching points by id.
- Python client (`clients/python`) and an MCP memory server (`glider-mcp`)
  with `remember`, `recall`, `forget` and `memory_count` tools. The client
  binds to a collection (`Client(collection=...)`, `client.collection(name)`)
  and manages them (`create_collection`, `list_collections`,
  `get_collection`, `delete_collection`); with `GLIDER_COLLECTION` the MCP
  server creates its cosine collection on first use, so the server no longer
  needs `GLIDER_DIMENSIONS`.

## [1.0.1] - 2026-10-03

### Security

- Update `rustls` to 0.23.45 (RUSTSEC-2026-0285, TLS 1.3 handshake
  messages accepted across encryption levels); it is used by the S3 HTTPS
  client.

### Added

- Multi-platform Docker image (`linux/amd64`, `linux/arm64`) published to
  GitHub Container Registry for each version tag.
- `cargo audit` in CI, Dependabot, issue and pull request templates, and a
  security policy ([SECURITY.md](SECURITY.md)).

## [1.0.0] - 2026-10-03

First release: a single-node, single-writer vector database with S3 as the
authoritative store. Milestones M1–M39 in [ROADMAP.md](ROADMAP.md); design
decisions in [docs/EVOLUTION.md](docs/EVOLUTION.md).

### Server and operations

- `glider-server`: one collection over HTTP/JSON with atomic write batches,
  k-NN queries with equality filters, point reads, request-ID lookups,
  status, health and Prometheus metrics; optional bearer-token
  authentication ([API reference](docs/API.md)).
- Query results can include each hit's metadata and vector.
- Writes are acknowledged only after durable publication; every write
  carries a request ID, and resending it returns the original outcome
  within a 128-commit window.
- Automatic takeover after a crash or restart: a renewed writer lease plus
  permanent fence objects, so a paused former owner cannot commit.
- `glider-admin` with `status`, `backup`, `restore` and `convert`; local
  failure drills in `tools/drills.py`.
- Docker image with `glider-server` and `glider-admin`, plus a Docker Compose
  quickstart with MinIO.

### Storage engine

- Segmented engine: immutable mutation logs (group commit), sealed packs
  of vector-local blocks with persisted five-bit routing sketches, run
  indexes and root generations; idle background maintenance for sealing,
  consolidation, pruning, reclamation and cleanup.
- Root v5: each root references per-run manifests, so a publication
  rewrites only the runs that changed.
- Global clustered view: centroids and cluster-contiguous postings, kept
  clustered by new seals and posting merges; built automatically at
  250,000 rows (`GLIDER_AUTO_CLUSTER_ROWS`) and rebuilt with more clusters
  as the collection grows (`GLIDER_AUTO_RECLUSTER_FACTOR`).
- Unfiltered queries read at most 8 remote ranges and 1 MiB and rerank
  exactly; queries run in parallel on immutable published views with
  read-your-writes.
- Disposable local block cache (RAM and SSD), authenticated against the
  selected root and warmed in the background.
- Open reads run indexes and sketch frames with up to 32 requests in
  flight and keeps digest-verified copies in the local cache for the next
  open (1,000,000 vectors on S3: open 3.15 s, reopen 4.00 s).
- Distances: squared Euclidean, Manhattan and cosine. Filters: one exact
  resident predicate, up to four routed keys, other equality filters
  post-filtered.
- S3-compatible backend with bounded, idempotent read retries; tested
  against MinIO in CI.
- The resident in-memory `Database` engine, chunked snapshots, compaction,
  streaming reads and IVF-Flat remain available as a library
  ([library guide](docs/LIBRARY.md)).

### Fixed before release

- A point too large for one block is rejected with `400` before it is
  acknowledged. Previously it was acknowledged, sealing then failed
  permanently, and after 64 unsealed log objects every write returned `503`.
- The admission queue holds 1 MiB (the maximum request size) instead of
  320 KiB, so a valid write between the two sizes is no longer refused with
  `429` on every retry. A query over 1 MiB is now `400`, not `429`.
- Framework rejections (malformed JSON, `404`, `405`, `413`, `415`, `422`)
  return the same `{"error": ...}` JSON body as other errors.

### Known limitations

- Single node: no replication, sharding, read replicas or standby; one
  collection per server process.
- Equality filters only; non-resident filters are approximate and may
  return fewer than `k` results.
- At 1,000,000 vectors, peak RSS exceeds the 192 MiB target, and open
  time on S3 exceeds its target
  ([benchmarks/M39.md](benchmarks/M39.md)).
- A point must fit in one 120 KiB storage block; larger points are rejected
  with `400` ([limits](docs/API.md#post-v1write)).
- No TLS; use a reverse proxy.

[Unreleased]: https://github.com/omerfeyzioglu/glider/compare/v1.0.1...HEAD
[1.0.1]: https://github.com/omerfeyzioglu/glider/releases/tag/v1.0.1
[1.0.0]: https://github.com/omerfeyzioglu/glider/releases/tag/v1.0.0
