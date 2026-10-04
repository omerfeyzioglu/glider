# Changelog

All notable user-visible changes are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.0.0] - 2026-10-04

First public release: a single-node vector database with S3 as the
authoritative store.

### Server

- `glider-server` over HTTP/JSON. Without `GLIDER_DIMENSIONS` one server
  creates, lists, deletes and serves many collections
  (`/v1/collections`), each in its own namespace under a versioned catalog;
  collections open lazily, idle ones close after
  `GLIDER_COLLECTION_IDLE_SECONDS`, and the least recently used close beyond
  `GLIDER_MAX_OPEN_COLLECTIONS`.
- Atomic write batches acknowledged only after durable publication; every
  write carries a request ID, and resending it returns the original outcome.
- k-NN queries with optional metadata and vectors in results, `exact: true`
  for exhaustive search, and optional query profiles (search mode, latency,
  queue wait, remote reads).
- Metadata filters with equality, inequality, set membership, existence,
  numeric ranges and nested logic for queries and scans.
- `POST /v1/points/get` for up to 1000 points per request and `POST /v1/scan`
  to count, list and page matching points by ID.
- Optional server-side text embedding with a local ONNX model or any
  OpenAI-compatible endpoint: `POST /v1/embed` and `{"text": ...}` queries.
- Built-in web console at `/console` for collections, browsing points and
  text or vector search; `GLIDER_CONSOLE=0` disables it.
- Status, health and Prometheus metrics; optional bearer-token
  authentication ([API reference](docs/API.md)).
- Automatic takeover after a crash or restart: a renewed writer lease plus
  permanent fence objects, so a paused former owner cannot commit.
- `glider-admin` with `status`, `backup`, `restore` and `convert`.
- Multi-platform Docker image (`linux/amd64`, `linux/arm64`) on GitHub
  Container Registry, and a Docker Compose quickstart with MinIO.

### Storage engine

- Immutable mutation logs with group commit, sealed packs of vector-local
  blocks with persisted routing sketches, per-run manifests and root
  generations; idle background sealing, consolidation, pruning,
  reclamation and cleanup.
- Global clustered view (centroids and cluster-contiguous postings), built
  automatically at 250,000 rows and rebuilt with more clusters as the
  collection grows.
- Unfiltered queries read at most 8 remote ranges and 1 MiB and rerank
  exactly; queries run in parallel on immutable published views with
  read-your-writes.
- Disposable local block cache (RAM and SSD), authenticated against the
  selected root and warmed in the background.
- Distances: squared Euclidean, Manhattan and cosine.
- S3-compatible backend with bounded, idempotent read retries; tested on
  AWS S3 and MinIO.
- The resident in-memory engine, chunked snapshots, compaction, streaming
  reads and IVF-Flat remain available as a Rust library
  ([library guide](docs/LIBRARY.md)).

### Clients

- Dependency-free Python client (`clients/python`) and an MCP memory server
  (`glider-mcp`) with `remember`, `recall`, `forget` and `memory_count`.

### Known limitations

- Single node: no replication, sharding, read replicas or standby.
- Filtered approximate queries may return fewer than `k` results; use
  `exact: true` for exhaustive results.
- A point must fit in one 120 KiB storage block; larger points are rejected
  with `400` ([limits](docs/API.md#post-v1write)).
- No TLS; use a reverse proxy.

[Unreleased]: https://github.com/omerfeyzioglu/glider/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/omerfeyzioglu/glider/releases/tag/v1.0.0
