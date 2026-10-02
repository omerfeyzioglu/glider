# Changelog

All notable user-visible changes are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
uses [Semantic Versioning](https://semver.org/).

## [1.0.0] - Unreleased

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
- Docker image and a Docker Compose quickstart with MinIO.

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
- Distances: squared Euclidean, Manhattan and cosine. Filters: one exact
  resident predicate, up to four routed keys, other equality filters
  post-filtered.
- S3-compatible backend with bounded, idempotent read retries; tested
  against MinIO in CI.
- The resident in-memory `Database` engine, chunked snapshots, compaction,
  streaming reads and IVF-Flat remain available as a library
  ([library guide](docs/LIBRARY.md)).

### Known limitations

- Single node: no replication, sharding, read replicas or standby; one
  collection per server process.
- Equality filters only; non-resident filters are approximate and may
  return fewer than `k` results.
- At 1,000,000 vectors, peak RSS exceeds the 192 MiB target, and open
  time and write p95 on S3 exceed their targets
  ([benchmarks/M39.md](benchmarks/M39.md)).
- A point larger than the 120 KiB block limit is acknowledged but blocks
  sealing ([details](docs/API.md#known-issues)).
- No TLS; use a reverse proxy.

[1.0.0]: https://github.com/omerfeyzioglu/glider/tree/main
