# Glider

Glider is a Rust-based, object-storage-native vector database and search engine
for durable similarity search at scale.

The project is designed around a simple principle: search performance must grow
without weakening correctness, durability, or recovery guarantees.

## What Glider Is Building

Glider separates persistent truth from the structures used to serve queries:

```text
Client/API
  -> validation and query planning
  -> exact or approximate candidate generation
  -> metadata filtering and exact reranking
  -> deterministic top-k results

Mutation path
  -> durable commit protocol
  -> immutable logs and persistent segments
  -> published authoritative state
  -> rebuildable indexes and caches
```

Authoritative data is versioned, immutable where appropriate, and published only
when complete. In-memory state, search indexes, caches, and ANN structures are
derived from that data and can be rebuilt after restart or failure.

## Design Priorities

- **Correctness first:** exact search remains the reference for validating every
  approximate search optimization.
- **Object-storage-native durability:** the engine is designed around immutable
  objects and explicit publication semantics rather than filesystem-specific
  behavior.
- **Predictable recovery:** acknowledgement, crash behavior, recovery, and
  reader visibility are defined for every durable write path.
- **Measured performance:** changes are evaluated with reproducible workloads,
  fixed seeds, retained raw measurements, and before/after comparisons.
- **Composable evolution:** storage, persistence, query execution, filtering,
  and indexing remain separate so each can evolve without weakening the others.

## Evolution Path

Glider evolves from a durable exact-search foundation toward a complete vector
search engine:

1. Durable authoritative state and deterministic exact search.
2. S3-compatible object storage with the same correctness contract.
3. Immutable segments and compaction for efficient persistence and recovery.
4. Metadata filtering and query planning.
5. Rebuildable ANN indexes, evaluated against exact search using recall, latency,
   throughput, and resource measurements.
6. Further scaling mechanisms such as concurrency, sharding, and replication
   when workload measurements justify them.

Every stage preserves logical results and makes architectural trade-offs
measurable.

## Engineering Model

Glider treats persisted formats, manifests, segments, and indexes as explicit
versioned contracts. Derived indexes are never the only source of truth, and a
partial publication must never become authoritative.

Performance results are retained as reproducible benchmark artifacts so changes
can be compared against the same workload, environment, and implementation
revision over time.

See [DESIGN.md](DESIGN.md) for the architecture and durability model, and
[ROADMAP.md](ROADMAP.md) for the project development path.
