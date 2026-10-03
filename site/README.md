# Website

Static HTML, CSS and JavaScript; no package install or build step. Deploy this
directory with its assets intact. From the repository root:

```sh
python3 -m http.server 8765 --bind 127.0.0.1 --directory site
```

The query playground replays engine-recorded states from `explorer-data.json`.
The browser selects a recording; it does not implement a second search engine.
`src/segmented/explorer_tests.rs` generates the dataset (4,096 uniform seeded
vectors, 16 dimensions, seed 42), explicitly converts to 16 centroids and runs
36 combinations of two queries, three search breadths, three cache states and
before/after an acknowledged tail write. It records real routing, read spans,
cache sources, full-precision results and recall@5 against exact search.

The backend implements the in-memory ObjectStore contract; the SSD cache uses
a temporary filesystem directory. Cold starts with an empty cache, SSD uses
`warm_cache_step` to populate the cache, and RAM repeats the SSD query. Warm-up
reads are excluded from query counters. Both fresh vectors are committed in
one real request, without sealing. This dataset is for inspecting execution;
production performance measurements remain in the benchmark archive. No
latency or AWS transport measurement is presented in the playground.

```sh
GLIDER_EXPLORER_OUTPUT=site/explorer-data.json \
  cargo test --release --locked --lib website_recordings_match_clustered_engine
cargo test --release --locked --lib website_recordings_match_clustered_engine
node --check site/playground.mjs
node --test site/playground.test.mjs
```

The Rust test regenerates and compares every recorded state with the checked-in
JSON, normalizing random object identities into stable block IDs. It also checks
read budgets, cache-source counts and visibility of the new tail vectors.
CI runs it in the library test suite and tests all UI selections with Node.
Check desktop, mobile and keyboard interactions when changing the UI.
