# Website

Static HTML, CSS and JavaScript; no package install or build step. Deploy this
directory with its assets intact. From the repository root:

```sh
python3 -m http.server 8765 --bind 127.0.0.1 --directory site
```

The playground searches a curated collection of 24 documentation summaries
using real text embeddings and engine-recorded results. Visitors choose one of
three questions, read linked documents, and repeat the search to see cached
reads. Centroid routing, read sources, budgets and recall are under “Inside the
search”; default search probes all four centroids. It uses cosine distance,
not invented relevance or confidence percentages.

`tests/fixtures/site-search-input.json` contains the document and question
embeddings. `tools/site_search_corpus.py` encodes them with the pinned
[all-MiniLM-L6-v2 model](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2)
revision in that script, on CPU with normalized 384-dimensional vectors. Model
inference happens when preparing the input, never in the user's browser or in
Glider. Regenerate embeddings when editing the curated text:

```sh
python3 -m venv /tmp/glider-site-embeddings-venv
/tmp/glider-site-embeddings-venv/bin/pip install sentence-transformers==5.1.1
/tmp/glider-site-embeddings-venv/bin/python tools/site_search_corpus.py
```

`src/segmented/explorer_tests.rs` writes those vectors through the actual
segmented engine, explicitly converts to four centroids (seed 42), and records
27 query/breadth/cache combinations in `explorer-data.json`. Results carry the
metadata of the indexed documents. Routing, range reads, cache-source counts
and recall@3 against exact search come from engine execution.

The recording backend implements the in-memory ObjectStore contract and the
block cache uses a temporary filesystem directory. Cold starts with an empty
cache; SSD uses `warm_cache_step`; RAM repeats the SSD query. Warm-up reads are
excluded from query counters. No network latency or AWS transport measurement
is presented. This small documentation corpus demonstrates retrieval and the
read path; production performance remains in the benchmark archive.

```sh
GLIDER_EXPLORER_OUTPUT=site/explorer-data.json \
  cargo test --release --locked --lib website_recordings_match_clustered_engine
cargo test --release --locked --lib website_recordings_match_clustered_engine
node --check site/playground.mjs
node --test site/playground.test.mjs
python3 -m unittest discover -s tests -p 'test_site_setup.py'
```

CI compares all recordings with regenerated engine results, checks read budgets,
cache counts, metadata and exact-search recall, and tests every UI selection.
Quickstart and MCP setup have separate numbered command cards and independent
Copy buttons; labels are excluded from clipboard payloads. Check desktop,
mobile and keyboard interactions when editing either interface.
