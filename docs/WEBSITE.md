# Website

`site/` is static HTML, CSS and vanilla JavaScript with no build step. Preview:

```sh
python3 -m http.server 8765 --bind 127.0.0.1 --directory site
```

## Storage playground

The playground is a deterministic, browser-only simulation, not a benchmark;
no data is fetched. `site/sim-model.mjs` represents 100K, 1M or 10M synthetic
vectors by seeded ID generation and immutable block descriptors.
`site/sim-view.mjs` draws a 30K sample and animates queries, writes, sealing,
cache loss and fenced recovery; `site/sim.css` supplies the layout.
The query details show the computed synthetic top-10 IDs and squared Euclidean
distances in the 2D model. They are exact within the selected blocks and tail,
not exhaustive neighbors over the dataset or results from the Rust engine.
The seeded projection uses irregular Gaussian-mixture islands, spaced centres
and varied populations; projected clusters can overlap. Presets are fixed,
and “Run again” repeats the last point. Query execution fills the caches and
publishes statistics immediately; the interruptible 1.2-second animation
only illustrates that completed plan. Each panel keeps technical details
in a collapsed disclosure.
Queries highlight sampled members of the selected clusters and mark their
centres with small dots; the map legend reports the number of clusters searched.

The illustration uses the segmented structures and budgets in
[DESIGN.md](../DESIGN.md#segmented-serving): 12 candidates, at most 8 remote
ranges / 1 MiB, 32-log sealing, manifests and root publication, disposable
LRU caches and SSD-only idle warm-up. The selected dataset starts sealed and
clustered, even at 100K. Eight nominal 120 KiB blocks fit below the pack's
1 MiB block limit. The 2D projection uses simplified shared five-bit codes;
reranking examines every represented row in selected blocks plus the tail.
Sizes assume 128 dimensions and latency uses a simple explicit formula,
both disclosed below the playground. Neither predicts engine performance
or ANN recall. Lease waiting is compressed into the restart animation.

CI checks model determinism, read budgets, caching, LRU capacity,
publication/replay, acknowledged IDs surviving crashes, and view behavior
with interrupted or undelivered animation frames and reduced motion. These
checks validate the illustration; engine performance and recall require the
separate [benchmark protocol](../BENCHMARKS.md).

```sh
node --check site/sim-model.mjs
node --check site/sim-view.mjs
node --test site/sim-model.test.mjs
python3 -m unittest discover -s tests -p 'test_site_setup.py'
```

Check desktop and 375 px layouts, preset queries by keyboard and reduced
motion in a browser when editing the playground.
