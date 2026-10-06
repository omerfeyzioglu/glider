# Single-node correctness and admission hardening

The HTTP request-ID validation, immutable catalog lifecycle, bounded orphan
sweep and segmented index write-admission changes are validated on local storage
and the pinned disposable MinIO image. No AWS requests were made.

The full Rust release/server run at `23df585` passed 295 tests (34 ignored).
The final MinIO runner passed 32 checks in eight suites, including abrupt server
restart and the pinned conditional-DELETE capability test. Python: 66
passed. Formatting and release/server Clippy with all targets and warnings as
errors passed. Optional embedding providers and AWS/1M-scale workloads were not
exercised by this change.

## Cleanup requests

The [raw request counts](hardening-2026-10-06/sweep.json) compare a startup plus
its first minute tick with 100 active generations and 100 orphan generations,
one opaque object per generation, over 62 seconds. Collections stay closed;
the fixture measures catalog cleanup only. Source refs: before `10aa3ad`, after
`396d1c3` (the subsequent Clippy-only loop enumeration is equivalent).

| Request | Before | After |
|---|---:|---:|
| Catalog GET | 400 | 128 |
| Catalog LIST | 3 | 0 |
| History LIST | 0 | 100 |
| Data LIST | 103 | 30 |
| Data DELETE | 100 | 28 |

Catalog GETs fall 68%. The new limit deliberately leaves 72 orphans for later
ticks; the comparison measures bounded work/cost, not equal-work throughput.
Each batch checks at most 64 generations and deletes at most 64 objects. Full
data discovery and each orphan's key inventory still use complete listings.
Local cursor tests verify partial deletion, bounded checks, wraparound and
preservation of recreated generations. Removing the second periodic scheduler
also removes its repeated full sweep. Deletion cleans only its own generation.

The lifecycle protocol adds a history LIST to a cold existing-name lookup and a
state GET after lifecycle changes. First creation now checks current state before
publication. Warm vector writes and queries make no catalog requests. History
retains one small object per delete/recreate; it is never reclaimed. The pinned
MinIO ignores If-Match on DELETE, so generation-bound immutable state publication
uses conditional PUT rather than conditional DELETE.

## Index admission regression

The [raw paired runs](hardening-2026-10-06/index.json) retain all trial results and
medians. The workload is 10,000 synthetic SplitMix64 vectors (seed 42) at 128
and 768 dimensions, batches of 100 and 50 independent queries (seed XOR
`0xd1b54a32d192ed03`). Each batch advances maintenance until idle; a seal starts
at eight log objects. Cache, resident predicate, routed keys and automatic
conversion are disabled. Both profiles use a 128 MiB index watermark, far above
this fixture's charge. Exact search computes recall outside the query timer;
whole-process peak RSS includes input generation, maintenance and the oracle.

Before/after run order alternates, with three pairs per dimension on the same
macOS machine. Code compiled with opt-level 3 and locked dependencies. The
baseline binary was built before index admission from the release library at
`396d1c3`, whose segmented source is identical to `main` at `30ebc6c`; the
baseline probe has the same measured operations as the saved example, without
its untimed fresh-directory assertion. The after library is `23df585`. Raw files
record compiler/hardware, binary and probe-source hashes. These small synthetic
runs do not establish statistically significant tail latency or a million-row
memory envelope. The watermark is not a total-RSS cap; recovery, conversion,
directories and pinned views require separate resource measurements.

| Dimension | Metric | Before | After |
|---|---|---:|---:|
| 128 | 100-row write p50 (ms) | 13.78 | 13.83 |
| 128 | 100-row write p95 (ms) | 15.19 | 16.01 |
| 128 | Selective query p50 (ms) | 6.90 | 6.90 |
| 128 | Selective query p95 (ms) | 7.30 | 7.38 |
| 128 | Reopen (ms) | 82.12 | 79.85 |
| 128 | Whole-process peak RSS (MiB) | 18.52 | 18.27 |
| 768 | 100-row write p50 (ms) | 20.85 | 20.86 |
| 768 | 100-row write p95 (ms) | 28.73 | 28.65 |
| 768 | Selective query p50 (ms) | 12.43 | 12.58 |
| 768 | Selective query p95 (ms) | 13.61 | 14.03 |
| 768 | Reopen (ms) | 367.32 | 353.18 |
| 768 | Whole-process peak RSS (MiB) | 27.33 | 26.80 |

Result fingerprints and index charges match in every pair. Mean recall@10 is
0.974 at 128 dimensions and 0.902 at 768 in both versions. Write medians change
by +0.32% and approximately 0%; query p95 changes by +1.02% and +3.06%. These
samples do not establish causation or a tail-latency guarantee. Peak RSS stays
close in this small no-conversion fixture; it does not demonstrate a hard memory
limit or resolve the existing million-row conversion/recovery envelope.

## Reproduce

Build the current probes:

```sh
cargo build --release --locked --features server \
  --example index_admission_probe --example catalog_sweep_seed
```

Build the before probe in a separate temporary source directory, using the same
saved probe source and compiler configuration:

```sh
probe_before=$(mktemp -d)
git archive 396d1c3 | tar -x -C "$probe_before"
cp examples/index_admission_probe.rs "$probe_before/examples/"
cargo build --manifest-path "$probe_before/Cargo.toml" --release --locked \
  --features server --example index_admission_probe
python3 tools/bench_index_admission.py \
  --before-binary "$probe_before/target/release/examples/index_admission_probe" \
  --after-binary target/release/examples/index_admission_probe \
  --before-revision 396d1c3 --after-revision 23df585 \
  --output /tmp/index-admission-comparison.json
```

For the sweep comparison, build server binaries from `10aa3ad` and `396d1c3`
in separate source directories, then pass their paths. The runner owns and
removes an isolated local MinIO container, clears inherited AWS/Glider settings,
seeds distinct namespaces and records transport attempts without credentials:

```sh
python3 tools/bench_catalog_sweep.py \
  --before-server /absolute/path/to/before/glider-server \
  --after-server /absolute/path/to/after/glider-server \
  --output /tmp/catalog-sweep-comparison.json
```

Run comparisons serially with no competing compile/test workload. Both runners
require new output paths and fail on unsuccessful commands or mismatched query
results. For correctness, rerun `cargo test --release --locked --features server`,
`python3 tools/test_s3.py`, and
`python3 -m unittest discover -s tests -p 'test_*.py'`.
