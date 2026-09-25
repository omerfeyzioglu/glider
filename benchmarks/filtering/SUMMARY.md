# Benchmark summary

12 raw reports; 0 compatible before/after result pairs. History is preserved in `runs/` and `baselines/`.

Start here or with [latest.json](latest.json). Full tables/index: `python3 tools/benchmarks.py summary --full` (generates ignored `HISTORY.md` and `index.json`).

Latest means greatest recorded timestamp per backend/scope; ties use content hash and run index. Baselines require phase=baseline. Observations may be experiments with different workloads. Missing timestamps are excluded. These pointers never select comparison pairs.

| Kind | Backend / scope | Raw run | Commit | Workload | p50 ns | Objects |
|---|---|---|---|---|---|---|
| baseline | local / search | [26bcf1ad9455:0](runs/26bcf1ad9455636c874dbb47f97425b1b41d29f8028008aa7fc6be67166799f2.json) | edbdf3e41498 | rows=512; dimensions=64; mutations=5000; operations=200; queries=24; samples=2; seed=42; ivf_partitions=16; ivf_probes=16; ivf_iterations=8 | 16042 | 513 |
| observation | local / search | [26bcf1ad9455:0](runs/26bcf1ad9455636c874dbb47f97425b1b41d29f8028008aa7fc6be67166799f2.json) | edbdf3e41498 | rows=512; dimensions=64; mutations=5000; operations=200; queries=24; samples=2; seed=42; ivf_partitions=16; ivf_probes=16; ivf_iterations=8 | 16042 | 513 |

Timing is descriptive, not a regression gate. Compare explicit raw reports with `python3 tools/benchmarks.py compare BEFORE AFTER`; incompatible or incomplete identities are rejected. Seeds, source fingerprints, environment, raw samples and backend metrics remain in the linked reports.
