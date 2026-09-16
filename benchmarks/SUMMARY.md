# Benchmark summary

55 raw reports; 7 compatible before/after result pairs. History is preserved in `runs/` and `baselines/`.

Start here or with [latest.json](latest.json). Full tables/index: `python3 tools/benchmarks.py summary --full` (generates ignored `HISTORY.md` and `index.json`).

Latest means greatest recorded timestamp per backend/scope; ties use content hash and run index. Baselines require phase=baseline. Observations may be experiments with different workloads. Missing timestamps are excluded. These pointers never select comparison pairs.

| Kind | Backend / scope | Raw run | Commit | Workload | p50 ns | Objects |
|---|---|---|---|---|---|---|
| baseline | local / commit/delete | [de3314f9c482:0](runs/de3314f9c482c26d3fd25d46c10d160c41a3ab2ac87af15241ac10d2f46d4192.json) | d71a5ebc1ac6 | rows=10; dimensions=4; mutations=30; operations=5; queries=5; samples=2; seed=42 | 38950792 | N/A |
| baseline | local / commit/insert | [de3314f9c482:0](runs/de3314f9c482c26d3fd25d46c10d160c41a3ab2ac87af15241ac10d2f46d4192.json) | d71a5ebc1ac6 | rows=10; dimensions=4; mutations=30; operations=5; queries=5; samples=2; seed=42 | 37900500 | N/A |
| baseline | local / commit/overwrite | [de3314f9c482:0](runs/de3314f9c482c26d3fd25d46c10d160c41a3ab2ac87af15241ac10d2f46d4192.json) | d71a5ebc1ac6 | rows=10; dimensions=4; mutations=30; operations=5; queries=5; samples=2; seed=42 | 29967667 | N/A |
| baseline | local / recovery/total | [de3314f9c482:0](runs/de3314f9c482c26d3fd25d46c10d160c41a3ab2ac87af15241ac10d2f46d4192.json) | d71a5ebc1ac6 | rows=10; dimensions=4; mutations=30; operations=5; queries=5; samples=2; seed=42 | 231326125 | 31 |
| baseline | local / search | [48d97dd52900:0](runs/48d97dd529009ad806832cacfeb5283d43312cbf2094835027736c727fbca0c8.json) | 8d2a6b6242d1 | rows=512; dimensions=64; mutations=5000; operations=200; queries=24; samples=3; seed=42; distribution=clustered | 34417 | 513 |
| baseline | s3 / commit/delete | [af655c5cf5f7:0](runs/af655c5cf5f7cd9f15edf544ea46100853fde9ee2fadae9e098fc466cfa6c388.json) | d71a5ebc1ac6 | rows=10; dimensions=4; mutations=30; operations=5; queries=5; samples=2; seed=42 | 14542708 | N/A |
| baseline | s3 / commit/insert | [af655c5cf5f7:0](runs/af655c5cf5f7cd9f15edf544ea46100853fde9ee2fadae9e098fc466cfa6c388.json) | d71a5ebc1ac6 | rows=10; dimensions=4; mutations=30; operations=5; queries=5; samples=2; seed=42 | 12202833 | N/A |
| baseline | s3 / commit/overwrite | [af655c5cf5f7:0](runs/af655c5cf5f7cd9f15edf544ea46100853fde9ee2fadae9e098fc466cfa6c388.json) | d71a5ebc1ac6 | rows=10; dimensions=4; mutations=30; operations=5; queries=5; samples=2; seed=42 | 8328334 | N/A |
| baseline | s3 / recovery/total | [af655c5cf5f7:0](runs/af655c5cf5f7cd9f15edf544ea46100853fde9ee2fadae9e098fc466cfa6c388.json) | d71a5ebc1ac6 | rows=10; dimensions=4; mutations=30; operations=5; queries=5; samples=2; seed=42 | 15329125 | 31 |
| baseline | s3 / search | [af655c5cf5f7:0](runs/af655c5cf5f7cd9f15edf544ea46100853fde9ee2fadae9e098fc466cfa6c388.json) | d71a5ebc1ac6 | rows=10; dimensions=4; mutations=30; operations=5; queries=5; samples=2; seed=42 | 167 | 11 |
| observation | local / commit/delete | [de3314f9c482:0](runs/de3314f9c482c26d3fd25d46c10d160c41a3ab2ac87af15241ac10d2f46d4192.json) | d71a5ebc1ac6 | rows=10; dimensions=4; mutations=30; operations=5; queries=5; samples=2; seed=42 | 38950792 | N/A |
| observation | local / commit/insert | [de3314f9c482:0](runs/de3314f9c482c26d3fd25d46c10d160c41a3ab2ac87af15241ac10d2f46d4192.json) | d71a5ebc1ac6 | rows=10; dimensions=4; mutations=30; operations=5; queries=5; samples=2; seed=42 | 37900500 | N/A |
| observation | local / commit/overwrite | [de3314f9c482:0](runs/de3314f9c482c26d3fd25d46c10d160c41a3ab2ac87af15241ac10d2f46d4192.json) | d71a5ebc1ac6 | rows=10; dimensions=4; mutations=30; operations=5; queries=5; samples=2; seed=42 | 29967667 | N/A |
| observation | local / recovery/total | [8e922662f8e0:0](runs/8e922662f8e08c4a87fc61937389763456c862b684c416ba6bc743e56f908a58.json) | 2d2064a10749 | rows=100; dimensions=32; mutations=300; operations=200; queries=100; samples=3; seed=42; checkpoint_at=300; compact_at=300 | 37140458 | 2 |
| observation | local / search | [48d97dd52900:0](runs/48d97dd529009ad806832cacfeb5283d43312cbf2094835027736c727fbca0c8.json) | 8d2a6b6242d1 | rows=512; dimensions=64; mutations=5000; operations=200; queries=24; samples=3; seed=42; distribution=clustered | 34417 | 513 |
| observation | s3 / commit/delete | [af655c5cf5f7:0](runs/af655c5cf5f7cd9f15edf544ea46100853fde9ee2fadae9e098fc466cfa6c388.json) | d71a5ebc1ac6 | rows=10; dimensions=4; mutations=30; operations=5; queries=5; samples=2; seed=42 | 14542708 | N/A |
| observation | s3 / commit/insert | [af655c5cf5f7:0](runs/af655c5cf5f7cd9f15edf544ea46100853fde9ee2fadae9e098fc466cfa6c388.json) | d71a5ebc1ac6 | rows=10; dimensions=4; mutations=30; operations=5; queries=5; samples=2; seed=42 | 12202833 | N/A |
| observation | s3 / commit/overwrite | [af655c5cf5f7:0](runs/af655c5cf5f7cd9f15edf544ea46100853fde9ee2fadae9e098fc466cfa6c388.json) | d71a5ebc1ac6 | rows=10; dimensions=4; mutations=30; operations=5; queries=5; samples=2; seed=42 | 8328334 | N/A |
| observation | s3 / recovery/total | [78f2dbf2f5dc:0](runs/78f2dbf2f5dc871683a6c51e0a6f3337031afe7b75e709a4f721d73169e6be2e.json) | 2d2064a10749 | rows=100; dimensions=32; mutations=300; operations=200; queries=100; samples=3; seed=42; checkpoint_at=300; compact_at=300 | 2626916 | 2 |
| observation | s3 / search | [af655c5cf5f7:0](runs/af655c5cf5f7cd9f15edf544ea46100853fde9ee2fadae9e098fc466cfa6c388.json) | d71a5ebc1ac6 | rows=10; dimensions=4; mutations=30; operations=5; queries=5; samples=2; seed=42 | 167 | 11 |

Latest observed compaction (single maintenance operation; logical payload amplification):

| Backend | Latency ns | Written B | Removed objects | Additional write amplification | HTTP DELETE |
|---|---|---|---|---|---|
| local | 3537080000 | 36413 | 301 | 0.283 | N/A |
| s3 | 150212584 | 36413 | 301 | 0.283 | 301 |

Timing is descriptive, not a regression gate. Compare explicit raw reports with `python3 tools/benchmarks.py compare BEFORE AFTER`; incompatible or incomplete identities are rejected. Seeds, source fingerprints, environment, raw samples and backend metrics remain in the linked reports.
