# Benchmark summary

70 raw reports; 7 compatible before/after result pairs. History is preserved in `runs/` and `baselines/`.

Start here or with [latest.json](latest.json). Full tables/index: `python3 tools/benchmarks.py summary --full` (generates ignored `HISTORY.md` and `index.json`).

Latest means greatest recorded timestamp per backend/scope; ties use content hash and run index. Baselines require phase=baseline. Observations may be experiments with different workloads. Missing timestamps are excluded. These pointers never select comparison pairs.

| Kind | Backend / scope | Raw run | Commit | Workload | p50 ns | Objects |
|---|---|---|---|---|---|---|
| baseline | local / commit/delete | [500ee011a672:0](runs/500ee011a6721e6b6d96db6d6940640a70d2a938c10f0941573f302b17a3a74c.json) | 8791ebcfaae4 | rows=1000; dimensions=64; mutations=5000; operations=100; queries=100; samples=5; seed=42 | 12004708 | N/A |
| baseline | local / commit/insert | [500ee011a672:0](runs/500ee011a6721e6b6d96db6d6940640a70d2a938c10f0941573f302b17a3a74c.json) | 8791ebcfaae4 | rows=1000; dimensions=64; mutations=5000; operations=100; queries=100; samples=5; seed=42 | 17641917 | N/A |
| baseline | local / commit/overwrite | [500ee011a672:0](runs/500ee011a6721e6b6d96db6d6940640a70d2a938c10f0941573f302b17a3a74c.json) | 8791ebcfaae4 | rows=1000; dimensions=64; mutations=5000; operations=100; queries=100; samples=5; seed=42 | 18245250 | N/A |
| baseline | local / recovery/total | [aa0c760e4a6d:0](runs/aa0c760e4a6df6a065204e425f7f8412aee630080f1b0d3df5065e8c03281d97.json) | 8791ebcfaae4 | rows=200; dimensions=64; mutations=2000; operations=200; queries=100; samples=3; seed=42 | 173979125 | 2001 |
| baseline | local / search | [4ef7fee12085:0](runs/4ef7fee12085aa1645469fd7b0a6c4479250c2e6a13c0fc82153d04e985e0d4e.json) | 8791ebcfaae4 | rows=2000; dimensions=64; mutations=5000; operations=200; queries=1000; samples=1; seed=42; distribution=clustered; ivf_partitions=32; ivf_probes=8; ivf_iterations=8 | 7542 | 2001 |
| baseline | s3 / commit/delete | [fd3472ec0030:0](runs/fd3472ec00302f7188dc5e5267aab275ea6c4ed60bde76d6a0191236f1d17240.json) | 8791ebcfaae4 | rows=1000; dimensions=64; mutations=5000; operations=100; queries=100; samples=5; seed=42 | 969583 | N/A |
| baseline | s3 / commit/insert | [fd3472ec0030:0](runs/fd3472ec00302f7188dc5e5267aab275ea6c4ed60bde76d6a0191236f1d17240.json) | 8791ebcfaae4 | rows=1000; dimensions=64; mutations=5000; operations=100; queries=100; samples=5; seed=42 | 1015042 | N/A |
| baseline | s3 / commit/overwrite | [fd3472ec0030:0](runs/fd3472ec00302f7188dc5e5267aab275ea6c4ed60bde76d6a0191236f1d17240.json) | 8791ebcfaae4 | rows=1000; dimensions=64; mutations=5000; operations=100; queries=100; samples=5; seed=42 | 975625 | N/A |
| baseline | s3 / recovery/total | [f476cc0de892:0](runs/f476cc0de892b072e51721b0251dcaa6abd82992cfba3cd058dae02e3bac50b8.json) | 8791ebcfaae4 | rows=200; dimensions=64; mutations=2000; operations=200; queries=100; samples=3; seed=42 | 927690125 | 2001 |
| baseline | s3 / search | [dbee064bd603:0](runs/dbee064bd603a9c009b96e46aa308f035fadd06910fa016ea8e87aea0e675150.json) | 8791ebcfaae4 | rows=2000; dimensions=64; mutations=5000; operations=200; queries=1000; samples=1; seed=42; ivf_partitions=32; ivf_probes=8; ivf_iterations=8 | 6542 | 2001 |
| observation | local / commit/delete | [500ee011a672:0](runs/500ee011a6721e6b6d96db6d6940640a70d2a938c10f0941573f302b17a3a74c.json) | 8791ebcfaae4 | rows=1000; dimensions=64; mutations=5000; operations=100; queries=100; samples=5; seed=42 | 12004708 | N/A |
| observation | local / commit/insert | [500ee011a672:0](runs/500ee011a6721e6b6d96db6d6940640a70d2a938c10f0941573f302b17a3a74c.json) | 8791ebcfaae4 | rows=1000; dimensions=64; mutations=5000; operations=100; queries=100; samples=5; seed=42 | 17641917 | N/A |
| observation | local / commit/overwrite | [500ee011a672:0](runs/500ee011a6721e6b6d96db6d6940640a70d2a938c10f0941573f302b17a3a74c.json) | 8791ebcfaae4 | rows=1000; dimensions=64; mutations=5000; operations=100; queries=100; samples=5; seed=42 | 18245250 | N/A |
| observation | local / recovery/total | [aa0c760e4a6d:0](runs/aa0c760e4a6df6a065204e425f7f8412aee630080f1b0d3df5065e8c03281d97.json) | 8791ebcfaae4 | rows=200; dimensions=64; mutations=2000; operations=200; queries=100; samples=3; seed=42 | 173979125 | 2001 |
| observation | local / search | [4ef7fee12085:0](runs/4ef7fee12085aa1645469fd7b0a6c4479250c2e6a13c0fc82153d04e985e0d4e.json) | 8791ebcfaae4 | rows=2000; dimensions=64; mutations=5000; operations=200; queries=1000; samples=1; seed=42; distribution=clustered; ivf_partitions=32; ivf_probes=8; ivf_iterations=8 | 7542 | 2001 |
| observation | s3 / commit/delete | [fd3472ec0030:0](runs/fd3472ec00302f7188dc5e5267aab275ea6c4ed60bde76d6a0191236f1d17240.json) | 8791ebcfaae4 | rows=1000; dimensions=64; mutations=5000; operations=100; queries=100; samples=5; seed=42 | 969583 | N/A |
| observation | s3 / commit/insert | [fd3472ec0030:0](runs/fd3472ec00302f7188dc5e5267aab275ea6c4ed60bde76d6a0191236f1d17240.json) | 8791ebcfaae4 | rows=1000; dimensions=64; mutations=5000; operations=100; queries=100; samples=5; seed=42 | 1015042 | N/A |
| observation | s3 / commit/overwrite | [fd3472ec0030:0](runs/fd3472ec00302f7188dc5e5267aab275ea6c4ed60bde76d6a0191236f1d17240.json) | 8791ebcfaae4 | rows=1000; dimensions=64; mutations=5000; operations=100; queries=100; samples=5; seed=42 | 975625 | N/A |
| observation | s3 / recovery/total | [f476cc0de892:0](runs/f476cc0de892b072e51721b0251dcaa6abd82992cfba3cd058dae02e3bac50b8.json) | 8791ebcfaae4 | rows=200; dimensions=64; mutations=2000; operations=200; queries=100; samples=3; seed=42 | 927690125 | 2001 |
| observation | s3 / search | [dbee064bd603:0](runs/dbee064bd603a9c009b96e46aa308f035fadd06910fa016ea8e87aea0e675150.json) | 8791ebcfaae4 | rows=2000; dimensions=64; mutations=5000; operations=200; queries=1000; samples=1; seed=42; ivf_partitions=32; ivf_probes=8; ivf_iterations=8 | 6542 | 2001 |

Timing is descriptive, not a regression gate. Compare explicit raw reports with `python3 tools/benchmarks.py compare BEFORE AFTER`; incompatible or incomplete identities are rejected. Seeds, source fingerprints, environment, raw samples and backend metrics remain in the linked reports.
