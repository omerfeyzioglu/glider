# Tools

Names such as `m24_` and `m37_` refer to milestones in [ROADMAP.md](../ROADMAP.md); their measurements are in [benchmarks/](../benchmarks/).

| File | What it does |
|---|---|
| `ann_benchmark.py` | Runs a short exact-versus-IVF comparison. |
| `aws_acceptance.py` | Runs acceptance on a tagged EC2 instance beside S3 Standard. |
| `aws_result.py` | Assembles an in-region acceptance report from example output. |
| `aws_user_data.sh` | Boots the EC2 acceptance run and terminates its instance afterward. |
| `benchmark_smoke.py` | Validates small, real all-scenario benchmark output. |
| `benchmarks.py` | Archives raw benchmark reports and makes deterministic comparisons. |
| `compaction_benchmark.py` | Measures mutation, checkpoint, compaction and warm recovery. |
| `drills.py` | Runs local crash, writer, cache, backup, restore and conversion drills. |
| `filter_benchmark.py` | Compares reproducible filtered exact and IVF quality. |
| `m10_benchmark.py` | Measures long-tail recovery after individual and batched writes. |
| `m11_benchmark.py` | Runs the filtered streaming probe on disposable MinIO. |
| `m13_soak.py` | Runs a paced six-process MinIO serving soak or a short smoke run. |
| `m16_benchmark.py` | Compares mutex and worker serving on disposable MinIO. |
| `m19_benchmark.py` | Runs bounded SIFT capacity steps and a selected-envelope rehearsal. |
| `m24_acceptance.py` | Runs segmented SIFT1M acceptance and restart checks on MinIO. |
| `m24_layout_probe.py` | Measures the eight-block recall ceiling of the SIFT1M layout. |
| `m24_vector_layout_probe.py` | Probes balanced vector-local block quality offline. |
| `minio_harness.py` | Provides bounded subprocesses and diagnostics for disposable MinIO tests. |
| `profile_search.py` | Samples the warm search path with the macOS profiler. |
| `s3_pilot.py` | Runs a bounded provider correctness probe, using MinIO in CI. |
| `search_benchmark.py` | Characterizes warm exact search with the Cargo harness. |
| `test_s3.py` | Runs S3 integration tests in an isolated disposable MinIO container. |
