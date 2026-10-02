# Examples

Names such as `m24_` and `m37_` refer to milestones in [ROADMAP.md](../ROADMAP.md); their measurements are in [benchmarks/](../benchmarks/).

| File | What it does |
|---|---|
| `ivf_cache_benchmark.rs` | Measures local IVF training against loading a persisted cache. |
| `m11_filter_probe.rs` | Measures filtered streaming queries, with setup and oracle work untimed. |
| `m12_quality_gate.rs` | Computes ANN layout costs from serialized candidate objects for a feasibility gate. |
| `m13_soak.rs` | Runs one restartable epoch of the serving soak under a MinIO supervisor. |
| `m16_concurrency.rs` | Runs one bounded MinIO concurrency comparison phase. |
| `m19_capacity.rs` | Runs a bounded SIFT capacity study. |
| `m22_capacity.rs` | Probes segmented SIFT1M load and recovery. |
| `m23_cache.rs` | Probes the block cache on an existing segmented SIFT1M namespace. |
| `m24_acceptance.rs` | Loads, serves and verifies the segmented SIFT1M acceptance workload across processes. |
| `m24_layout_probe.rs` | Probes exact-oracle bounds and sketch routing for segmented blocks. |
| `m30_filter_probe.rs` | Measures filtered selective-search quality on the SIFT1M layout. |
| `m31_latency_probe.rs` | Runs a reproducible local object-store admission workload for latency investigation. |
| `m37_cluster_probe.rs` | Probes offline clustering on an fvecs corpus prefix. |
| `m37_conversion_probe.rs` | Measures recall of the clustered layout after conversion. |
| `m39_open_probe.rs` | Probes open time with emulated first-byte latency and bandwidth. |
| `s3_pilot.rs` | Runs a small provider probe under the pilot runner. |
| `streaming_memory.rs` | Compares resident memory use for full and streaming exact readers. |
| `support/pilot_transport.rs` | Supplies the shared HTTP request budget for the provider probe. |
