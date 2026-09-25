# Filtered exact versus IVF-Flat

Synthetic vector-independent equality filter; every Nth document matches.
Recall uses an independent filtered exact oracle. Distance counts include centroid routing.
The short runs characterize quality and work counts, not production latency.

| Every Nth ID | Algorithm | Eligible | Mean recall@k % | Distances/query | Queries below exact | Raw run |
|---:|---|---:|---:|---:|---:|---|
| 1 | exact | 512 | 100.00 | 512.00 | 0/24 | [raw](filtering/runs/e358622d8cd1cf3db757b86af64061ebfa1054c020a340efa0f5c5fdc937bc9f.json) |
| 1 | IVF probes=1 | 512 | 20.42 | 59.17 | 0/24 | [raw](filtering/runs/b8ed600d639c509ea6ac03885226637b11d492cb4386e3983eda333817fbf5c1.json) |
| 1 | IVF probes=4 | 512 | 57.50 | 180.33 | 0/24 | [raw](filtering/runs/91511b6e8308d0c9e57f79a9a836352ab546f4c4537b1046c698488661ae9204.json) |
| 1 | IVF probes=16 | 512 | 100.00 | 528.00 | 0/24 | [raw](filtering/runs/ba38f95b54e56baf24083c4a1c904cf76f9aecb5cade44a9867fc35dbc18cb77.json) |
| 8 | exact | 64 | 100.00 | 64.00 | 0/24 | [raw](filtering/runs/15599eae350314173e3fbead67b8633969f14c421165e2abc01f79a98b82f073.json) |
| 8 | IVF probes=1 | 64 | 18.33 | 21.54 | 24/24 | [raw](filtering/runs/47b6b400239422de9a709bdb088a59001d3c9893c1c05439e894bf0e1ce9b2fe.json) |
| 8 | IVF probes=4 | 64 | 53.33 | 36.38 | 0/24 | [raw](filtering/runs/cf27dd8480e2009349f0b7808793ba48dc3251cc9f275a9dac435a0fd01b7411.json) |
| 8 | IVF probes=16 | 64 | 100.00 | 80.00 | 0/24 | [raw](filtering/runs/c2572bbc2b003a943e89f7a501489270595d1d220b9b05ce15d7f806204aaa6e.json) |
| 32 | exact | 16 | 100.00 | 16.00 | 0/24 | [raw](filtering/runs/7e79550a5b8c36c98ff003b1d1d7ff4e71c44a06cc8a4340267622e3105c0830.json) |
| 32 | IVF probes=1 | 16 | 10.00 | 17.38 | 24/24 | [raw](filtering/runs/e32c14c230be070b4adfd415bbfe3e60619904e87f760aae86b13efd5d872f5c.json) |
| 32 | IVF probes=4 | 16 | 37.08 | 20.96 | 24/24 | [raw](filtering/runs/c34a9a68b7bd3800c5f7e486fd4d83ce97c00a917215c9f3023868853b2ed414.json) |
| 32 | IVF probes=16 | 16 | 100.00 | 32.00 | 0/24 | [raw](filtering/runs/26bcf1ad9455636c874dbb47f97425b1b41d29f8028008aa7fc6be67166799f2.json) |

Rows=512, dimensions=64, k=10, seed=42, queries=24, batches=2.
All runs use the local backend and warm in-memory queries. Raw reports include source, environment, inputs, answers, counters and timing samples.
A vector-independent filter can be harder for IVF than a correlated one; no workload-wide recall or latency target follows from these results.
