# Exact versus IVF-Flat

Warm synthetic search; two invocations per setting. Ranges show both runs.
Latency includes result allocation/destruction. Build time is excluded from query latency.
Recall uses the exact oracle; distance counts include centroid routing. No timing gates.

| Data / algorithm | p50 µs | p95 µs | Recall@k % | Distances/query | Build ms | Raw runs |
|---|---:|---:|---:|---:|---:|---|
| uniform / exact | 33.21–39.33 | 48.79–67.71 | 100.00–100.00 | 512.00–512.00 | 0.00–0.00 | [r1](runs/7b94d88263bf8718258c95cafa1b1b7dea0fbc8ff1bccdb86ec54e286ee9d179.json), [r2](runs/b8d997a53516e65db34c8b958673178232248b571ecf54b3a87ba27c8081011c.json) |
| uniform / IVF probes=1 | 5.75–5.83 | 8.58–8.67 | 20.42–20.42 | 59.17–59.17 | 6.12–6.86 | [r1](runs/c7890cb201d998f6730bec0f8287d3c03ab9fc5617670c1d469b08b73c670d4c.json), [r2](runs/f6ee08a8ece3d228ab32d82f31031542c8f7309ea8b395ea6e4d73b7c5970046.json) |
| uniform / IVF probes=4 | 9.58–17.67 | 12.12–22.12 | 57.50–57.50 | 180.33–180.33 | 5.99–6.04 | [r1](runs/9808977efba210c72009fccdb29d5c1c4f6b2b2740955e50370314cfafb15db3.json), [r2](runs/d722dd8f4ecd24ae9d91ba18a40308d7caf1dfd48ff5ac52c7c21d01d0357786.json) |
| uniform / IVF probes=16 | 39.21–51.67 | 43.58–61.75 | 100.00–100.00 | 528.00–528.00 | 5.96–6.49 | [r1](runs/c7f93e7265ed4da4b58c0e1d30845857be3a99eb7f7214f479b27afcad77c4e5.json), [r2](runs/37341d90097560a64e0145337a6ff00082093a8dbe172eff7559e856f1196d58.json) |
| clustered / exact | 34.42–38.46 | 65.83–68.75 | 100.00–100.00 | 512.00–512.00 | 0.00–0.00 | [r1](runs/6535a761afb195a9ebaea2911ae6ff96f2d707a8f8eb8ab460aaba73123f9cbe.json), [r2](runs/48d97dd529009ad806832cacfeb5283d43312cbf2094835027736c727fbca0c8.json) |
| clustered / IVF probes=1 | 3.92–4.96 | 4.29–6.08 | 100.00–100.00 | 48.00–48.00 | 6.04–6.50 | [r1](runs/946392402b1feb3a30aa5915e178703bf2215f5e6bc85edfe5b31cd4c71ad5c7.json), [r2](runs/1c21d3746498e0021dfc565ebec6333ee5f32b1f00784666d1bbabb8225a1d0f.json) |
| clustered / IVF probes=4 | 14.33–14.38 | 14.83–15.46 | 100.00–100.00 | 144.00–144.00 | 6.18–6.32 | [r1](runs/08ec695b87c6318e49500279f8ff83b97ae4acfcd3b359f21ad7e57159a8b535.json), [r2](runs/85156b0e14c299ff980085473afdd83932c20ed62427fd56018195abd6319b01.json) |
| clustered / IVF probes=16 | 27.38–39.50 | 28.08–44.88 | 100.00–100.00 | 528.00–528.00 | 6.16–7.07 | [r1](runs/a5925b70cb84e0a9c4cde79bbaae0c0f706afab79e90ab973ef5d8d1506b2033.json), [r2](runs/2dc17834c4124466dee8f36bef7e5ab2ccc6c97aca6f0cb580881618b56ad43f.json) |

Rows=512, dimensions=64, k=10, seed=42, queries=24, batches=3.
Raw reports retain source/commit/environment, input fingerprints, all timings, answers, resources and backend counters.
Synthetic clustered data favors partitioning; uniform data is a stress case. Neither establishes production recall or tail latency.
