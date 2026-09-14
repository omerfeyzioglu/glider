# Benchmark archive

Generated from raw JSON; do not edit. Full metadata and exact metric keys: [index.json](index.json).

| Raw reports | Runs | Result rows | Valid pairs | Repeatability pairs | Unpaired before/after rows |
|---|---|---|---|---|---|
| 12 | 20 | 65 | 1 | 1 | 18 |

## Before / after

Pairs require one before and one after with identical backend, workload, inputs, protocol and stable environment. Delta is (after − before) / before; positive means an increase, not necessarily an improvement.
Repeatability and local-vs-S3 smoke results are not feature improvements.

### archive-validation / aa-search-100-d32 / search

| Backend | Before → after | Samples each | Latency scope | Interpretation |
|---|---|---|---|---|
| local | R10 → R20 | 500 | individual query | Repeatability; no feature effect |

| Metric | Before | After | Change % |
|---|---|---|---|
| p50 (ns) | 3084 | 1417 | -54.053 |
| p95 (ns) | 3625 | 1750 | -51.724 |
| p99 (ns) | 3750 | 1875 | -50 |
| Max (ns) | 13708 | 2916 | -78.728 |
| Ops/s | 313635.557 | 688706.183 | 119.588 |
| User CPU (ns) | 1589000 | 725000 | -54.374 |
| System CPU (ns) | 6000 | 4000 | -33.333 |
| Peak RSS (B) | 2490368 | 2490368 | 0 |
| Read (B) | 0 | 0 | N/A |
| Written (B) | 0 | 0 | N/A |
| GET | 0 | 0 | N/A |
| CREATE | 0 | 0 | N/A |
| Objects | 101 | 101 | 0 |
| Files | 202 | 202 | 0 |
| File bytes | 48561 | 48561 | 0 |
| LIST | 0 | 0 | N/A |

Unpaired runs: R11, R12, R13, R16, R17, R19. Missing counterpart/metadata, incompatible inputs/environment, or duplicate phase; no percentage is inferred.

## Run catalog

Run references apply to every table below. All archived runs are retained; no latest-run selection or averaging.

| Run / raw JSON | Backend | Feature | Phase | Group | Git (short) | Env | Workload |
|---|---|---|---|---|---|---|---|
| [R1](baselines/2026-09-13-local.json) | local | N/A | N/A | N/A | a374b6507c13 | E1 | scenario=search; dimensions=32; seed=42; rows=1000; queries=100; samples=5; k=10 |
| [R2](baselines/2026-09-13-local.json) | local | N/A | N/A | N/A | a374b6507c13 | E1 | scenario=search; dimensions=32; seed=42; rows=10000; queries=100; samples=5; k=10 |
| [R3](baselines/2026-09-13-local.json) | local | N/A | N/A | N/A | a374b6507c13 | E1 | scenario=commit; dimensions=32; seed=42; operations=200 |
| [R4](baselines/2026-09-13-local.json) | local | N/A | N/A | N/A | a374b6507c13 | E1 | scenario=search; dimensions=128; seed=42; rows=1000; queries=100; samples=5; k=10 |
| [R5](baselines/2026-09-13-local.json) | local | N/A | N/A | N/A | a374b6507c13 | E1 | scenario=search; dimensions=128; seed=42; rows=10000; queries=100; samples=5; k=10 |
| [R6](baselines/2026-09-13-local.json) | local | N/A | N/A | N/A | a374b6507c13 | E1 | scenario=commit; dimensions=128; seed=42; operations=200 |
| [R7](baselines/2026-09-13-local.json) | local | N/A | N/A | N/A | a374b6507c13 | E1 | scenario=recovery; dimensions=32; seed=42; rows=100; mutations=100; samples=5 |
| [R8](baselines/2026-09-13-local.json) | local | N/A | N/A | N/A | a374b6507c13 | E1 | scenario=recovery; dimensions=32; seed=42; rows=100; mutations=1000; samples=5 |
| [R9](baselines/2026-09-13-local.json) | local | N/A | N/A | N/A | a374b6507c13 | E1 | scenario=recovery; dimensions=32; seed=42; rows=100; mutations=5000; samples=5 |
| [R10](runs/0306843a08c0edb18119fc4bd312343e203749bb1687b7dfcd8fd3687f6a3227.json) | local | archive-validation | before | aa-search-100-d32 | 06043457317a | E6 | scenario=search; dimensions=32; seed=42; rows=100; queries=100; samples=5; k=10 |
| [R11](runs/2c457dc87323c82c4d5b779c2aca2b902f8d603267bd1eca05a64f6ddcf2caf2.json) | local | segments | after | m3-recovery-300 | 47a29007ee91 | E4 | scenario=recovery; dimensions=32; seed=42; checkpoint_at=270; rows=30; mutations=300; samples=5 |
| [R12](runs/47f6213493e70c4bc9603311e1af874518d0ffc000b563814b6a1fc4a3ad50d6.json) | local | segments | before | m3-recovery-1000 | 47a29007ee91 | E2 | scenario=recovery; dimensions=32; seed=42; rows=100; mutations=1000; samples=5 |
| [R13](runs/5037572df8e046401eafe7545a7127ff6263c02e486c61a434e6ffdf7052889b.json) | s3 | segments | before | m3-recovery-300 | 47a29007ee91 | E3 | scenario=recovery; dimensions=32; seed=42; rows=30; mutations=300; samples=5 |
| [R14](runs/73aef1437e76edc44d9f2e0282c28d66da452bf9ad1fba9054f33a69c8d5d4c9.json) | local | m1 | baseline | local-v2 | 06043457317a | E5 | scenario=all; dimensions=32; seed=42; rows=100; mutations=300; operations=30; queries=100; samples=5; k=10 |
| [R15](runs/af655c5cf5f7cd9f15edf544ea46100853fde9ee2fadae9e098fc466cfa6c388.json) | s3 | s3-benchmarks | baseline | backend-smoke | d71a5ebc1ac6 | E7 | scenario=all; dimensions=4; seed=42; rows=10; mutations=30; operations=5; queries=5; samples=2; k=3 |
| [R16](runs/c4ccabbc634a4f9c9994f89c0b277754f1d893993a6d3afd881a7117585b5615.json) | local | segments | after | m3-recovery-1000 | 47a29007ee91 | E2 | scenario=recovery; dimensions=32; seed=42; checkpoint_at=900; rows=100; mutations=1000; samples=5 |
| [R17](runs/d56d3a592a525ab8c0d9cd3f65a307dd899c40cd8d750b0da0cac65f843cd09d.json) | local | segments | before | m3-recovery-300 | 47a29007ee91 | E4 | scenario=recovery; dimensions=32; seed=42; rows=30; mutations=300; samples=5 |
| [R18](runs/de3314f9c482c26d3fd25d46c10d160c41a3ab2ac87af15241ac10d2f46d4192.json) | local | s3-benchmarks | baseline | backend-smoke | d71a5ebc1ac6 | E8 | scenario=all; dimensions=4; seed=42; rows=10; mutations=30; operations=5; queries=5; samples=2; k=3 |
| [R19](runs/edc3eed7d19266c18f95077c2ed9fd7c70f289eabc0054f83e05dbce8fc75596.json) | s3 | segments | after | m3-recovery-300 | 47a29007ee91 | E3 | scenario=recovery; dimensions=32; seed=42; checkpoint_at=270; rows=30; mutations=300; samples=5 |
| [R20](runs/ff77a79fbdca3098c81e51def0bffbc300455e2aaf21862cede9ca0080947d81.json) | local | archive-validation | after | aa-search-100-d32 | 06043457317a | E6 | scenario=search; dimensions=32; seed=42; rows=100; queries=100; samples=5; k=10 |

### Environments

| Env | CPU | Arch | Conditions | S3 service |
|---|---|---|---|---|
| E1 | Apple M4 | aarch64 | Apple M4; internal APFS SSD; FileVault enabled; desktop session; power and competing load uncontrolled | N/A |
| E2 | Apple M4 | aarch64 | Apple M4; internal APFS SSD; desktop load and power uncontrolled | N/A |
| E3 | Apple M4 | aarch64 | M3 recovery experiment; desktop load and power uncontrolled | http://127.0.0.1:58848; us-east-1; glider-test; quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e; Docker 29.1.3 |
| E4 | Apple M4 | aarch64 | M3 recovery experiment; desktop load and power uncontrolled | N/A |
| E5 | Apple M4 | aarch64 | internal APFS SSD; desktop load uncontrolled | N/A |
| E6 | Apple M4 | aarch64 | same-code repeatability; internal APFS SSD; load uncontrolled | N/A |
| E7 | Apple M4 | aarch64 | smoke validation; desktop session; power and competing load uncontrolled | http://127.0.0.1:54814; us-east-1; glider-test; quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e; Docker 29.1.3 |
| E8 | Apple M4 | aarch64 | smoke validation; desktop session; power and competing load uncontrolled | N/A |

## Results by backend and scenario

Latency and CPU: ns; throughput: operations/s; memory, I/O and footprints: bytes. N/A means unavailable or not applicable, never zero. Entirely unmeasured metric columns/rows are omitted.
Samples count timed queries (queries × batches), legacy query batches, individual commits, or reopens according to the latency scope.

### local / commit

**Latency**

| Run | Scope | Samples | Latency scope | p50 (ns) | p95 (ns) | p99 (ns) | Max (ns) | Ops/s |
|---|---|---|---|---|---|---|---|---|
| R3 | insert | 200 | individual commit | 17955250 | 22353083 | N/A | 25273333 | 59.059 |
| R3 | overwrite | 200 | individual commit | 19089292 | 22123958 | N/A | 30939834 | 54.6 |
| R3 | delete | 200 | individual commit | 18890084 | 21224042 | N/A | 34016208 | 57.791 |
| R6 | insert | 200 | individual commit | 19043958 | 23649625 | N/A | 37305875 | 54.8 |
| R6 | overwrite | 200 | individual commit | 18714500 | 21234459 | N/A | 34069833 | 59.214 |
| R6 | delete | 200 | individual commit | 17127416 | 23103583 | N/A | 44922458 | 59.763 |
| R14 | insert | 30 | individual commit | 9045667 | 14022042 | 18788292 | 18788292 | 102.418 |
| R14 | overwrite | 30 | individual commit | 9826125 | 13994458 | 15957458 | 15957458 | 101.343 |
| R14 | delete | 30 | individual commit | 9759375 | 10061916 | 14983542 | 14983542 | 103.908 |
| R18 | insert | 5 | individual commit | 37900500 | 44990959 | 44990959 | 44990959 | 28.09 |
| R18 | overwrite | 5 | individual commit | 29967667 | 41012417 | 41012417 | 41012417 | 30.897 |
| R18 | delete | 5 | individual commit | 38950792 | 41042375 | 41042375 | 41042375 | 27.483 |

**Client process resources**

| Run | Scope | User CPU (ns) | System CPU (ns) | Peak RSS (B) |
|---|---|---|---|---|
| R14 | insert | 793000 | 15207000 | 2654208 |
| R14 | overwrite | 1356000 | 26088000 | 2654208 |
| R14 | delete | 1084000 | 24918000 | 2670592 |
| R18 | insert | 259000 | 5252000 | 2408448 |
| R18 | overwrite | 236000 | 4725000 | 2473984 |
| R18 | delete | 254000 | 5286000 | 2473984 |

**Logical I/O**

| Run | Scope | GET | LIST | CREATE | Read (B) | Written (B) |
|---|---|---|---|---|---|---|
| R3 | insert | 0 | 0 | 200 | 0 | 85820 |
| R3 | overwrite | 0 | 0 | 200 | 0 | 85909 |
| R3 | delete | 0 | 0 | 200 | 0 | 13090 |
| R6 | insert | 0 | 0 | 200 | 0 | 299466 |
| R6 | overwrite | 0 | 0 | 200 | 0 | 299579 |
| R6 | delete | 0 | 0 | 200 | 0 | 13090 |
| R14 | insert | 0 | 0 | 30 | 0 | 12845 |
| R14 | overwrite | 0 | 0 | 30 | 0 | 12848 |
| R14 | delete | 0 | 0 | 30 | 0 | 1910 |
| R18 | insert | 0 | 0 | 5 | 0 | 578 |
| R18 | overwrite | 0 | 0 | 5 | 0 | 572 |
| R18 | delete | 0 | 0 | 5 | 0 | 315 |

**Footprint**

| Run | Scope | Live docs | Mutation history | Objects | Files | File bytes |
|---|---|---|---|---|---|---|
| R3 | final-footprint | 0 | 600 | 601 | 1202 | 218544 |
| R6 | final-footprint | 0 | 600 | 601 | 1202 | 645861 |
| R14 | final-footprint | 0 | 90 | 91 | 182 | 32768 |
| R18 | final-footprint | 0 | 15 | 16 | 32 | 2429 |

### local / recovery

**Latency**

| Run | Scope | Samples | Latency scope | p50 (ns) | p95 (ns) | p99 (ns) | Max (ns) | Ops/s |
|---|---|---|---|---|---|---|---|---|
| R7 | total | 5 | full open | 15075667 | 199010750 | N/A | 199010750 | 18.681 |
| R7 | local-store | 5 | local store open | 8659500 | 193292834 | N/A | 193292834 | 22.076 |
| R7 | replay | 5 | database replay | 6551375 | 14306125 | N/A | 14306125 | 121.476 |
| R8 | total | 5 | full open | 142351750 | 223719209 | N/A | 223719209 | 6.4 |
| R8 | local-store | 5 | local store open | 71201625 | 152984417 | N/A | 152984417 | 11.565 |
| R8 | replay | 5 | database replay | 70734750 | 80883042 | N/A | 80883042 | 14.332 |
| R9 | total | 5 | full open | 481341500 | 507653000 | N/A | 507653000 | 2.054 |
| R9 | local-store | 5 | local store open | 223797708 | 253944209 | N/A | 253944209 | 4.335 |
| R9 | replay | 5 | database replay | 249706125 | 286996334 | N/A | 286996334 | 3.902 |
| R11 | total | 5 | full open | 18703833 | 57837041 | 57837041 | 57837041 | 36.862 |
| R11 | local-store | 5 | local store open | 11436541 | 50289791 | 50289791 | 50289791 | 50.682 |
| R11 | replay | 5 | database replay | 7465250 | 8319084 | 8319084 | 8319084 | 135.198 |
| R12 | total | 5 | full open | 81188375 | 96541958 | 96541958 | 96541958 | 11.834 |
| R12 | local-store | 5 | local store open | 39934000 | 54776125 | 54776125 | 54776125 | 23.87 |
| R12 | replay | 5 | database replay | 41765542 | 47775875 | 47775875 | 47775875 | 23.469 |
| R14 | total | 5 | full open | 28083500 | 29289375 | 29289375 | 29289375 | 35.94 |
| R14 | local-store | 5 | local store open | 13278625 | 14049042 | 14049042 | 14049042 | 75.969 |
| R14 | replay | 5 | database replay | 14716125 | 15941000 | 15941000 | 15941000 | 68.209 |
| R16 | total | 5 | full open | 60323042 | 85390583 | 85390583 | 85390583 | 15.436 |
| R16 | local-store | 5 | local store open | 37144292 | 62272166 | 62272166 | 62272166 | 23.992 |
| R16 | replay | 5 | database replay | 23118208 | 24551875 | 24551875 | 24551875 | 43.287 |
| R17 | total | 5 | full open | 23295667 | 42436875 | 42436875 | 42436875 | 36.983 |
| R17 | local-store | 5 | local store open | 10916166 | 27129166 | 27129166 | 27129166 | 70.467 |
| R17 | replay | 5 | database replay | 12425791 | 15307333 | 15307333 | 15307333 | 77.833 |
| R18 | total | 2 | full open | 231326125 | 240214833 | 240214833 | 240214833 | 4.241 |
| R18 | local-store | 2 | local store open | 226846166 | 236420333 | 236420333 | 236420333 | 4.317 |
| R18 | replay | 2 | database replay | 3793208 | 4478792 | 4478792 | 4478792 | 241.779 |

**Client process resources**

| Run | Scope | User CPU (ns) | System CPU (ns) | Peak RSS (B) |
|---|---|---|---|---|
| R11 | total | 10785000 | 90419000 | 2883584 |
| R12 | total | 48011000 | 347538000 | 2555904 |
| R14 | total | 15723000 | 121526000 | 2998272 |
| R16 | total | 34403000 | 264613000 | 2654208 |
| R17 | total | 15808000 | 112333000 | 2473984 |
| R18 | total | 1799000 | 20547000 | 2686976 |

**Logical I/O**

| Run | Scope | GET | LIST | CREATE | Read (B) | Written (B) |
|---|---|---|---|---|---|---|
| R7 | total | 505 | 5 | 0 | 214525 | 0 |
| R7 | replay | 505 | 5 | 0 | 214525 | 0 |
| R8 | total | 5005 | 5 | 0 | 2144975 | 0 |
| R8 | replay | 5005 | 5 | 0 | 2144975 | 0 |
| R9 | total | 25005 | 5 | 0 | 10744045 | 0 |
| R9 | replay | 25005 | 5 | 0 | 10744045 | 0 |
| R11 | total | 160 | 5 | 0 | 119665 | 0 |
| R11 | replay | 160 | 5 | 0 | 119665 | 0 |
| R12 | total | 5005 | 5 | 0 | 2144975 | 0 |
| R12 | replay | 5005 | 5 | 0 | 2144975 | 0 |
| R14 | total | 1505 | 5 | 0 | 643470 | 0 |
| R14 | replay | 1505 | 5 | 0 | 643470 | 0 |
| R16 | total | 510 | 5 | 0 | 396985 | 0 |
| R16 | replay | 510 | 5 | 0 | 396985 | 0 |
| R17 | total | 1505 | 5 | 0 | 643120 | 0 |
| R17 | replay | 1505 | 5 | 0 | 643120 | 0 |
| R18 | total | 62 | 2 | 0 | 7050 | 0 |
| R18 | replay | 62 | 2 | 0 | 7050 | 0 |

**Footprint**

| Run | Scope | Live docs | Mutation history | Objects | Files | File bytes |
|---|---|---|---|---|---|---|
| R7 | total | 100 | 100 | 101 | 202 | 48561 |
| R8 | total | 100 | 1000 | 1001 | 2002 | 485051 |
| R9 | total | 100 | 5000 | 5001 | 10002 | 2428865 |
| R11 | total | 30 | 300 | 302 | 604 | 156541 |
| R12 | total | 100 | 1000 | 1001 | 2002 | 485051 |
| R14 | total | 100 | 300 | 301 | 602 | 145550 |
| R16 | total | 100 | 1000 | 1002 | 2004 | 521531 |
| R17 | total | 30 | 300 | 301 | 602 | 145480 |
| R18 | total | 10 | 30 | 31 | 62 | 5261 |

### local / search

**Latency**

| Run | Scope | Samples | Latency scope | p50 (ns) | p95 (ns) | p99 (ns) | Max (ns) | Ops/s |
|---|---|---|---|---|---|---|---|---|
| R1 | search | 5 | query batch | 3990709 | 4015209 | N/A | 4015209 | 26622.058 |
| R2 | search | 5 | query batch | 23854958 | 27396042 | N/A | 27396042 | 4071.588 |
| R4 | search | 5 | query batch | 7594833 | 9704459 | N/A | 9704459 | 12660.458 |
| R5 | search | 5 | query batch | 58534083 | 58788333 | N/A | 58788333 | 1712.593 |
| R10 | search | 500 | individual query | 3084 | 3625 | 3750 | 13708 | 313635.557 |
| R14 | search | 500 | individual query | 4541 | 4917 | 5583 | 11250 | 217324.312 |
| R18 | search | 10 | individual query | 291 | 10333 | 10333 | 10333 | 710075.978 |
| R20 | search | 500 | individual query | 1417 | 1750 | 1875 | 2916 | 688706.183 |

**Client process resources**

| Run | Scope | User CPU (ns) | System CPU (ns) | Peak RSS (B) |
|---|---|---|---|---|
| R10 | search | 1589000 | 6000 | 2490368 |
| R14 | search | 2294000 | 16000 | 2473984 |
| R18 | search | 10000 | 12000 | 2277376 |
| R20 | search | 725000 | 4000 | 2490368 |

**Logical I/O**

| Run | Scope | GET | LIST | CREATE | Read (B) | Written (B) |
|---|---|---|---|---|---|---|
| R1 | search | 0 | 0 | 0 | 0 | 0 |
| R2 | search | 0 | 0 | 0 | 0 | 0 |
| R4 | search | 0 | 0 | 0 | 0 | 0 |
| R5 | search | 0 | 0 | 0 | 0 | 0 |
| R10 | search | 0 | 0 | 0 | 0 | 0 |
| R14 | search | 0 | 0 | 0 | 0 | 0 |
| R18 | search | 0 | 0 | 0 | 0 | 0 |
| R20 | search | 0 | 0 | 0 | 0 | 0 |

**Footprint**

| Run | Scope | Live docs | Mutation history | Objects | Files | File bytes |
|---|---|---|---|---|---|---|
| R1 | search | 1000 | 1000 | 1001 | 2002 | 486041 |
| R2 | search | 10000 | 10000 | 10001 | 20002 | 4878691 |
| R4 | search | 1000 | 1000 | 1001 | 2002 | 1554131 |
| R5 | search | 10000 | 10000 | 10001 | 20002 | 15562421 |
| R10 | search | 100 | 100 | 101 | 202 | 48561 |
| R14 | search | 100 | 100 | 101 | 202 | 48561 |
| R18 | search | 10 | 10 | 11 | 22 | 1832 |
| R20 | search | 100 | 100 | 101 | 202 | 48561 |

### s3 / commit

**Latency**

| Run | Scope | Samples | Latency scope | p50 (ns) | p95 (ns) | p99 (ns) | Max (ns) | Ops/s |
|---|---|---|---|---|---|---|---|---|
| R15 | insert | 5 | individual commit | 12202833 | 19407333 | 19407333 | 19407333 | 74.341 |
| R15 | overwrite | 5 | individual commit | 8328334 | 17360375 | 17360375 | 17360375 | 96.9 |
| R15 | delete | 5 | individual commit | 14542708 | 16980208 | 16980208 | 16980208 | 69.177 |

**Client process resources**

| Run | Scope | User CPU (ns) | System CPU (ns) | Peak RSS (B) |
|---|---|---|---|---|
| R15 | insert | 738000 | 194000 | 10272768 |
| R15 | overwrite | 1008000 | 211000 | 10272768 |
| R15 | delete | 739000 | 204000 | 10272768 |

**Logical I/O**

| Run | Scope | GET | LIST | CREATE | Read (B) | Written (B) |
|---|---|---|---|---|---|---|
| R15 | insert | 0 | 0 | 5 | 0 | 578 |
| R15 | overwrite | 0 | 0 | 5 | 0 | 572 |
| R15 | delete | 0 | 0 | 5 | 0 | 315 |

**Footprint**

| Run | Scope | Live docs | Mutation history | Objects | Object bytes |
|---|---|---|---|---|---|
| R15 | final-footprint | 0 | 15 | 16 | 2301 |

**HTTP requests**

| Run | Scope | HTTP GET | HTTP LIST | HTTP PUT | HTTP other | Request body (B) | HTTP errors | Transport errors |
|---|---|---|---|---|---|---|---|---|
| R15 | insert | 0 | 0 | 5 | 0 | 818 | 0 | 0 |
| R15 | overwrite | 0 | 0 | 5 | 0 | 812 | 0 | 0 |
| R15 | delete | 0 | 0 | 5 | 0 | 555 | 0 | 0 |

### s3 / recovery

**Latency**

| Run | Scope | Samples | Latency scope | p50 (ns) | p95 (ns) | p99 (ns) | Max (ns) | Ops/s |
|---|---|---|---|---|---|---|---|---|
| R13 | total | 5 | full open | 130518250 | 137814292 | 137814292 | 137814292 | 7.749 |
| R13 | s3-store | 5 | S3 client construction | 31959 | 39750 | 39750 | 39750 | 31039.513 |
| R13 | replay | 5 | database replay | 130478417 | 137788125 | 137788125 | 137788125 | 7.751 |
| R15 | total | 2 | full open | 15329125 | 23951292 | 23951292 | 23951292 | 50.916 |
| R15 | s3-store | 2 | S3 client construction | 47333 | 61708 | 61708 | 61708 | 18341.725 |
| R15 | replay | 2 | database replay | 15281500 | 23889208 | 23889208 | 23889208 | 51.059 |
| R19 | total | 5 | full open | 19564042 | 20529916 | 20529916 | 20529916 | 52.032 |
| R19 | s3-store | 5 | S3 client construction | 34042 | 38667 | 38667 | 38667 | 31104.006 |
| R19 | replay | 5 | database replay | 19529833 | 20502875 | 20502875 | 20502875 | 52.12 |

**Client process resources**

| Run | Scope | User CPU (ns) | System CPU (ns) | Peak RSS (B) |
|---|---|---|---|---|
| R13 | total | 65981000 | 30210000 | 11272192 |
| R15 | total | 2717000 | 1608000 | 10600448 |
| R19 | total | 13136000 | 3775000 | 11173888 |

**Logical I/O**

| Run | Scope | GET | LIST | CREATE | Read (B) | Written (B) |
|---|---|---|---|---|---|---|
| R13 | total | 1505 | 5 | 0 | 643120 | 0 |
| R13 | replay | 1505 | 5 | 0 | 643120 | 0 |
| R15 | total | 62 | 2 | 0 | 7050 | 0 |
| R15 | replay | 62 | 2 | 0 | 7050 | 0 |
| R19 | total | 160 | 5 | 0 | 119665 | 0 |
| R19 | replay | 160 | 5 | 0 | 119665 | 0 |

**Footprint**

| Run | Scope | Live docs | Mutation history | Objects | Object bytes |
|---|---|---|---|---|---|
| R13 | total | 30 | 300 | 301 | 143072 |
| R15 | total | 10 | 30 | 31 | 5013 |
| R19 | total | 30 | 300 | 302 | 154125 |

**HTTP requests**

| Run | Scope | HTTP GET | HTTP LIST | HTTP PUT | HTTP other | Request body (B) | HTTP errors | Transport errors |
|---|---|---|---|---|---|---|---|---|
| R13 | total | 1505 | 5 | 0 | 0 | 0 | 0 | 0 |
| R13 | replay | 1505 | 5 | 0 | 0 | 0 | 0 | 0 |
| R15 | total | 62 | 2 | 0 | 0 | 0 | 0 | 0 |
| R15 | replay | 62 | 2 | 0 | 0 | 0 | 0 | 0 |
| R19 | total | 160 | 5 | 0 | 0 | 0 | 0 | 0 |
| R19 | replay | 160 | 5 | 0 | 0 | 0 | 0 | 0 |

### s3 / search

**Latency**

| Run | Scope | Samples | Latency scope | p50 (ns) | p95 (ns) | p99 (ns) | Max (ns) | Ops/s |
|---|---|---|---|---|---|---|---|---|
| R15 | search | 10 | individual query | 167 | 292 | 292 | 292 | 3808073.115 |

**Client process resources**

| Run | Scope | User CPU (ns) | System CPU (ns) | Peak RSS (B) |
|---|---|---|---|---|
| R15 | search | 4000 | 3000 | 10059776 |

**Logical I/O**

| Run | Scope | GET | LIST | CREATE | Read (B) | Written (B) |
|---|---|---|---|---|---|---|
| R15 | search | 0 | 0 | 0 | 0 | 0 |

**Footprint**

| Run | Scope | Live docs | Mutation history | Objects | Object bytes |
|---|---|---|---|---|---|
| R15 | search | 10 | 10 | 11 | 1744 |

**HTTP requests**

| Run | Scope | HTTP GET | HTTP LIST | HTTP PUT | HTTP other | Request body (B) | HTTP errors | Transport errors |
|---|---|---|---|---|---|---|---|---|
| R15 | search | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

## Measurement notes

- Recovery loads a checkpoint plus its tail, or the full log when disabled. LocalStore still validates and synchronizes all retained files. Recovery total/store/replay rows overlap; do not sum them.
- CPU covers measured loops; RSS is the client process lifetime peak including setup. Counters are totals across measured operations/reopens. HTTP bytes count request bodies, not wire traffic; logical bytes exclude envelopes. Untimed inventory and search setup requests are excluded from measured I/O.
- Legacy batch latency is not per-query latency. Small samples do not establish population tails; uncontrolled load/cache and MinIO smoke runs do not establish production or cross-backend speedups. Missing measurements and zero-denominator deltas remain N/A.
