# Benchmark archive

Generated from raw JSON; do not edit. Missing measurements are `null`.

Latency is in ns; throughput in ops/s; CPU in ns; RSS/footprints/I/O in bytes.
CPU covers measured loops; RSS is process-lifetime peak (including setup). Recovery rows overlap; do not sum them.
Legacy search latency describes a batch, not an individual query. Sample maxima are not population tail guarantees.

## Runs

| ID / raw JSON | Backend | Feature | Phase | Group | Git | Workload | Environment | Latency unit |
|---|---|---|---|---|---|---|---|---|
| [edde11692eb0:5:0:commit/delete](baselines/2026-09-13-local.json) | local | null | null | null | a374b6507c1307000d5dd7a7ebfb28af746eb4a2 | commit/delete; d=128; seed=42; operations/phase=200; live after=0; history after=600 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=Apple M4; internal APFS SSD; FileVault enabled; desktop session; power and competing load uncontrolled | individual commit |
| [edde11692eb0:2:0:commit/delete](baselines/2026-09-13-local.json) | local | null | null | null | a374b6507c1307000d5dd7a7ebfb28af746eb4a2 | commit/delete; d=32; seed=42; operations/phase=200; live after=0; history after=600 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=Apple M4; internal APFS SSD; FileVault enabled; desktop session; power and competing load uncontrolled | individual commit |
| [edde11692eb0:5:0:commit/final-footprint](baselines/2026-09-13-local.json) | local | null | null | null | a374b6507c1307000d5dd7a7ebfb28af746eb4a2 | commit/final-footprint; d=128; seed=42; operations/phase=200; live after=0; history after=600 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=Apple M4; internal APFS SSD; FileVault enabled; desktop session; power and competing load uncontrolled | inventory only |
| [edde11692eb0:2:0:commit/final-footprint](baselines/2026-09-13-local.json) | local | null | null | null | a374b6507c1307000d5dd7a7ebfb28af746eb4a2 | commit/final-footprint; d=32; seed=42; operations/phase=200; live after=0; history after=600 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=Apple M4; internal APFS SSD; FileVault enabled; desktop session; power and competing load uncontrolled | inventory only |
| [edde11692eb0:5:0:commit/insert](baselines/2026-09-13-local.json) | local | null | null | null | a374b6507c1307000d5dd7a7ebfb28af746eb4a2 | commit/insert; d=128; seed=42; operations/phase=200; live after=200; history after=200 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=Apple M4; internal APFS SSD; FileVault enabled; desktop session; power and competing load uncontrolled | individual commit |
| [edde11692eb0:2:0:commit/insert](baselines/2026-09-13-local.json) | local | null | null | null | a374b6507c1307000d5dd7a7ebfb28af746eb4a2 | commit/insert; d=32; seed=42; operations/phase=200; live after=200; history after=200 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=Apple M4; internal APFS SSD; FileVault enabled; desktop session; power and competing load uncontrolled | individual commit |
| [edde11692eb0:5:0:commit/overwrite](baselines/2026-09-13-local.json) | local | null | null | null | a374b6507c1307000d5dd7a7ebfb28af746eb4a2 | commit/overwrite; d=128; seed=42; operations/phase=200; live after=200; history after=400 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=Apple M4; internal APFS SSD; FileVault enabled; desktop session; power and competing load uncontrolled | individual commit |
| [edde11692eb0:2:0:commit/overwrite](baselines/2026-09-13-local.json) | local | null | null | null | a374b6507c1307000d5dd7a7ebfb28af746eb4a2 | commit/overwrite; d=32; seed=42; operations/phase=200; live after=200; history after=400 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=Apple M4; internal APFS SSD; FileVault enabled; desktop session; power and competing load uncontrolled | individual commit |
| [edde11692eb0:6:0:recovery/local-store](baselines/2026-09-13-local.json) | local | null | null | null | a374b6507c1307000d5dd7a7ebfb28af746eb4a2 | recovery/local-store; d=32; seed=42; live rows=100; input mutations=100; reopens=5 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=Apple M4; internal APFS SSD; FileVault enabled; desktop session; power and competing load uncontrolled | local store open |
| [edde11692eb0:7:0:recovery/local-store](baselines/2026-09-13-local.json) | local | null | null | null | a374b6507c1307000d5dd7a7ebfb28af746eb4a2 | recovery/local-store; d=32; seed=42; live rows=100; input mutations=1000; reopens=5 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=Apple M4; internal APFS SSD; FileVault enabled; desktop session; power and competing load uncontrolled | local store open |
| [edde11692eb0:8:0:recovery/local-store](baselines/2026-09-13-local.json) | local | null | null | null | a374b6507c1307000d5dd7a7ebfb28af746eb4a2 | recovery/local-store; d=32; seed=42; live rows=100; input mutations=5000; reopens=5 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=Apple M4; internal APFS SSD; FileVault enabled; desktop session; power and competing load uncontrolled | local store open |
| [edde11692eb0:6:0:recovery/replay](baselines/2026-09-13-local.json) | local | null | null | null | a374b6507c1307000d5dd7a7ebfb28af746eb4a2 | recovery/replay; d=32; seed=42; live rows=100; input mutations=100; reopens=5 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=Apple M4; internal APFS SSD; FileVault enabled; desktop session; power and competing load uncontrolled | database replay |
| [edde11692eb0:7:0:recovery/replay](baselines/2026-09-13-local.json) | local | null | null | null | a374b6507c1307000d5dd7a7ebfb28af746eb4a2 | recovery/replay; d=32; seed=42; live rows=100; input mutations=1000; reopens=5 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=Apple M4; internal APFS SSD; FileVault enabled; desktop session; power and competing load uncontrolled | database replay |
| [edde11692eb0:8:0:recovery/replay](baselines/2026-09-13-local.json) | local | null | null | null | a374b6507c1307000d5dd7a7ebfb28af746eb4a2 | recovery/replay; d=32; seed=42; live rows=100; input mutations=5000; reopens=5 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=Apple M4; internal APFS SSD; FileVault enabled; desktop session; power and competing load uncontrolled | database replay |
| [edde11692eb0:6:0:recovery/total](baselines/2026-09-13-local.json) | local | null | null | null | a374b6507c1307000d5dd7a7ebfb28af746eb4a2 | recovery/total; d=32; seed=42; live rows=100; input mutations=100; reopens=5 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=Apple M4; internal APFS SSD; FileVault enabled; desktop session; power and competing load uncontrolled | full open |
| [edde11692eb0:7:0:recovery/total](baselines/2026-09-13-local.json) | local | null | null | null | a374b6507c1307000d5dd7a7ebfb28af746eb4a2 | recovery/total; d=32; seed=42; live rows=100; input mutations=1000; reopens=5 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=Apple M4; internal APFS SSD; FileVault enabled; desktop session; power and competing load uncontrolled | full open |
| [edde11692eb0:8:0:recovery/total](baselines/2026-09-13-local.json) | local | null | null | null | a374b6507c1307000d5dd7a7ebfb28af746eb4a2 | recovery/total; d=32; seed=42; live rows=100; input mutations=5000; reopens=5 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=Apple M4; internal APFS SSD; FileVault enabled; desktop session; power and competing load uncontrolled | full open |
| [edde11692eb0:3:0:search](baselines/2026-09-13-local.json) | local | null | null | null | a374b6507c1307000d5dd7a7ebfb28af746eb4a2 | search; d=128; seed=42; rows=1000; queries=100; batches=5; k=10 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=Apple M4; internal APFS SSD; FileVault enabled; desktop session; power and competing load uncontrolled | query batch |
| [edde11692eb0:4:0:search](baselines/2026-09-13-local.json) | local | null | null | null | a374b6507c1307000d5dd7a7ebfb28af746eb4a2 | search; d=128; seed=42; rows=10000; queries=100; batches=5; k=10 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=Apple M4; internal APFS SSD; FileVault enabled; desktop session; power and competing load uncontrolled | query batch |
| [edde11692eb0:0:0:search](baselines/2026-09-13-local.json) | local | null | null | null | a374b6507c1307000d5dd7a7ebfb28af746eb4a2 | search; d=32; seed=42; rows=1000; queries=100; batches=5; k=10 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=Apple M4; internal APFS SSD; FileVault enabled; desktop session; power and competing load uncontrolled | query batch |
| [edde11692eb0:1:0:search](baselines/2026-09-13-local.json) | local | null | null | null | a374b6507c1307000d5dd7a7ebfb28af746eb4a2 | search; d=32; seed=42; rows=10000; queries=100; batches=5; k=10 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=Apple M4; internal APFS SSD; FileVault enabled; desktop session; power and competing load uncontrolled | query batch |
| [ff77a79fbdca:0:0:search](runs/ff77a79fbdca3098c81e51def0bffbc300455e2aaf21862cede9ca0080947d81.json) | local | archive-validation | after | aa-search-100-d32 | 06043457317a9304885dca41dcc1a12440aad52d | search; d=32; seed=42; rows=100; queries=100; batches=5; k=10 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=same-code repeatability; internal APFS SSD; load uncontrolled | individual query |
| [0306843a08c0:0:0:search](runs/0306843a08c0edb18119fc4bd312343e203749bb1687b7dfcd8fd3687f6a3227.json) | local | archive-validation | before | aa-search-100-d32 | 06043457317a9304885dca41dcc1a12440aad52d | search; d=32; seed=42; rows=100; queries=100; batches=5; k=10 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=same-code repeatability; internal APFS SSD; load uncontrolled | individual query |
| [73aef1437e76:0:1:commit/delete](runs/73aef1437e76edc44d9f2e0282c28d66da452bf9ad1fba9054f33a69c8d5d4c9.json) | local | m1 | baseline | local-v2 | 06043457317a9304885dca41dcc1a12440aad52d | commit/delete; d=32; seed=42; operations/phase=30; live after=0; history after=90 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=internal APFS SSD; desktop load uncontrolled | individual commit |
| [73aef1437e76:0:1:commit/final-footprint](runs/73aef1437e76edc44d9f2e0282c28d66da452bf9ad1fba9054f33a69c8d5d4c9.json) | local | m1 | baseline | local-v2 | 06043457317a9304885dca41dcc1a12440aad52d | commit/final-footprint; d=32; seed=42; operations/phase=30; live after=0; history after=90 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=internal APFS SSD; desktop load uncontrolled | inventory only |
| [73aef1437e76:0:1:commit/insert](runs/73aef1437e76edc44d9f2e0282c28d66da452bf9ad1fba9054f33a69c8d5d4c9.json) | local | m1 | baseline | local-v2 | 06043457317a9304885dca41dcc1a12440aad52d | commit/insert; d=32; seed=42; operations/phase=30; live after=30; history after=30 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=internal APFS SSD; desktop load uncontrolled | individual commit |
| [73aef1437e76:0:1:commit/overwrite](runs/73aef1437e76edc44d9f2e0282c28d66da452bf9ad1fba9054f33a69c8d5d4c9.json) | local | m1 | baseline | local-v2 | 06043457317a9304885dca41dcc1a12440aad52d | commit/overwrite; d=32; seed=42; operations/phase=30; live after=30; history after=60 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=internal APFS SSD; desktop load uncontrolled | individual commit |
| [73aef1437e76:0:2:recovery/local-store](runs/73aef1437e76edc44d9f2e0282c28d66da452bf9ad1fba9054f33a69c8d5d4c9.json) | local | m1 | baseline | local-v2 | 06043457317a9304885dca41dcc1a12440aad52d | recovery/local-store; d=32; seed=42; live rows=100; input mutations=300; reopens=5 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=internal APFS SSD; desktop load uncontrolled | local store open |
| [73aef1437e76:0:2:recovery/replay](runs/73aef1437e76edc44d9f2e0282c28d66da452bf9ad1fba9054f33a69c8d5d4c9.json) | local | m1 | baseline | local-v2 | 06043457317a9304885dca41dcc1a12440aad52d | recovery/replay; d=32; seed=42; live rows=100; input mutations=300; reopens=5 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=internal APFS SSD; desktop load uncontrolled | database replay |
| [73aef1437e76:0:2:recovery/total](runs/73aef1437e76edc44d9f2e0282c28d66da452bf9ad1fba9054f33a69c8d5d4c9.json) | local | m1 | baseline | local-v2 | 06043457317a9304885dca41dcc1a12440aad52d | recovery/total; d=32; seed=42; live rows=100; input mutations=300; reopens=5 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=internal APFS SSD; desktop load uncontrolled | full open |
| [73aef1437e76:0:0:search](runs/73aef1437e76edc44d9f2e0282c28d66da452bf9ad1fba9054f33a69c8d5d4c9.json) | local | m1 | baseline | local-v2 | 06043457317a9304885dca41dcc1a12440aad52d | search; d=32; seed=42; rows=100; queries=100; batches=5; k=10 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=internal APFS SSD; desktop load uncontrolled | individual query |
| [de3314f9c482:0:1:commit/delete](runs/de3314f9c482c26d3fd25d46c10d160c41a3ab2ac87af15241ac10d2f46d4192.json) | local | s3-benchmarks | baseline | backend-smoke | d71a5ebc1ac6c1c629ed330361dc7a8ca6f9e25d | commit/delete; d=4; seed=42; operations/phase=5; live after=0; history after=15 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=smoke validation; desktop session; power and competing load uncontrolled | individual commit |
| [af655c5cf5f7:0:1:commit/delete](runs/af655c5cf5f7cd9f15edf544ea46100853fde9ee2fadae9e098fc466cfa6c388.json) | s3 | s3-benchmarks | baseline | backend-smoke | d71a5ebc1ac6c1c629ed330361dc7a8ca6f9e25d | commit/delete; d=4; seed=42; operations/phase=5; live after=0; history after=15 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=smoke validation; desktop session; power and competing load uncontrolled; http://127.0.0.1:54814; region=us-east-1; bucket=glider-test; service=quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e; Docker 29.1.3 | individual commit |
| [de3314f9c482:0:1:commit/final-footprint](runs/de3314f9c482c26d3fd25d46c10d160c41a3ab2ac87af15241ac10d2f46d4192.json) | local | s3-benchmarks | baseline | backend-smoke | d71a5ebc1ac6c1c629ed330361dc7a8ca6f9e25d | commit/final-footprint; d=4; seed=42; operations/phase=5; live after=0; history after=15 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=smoke validation; desktop session; power and competing load uncontrolled | inventory only |
| [af655c5cf5f7:0:1:commit/final-footprint](runs/af655c5cf5f7cd9f15edf544ea46100853fde9ee2fadae9e098fc466cfa6c388.json) | s3 | s3-benchmarks | baseline | backend-smoke | d71a5ebc1ac6c1c629ed330361dc7a8ca6f9e25d | commit/final-footprint; d=4; seed=42; operations/phase=5; live after=0; history after=15 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=smoke validation; desktop session; power and competing load uncontrolled; http://127.0.0.1:54814; region=us-east-1; bucket=glider-test; service=quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e; Docker 29.1.3 | inventory only |
| [de3314f9c482:0:1:commit/insert](runs/de3314f9c482c26d3fd25d46c10d160c41a3ab2ac87af15241ac10d2f46d4192.json) | local | s3-benchmarks | baseline | backend-smoke | d71a5ebc1ac6c1c629ed330361dc7a8ca6f9e25d | commit/insert; d=4; seed=42; operations/phase=5; live after=5; history after=5 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=smoke validation; desktop session; power and competing load uncontrolled | individual commit |
| [af655c5cf5f7:0:1:commit/insert](runs/af655c5cf5f7cd9f15edf544ea46100853fde9ee2fadae9e098fc466cfa6c388.json) | s3 | s3-benchmarks | baseline | backend-smoke | d71a5ebc1ac6c1c629ed330361dc7a8ca6f9e25d | commit/insert; d=4; seed=42; operations/phase=5; live after=5; history after=5 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=smoke validation; desktop session; power and competing load uncontrolled; http://127.0.0.1:54814; region=us-east-1; bucket=glider-test; service=quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e; Docker 29.1.3 | individual commit |
| [de3314f9c482:0:1:commit/overwrite](runs/de3314f9c482c26d3fd25d46c10d160c41a3ab2ac87af15241ac10d2f46d4192.json) | local | s3-benchmarks | baseline | backend-smoke | d71a5ebc1ac6c1c629ed330361dc7a8ca6f9e25d | commit/overwrite; d=4; seed=42; operations/phase=5; live after=5; history after=10 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=smoke validation; desktop session; power and competing load uncontrolled | individual commit |
| [af655c5cf5f7:0:1:commit/overwrite](runs/af655c5cf5f7cd9f15edf544ea46100853fde9ee2fadae9e098fc466cfa6c388.json) | s3 | s3-benchmarks | baseline | backend-smoke | d71a5ebc1ac6c1c629ed330361dc7a8ca6f9e25d | commit/overwrite; d=4; seed=42; operations/phase=5; live after=5; history after=10 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=smoke validation; desktop session; power and competing load uncontrolled; http://127.0.0.1:54814; region=us-east-1; bucket=glider-test; service=quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e; Docker 29.1.3 | individual commit |
| [de3314f9c482:0:2:recovery/local-store](runs/de3314f9c482c26d3fd25d46c10d160c41a3ab2ac87af15241ac10d2f46d4192.json) | local | s3-benchmarks | baseline | backend-smoke | d71a5ebc1ac6c1c629ed330361dc7a8ca6f9e25d | recovery/local-store; d=4; seed=42; live rows=10; input mutations=30; reopens=2 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=smoke validation; desktop session; power and competing load uncontrolled | local store open |
| [de3314f9c482:0:2:recovery/replay](runs/de3314f9c482c26d3fd25d46c10d160c41a3ab2ac87af15241ac10d2f46d4192.json) | local | s3-benchmarks | baseline | backend-smoke | d71a5ebc1ac6c1c629ed330361dc7a8ca6f9e25d | recovery/replay; d=4; seed=42; live rows=10; input mutations=30; reopens=2 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=smoke validation; desktop session; power and competing load uncontrolled | database replay |
| [af655c5cf5f7:0:2:recovery/replay](runs/af655c5cf5f7cd9f15edf544ea46100853fde9ee2fadae9e098fc466cfa6c388.json) | s3 | s3-benchmarks | baseline | backend-smoke | d71a5ebc1ac6c1c629ed330361dc7a8ca6f9e25d | recovery/replay; d=4; seed=42; live rows=10; input mutations=30; reopens=2 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=smoke validation; desktop session; power and competing load uncontrolled; http://127.0.0.1:54814; region=us-east-1; bucket=glider-test; service=quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e; Docker 29.1.3 | database replay |
| [af655c5cf5f7:0:2:recovery/s3-store](runs/af655c5cf5f7cd9f15edf544ea46100853fde9ee2fadae9e098fc466cfa6c388.json) | s3 | s3-benchmarks | baseline | backend-smoke | d71a5ebc1ac6c1c629ed330361dc7a8ca6f9e25d | recovery/s3-store; d=4; seed=42; live rows=10; input mutations=30; reopens=2 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=smoke validation; desktop session; power and competing load uncontrolled; http://127.0.0.1:54814; region=us-east-1; bucket=glider-test; service=quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e; Docker 29.1.3 | S3 client construction |
| [de3314f9c482:0:2:recovery/total](runs/de3314f9c482c26d3fd25d46c10d160c41a3ab2ac87af15241ac10d2f46d4192.json) | local | s3-benchmarks | baseline | backend-smoke | d71a5ebc1ac6c1c629ed330361dc7a8ca6f9e25d | recovery/total; d=4; seed=42; live rows=10; input mutations=30; reopens=2 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=smoke validation; desktop session; power and competing load uncontrolled | full open |
| [af655c5cf5f7:0:2:recovery/total](runs/af655c5cf5f7cd9f15edf544ea46100853fde9ee2fadae9e098fc466cfa6c388.json) | s3 | s3-benchmarks | baseline | backend-smoke | d71a5ebc1ac6c1c629ed330361dc7a8ca6f9e25d | recovery/total; d=4; seed=42; live rows=10; input mutations=30; reopens=2 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=smoke validation; desktop session; power and competing load uncontrolled; http://127.0.0.1:54814; region=us-east-1; bucket=glider-test; service=quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e; Docker 29.1.3 | full open |
| [de3314f9c482:0:0:search](runs/de3314f9c482c26d3fd25d46c10d160c41a3ab2ac87af15241ac10d2f46d4192.json) | local | s3-benchmarks | baseline | backend-smoke | d71a5ebc1ac6c1c629ed330361dc7a8ca6f9e25d | search; d=4; seed=42; rows=10; queries=5; batches=2; k=3 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=smoke validation; desktop session; power and competing load uncontrolled | individual query |
| [af655c5cf5f7:0:0:search](runs/af655c5cf5f7cd9f15edf544ea46100853fde9ee2fadae9e098fc466cfa6c388.json) | s3 | s3-benchmarks | baseline | backend-smoke | d71a5ebc1ac6c1c629ed330361dc7a8ca6f9e25d | search; d=4; seed=42; rows=10; queries=5; batches=2; k=3 | Apple M4 / aarch64 / Darwin 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64; label=smoke validation; desktop session; power and competing load uncontrolled; http://127.0.0.1:54814; region=us-east-1; bucket=glider-test; service=quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e; Docker 29.1.3 | individual query |

## Latency and process resources

| ID | p50_latency_ns | p95_latency_ns | p99_latency_ns | max_latency_ns | throughput_ops_per_second | user_cpu_ns | system_cpu_ns | process_max_rss_bytes |
|---|---|---|---|---|---|---|---|---|
| edde11692eb0:5:0:commit/delete | 17127416.000 | 23103583.000 | null | 44922458.000 | 59.763 | null | null | null |
| edde11692eb0:2:0:commit/delete | 18890084.000 | 21224042.000 | null | 34016208.000 | 57.791 | null | null | null |
| edde11692eb0:5:0:commit/final-footprint | null | null | null | null | null | null | null | null |
| edde11692eb0:2:0:commit/final-footprint | null | null | null | null | null | null | null | null |
| edde11692eb0:5:0:commit/insert | 19043958.000 | 23649625.000 | null | 37305875.000 | 54.800 | null | null | null |
| edde11692eb0:2:0:commit/insert | 17955250.000 | 22353083.000 | null | 25273333.000 | 59.059 | null | null | null |
| edde11692eb0:5:0:commit/overwrite | 18714500.000 | 21234459.000 | null | 34069833.000 | 59.214 | null | null | null |
| edde11692eb0:2:0:commit/overwrite | 19089292.000 | 22123958.000 | null | 30939834.000 | 54.600 | null | null | null |
| edde11692eb0:6:0:recovery/local-store | 8659500.000 | 193292834.000 | null | 193292834.000 | 22.076 | null | null | null |
| edde11692eb0:7:0:recovery/local-store | 71201625.000 | 152984417.000 | null | 152984417.000 | 11.565 | null | null | null |
| edde11692eb0:8:0:recovery/local-store | 223797708.000 | 253944209.000 | null | 253944209.000 | 4.335 | null | null | null |
| edde11692eb0:6:0:recovery/replay | 6551375.000 | 14306125.000 | null | 14306125.000 | 121.476 | null | null | null |
| edde11692eb0:7:0:recovery/replay | 70734750.000 | 80883042.000 | null | 80883042.000 | 14.332 | null | null | null |
| edde11692eb0:8:0:recovery/replay | 249706125.000 | 286996334.000 | null | 286996334.000 | 3.902 | null | null | null |
| edde11692eb0:6:0:recovery/total | 15075667.000 | 199010750.000 | null | 199010750.000 | 18.681 | null | null | null |
| edde11692eb0:7:0:recovery/total | 142351750.000 | 223719209.000 | null | 223719209.000 | 6.400 | null | null | null |
| edde11692eb0:8:0:recovery/total | 481341500.000 | 507653000.000 | null | 507653000.000 | 2.054 | null | null | null |
| edde11692eb0:3:0:search | 7594833.000 | 9704459.000 | null | 9704459.000 | 12660.458 | null | null | null |
| edde11692eb0:4:0:search | 58534083.000 | 58788333.000 | null | 58788333.000 | 1712.593 | null | null | null |
| edde11692eb0:0:0:search | 3990709.000 | 4015209.000 | null | 4015209.000 | 26622.058 | null | null | null |
| edde11692eb0:1:0:search | 23854958.000 | 27396042.000 | null | 27396042.000 | 4071.588 | null | null | null |
| ff77a79fbdca:0:0:search | 1417.000 | 1750.000 | 1875.000 | 2916.000 | 688706.183 | 725000 | 4000 | 2490368 |
| 0306843a08c0:0:0:search | 3084.000 | 3625.000 | 3750.000 | 13708.000 | 313635.557 | 1589000 | 6000 | 2490368 |
| 73aef1437e76:0:1:commit/delete | 9759375.000 | 10061916.000 | 14983542.000 | 14983542.000 | 103.908 | 1084000 | 24918000 | 2670592 |
| 73aef1437e76:0:1:commit/final-footprint | null | null | null | null | null | null | null | null |
| 73aef1437e76:0:1:commit/insert | 9045667.000 | 14022042.000 | 18788292.000 | 18788292.000 | 102.418 | 793000 | 15207000 | 2654208 |
| 73aef1437e76:0:1:commit/overwrite | 9826125.000 | 13994458.000 | 15957458.000 | 15957458.000 | 101.343 | 1356000 | 26088000 | 2654208 |
| 73aef1437e76:0:2:recovery/local-store | 13278625.000 | 14049042.000 | 14049042.000 | 14049042.000 | 75.969 | null | null | null |
| 73aef1437e76:0:2:recovery/replay | 14716125.000 | 15941000.000 | 15941000.000 | 15941000.000 | 68.209 | null | null | null |
| 73aef1437e76:0:2:recovery/total | 28083500.000 | 29289375.000 | 29289375.000 | 29289375.000 | 35.940 | 15723000 | 121526000 | 2998272 |
| 73aef1437e76:0:0:search | 4541.000 | 4917.000 | 5583.000 | 11250.000 | 217324.312 | 2294000 | 16000 | 2473984 |
| de3314f9c482:0:1:commit/delete | 38950792.000 | 41042375.000 | 41042375.000 | 41042375.000 | 27.483 | 254000 | 5286000 | 2473984 |
| af655c5cf5f7:0:1:commit/delete | 14542708.000 | 16980208.000 | 16980208.000 | 16980208.000 | 69.177 | 739000 | 204000 | 10272768 |
| de3314f9c482:0:1:commit/final-footprint | null | null | null | null | null | null | null | null |
| af655c5cf5f7:0:1:commit/final-footprint | null | null | null | null | null | null | null | null |
| de3314f9c482:0:1:commit/insert | 37900500.000 | 44990959.000 | 44990959.000 | 44990959.000 | 28.090 | 259000 | 5252000 | 2408448 |
| af655c5cf5f7:0:1:commit/insert | 12202833.000 | 19407333.000 | 19407333.000 | 19407333.000 | 74.341 | 738000 | 194000 | 10272768 |
| de3314f9c482:0:1:commit/overwrite | 29967667.000 | 41012417.000 | 41012417.000 | 41012417.000 | 30.897 | 236000 | 4725000 | 2473984 |
| af655c5cf5f7:0:1:commit/overwrite | 8328334.000 | 17360375.000 | 17360375.000 | 17360375.000 | 96.900 | 1008000 | 211000 | 10272768 |
| de3314f9c482:0:2:recovery/local-store | 226846166.000 | 236420333.000 | 236420333.000 | 236420333.000 | 4.317 | null | null | null |
| de3314f9c482:0:2:recovery/replay | 3793208.000 | 4478792.000 | 4478792.000 | 4478792.000 | 241.779 | null | null | null |
| af655c5cf5f7:0:2:recovery/replay | 15281500.000 | 23889208.000 | 23889208.000 | 23889208.000 | 51.059 | null | null | null |
| af655c5cf5f7:0:2:recovery/s3-store | 47333.000 | 61708.000 | 61708.000 | 61708.000 | 18341.725 | null | null | null |
| de3314f9c482:0:2:recovery/total | 231326125.000 | 240214833.000 | 240214833.000 | 240214833.000 | 4.241 | 1799000 | 20547000 | 2686976 |
| af655c5cf5f7:0:2:recovery/total | 15329125.000 | 23951292.000 | 23951292.000 | 23951292.000 | 50.916 | 2717000 | 1608000 | 10600448 |
| de3314f9c482:0:0:search | 291.000 | 10333.000 | 10333.000 | 10333.000 | 710075.978 | 10000 | 12000 | 2277376 |
| af655c5cf5f7:0:0:search | 167.000 | 292.000 | 292.000 | 292.000 | 3808073.115 | 4000 | 3000 | 10059776 |

## Logical I/O and storage footprint

| ID | logical_bytes_read | logical_bytes_written | get_count | create_count | logical_object_count | physical_file_count | file_footprint_bytes | list_count | object_footprint_bytes |
|---|---|---|---|---|---|---|---|---|---|
| edde11692eb0:5:0:commit/delete | 0 | 13090 | 0 | 200 | null | null | null | 0 | null |
| edde11692eb0:2:0:commit/delete | 0 | 13090 | 0 | 200 | null | null | null | 0 | null |
| edde11692eb0:5:0:commit/final-footprint | null | null | null | null | 601 | 1202 | 645861 | null | null |
| edde11692eb0:2:0:commit/final-footprint | null | null | null | null | 601 | 1202 | 218544 | null | null |
| edde11692eb0:5:0:commit/insert | 0 | 299466 | 0 | 200 | null | null | null | 0 | null |
| edde11692eb0:2:0:commit/insert | 0 | 85820 | 0 | 200 | null | null | null | 0 | null |
| edde11692eb0:5:0:commit/overwrite | 0 | 299579 | 0 | 200 | null | null | null | 0 | null |
| edde11692eb0:2:0:commit/overwrite | 0 | 85909 | 0 | 200 | null | null | null | 0 | null |
| edde11692eb0:6:0:recovery/local-store | null | null | null | null | null | null | null | null | null |
| edde11692eb0:7:0:recovery/local-store | null | null | null | null | null | null | null | null | null |
| edde11692eb0:8:0:recovery/local-store | null | null | null | null | null | null | null | null | null |
| edde11692eb0:6:0:recovery/replay | 214525 | 0 | 505 | 0 | null | null | null | 5 | null |
| edde11692eb0:7:0:recovery/replay | 2144975 | 0 | 5005 | 0 | null | null | null | 5 | null |
| edde11692eb0:8:0:recovery/replay | 10744045 | 0 | 25005 | 0 | null | null | null | 5 | null |
| edde11692eb0:6:0:recovery/total | 214525 | 0 | 505 | 0 | 101 | 202 | 48561 | 5 | null |
| edde11692eb0:7:0:recovery/total | 2144975 | 0 | 5005 | 0 | 1001 | 2002 | 485051 | 5 | null |
| edde11692eb0:8:0:recovery/total | 10744045 | 0 | 25005 | 0 | 5001 | 10002 | 2428865 | 5 | null |
| edde11692eb0:3:0:search | 0 | 0 | 0 | 0 | 1001 | 2002 | 1554131 | 0 | null |
| edde11692eb0:4:0:search | 0 | 0 | 0 | 0 | 10001 | 20002 | 15562421 | 0 | null |
| edde11692eb0:0:0:search | 0 | 0 | 0 | 0 | 1001 | 2002 | 486041 | 0 | null |
| edde11692eb0:1:0:search | 0 | 0 | 0 | 0 | 10001 | 20002 | 4878691 | 0 | null |
| ff77a79fbdca:0:0:search | 0 | 0 | 0 | 0 | 101 | 202 | 48561 | 0 | null |
| 0306843a08c0:0:0:search | 0 | 0 | 0 | 0 | 101 | 202 | 48561 | 0 | null |
| 73aef1437e76:0:1:commit/delete | 0 | 1910 | 0 | 30 | null | null | null | 0 | null |
| 73aef1437e76:0:1:commit/final-footprint | null | null | null | null | 91 | 182 | 32768 | null | null |
| 73aef1437e76:0:1:commit/insert | 0 | 12845 | 0 | 30 | null | null | null | 0 | null |
| 73aef1437e76:0:1:commit/overwrite | 0 | 12848 | 0 | 30 | null | null | null | 0 | null |
| 73aef1437e76:0:2:recovery/local-store | null | null | null | null | null | null | null | null | null |
| 73aef1437e76:0:2:recovery/replay | 643470 | 0 | 1505 | 0 | null | null | null | 5 | null |
| 73aef1437e76:0:2:recovery/total | 643470 | 0 | 1505 | 0 | 301 | 602 | 145550 | 5 | null |
| 73aef1437e76:0:0:search | 0 | 0 | 0 | 0 | 101 | 202 | 48561 | 0 | null |
| de3314f9c482:0:1:commit/delete | 0 | 315 | 0 | 5 | null | null | null | 0 | null |
| af655c5cf5f7:0:1:commit/delete | 0 | 315 | 0 | 5 | null | null | null | 0 | null |
| de3314f9c482:0:1:commit/final-footprint | null | null | null | null | 16 | 32 | 2429 | null | null |
| af655c5cf5f7:0:1:commit/final-footprint | null | null | null | null | 16 | null | null | null | 2301 |
| de3314f9c482:0:1:commit/insert | 0 | 578 | 0 | 5 | null | null | null | 0 | null |
| af655c5cf5f7:0:1:commit/insert | 0 | 578 | 0 | 5 | null | null | null | 0 | null |
| de3314f9c482:0:1:commit/overwrite | 0 | 572 | 0 | 5 | null | null | null | 0 | null |
| af655c5cf5f7:0:1:commit/overwrite | 0 | 572 | 0 | 5 | null | null | null | 0 | null |
| de3314f9c482:0:2:recovery/local-store | null | null | null | null | null | null | null | null | null |
| de3314f9c482:0:2:recovery/replay | 7050 | 0 | 62 | 0 | null | null | null | 2 | null |
| af655c5cf5f7:0:2:recovery/replay | 7050 | 0 | 62 | 0 | null | null | null | 2 | null |
| af655c5cf5f7:0:2:recovery/s3-store | null | null | null | null | null | null | null | null | null |
| de3314f9c482:0:2:recovery/total | 7050 | 0 | 62 | 0 | 31 | 62 | 5261 | 2 | null |
| af655c5cf5f7:0:2:recovery/total | 7050 | 0 | 62 | 0 | 31 | null | null | 2 | 5013 |
| de3314f9c482:0:0:search | 0 | 0 | 0 | 0 | 11 | 22 | 1832 | 0 | null |
| af655c5cf5f7:0:0:search | 0 | 0 | 0 | 0 | 11 | null | null | 0 | 1744 |

## HTTP client requests (S3 only)

| ID | http_get_count | http_list_count | http_put_count | http_other_count | request_body_bytes | http_error_count | transport_error_count |
|---|---|---|---|---|---|---|---|
| edde11692eb0:5:0:commit/delete | null | null | null | null | null | null | null |
| edde11692eb0:2:0:commit/delete | null | null | null | null | null | null | null |
| edde11692eb0:5:0:commit/final-footprint | null | null | null | null | null | null | null |
| edde11692eb0:2:0:commit/final-footprint | null | null | null | null | null | null | null |
| edde11692eb0:5:0:commit/insert | null | null | null | null | null | null | null |
| edde11692eb0:2:0:commit/insert | null | null | null | null | null | null | null |
| edde11692eb0:5:0:commit/overwrite | null | null | null | null | null | null | null |
| edde11692eb0:2:0:commit/overwrite | null | null | null | null | null | null | null |
| edde11692eb0:6:0:recovery/local-store | null | null | null | null | null | null | null |
| edde11692eb0:7:0:recovery/local-store | null | null | null | null | null | null | null |
| edde11692eb0:8:0:recovery/local-store | null | null | null | null | null | null | null |
| edde11692eb0:6:0:recovery/replay | null | null | null | null | null | null | null |
| edde11692eb0:7:0:recovery/replay | null | null | null | null | null | null | null |
| edde11692eb0:8:0:recovery/replay | null | null | null | null | null | null | null |
| edde11692eb0:6:0:recovery/total | null | null | null | null | null | null | null |
| edde11692eb0:7:0:recovery/total | null | null | null | null | null | null | null |
| edde11692eb0:8:0:recovery/total | null | null | null | null | null | null | null |
| edde11692eb0:3:0:search | null | null | null | null | null | null | null |
| edde11692eb0:4:0:search | null | null | null | null | null | null | null |
| edde11692eb0:0:0:search | null | null | null | null | null | null | null |
| edde11692eb0:1:0:search | null | null | null | null | null | null | null |
| ff77a79fbdca:0:0:search | null | null | null | null | null | null | null |
| 0306843a08c0:0:0:search | null | null | null | null | null | null | null |
| 73aef1437e76:0:1:commit/delete | null | null | null | null | null | null | null |
| 73aef1437e76:0:1:commit/final-footprint | null | null | null | null | null | null | null |
| 73aef1437e76:0:1:commit/insert | null | null | null | null | null | null | null |
| 73aef1437e76:0:1:commit/overwrite | null | null | null | null | null | null | null |
| 73aef1437e76:0:2:recovery/local-store | null | null | null | null | null | null | null |
| 73aef1437e76:0:2:recovery/replay | null | null | null | null | null | null | null |
| 73aef1437e76:0:2:recovery/total | null | null | null | null | null | null | null |
| 73aef1437e76:0:0:search | null | null | null | null | null | null | null |
| de3314f9c482:0:1:commit/delete | null | null | null | null | null | null | null |
| af655c5cf5f7:0:1:commit/delete | 0 | 0 | 5 | 0 | 555 | 0 | 0 |
| de3314f9c482:0:1:commit/final-footprint | null | null | null | null | null | null | null |
| af655c5cf5f7:0:1:commit/final-footprint | null | null | null | null | null | null | null |
| de3314f9c482:0:1:commit/insert | null | null | null | null | null | null | null |
| af655c5cf5f7:0:1:commit/insert | 0 | 0 | 5 | 0 | 818 | 0 | 0 |
| de3314f9c482:0:1:commit/overwrite | null | null | null | null | null | null | null |
| af655c5cf5f7:0:1:commit/overwrite | 0 | 0 | 5 | 0 | 812 | 0 | 0 |
| de3314f9c482:0:2:recovery/local-store | null | null | null | null | null | null | null |
| de3314f9c482:0:2:recovery/replay | null | null | null | null | null | null | null |
| af655c5cf5f7:0:2:recovery/replay | 62 | 2 | 0 | 0 | 0 | 0 | 0 |
| af655c5cf5f7:0:2:recovery/s3-store | null | null | null | null | null | null | null |
| de3314f9c482:0:2:recovery/total | null | null | null | null | null | null | null |
| af655c5cf5f7:0:2:recovery/total | 62 | 2 | 0 | 0 | 0 | 0 | 0 |
| de3314f9c482:0:0:search | null | null | null | null | null | null | null |
| af655c5cf5f7:0:0:search | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

## Before / after comparisons

Only one before and one after with identical backend, workload, inputs, timing protocol and stable environment are paired. LocalStore and S3Store are separate backend results, never a before/after feature pair.
Revision and measured outputs may differ. Positive delta means an increase; only throughput generally prefers an increase.
Missing values or a zero denominator produce `null` deltas. No cross-workload or cross-environment percentages are reported.

### archive-validation / aa-search-100-d32 / search

Before: `0306843a08c0:0:0:search` (06043457317a9304885dca41dcc1a12440aad52d); after: `ff77a79fbdca:0:0:search` (06043457317a9304885dca41dcc1a12440aad52d).
Workload/environment key: `a8fd08ae1b06a77b8ac774a83d693c9085c4095c5b72a2ebbeeac024afafc62f`. Full inputs and environment are in [index.json](index.json).

Same Git revision and source fingerprint: this is a repeatability comparison, not evidence of a feature effect.

| Metric | Before | After | Change % |
|---|---:|---:|---:|
| p50_latency_ns | 3084.000 | 1417.000 | -54.053 |
| p95_latency_ns | 3625.000 | 1750.000 | -51.724 |
| p99_latency_ns | 3750.000 | 1875.000 | -50.000 |
| max_latency_ns | 13708.000 | 2916.000 | -78.728 |
| throughput_ops_per_second | 313635.557 | 688706.183 | 119.588 |
| user_cpu_ns | 1589000 | 725000 | -54.374 |
| system_cpu_ns | 6000 | 4000 | -33.333 |
| process_max_rss_bytes | 2490368 | 2490368 | 0.000 |
| logical_bytes_read | 0 | 0 | null |
| logical_bytes_written | 0 | 0 | null |
| get_count | 0 | 0 | null |
| create_count | 0 | 0 | null |
| logical_object_count | 101 | 101 | 0.000 |
| physical_file_count | 202 | 202 | 0.000 |
| file_footprint_bytes | 48561 | 48561 | 0.000 |
| list_count | 0 | 0 | null |
| object_footprint_bytes | null | null | null |
| http_get_count | null | null | null |
| http_list_count | null | null | null |
| http_put_count | null | null | null |
| http_other_count | null | null | null |
| request_body_bytes | null | null | null |
| http_error_count | null | null | null |
| transport_error_count | null | null | null |
