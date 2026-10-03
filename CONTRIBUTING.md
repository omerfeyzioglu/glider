# Contributing

Glider is developed as a real storage system: correctness and durability come
before performance, and every claim must be backed by a test or a
reproducible measurement. [AGENTS.md](AGENTS.md) holds the full engineering
rules; this page summarizes the workflow.

## Branches and pull requests

- Make each logical change on its own branch, named with a prefix such as
  `feature/`, `fix/`, `bench/`, `docs/`, `ci/`, `chore/` or `refactor/`.
- Open a pull request into `main`; merge only after CI passes. Keep
  unrelated changes in separate branches.
- Before pushing, merge the latest `main` into the branch and run the checks
  below locally.

## Required checks

These are the commands CI runs (Rust 1.98.1):

```sh
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --release --locked
cargo test --release --all-features --locked
python3 -m unittest discover -s tests -p 'test_*.py'
python3 tools/benchmarks.py summary --check
python3 tools/benchmarks.py summary --archive benchmarks/filtering --check
python3 tools/check_links.py
node --check site/sim-model.mjs
node --check site/sim-view.mjs
node --test site/sim-model.test.mjs
python3 tools/drills.py --seed 29   # recovery drills; needs release server binaries
GLIDER_SERVER_BIN=target/release/glider-server \
  python3 -m unittest discover -s clients/python/tests
python3 tools/test_s3.py   # requires Docker; disposable MinIO
python3 tools/quickstart_smoke.py  # requires Docker; Compose quickstart
```

The drills and Python client tests need
`cargo build --release --locked --features server --bin glider-server --bin glider-admin`
first.

## Tests

- Add or update tests whenever observable behavior changes.
- Durable write paths need crash, restart or failure-path tests: state the
  acknowledgement point, the authoritative state, the crash behavior and
  the recovery.
- Persisted formats are explicitly versioned; older versions must open or
  fail with a clear error.
- Randomized tests must be reproducible and print their seed on failure.
- Fix root causes; do not hide failures with retries, sleeps or longer
  timeouts.

## Benchmarks and performance changes

- Exact search is the oracle for approximate search; report recall@k
  against it with reproducible queries.
- An optimization needs a baseline, a hypothesis, the change and a
  measurement after it.
- Never report a measurement, test or command as successful unless it was
  run. Record dataset, configuration, seed, backend, revision and
  environment with every result ([BENCHMARKS.md](BENCHMARKS.md)).
- Benchmarks and acceptance runs (`tools/m24_acceptance.py` on MinIO,
  `tools/aws_acceptance.py` on EC2 and S3) are described in
  [BENCHMARKS.md](BENCHMARKS.md).

## Documentation

- [DESIGN.md](DESIGN.md) describes the current architecture, invariants,
  formats and guarantees; update it when one of them changes.
- [ROADMAP.md](ROADMAP.md) tracks milestone status; [README.md](README.md)
  and [docs/](docs/API.md) cover public usage.
- Add an entry to [CHANGELOG.md](CHANGELOG.md) for user-visible changes.

By contributing you agree that your contributions are dual licensed under
the MIT and Apache-2.0 licenses, as described in the [README](README.md#license).
