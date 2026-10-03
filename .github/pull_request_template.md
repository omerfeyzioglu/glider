## Summary

<!-- What changes and why. -->

## Durability and formats

<!-- For write paths: acknowledgement point, authoritative state, crash behavior and recovery. Note any persisted-format change and its version. Write "none" if not applicable. -->

## Testing

- [ ] `cargo fmt --check` and both Clippy commands
- [ ] `cargo test --release --all-features --locked`
- [ ] `python3 tools/test_s3.py` (object-store changes)
- [ ] `python3 tools/drills.py --seed 29` (server, takeover, cache or conversion changes)
- [ ] Measurements recorded with dataset, seed, backend and revision (performance changes)
