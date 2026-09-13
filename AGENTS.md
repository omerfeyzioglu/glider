# AGENTS.md

## Project

The canonical project and crate name is `glider`; do not introduce alternate
project names.

Build an object-storage-native vector database / search engine with durable
storage, recovery, indexing, approximate nearest-neighbor search, filtering,
and reproducible performance evaluation.

Treat this as a real systems project. Do not simplify designs merely for
educational purposes, but do not introduce complexity without a concrete
technical reason.

Before substantial work, read `DESIGN.md` and inspect the relevant existing
code and tests.

## Engineering rules

1. Correctness before performance.

2. Prefer the simplest correct design that satisfies the actual requirements.
   Avoid speculative abstractions, features, and dependencies.

3. Do not silently change architecture, persistence semantics, durability or
   recovery guarantees, persisted formats, or major subsystem boundaries.
   Compare materially different alternatives when such decisions arise, and
   update `DESIGN.md` when the decision establishes a durable architectural fact.

4. Every durable write path must have explicit semantics for:
   - acknowledgement,
   - authoritative state,
   - crash behavior,
   - recovery.

5. Persisted formats must be explicitly versioned.

6. Design against object-store semantics. Engine correctness must not depend on
   POSIX-only behavior such as atomic rename, in-place mutation, filesystem
   locking, or shared local disk.

7. Exact kNN is the correctness oracle for approximate search. ANN changes must
   be evaluated against exact search with reproducible queries and an explicit
   metric such as recall@k.

8. Measure before optimizing. A meaningful optimization requires a baseline,
   a hypothesis, the change, and a post-change measurement.

9. Never invent measurements or report tests, benchmarks, or commands as
   successful unless they were actually executed successfully. Keep benchmarks
   reproducible and record the relevant dataset, configuration, seed, backend,
   and environment.

10. Tests are part of the implementation. Add or update tests when observable
    behavior changes. Persistence and recovery work should include crash,
    restart, or failure-path tests where appropriate.

11. Randomized tests must be reproducible and report their seed on failure.

12. Fix root causes. Do not mask failures with retries, sleeps, larger timeouts,
    or fallbacks unless that behavior is explicitly part of the design.

13. Use external dependencies for supporting infrastructure when appropriate,
    but do not replace a core subsystem we intentionally want to own with an
    external database or storage engine.

14. Preserve unrelated repository changes and avoid destructive Git operations.
    Do not commit secrets, credentials, local runtime state, or unintended
    generated artifacts.

15. Treat the repository as the durable source of project knowledge. Information
    required by future work must not exist only in agent conversations.

    Preserve only the minimum durable knowledge introduced by a change:
    - behavior -> tests
    - architecture / invariants / durability / recovery -> `DESIGN.md`
    - stable repository-wide agent rules -> `AGENTS.md`
    - public usage / setup -> relevant documentation

    Do not persist temporary debugging notes, speculation, obvious code behavior,
    or incidental implementation details.

    Keep durable documentation concise and current. Prefer updating or replacing
    existing statements over appending historical narrative. `DESIGN.md` describes
    the current architecture and guarantees, not a changelog.

16. Keep the repository buildable and testable as the system evolves.

17. Use a separate branch for each logical change and open a pull request
   before merging into `main`. Use descriptive prefixes such as `feature/`,
   `fix/`, `ci/`, `docs/`, `chore/`, or `refactor/`. Keep unrelated changes
   out of the same branch. Do not create branches for read-only analysis,
   explanations, or commands that make no repository changes. Group small
   related documentation or maintenance edits into one branch instead of
   creating a branch for every individual line.

## Reference systems

Turbopuffer, SlateDB, OpenData, research papers, and other databases are
references, not specifications.

When borrowing an idea, understand the problem it solves and the assumptions
behind it, then justify its use independently for this project's workload and
invariants.

Do not copy architecture solely because another system uses it.

## Scope

Advanced mechanisms such as quantization, sharding, replication, consensus,
GPU execution, additional index families, or sophisticated query planning are
allowed when justified by requirements or measurements.

Do not optimize for feature parity with an existing database.

Establish single-node correctness before introducing distributed execution.
