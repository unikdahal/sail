# Adaptive query execution on Celeborn: progress

This document tracks the implementation of adaptive query execution (AQE) for the
Celeborn shuffle backend, following the "Sail AQE — High-Level Design". It lists what
is implemented, how it was validated, the decisions that differ from the design, and
what remains. The Flight shuffle path, Flight durability and AQE for the storage backend
are out of scope.

- Base: `6f0de46` (`fix: memory leak in SQL parsers (#2789)`)
- Development branch: `ccr-eb492ce0-ydmk3l` (pull request #1, the whole change)
- Review stack: the same final tree as five stacked branches, each one a pull request
  against the previous one.

| Layer | Branch | Content |
|---|---|---|
| 1 | `aqe/1-celeborn-committed-reads` | Celeborn client correctness and committed reads |
| 2 | `aqe/2-committed-statistics` | Committed shuffle statistics |
| 3 | `aqe/3-adaptive-planning` | Adaptive re-planning, coalescing and join decisions |
| 4 | `aqe/4-skew-splits` | Skew splits with Celeborn map-range reads |
| 5 | `aqe/5-ci-and-benchmark` | CI, benchmark and this document |

Every layer builds, passes `cargo clippy --all-targets -D warnings`, nightly `rustfmt`
and the Rust tests of the changed crates on its own.

## Implemented

### Layer 1: Celeborn client correctness and committed reads

- Batch headers use Celeborn's little-endian layout (`mapId`, `attemptId`, `batchId`,
  `length`). The worker sorts a partition file by map id with that layout, so map-range
  reads depend on it.
- Readers keep only the batches of the committed attempt of each mapper and drop
  duplicate batches. Before, a failed attempt that had pushed part of its output before
  failing was read in addition to the retried attempt, which duplicated rows. This was
  reproduced end to end (206,144 rows instead of 200,000) and is covered by an E2E test.
- Map-range reads (`OpenStream` with start and end map indices) for one partition across
  all its split locations, with a `PartitionReadOptions` API in the client and the Python
  binding. Readers fail clearly on DFS-backed partitions, which the native client does not
  read (see "Blockers").
- The lifecycle manager reports each shuffle commit (the selected attempt of every mapper
  and the result) to an observer before acknowledging the last mapper. After a failed
  `commit_files`, the mapper that triggered it is retried with a new commit instead of
  being acknowledged with unverified data (open question 3 of the design).
- The driver records the committed attempts per stage. Task definitions pass them to
  Celeborn readers, and reading a shuffle whose commit failed fails the consumer task.
- A producer's output stays readable until every consumer region has succeeded (or the
  region also reruns the producer), so a retried consumer region never reads cleaned-up
  data.
- Celeborn partition streams are opened lazily, at most eight at a time per task.
- `TCP_NODELAY` on Celeborn connections, and one write for each frame's prefix. Each
  request waits for its response, so Nagle's algorithm delayed every push by a delayed
  acknowledgement: a 2M-row aggregate took 91 s instead of 0.6 s.

### Layer 2: committed shuffle statistics

- Shuffle sinks count rows and logical (uncompressed Arrow) bytes per channel, and the
  Celeborn sink counts the compressed bytes it pushes. Counts are published only after the
  output is committed, and only when the task definition asks for them: the driver sets
  `collect_statistics` only when adaptive execution is enabled on Celeborn, so execution
  without it is unchanged (design invariant 10).
- Tasks report exact totals and per-channel sizes as `ChannelSizes`: a 16-bit code
  `1 + ceil(log_1.01(size))` per channel (within 1%, every `u64` representable), or above
  2,000 channels a sparse form (nonempty bitmap, accurate codes for channels above twice
  the average, one average for the rest) when it is smaller. Empty stays exactly zero.
- The driver keeps the reported form, binds statistics to the attempt Celeborn committed,
  folds them per bucket once per producer stage and caches the fold, and releases them
  once every consumer has made its decision. Received and peak retained bytes are logged
  per job.
- `system.execution.stages` shows rows, physical and logical bytes, and the largest and
  median bucket size of each committed stage.

### Layer 3: adaptive re-planning, coalescing and join decisions

- A stage is re-planned once, just before any of its regions runs, from an immutable
  snapshot of its plan and the committed statistics of its producers. Rules are pure
  functions of the snapshot. A rewrite is validated (alignment groups, exact coverage of
  every bucket, join modes, ordering requirements, consumer placeholders, the slot cap and
  a codec round trip) and committed atomically with the rebuilt topology, or not at all.
  The decision is frozen, so retries reuse it.
- Rewritten inputs declare `UnknownPartitioning` to DataFusion and carry the coalesced
  hash property as Sail metadata, so no operator relies on a partitioning that is false.
- Hash coalescing follows Spark's algorithm per alignment group, with hard caps on every
  coalesced range: uncompressed bytes (`cluster.adaptive.coalesce.max_logical_size`) and
  the build side of every partitioned hash join in the group.
- Join decisions: empty-input pruning, build/probe swap and hash join to sort-merge join.
- Sizes of operator outputs come from sound bounds of the committed input sizes: exact
  through reordering projections and sorts, an upper bound through filters, limits and
  column-dropping projections, and no bound otherwise. A swap needs an exact build size
  larger than the probe's upper bound. A sort-merge conversion needs an exact build size
  above the cap. A group whose hash-join build side has no bound is not coalesced.
- User-chosen partition counts (`repartition(n, ...)`) are kept.
- `EXPLAIN` notes that adaptive execution may re-plan the initial job graph, and
  `system.execution.stages` shows which stages were re-planned, by which rules, and their
  initial partition counts.

### Layer 4: skew splits

- A skewed bucket of a join input (larger than both `cluster.adaptive.skew.factor` times
  the median and `cluster.adaptive.skew.threshold`) is split by producer partition with
  Spark's `splitSizeListByTargetSize`. Each slice is read with a Celeborn map-range read of
  the committed attempts, and paired with the whole bucket of the other input, or with each
  slice of the other input when both sides are split.
- Only the sides a join type allows are split (full outer joins never), only through
  operators that give the same rows on any subset of their input, and never below an
  operator that requires a distribution.
- The exact-coverage validator checks that the parts of a split bucket tile its producers
  and that the partitions are exactly the cross product of the sides' parts.

### Layer 5: CI and benchmark

- The Build workflow runs the Celeborn integration tests (Docker fixtures) when the
  Celeborn client, the execution crate or the Celeborn tests change, or on request
  (`[celeborn tests]` in the commit message or the `run celeborn tests` label).
- A manually triggered workflow benchmarks AQE off and on against a native Celeborn
  cluster and posts the table to the job summary.

## Validation

All results are from the final tree unless noted.

| Check | Result |
|---|---|
| `cargo clippy --all-targets -- -D warnings` (changed crates) | clean, every layer |
| `cargo +nightly fmt --check`, `hatch fmt --check` | clean |
| `sail-execution` unit and property tests | 219 passed (layer 1: 143, layer 2: 159, layer 3: 205) |
| `sail-celeborn` / `sail-common` / `sail-system-store` tests | 12 / 12 / 9 passed |
| Celeborn client integration tests, Docker (`pysail.tests.celeborn`) | 18 passed |
| Spark Celeborn tests, Docker (`test_shuffle_celeborn`, `test_shuffle_celeborn_adaptive`) | 10 passed |
| Same suites against a native Celeborn 0.6.3 cluster | passed |

Property tests cover the size codes (1% bound, emptiness, totals, proto round trip), the
coalescer (coverage and caps), the exact-coverage contract (valid split specs pair every
row exactly once, and any corrupted spec that the contract still accepts does too) and
Spark's split helper.

Failure injection: a producer task that pushes part of its output and then fails is
retried, and its output is read exactly once (with and without AQE); a failed Celeborn
commit fails the consumer's definition; a rule failure keeps the static plan and freezes
the decision; retried consumer tasks reuse the adapted plan; statistics of an attempt that
Celeborn did not commit are never used.

### Benchmark

`scripts/celeborn/benchmark_aqe.py`, release build, native Celeborn 0.6.3 with two workers,
local-cluster mode on a 4-core machine, parallelism 200, coalesce target and skew threshold
4 MiB, median of three runs. Results are identical with AQE off and on for every workload.

| Workload | AQE | Wall s | Tasks | Partitions (initial) | Rules | Task s |
|---|---|---|---|---|---|---|
| over-partitioned aggregate | off | 2.80 | 604 | 604 | - | 381.8 |
| over-partitioned aggregate | on | 2.37 | 409 | 409 (604) | coalesce | 230.4 |
| skewed join | off | 7.36 | 1005 | 1005 | - | 1088.1 |
| skewed join | on | 6.97 | 624 | 624 (1005) | coalesce, skew_split | 877.2 |
| join with a larger planned build side | off | 5.76 | 1005 | 1005 | - | 789.7 |
| join with a larger planned build side | on | 5.47 | 622 | 622 (1005) | coalesce | 696.5 |
| join with an empty side | off | 3.51 | 805 | 805 | - | 511.5 |
| join with an empty side | on | 3.61 | 805 | 805 | empty_input | 542.8 |

Where coalescing applies, AQE removes 32-38% of the tasks and 12-40% of the task time.
Wall time changes little on one 4-core machine, where the longest task dominates. No swap fires in the third
workload: the computed join keys come from a projection in the producer stage, and the
static plan already builds from the smaller side.

## Decisions and deviations from the design

- **Skew splits on Celeborn.** The design leaves them out for Celeborn and asks whether
  map-range reads are planned (open question 9). The worker already serves map-range reads
  of a partition sorted by map id, so the native client supports them, filtered by the
  committed attempts.
- **Statistics encoding.** The design keeps per-channel rows and one-byte log sizes with
  about 10% error. Per-channel rows were unused and doubled the volume, and one byte
  saturates above about 36 GB, so tasks report exact totals and two-byte sizes within 1%,
  with a sparse form for many channels. The driver keeps that form instead of re-encoding.
- **Statistics only with AQE.** The design collects statistics for observability before
  any rule ships. They are collected only when AQE is enabled on Celeborn, so execution
  without AQE is unchanged.
- **Sound sizes.** The design's build guard sums the logical bytes of every input below the
  build side. That is not a bound through a nested join or a widening projection, so the
  guard uses the bounds above and disables coalescing where the build side has none. A
  logical-size cap also applies to every coalesced partition, since compressed sizes can
  understate the data a task reads by a large factor.
- **Minimum-size merges** run left to right in linear time instead of always merging the
  globally smallest range, which can give different (still valid) ranges in ties.
- **Re-commit after a failed `commit_files`** is implemented (open question 3).
- **The table** is `system.execution.stages` (the design refers to `cluster.stages`).
- **Topology rebuilds.** A rewrite rebuilds the job topology, at most once per stage per
  job. `refresh_job` already does O(tasks) work on every scheduler event, so this does not
  change the scheduler's complexity, and batching rewrites would weaken the per-stage
  atomic validation.

## Blockers and limitations

- **DFS-backed Celeborn partitions** are not read by the native client (the worker returns
  a full path instead of a stream). Readers fail clearly instead of returning partial data.
  Celeborn's default local-disk storage is unaffected.
- **Re-commit after a failed `commit_files`** is covered by reasoning and the driver-side
  tests (a failed commit fails the consumer; a new commit replaces the old one and drops
  cached statistics), not by an integration test that makes a worker fail `commitFiles`.
- **Per-layer Python tests.** The Python suites ran on the final tree. For the lower layers
  they run in CI on their pull requests.
- **Benchmarks** ran on one 4-core machine. The manual workflow can repeat them with larger
  scales on GitHub-hosted runners.

## Remaining work

- Measure peak hash-join build memory (`build_mem_used`) against the build cap on
  representative queries before raising the default cap.
- Larger-scale benchmark runs (`--scale`) on bigger machines.
- AQE for the storage backend (file-range skew splits) and the Flight path are out of scope.

## Running the tests locally

```bash
# Celeborn in Docker (the default for the test fixtures)
hatch run pytest -m integration --pyargs pysail.tests.celeborn \
  pysail.tests.spark.execution.test_shuffle_celeborn \
  pysail.tests.spark.execution.test_shuffle_celeborn_adaptive

# Or a native Celeborn cluster without Docker
scripts/celeborn/local-cluster.sh start
eval "$(scripts/celeborn/local-cluster.sh env)"
hatch run pytest -m integration --pyargs pysail.tests.celeborn

# Benchmark (with a release build of pysail installed)
python scripts/celeborn/benchmark_aqe.py --master 127.0.0.1:12097 --output results.json
```
