# Adaptive query execution on remote shuffle services: progress

This document tracks the implementation of adaptive query execution (AQE) for remote
shuffle service (RSS) backends, following the "Sail AQE — High-Level Design". It lists
what is implemented, how it was validated, the decisions that differ from the design, and
what remains. The scheduler, statistics and adaptive rules depend only on the remote
shuffle service contract (committed map attempts, map-range reads, committed statistics);
Celeborn is the first service that implements it. The Flight shuffle path, Flight durability and AQE for the storage backend
are out of scope.

- Base: `6f0de46` (`fix: memory leak in SQL parsers (#2789)`)
- Development branch: `ccr-eb492ce0-ydmk3l` (pull request #1, the whole change)
- Review stack: the same final tree as five stacked branches, each one a pull request
  against the previous one.

| Layer | Branch | Content |
|---|---|---|
| 1 | `aqe/1-shuffle-service-committed-reads` | Remote shuffle service committed reads (Celeborn client) |
| 2 | `aqe/2-committed-statistics` | Committed shuffle statistics |
| 3 | `aqe/3-adaptive-planning` | Adaptive re-planning, coalescing and join decisions |
| 4 | `aqe/4-skew-splits` | Skew splits with map-range reads |
| 5 | `aqe/5-ci-and-benchmark` | CI, benchmark and this document |

Every layer builds, passes `cargo clippy --all-targets -D warnings`, nightly `rustfmt`
and the Rust tests of the changed crates on its own.

## Implemented

### Layer 1: remote shuffle service committed reads

- Batch headers use Celeborn's little-endian layout (`mapId`, `attemptId`, `batchId`,
  `length`). The worker sorts a partition file by map id with that layout, so map-range
  reads depend on it.
- Readers keep only the batches of the committed attempt of each mapper and drop
  duplicate batches. Before, a failed attempt that had pushed part of its output before
  failing was read in addition to the retried attempt, which duplicated rows. This was
  reproduced end to end (206,144 rows instead of 200,000) and is covered by an E2E test.
- Map-range reads (`OpenStream` with start and end map indices) for one partition across
  all its split locations, with a `PartitionReadOptions` API in the client and the Python
  binding.
- A shuffle is committed once. The selected attempt of every mapper (the first attempt
  of each to end) is fixed when the last mapper ends and is never revised, since a worker
  that has committed stops storing pushes for the shuffle. Workers commit concurrently,
  and a failed worker commit is retried with the identical request, which workers answer
  idempotently. If it keeps failing, the shuffle is lost: every `mapper_end` and every
  reader of it fails, so the query fails instead of returning partial data (open
  question 3 of the design).
- Workers serve committed files on HDFS, S3 or OSS as streams without chunks, which would
  read as empty. The commit response names the storage of every committed file, so a
  shuffle with files on distributed storage is lost at commit time and the query fails
  (see "Blockers").
- The lifecycle manager reports the commit (selected attempts and result) to an observer
  before answering any `mapper_end` that waits for it. `mapper_end` calls made during the
  commit wait for its outcome, so no producer task succeeds before the commit is known.
- The driver records the committed attempts per stage from a backend-neutral
  `ShuffleCommitted` message and keeps the first report. Task definitions pass them to
  remote shuffle service readers as the most common attempt plus the partitions with
  another one, so their size does not grow with the number of producer partitions.
  Reading a shuffle whose commit failed, or whose commit is not known, fails the consumer
  task.
- A producer's output stays readable until every consumer region has succeeded (or the
  region also reruns the producer), so a retried consumer region never reads cleaned-up
  data.
- Celeborn partition streams are opened lazily, at most eight at a time per task.
- `TCP_NODELAY` on Celeborn connections, and one write for each frame's prefix. Each
  request waits for its response, so Nagle's algorithm delayed every push by a delayed
  acknowledgement: a 2M-row aggregate took 91 s instead of 0.6 s.

### Layer 2: committed shuffle statistics

- Shuffle sinks count rows and logical (uncompressed Arrow) bytes per channel, and the
  remote shuffle service sink counts the compressed bytes it pushes. Counts are published only after the
  output is committed, and only when the task definition asks for them: the driver sets
  `collect_statistics` only when adaptive execution is enabled on a remote shuffle service, so execution
  without it is unchanged (design invariant 10).
- Tasks report exact totals and per-channel sizes as `ChannelSizes`. Logical bytes use
  a dense 16-bit code per channel, rounded upward within 1%, so averages cannot
  understate a coalescing cap. Physical bytes may use a sparse form above 2,000
  channels (nonempty bitmap, accurate codes for large channels, an average for the
  rest) when it is smaller. Legacy sparse logical reports conservatively bound each
  nonempty channel by the task's total. Empty stays exactly zero.
- The driver keeps the reported form, binds statistics to the attempt the shuffle service committed,
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
- Sizes of operator outputs come from bounds of the committed input sizes: exact
  through reordering projections, an upper bound through filters, limits and
  column-dropping projections, and no bound otherwise. A swap needs an exact build size
  larger than the probe's upper bound. A sort-merge conversion needs an exact build size
  above the cap. A group whose hash-join build side has no bound is not coalesced.
  Sorts do not preserve these sizes because rebatching can repeat dictionary values.
  Caps bound recorded logical shuffle bytes; rebatching and operator allocations
  can change execution memory. DataFusion receives exact row counts and inexact
  byte totals, and skew-replicated inputs receive no original exact totals.
- User-chosen partition counts (`repartition(n, ...)`) are kept.
- `EXPLAIN` notes that adaptive execution may re-plan the initial job graph, and
  `system.execution.stages` shows which stages were re-planned, by which rules, and their
  initial partition counts.

### Layer 4: skew splits

- A skewed bucket of a join input (larger than both `cluster.adaptive.skew.factor` times
  the median and `cluster.adaptive.skew.threshold`) is split by producer partition with
  Spark's `splitSizeListByTargetSize`. Each slice is read with a map-range read of
  the committed attempts, and paired with the whole bucket of the other input, or with each
  slice of the other input when both sides are split.
- Only the sides a join type allows are split (full outer joins never), only through
  operators that give the same rows on any subset of their input, and never below an
  operator that requires a distribution.
- The exact-coverage validator checks that the parts of a split bucket tile its producers
  and that the partitions are exactly the cross product of the sides' parts.

### Layer 5: CI and benchmark

- The Build workflow runs the remote shuffle service integration tests (Celeborn Docker
  fixtures) when the Celeborn client, the execution crate or those tests change, or on request
  (`[celeborn tests]` in the commit message or the `run celeborn tests` label).
- A manually triggered workflow benchmarks AQE off and on against a native Celeborn
  cluster (the reference remote shuffle service) and posts the table to the job summary.

## Validation

All results are from the final tree unless noted.

| Check | Result |
|---|---|
| `cargo clippy --all-targets -- -D warnings` (changed crates) | clean, every layer |
| `cargo +nightly fmt --check`, `hatch fmt --check` | clean |
| `sail-execution` unit and property tests | 224 passed (layer 1: 148, layer 2: 165, layer 3: 211) |
| `sail-celeborn` / `sail-common` / `sail-system-store` tests | 12 / 12 / 9 passed |
| Celeborn client integration tests, Docker (`pysail.tests.celeborn`) | 21 passed |
| Spark Celeborn tests, Docker (`test_shuffle_celeborn`, `test_adaptive_execution`) | 10 passed |
| Same suites against a native Celeborn 0.6.3 cluster | passed (before the commit protocol change) |
| Commit failure tests (`test_shuffle_commit`), 10 repetitions | 30 passed |

Property tests cover the size codes (1% bound, emptiness, totals, proto round trip), the
coalescer (coverage and caps), the exact-coverage contract (valid split specs pair every
row exactly once, and any corrupted spec that the contract still accepts does too) and
Spark's split helper.

Failure injection: a producer task that pushes part of its output and then fails is
retried, and its output is read exactly once (with and without AQE); a worker that
rejects `CommitFiles` once, before or after the other worker has committed, gets the
identical request again and every row is returned; a worker that keeps rejecting it
makes the query fail with a lost-shuffle error (Docker, through a frame-level proxy that
sets Celeborn's `mockFailure` flag); a failed commit fails the consumer's definition; a rule failure keeps the static plan and freezes
the decision; retried consumer tasks reuse the adapted plan; statistics of an attempt that
Celeborn did not commit are never used.

### Benchmark

The table below records the original run. Its task and partition counts include
telemetry-query jobs from the old measurement code, so the task-reduction percentages
are not reliable. The benchmark now observes each workload from a separate session;
a new run is required before using these counts for performance conclusions.

`scripts/adaptive/benchmark.py`, release build, native Celeborn 0.6.3 with two workers,
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

Wall time changes little on one 4-core machine, where the longest task dominates. No swap fires in the third
workload: the computed join keys come from a projection in the producer stage, and the
static plan already builds from the smaller side.

## Decisions and deviations from the design

- **Skew splits on a remote shuffle service.** The design leaves them out for Celeborn and
  asks whether map-range reads are planned (open question 9). Skew splitting requires
  map-range reads from the shuffle service; the Celeborn worker already serves them for a
  partition sorted by map id, so the native client supports them, filtered by the committed
  attempts. The first map-range read of a partition file makes the worker sort it by map
  id (as for Spark's skew splits on Celeborn), so a split costs one sort of the skewed
  partition on the worker.
- **Statistics encoding.** The design keeps per-channel rows and one-byte log sizes with
  about 10% error. Per-channel rows were unused and doubled the volume, and one byte
  saturates above about 36 GB, so tasks report exact totals and two-byte sizes within 1%,
  with a sparse form for many channels. The driver keeps that form instead of re-encoding.
- **Statistics only with AQE.** The design collects statistics for observability before
  any rule ships. They are collected only when AQE is enabled on a remote shuffle service, so execution
  without AQE is unchanged.
- **Sound sizes.** The design's build guard sums the logical bytes of every input below the
  build side. That is not a bound through a nested join or a widening projection, so the
  guard uses the bounds above and disables coalescing where the build side has none. A
  logical-size cap also applies to every coalesced partition, since compressed sizes can
  understate the data a task reads by a large factor.
- **Minimum-size merges** run left to right in linear time instead of always merging the
  globally smallest range, which can give different (still valid) ranges in ties.
- **Failed `commit_files`** (open question 3). Workers commit a shuffle once and then
  drop pushes for it, so the selected attempts can never change after any worker has
  committed. The commit is retried as is, and a commit that keeps failing loses the
  shuffle and fails the query. An earlier version re-selected attempts after a failure;
  a Docker reproduction showed it silently lost the rows of the worker that had already
  committed, and it was replaced by this protocol.
- **The table** is `system.execution.stages` (the design refers to `cluster.stages`).
- **Generic remote shuffle service naming.** Code outside the Celeborn client and its
  stream adapter is named for remote shuffle services (`is_remote_shuffle_service`,
  `ShuffleCommitted`, `ShuffleServiceOutput`), so another service can implement the same
  contract without touching the scheduler or the adaptive rules.
- **Topology rebuilds.** A rewrite rebuilds the job topology, at most once per stage per
  job. `refresh_job` already does O(tasks) work on every scheduler event, so this does not
  change the scheduler's complexity, and batching rewrites would weaken the per-stage
  atomic validation.

## Blockers and limitations

- **Celeborn partitions on distributed storage** (HDFS, S3, OSS) are not read by the
  native client. A shuffle with such files is lost at commit time, so the query fails
  instead of returning partial data. Celeborn's default local-disk and memory storage is
  unaffected.
- **A lost shuffle fails the query.** Recomputing the producer stage under a new shuffle
  ID, as Spark does after a fetch failure on Celeborn, is not implemented.
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
  pysail.tests.spark.execution.test_adaptive_execution

# Or a native Celeborn cluster without Docker
scripts/celeborn/local-cluster.sh start
eval "$(scripts/celeborn/local-cluster.sh env)"
hatch run pytest -m integration --pyargs pysail.tests.celeborn

# Benchmark (with a release build of pysail installed)
python scripts/adaptive/benchmark.py --master 127.0.0.1:12097 --output results.json
```
