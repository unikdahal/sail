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
- Commit freezes mapper selection and drains accepted revivals before snapshotting
  worker locations, so every published replacement epoch participates in the commit.
- Actor shutdown drains accepted requests before acknowledging Stop. Lifecycle
  shutdown also attempts cleanup for failed allocations with explicit shuffle IDs;
  remote cleanup remains best effort. Stopping one client preserves its shared
  lifecycle manager. Pools evict idle workers after the last shuffle cleanup,
  while already-held clients and readers retain their transport references.
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
  every bucket, join modes, single-partition and ordering requirements, whole-probe
  join boundaries, consumer placeholders, the slot cap and a codec round trip) and
  committed atomically with the rebuilt topology, or not at all.
  The decision is frozen, so retries reuse it.
- Rewritten inputs declare `UnknownPartitioning` to DataFusion and carry the coalesced
  hash property as Sail metadata, so no operator relies on a partitioning that is false.
- Hash coalescing follows Spark's algorithm per alignment group, with hard caps on every
  coalesced range: uncompressed bytes (`cluster.adaptive.coalesce.max_logical_size`) and
  the build side of every partitioned hash join in the group.
- Join decisions: empty-input pruning, build/probe swap and hash join to sort-merge join.
- Sizes of operator outputs come from bounds of the committed input sizes: exact
  through reordering projections, an upper bound through limits and
  column-dropping projections, and no bound otherwise. A swap needs an exact build size
  larger than the probe's upper bound. A sort-merge conversion needs an exact build size
  above the cap. A group whose hash-join build side has no bound is not coalesced.
  Filters and sorts do not preserve these sizes because rebatching can repeat dictionary values.
  Caps bound recorded logical shuffle bytes; rebatching and operator allocations
  can change execution memory. DataFusion receives exact row counts and inexact
  byte totals, and skew-replicated inputs receive no original exact totals.
- User-chosen partition counts (`repartition(n, ...)`) are kept.
- Checkpoint writer stages retain their planned partition boundaries because the
  saved checkpoint descriptor cannot yet represent an adaptive runtime layout.
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

The following results record the original stack. Follow-up validation is recorded
separately below.

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
  with a sparse form for physical bytes on many channels. Logical bytes stay dense
  to provide per-channel upper bounds. The driver keeps the reported form instead
  of re-encoding.
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

## Correctness follow-up (2026-10-17)

[PR #10](https://github.com/unikdahal/sail/pull/10) follows the top of the stack
at `0b236c9f` and fixes several issues found in repeated reviews:

- A canceled Celeborn request discards its socket, so a later request cannot read
  its partial or delayed response. Duplicate mapper completions wait for the
  driver's commit observer before returning success.
- Logical channel sizes are conservative upper bounds. Sparse averaging,
  floating-point rounding and overflowing sums cannot bypass coalescing caps.
  Stored physical bytes include the persisted batch header.
- Skew splitting rejects volatile projections and filters, and checks its Cartesian
  partition count before allocation. Sort rebatching no longer supplies a byte
  bound, and joins with pushed-down dynamic filters keep their other adaptive rules.
- Terminal jobs release attempt statistics and cached folds, including on failure;
  late task reports cannot retain them again. Validation checks every placeholder
  occurrence, and shuffle locators use each input's own partition domain.
- Benchmark and test telemetry use separate observer sessions. Build covers stacked
  PRs, configuration edits trigger Celeborn coverage, and Python CI runs the new
  benchmark regressions.

Local validation used Rust 1.99.1, Python 3.12.14 and Celeborn 0.6.3:

| Check | Result |
|---|---|
| `cargo test --locked -p sail-celeborn -p sail-execution --no-fail-fast` | 15 + 242 passed; doc tests passed |
| New cancellation and commit-observer tests, 20 consecutive rounds | 60 passed |
| `cargo clippy --locked -p sail-celeborn -p sail-execution --all-targets -- -D warnings` | passed |
| Native extension build (`maturin develop --locked`) | passed |
| Docker client, static shuffle and AQE integration suites | 31 passed; existing pytest configuration deprecation warning |
| `scripts/adaptive/test_benchmark.py` | 2 passed |
| Nightly Rust formatting, Python lint/format, actionlint, diff checks | passed |

Rust checks used `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`,
`CARGO_INCREMENTAL=0` and `CARGO_BUILD_JOBS=2`. This environment's Python sysconfig
names `/install/lib`; the linker and loader used the actual runtime library
directory through `LIBRARY_PATH` and `LD_LIBRARY_PATH`. The mounted Celeborn config
needed mode `0644` so the container user could read it. These were environment
corrections, without repository configuration changes.

GitHub reports the fork's Build workflow as `disabled_fork`; these results are
local checks, not GitHub Actions results. The corrected benchmark has not been
rerun for performance measurements.

### Ordering follow-up (2026-10-17)

A further review of `84b25f68` found that adapted inputs retained ordering and
partition-local constant metadata after combining shuffle buckets. A hash shuffle
with one sorted producer can legitimately advertise individually sorted buckets
without its explicit preserve-order option. Coalesced remote reads interleave
those buckets, so the old metadata could let a sort-merge join accept unsorted rows.

Adapted placeholders now clear orderings and partition-local constants and refresh
the cached output ordering. Valid column equivalences and global constants remain.
The existing validator rejects a proposal that loses required ordering, preserving
the original plan, task count and input specs and freezing the static decision.

The real JobGraph regression failed on the original code because the unsafe
four-to-one proposal passed validation. It now passes, along with a separate
regression for ordering inferred from partition-local constants. Two independent
reviews found no issues in the fix.

Local checks on this follow-up: 15 Celeborn and 244 execution Rust tests passed;
all 31 Docker integration tests passed; native build, clippy with warnings denied,
nightly formatting and diff checks passed. After strengthening the ordering error
assertions, all three focused coalescing tests and clippy passed again. The Docker
suite retains the existing pytest configuration deprecation warning. GitHub Build
is still `disabled_fork`; these are local results.

### Worker cleanup and fetch-limit follow-up (2026-10-17)

Fetched PR #10 and the AQE stack again before continuing from `e0c7ec67`;
the base remained `0b236c9f`. Two further defects were reproduced and fixed:

- Successful OpenStream registrations were never ended after reading. Celeborn
  0.6.3 retains their stream state, and an active stream can pin the original
  shuffle file while a map-range read creates a sorted replacement. The reader
  now sends the one-way `PbBufferStreamEnd(ChunkStream)` RPC on EOF, read errors
  and early drop. The socket is discarded if that write is interrupted. A guard
  captures the runtime for drops outside Tokio, and cancellation during cleanup
  leaves the stream ID armed for a best-effort retry. Cleanup does not replace
  the original read error.
- Finite embedded fetch limits were treated as partition-independent. Skew
  splitting could restart a filter's limit for each slice; coalescing could apply
  it once across several original buckets. Actual DataFusion joins demonstrate
  one match becoming two after splitting and four matches becoming one after
  coalescing. Alignment domains containing finite fetch limits now keep their
  boundaries through both rules. The restriction propagates through unions;
  independent union siblings and broadcast probe domains remain eligible.

The EOF wire regression timed out on the original reader because no stream-end
message arrived. Both filter eligibility regressions failed before their fixes;
the unsafe coalescing proposal also passed plan validation. Nine loopback
regressions cover normal/empty EOF, fetch and batch errors, invalid chunk counts,
unpolled/partial drops outside Tokio and cancellation during a pending fetch.
Planner controls cover filters on either join side and above joins, local limits,
fetched sorts/joins, a single-bucket skew domain, unions and broadcast isolation.
The production changes and expanded regression coverage were independently
reviewed with no blocking findings.

Latest local checks on the final code:

| Check | Result |
|---|---|
| Rust Celeborn and execution suites | 24 + 250 passed; doc tests passed |
| Nine new worker lifecycle regressions, 20 consecutive rounds | 180 passed |
| Clippy, all targets, warnings denied | passed |
| Native extension build | passed |
| Docker client, static shuffle and AQE integration suites | 31 passed; existing pytest configuration deprecation warning |
| Benchmark unit tests | 2 passed |
| Nightly Rust formatting and diff checks | passed |

The earlier 60 repeated cancellation/commit-observer checks, Python lint/format
and workflow lint results above remain historical validation, not fresh runs.
The current formatting check also reformatted two assertions from the ordering
follow-up. The fork's GitHub Build workflow is still `disabled_fork`; these are
local checks. Performance benchmarks have not been rerun.

Stream cleanup is best effort and requires a known server-generated stream ID.
If the initial OpenStream response is lost before its ID is decoded, cleanup
still depends on Celeborn's eventual shuffle cleanup. Normal stream-end also
leaves Celeborn's own per-shuffle numeric ID tracking until shuffle cleanup;
this fix releases the registered stream state and original-file pin.

### Broadcast metadata and lost-shuffle reads (2026-10-18)

Fetched the open stack and PR #10 again: its head was `40dad51d` and the top
of the stack remained `0b236c9f`. This review reproduced two new issues:

- A Celeborn broadcast locator repeated the same read group for every output
  partition of its consumer, although the broadcast input has one logical
  partition. Each task builds and serializes its own locator, causing avoidable
  quadratic metadata growth across a wide probe stage. A real JobGraph regression
  observed 1, 64 and 256 repeated groups, with per-task protobuf sizes of 19, 335
  and 1,295 bytes. The locator now contains one all-channel group independent of
  consumer width. The regression checks CollectLeft hash joins and cross joins,
  first/last tasks and nonuniform committed-attempt round trips.
- After a terminal global commit failure, lifecycle registration still returned
  its cached reservation. Direct Rust/Python readers refresh locations through
  this API, so they could read a healthy worker's output from a lost shuffle.
  Registration now rejects the terminal Lost state before consulting its cache.
  Successful and not-yet-terminal registration behavior is unchanged; the query
  engine already had its own failed-commit guard.

The broadcast regression failed before the fix because encoded size grew with
probe partitions. The actor regression caused a real loopback CommitFiles failure,
waited for mapper_end to report Lost, then reproduced the cached-reservation
bypass. Against the previously installed native extension, the Docker regression
also failed: a direct read completed without error after global failure. That
regression explicitly requests two workers, observes the healthy worker's commit,
then persistently fails partition 1's worker. Returned worker IDs alone do not
prove primary placement because Celeborn may include workers with empty slot lists.
Both fixes and their regressions received independent review.

Fresh local validation:

| Check | Result |
|---|---|
| Rust Celeborn and execution suites | 25 + 251 passed; doc tests passed |
| Clippy, all targets, warnings denied | passed |
| Native extension build | passed |
| Docker client, static shuffle and AQE integration suites | 32 passed; existing pytest configuration deprecation warning |
| Benchmark unit tests | 2 passed |
| Full Python lint/format, nightly Rust formatting and diff checks | passed |

The earlier 60 and 180 repeated concurrency/lifecycle checks remain historical
results; they were not rerun in this pass. The Docker suite retains its existing
pytest configuration deprecation warning. GitHub Build remains `disabled_fork`;
these checks are local. No new performance or peak-memory benchmark was run; the
broadcast regression measures locator count and encoded size.

### Cleanup draining, checkpoint layouts and validation bounds (2026-10-18)

Fetched PR #10 and the open AQE stack again. PR #10 was still open at
`21fd2903`, and `aqe/5-ci-and-benchmark` remained `0b236c9f`.
This pass reproduced and fixed four further defects:

- Unregister could finish while registration, revival or commit callbacks were
  still in flight. A delayed callback could restore cleared reservations or
  locations, or recreate resources after master cleanup. Both lifecycle and
  shuffle-client actors now close the ID to new work, drain tracked callbacks,
  coalesce duplicate cleanup requests, and then finish cleanup. Lifecycle callers
  waiting on cancelled work fail promptly. All workers' commits are awaited even
  if one fails; a failed unregister keeps the ID closed until an explicit retry
  succeeds. Local-only cleanup remains local, and successful cleanup allows ID
  reuse. A bounded lookup revision prevents delayed ID lookups from restoring
  local caches without retaining per-ID tombstones or discarding unrelated
  lookup results.
- AQE could split a hot hash bucket into three mapper slices and coalesce the
  other buckets, retaining four tasks while changing key boundaries. The
  checkpoint commit stage only reconciles the partition count, so it saved a
  false `Hash(..., 4)` descriptor. A real JobGraph regression reproduced this
  count-preserving rewrite; a DataFusion checkpoint readback join produced at
  most one of three matches when trusting that descriptor. Checkpoint writer
  stages now retain their planned layout until checkpoint metadata can carry the
  actual runtime distribution.
- An unlimited, all-pass FilterExec can rebatch dictionary arrays and repeat
  dictionary values. The real operator regression preserves all 103 rows while
  increasing recorded logical bytes by more than three times, invalidating its
  assumed upper byte bound. Filters now have no such bound, so size-dependent
  coalescing and join decisions fall back conservatively. Column-dropping
  projections remain covered by positive upper-bound controls.
- The benchmark compared only warmup results and discarded the timed results,
  allowing incorrect measured runs to report parity and exit successfully.
  Every timed result is now compared to its mode's warmup after stopping the
  timer. A mismatch persists even if a later repetition is correct. Regressions
  reproduce false success in either mode and retain an order-insensitive passing
  control.

The lifecycle registration regression and both original client drain regressions
failed before the cleanup changes. Eleven new controlled race tests cover
registration, revival, commits, the observer boundary, read refresh, push revival,
lookup caching, duplicate cleanup, failure/retry and ID reuse. The corrected
filter-bound regression failed against the old Upper classification, and the
checkpoint regression failed against the old accepted rewrite. The benchmark
regressions failed with false parity before the fix. The changes received
independent review; a detached read-reply boundary race and failed-cleanup
reopening were corrected during that review.

Fresh local validation:

| Check | Result |
|---|---|
| Rust Celeborn and execution suites | 36 + 256 passed; doc tests passed |
| Eleven new cleanup regressions, 20 consecutive rounds | 220 passed |
| Clippy, all targets, warnings denied | passed |
| Native extension build | passed |
| Docker client, static shuffle and AQE integration suites | 32 passed; existing pytest configuration deprecation warning |
| Benchmark unit tests | 5 passed |
| Full Python lint/format, nightly Rust formatting and diff checks | passed |

The earlier 60 and 180 repeated concurrency/lifecycle results remain historical;
the 220 checks above exercise this pass's new cleanup regressions. GitHub Build
was freshly checked and remains `disabled_fork`; these results are local.
Performance and peak-memory benchmarks have not been rerun. The cleanup drain
covers tracked operations before acknowledgement; actor shutdown and revocation
of already-delivered lazy read streams are outside this change. Cleanup progress
still depends on in-flight operations completing, including the documented
nonblocking commit-observer contract.

### Global-sort skew safety and in-flight revival (2026-10-19)

Fetched PR #10 and the open stack again: the follow-up was still open at
`d0e03330`, with `aqe/5-ci-and-benchmark` still at `0b236c9f`.
Two new defects were reproduced before production changes:

- Skew splitting accepted unlimited global SortExec nodes. A global sort keeps
  one output partition and reads input partition 0; splitting below it left
  later slices unread. With a supported one-bucket configuration and
  `skew_factor=0.5`, the real scheduler accepted this layout. An executable
  DataFusion join returned one match instead of two. Global sorts now prevent
  splitting, and rewrite validation enforces every SinglePartition input
  requirement. Unlimited partition-preserving sorts remain eligible. Controls
  cover hash and sort-merge joins, either/both sorted sides, actual row counts,
  and the real JobGraph's static fallback.
- Final mapper completion could snapshot worker locations while an already
  accepted revive was still reserving a replacement epoch. The late callback
  then exposed an epoch omitted from CommitFiles after global success. Mapper
  selection now freezes while pending revivals drain; the commit snapshots all
  completed replacements and retains the original selected attempts. New
  revivals are rejected after selection freezes. Duplicate mapper completions
  wait for the same outcome, failed revival still advances the commit, and
  unregister cancels the wait without starting a commit or hanging cleanup.

All six new regressions failed against the fetched head while the existing
36 Celeborn and 256 execution tests passed. Three lifecycle regressions use a
scripted master and two loopback worker endpoints, holding the replacement's
ReserveSlots response so connection serialization cannot mask the race. They
verify exact committed epoch IDs and attempts, failure handling and cancellation.
The lifecycle issue is a commit-boundary reliability defect: the wire tests
establish the missing revival barrier, and code review identified the later
publication of an uncommitted epoch. Silent loss of selected rows was not
demonstrated. Both fixes received independent review with no blocking findings.

Fresh local validation:

| Check | Result |
|---|---|
| Rust Celeborn and execution suites | 39 + 259 passed; doc tests passed |
| Fourteen cleanup/revive/commit regressions, 20 consecutive rounds | 280 passed |
| Clippy, all targets, warnings denied | passed |
| Native extension build | passed |
| Docker client, static shuffle and AQE integration suites | 32 passed; existing pytest configuration deprecation warning |
| Benchmark unit tests | 5 passed |
| Full Python lint/format, nightly Rust formatting and diff checks | passed |

Earlier 60, 180 and 220 repeated-test results remain historical; the 280 checks
above are fresh and include this pass's three lifecycle regressions. GitHub Build
was checked again and remains `disabled_fork`; these are local results.
Performance and peak-memory benchmarks have not been rerun.

### Shutdown, worker connections and whole-probe joins (2026-10-21–22)

Fetched the open stack and PR #10 again before review and after resuming.
The follow-up remained open at `c6de67f4`, and the stack top remained
`0b236c9f`. This review reproduced three further defects:

- Actor Stop bypassed the per-shuffle drain barriers. It could acknowledge
  shutdown while registration, revival, commit or observer work still ran, then
  abort callbacks or allow worker allocation to overtake cleanup. Both actors
  now reject new work, coalesce accepted Stop callers and drain tracked work
  before acknowledging. Lifecycle Stop attempts cleanup for every accepted
  allocation ID, including an explicit ID whose registration already failed
  after reserving resources. Client Stop remains local to its shared lifecycle
  manager. Controls cover lookups, commit/observer ordering, duplicate Stop,
  cleanup failures and rejected work after shutdown begins.
- Worker pools retained strong clients and their RPC/push/fetch sockets for every
  historical worker until actor destruction. Shuffle cleanup now evicts clients
  for workers absent from remaining live location histories/reservations;
  partial reservation errors also release unused cache entries. An actual TCP
  regression keeps one worker connection shared by two shuffles, proves the
  first cleanup preserves reuse, and observes EOF after the last cleanup.
  Existing clients/readers retain their own transport references. Independent
  later jobs may reconnect; no performance or peak-memory benchmark was run.
- Skew splitting beneath joins that need the whole probe side could expand an
  initially valid single-task stage. Build-match bitmaps and probe completion
  counters are local to each deserialized plan, so independently executed task
  partitions cannot coordinate final build-side output. The real JobGraph
  accepted a one-to-four rewrite, and executable isolated tasks lost an unmatched
  build row (one static row became zero). The regression covers nested-loop
  left anti and null-aware CollectLeft hash joins. Skew traversal now preserves
  whole-probe boundaries, and validation checks the same requirement. Controls
  cover other build-output join types, independent inner/right probe joins and
  PiecewiseMergeJoin's left/full validation requirements.

The first six shutdown regressions and the idle-worker TCP regression all failed
before the fixes while all 39 existing Celeborn tests passed. Three new AQE
regressions failed against the old rules/validator while all 259 existing
execution tests passed. Additional controls cover failed explicit allocation,
remote cleanup failure, lookup completion, existing-client ownership and
piecewise merge joins. Loopback fixtures were strengthened to observe socket EOF
and accept a fresh RPC connection when an ID is reused after cleanup. The fixes
received independent review with no blocking findings.

Validation for the final source (Rust/native/Python checks on October 21,
Docker integration on October 22):

| Check | Result |
|---|---|
| Rust Celeborn and execution suites | 51 + 263 passed; doc tests passed |
| Twenty-five cleanup/shutdown/commit regressions, 20 consecutive rounds | 500 passed |
| Clippy, all targets, warnings denied | passed |
| Native extension build | passed |
| Docker client, static shuffle and AQE integration suites | 32 passed; existing pytest configuration deprecation warning |
| Benchmark unit tests | 5 passed |
| Full Python lint/format, nightly Rust formatting and diff checks | passed |

Earlier repeated-test totals remain historical; the 500 checks above exercise
the current cleanup and shutdown code. GitHub Build was checked again and remains
`disabled_fork`; these are local results. The previous shutdown exclusion is now
closed for tracked actor operations. Already-delivered lazy read streams remain
externally owned and are not revoked. Lifecycle Stop retains its best-effort
`()` API: remote unregister failures are logged, and eventual server cleanup may
still be needed. Performance and peak-memory benchmarks have not been rerun.

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
