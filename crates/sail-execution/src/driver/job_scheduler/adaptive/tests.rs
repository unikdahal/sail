#![expect(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{JoinType, NullEquality};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::{InterleaveExec, UnionExec};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, PlanProperties};
use opentelemetry::logs::LoggerProvider;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use sail_celeborn::common::PartitionSplitMode;
use sail_telemetry::events::SystemEventReporter;

use super::*;
use crate::driver::job_scheduler::state::{
    JobDescriptor, JobState, ShuffleCommit, StageState, TaskAttemptDescriptor, TaskState,
};
use crate::driver::job_scheduler::{JobAction, JobSchedulerOptions};
use crate::id::TaskKey;
use crate::job_graph::{JobGraph, JobGraphOptions};
use crate::proto::RemoteExecutionCodec;
use crate::statistics::{CompactOutputStatistics, ShuffleStatistics, TaskOutputStatistics};
use crate::task::definition::TaskInputLocator;
use crate::task::scheduling::{TaskAssignment, TaskAssignmentGetter};

const MIB: u64 = 1024 * 1024;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]))
}

fn key() -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new("k", 0))
}

fn placeholder(input: usize, buckets: usize) -> Arc<dyn ExecutionPlan> {
    Arc::new(StageInputExec::new(
        input,
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema()),
            Partitioning::Hash(vec![key()], buckets),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )),
    ))
}

fn broadcast_placeholder(input: usize) -> Arc<dyn ExecutionPlan> {
    Arc::new(StageInputExec::new(
        input,
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )),
    ))
}

fn final_aggregate(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        AggregateExec::try_new(
            AggregateMode::FinalPartitioned,
            PhysicalGroupBy::new_single(vec![(key(), "k".to_string())]),
            vec![],
            vec![],
            input,
            schema(),
        )
        .unwrap(),
    )
}

fn global_aggregate(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::new_single(vec![]),
            vec![],
            vec![],
            input,
            schema(),
        )
        .unwrap(),
    )
}

fn hash_join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    mode: PartitionMode,
    join_type: JoinType,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        HashJoinExec::try_new(
            left,
            right,
            vec![(key(), key())],
            None,
            &join_type,
            None,
            mode,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    )
}

/// Statistics with the given physical bytes per bucket; logical bytes are twice as large.
fn statistics(physical: &[u64]) -> Arc<ShuffleStatistics> {
    Arc::new(ShuffleStatistics {
        total_rows: physical.iter().sum::<u64>() / 8,
        total_logical_bytes: physical.iter().sum::<u64>() * 2,
        total_physical_bytes: Some(physical.iter().sum()),
        rows: physical.iter().map(|x| x / 8).collect(),
        physical_bytes: Some(physical.to_vec()),
        logical_bytes: physical.iter().map(|x| x * 2).collect(),
    })
}

fn shuffle_input(statistics: Arc<ShuffleStatistics>) -> InputView {
    InputView {
        mode: InputMode::Shuffle,
        origin: ShuffleOrigin::Engine,
        layout: BucketLayout::Direct,
        hash_partitioned: true,
        buckets: statistics.rows.len(),
        statistics: Some(statistics),
    }
}

fn broadcast_input(statistics: Arc<ShuffleStatistics>) -> InputView {
    InputView {
        mode: InputMode::Broadcast,
        layout: BucketLayout::Opaque,
        ..shuffle_input(statistics)
    }
}

fn options() -> AdaptiveOptions {
    AdaptiveOptions {
        enabled: true,
        coalesce_target_size: 64 * MIB,
        coalesce_min_partition_size: MIB,
        ..AdaptiveOptions::default()
    }
}

fn view(plan: Arc<dyn ExecutionPlan>, inputs: Vec<InputView>) -> StageView {
    StageView {
        plan,
        inputs,
        options: options(),
    }
}

fn count(plan: &Arc<dyn ExecutionPlan>) -> usize {
    plan.output_partitioning().partition_count()
}

fn contains<T: ExecutionPlan + 'static>(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.exists(|node| Ok(node.is::<T>())).unwrap()
}

fn spec_ranges(input: &AdaptiveInput) -> Vec<std::ops::Range<usize>> {
    input
        .spec
        .iter()
        .map(|slices| {
            assert_eq!(slices.len(), 1);
            slices[0].buckets.clone()
        })
        .collect()
}

#[test]
fn coalesces_small_buckets_of_a_final_aggregate() {
    let plan = final_aggregate(placeholder(0, 8));
    let view = view(plan, vec![shuffle_input(statistics(&[10 * MIB; 8]))]);
    let rewrite = adapt(&view).unwrap().expect("a rewrite");
    assert_eq!(rewrite.rules, vec![RULE_COALESCE]);
    let input = rewrite.inputs[0].as_ref().expect("an adaptive input");
    assert_eq!(spec_ranges(input), vec![0..6, 6..8]);
    assert_eq!(
        input.property,
        AdaptiveProperty::CoalescedHash {
            from: 8,
            boundaries: vec![0, 6, 8]
        }
    );
    // The stage's task count follows from the rewritten input.
    assert_eq!(count(&rewrite.plan), 2);
    validate::validate_plan(&rewrite.plan).unwrap();
}

#[test]
fn partitioned_join_inputs_share_boundaries() {
    let plan = hash_join(
        placeholder(0, 4),
        placeholder(1, 4),
        PartitionMode::Partitioned,
        JoinType::Inner,
    );
    let view = view(
        plan,
        vec![
            shuffle_input(statistics(&[MIB, MIB, 30 * MIB, MIB])),
            shuffle_input(statistics(&[MIB, MIB, MIB, 30 * MIB])),
        ],
    );
    let rewrite = adapt(&view).unwrap().expect("a rewrite");
    let left = rewrite.inputs[0].as_ref().unwrap();
    let right = rewrite.inputs[1].as_ref().unwrap();
    assert_eq!(left, right);
    assert_eq!(rewrite.groups, vec![vec![0, 1]]);
    // Sizes are summed across the group: [2, 2, 31, 31] MiB.
    assert_eq!(spec_ranges(left), vec![0..3, 3..4]);
    assert_eq!(count(&rewrite.plan), 2);
    validate::validate_plan(&rewrite.plan).unwrap();
}

#[test]
fn user_chosen_partition_counts_are_kept() {
    let plan = final_aggregate(placeholder(0, 8));
    let mut input = shuffle_input(statistics(&[MIB; 8]));
    input.origin = ShuffleOrigin::User;
    assert!(adapt(&view(plan, vec![input])).unwrap().is_none());
}

#[test]
fn unknown_sizes_are_not_coalesced() {
    let plan = final_aggregate(placeholder(0, 8));
    let mut statistics = statistics(&[MIB; 8]).as_ref().clone();
    statistics.physical_bytes = None;
    statistics.total_physical_bytes = None;
    let input = shuffle_input(Arc::new(statistics));
    assert!(adapt(&view(plan, vec![input])).unwrap().is_none());
}

#[test]
fn union_children_are_coalesced_independently() {
    let plan = UnionExec::try_new(vec![
        final_aggregate(placeholder(0, 4)),
        final_aggregate(placeholder(1, 4)),
    ])
    .unwrap();
    let view = view(
        plan,
        vec![
            shuffle_input(statistics(&[MIB; 4])),
            shuffle_input(statistics(&[40 * MIB; 4])),
        ],
    );
    let rewrite = adapt(&view).unwrap().expect("a rewrite");
    assert_eq!(spec_ranges(rewrite.inputs[0].as_ref().unwrap()), vec![0..4]);
    assert!(rewrite.inputs[1].is_none());
    // The stage count is the sum of the children's counts.
    assert_eq!(count(&rewrite.plan), 5);
}

#[test]
fn union_paired_with_another_input_is_skipped() {
    let union = UnionExec::try_new(vec![placeholder(0, 2), placeholder(1, 2)]).unwrap();
    let plan = hash_join(
        union,
        placeholder(2, 4),
        PartitionMode::Partitioned,
        JoinType::Inner,
    );
    let view = view(
        plan,
        (0..3)
            .map(|_| shuffle_input(statistics(&[MIB; 2])))
            .collect(),
    );
    assert!(adapt(&view).is_err());
}

#[test]
fn interleave_stages_are_skipped() {
    let plan =
        Arc::new(InterleaveExec::try_new(vec![placeholder(0, 4), placeholder(1, 4)]).unwrap())
            as Arc<dyn ExecutionPlan>;
    let view = view(
        plan,
        (0..2)
            .map(|_| shuffle_input(statistics(&[MIB; 4])))
            .collect(),
    );
    assert!(adapt(&view).unwrap_err().contains("InterleaveExec"));
}

#[test]
fn broadcast_build_side_is_outside_every_group() {
    let plan = hash_join(
        broadcast_placeholder(0),
        placeholder(1, 8),
        PartitionMode::CollectLeft,
        JoinType::Inner,
    );
    let view = view(
        plan,
        vec![
            broadcast_input(statistics(&[MIB])),
            shuffle_input(statistics(&[MIB; 8])),
        ],
    );
    let rewrite = adapt(&view).unwrap().expect("a rewrite");
    assert!(rewrite.inputs[0].is_none());
    assert_eq!(spec_ranges(rewrite.inputs[1].as_ref().unwrap()), vec![0..8]);
    assert_eq!(rewrite.groups, vec![vec![1]]);
    validate::validate_plan(&rewrite.plan).unwrap();
}

#[test]
fn build_guard_bounds_coalesced_build_partitions() {
    let plan = hash_join(
        placeholder(0, 4),
        placeholder(1, 4),
        PartitionMode::Partitioned,
        JoinType::Inner,
    );
    let mut view = view(
        plan,
        vec![
            // Logical build bytes are twice the physical bytes: 20 MiB per bucket.
            shuffle_input(statistics(&[10 * MIB; 4])),
            shuffle_input(statistics(&[MIB; 4])),
        ],
    );
    view.options.join_max_build_partition_size = 50 * MIB;
    // Keep the build side as planned so that only the guard is exercised.
    view.options.join_enabled = false;
    let rewrite = adapt(&view).unwrap().expect("a rewrite");
    // Without the guard, the 44 MiB total would fit in one 64 MiB partition.
    assert_eq!(
        spec_ranges(rewrite.inputs[0].as_ref().unwrap()),
        vec![0..2, 2..4]
    );
}

#[test]
fn joins_with_an_empty_input_are_pruned() {
    let plan = global_aggregate(hash_join(
        placeholder(0, 4),
        placeholder(1, 4),
        PartitionMode::Partitioned,
        JoinType::Inner,
    ));
    let mut view = view(
        plan,
        vec![
            shuffle_input(statistics(&[0; 4])),
            shuffle_input(statistics(&[100 * MIB; 4])),
        ],
    );
    view.options.coalesce_enabled = false;
    let rewrite = adapt(&view).unwrap().expect("a rewrite");
    assert_eq!(rewrite.rules, vec![joins::RULE_EMPTY_INPUT]);
    assert!(!contains::<HashJoinExec>(&rewrite.plan));
    assert!(contains::<EmptyExec>(&rewrite.plan));
    // The global aggregate still returns its row.
    assert!(contains::<AggregateExec>(&rewrite.plan));
    assert_eq!(count(&rewrite.plan), 4);
}

#[test]
fn outer_joins_keep_rows_of_the_preserved_side() {
    for (join_type, pruned) in [
        (JoinType::Left, false),
        (JoinType::Right, true),
        (JoinType::Full, false),
        (JoinType::LeftAnti, false),
        (JoinType::RightSemi, true),
    ] {
        let plan = hash_join(
            placeholder(0, 4),
            placeholder(1, 4),
            PartitionMode::Partitioned,
            join_type,
        );
        let mut view = view(
            plan,
            vec![
                shuffle_input(statistics(&[MIB; 4])),
                shuffle_input(statistics(&[0; 4])),
            ],
        );
        view.options.coalesce_enabled = false;
        let rules = adapt(&view).unwrap().map(|x| x.rules).unwrap_or_default();
        assert_eq!(
            rules.contains(&joins::RULE_EMPTY_INPUT),
            pruned,
            "{join_type}"
        );
    }
}

#[test]
fn larger_build_side_is_swapped() {
    let plan = hash_join(
        placeholder(0, 4),
        placeholder(1, 4),
        PartitionMode::Partitioned,
        JoinType::Inner,
    );
    let mut view = view(
        plan,
        vec![
            shuffle_input(statistics(&[8 * MIB; 4])),
            shuffle_input(statistics(&[MIB; 4])),
        ],
    );
    view.options.coalesce_enabled = false;
    let rewrite = adapt(&view).unwrap().expect("a rewrite");
    assert_eq!(rewrite.rules, vec![joins::RULE_SWAP_JOIN_INPUTS]);
    let mut build_input = None;
    rewrite
        .plan
        .apply(|node| {
            if let Some(join) = node.downcast_ref::<HashJoinExec>() {
                build_input = join
                    .left()
                    .downcast_ref::<StageInputExec<usize>>()
                    .map(|x| *x.input());
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
    assert_eq!(build_input, Some(1));
    assert_eq!(rewrite.plan.schema(), view.plan.schema());
    validate::validate_plan(&rewrite.plan).unwrap();
}

#[test]
fn oversized_build_partitions_switch_to_sort_merge_join() {
    let plan = hash_join(
        placeholder(0, 4),
        placeholder(1, 4),
        PartitionMode::Partitioned,
        JoinType::Left,
    );
    let mut view = view(
        plan,
        vec![
            shuffle_input(statistics(&[MIB, MIB, 40 * MIB, MIB])),
            shuffle_input(statistics(&[100 * MIB; 4])),
        ],
    );
    view.options.coalesce_enabled = false;
    let rewrite = adapt(&view).unwrap().expect("a rewrite");
    assert_eq!(rewrite.rules, vec![joins::RULE_SORT_MERGE_JOIN]);
    assert!(contains::<SortMergeJoinExec>(&rewrite.plan));
    assert!(!contains::<HashJoinExec>(&rewrite.plan));
    assert_eq!(rewrite.plan.schema(), view.plan.schema());
    validate::validate_plan(&rewrite.plan).unwrap();
}

#[test]
fn rules_are_deterministic() {
    let plan = hash_join(
        placeholder(0, 8),
        placeholder(1, 8),
        PartitionMode::Partitioned,
        JoinType::Inner,
    );
    let view = view(
        plan,
        vec![
            shuffle_input(statistics(&[
                MIB,
                2 * MIB,
                3 * MIB,
                50 * MIB,
                0,
                0,
                7 * MIB,
                MIB,
            ])),
            shuffle_input(statistics(&[2 * MIB; 8])),
        ],
    );
    let a = adapt(&view).unwrap().unwrap();
    let b = adapt(&view).unwrap().unwrap();
    assert_eq!(a.inputs, b.inputs);
    assert_eq!(a.rules, b.rules);
}

#[test]
fn validator_rejects_partial_coverage_and_unequal_join_inputs() {
    // A partitioned join over inputs with different partition counts.
    let left = placeholder(0, 2);
    let right = placeholder(1, 4);
    let plan = hash_join(left, right, PartitionMode::Partitioned, JoinType::Inner);
    assert!(validate::validate_plan(&plan).is_err());
}

// ---------------------------------------------------------------------------
// Scheduler-level tests over a Celeborn job graph.
// ---------------------------------------------------------------------------

fn celeborn() -> ShuffleBackendKind {
    ShuffleBackendKind::Celeborn {
        master_endpoints: vec!["localhost:1".to_string()],
        compression: sail_celeborn::common::CompressionCodec::Lz4,
        endpoint_overrides: vec![],
        heartbeat_interval_secs: 10,
        partition_split_threshold: 1_i64 << 30,
        partition_split_mode: PartitionSplitMode::Soft,
    }
}

fn reporter() -> SystemEventReporter {
    SystemEventReporter::new(SdkLoggerProvider::builder().build().logger("test"))
}

/// `SELECT k FROM t GROUP BY k ORDER BY k` shaped as a job graph:
///   stage 0: scan, hash shuffle on `k` into 8 buckets (blocking under Celeborn)
///   stage 1: final aggregate and sort (pipelined)
///   stage 2: sort-preserving merge of stage 1 (merge input)
fn aggregate_graph() -> JobGraph {
    let input = MemorySourceConfig::try_new_exec(&vec![vec![]; 4], schema(), None).unwrap();
    let shuffle =
        Arc::new(RepartitionExec::try_new(input, Partitioning::Hash(vec![key()], 8)).unwrap());
    let aggregate = final_aggregate(shuffle);
    let ordering = datafusion::physical_expr::LexOrdering::new(vec![
        datafusion::physical_expr::PhysicalSortExpr::new_default(Arc::new(Column::new("k", 0))),
    ])
    .unwrap();
    let sort =
        Arc::new(SortExec::new(ordering.clone(), aggregate).with_preserve_partitioning(true));
    let merge = Arc::new(SortPreservingMergeExec::new(ordering, sort));
    JobGraph::try_new(
        merge,
        JobGraphOptions {
            shuffle_backend: celeborn(),
        },
    )
    .unwrap()
}

fn succeed(
    job: &mut JobDescriptor,
    stage: usize,
    statistics: impl Fn(usize) -> TaskOutputStatistics,
) {
    for (p, task) in job.stages[stage].tasks.iter_mut().enumerate() {
        task.attempts.push(TaskAttemptDescriptor {
            state: TaskState::Succeeded,
            messages: vec![],
            cause: None,
            job_output_fetched: false,
            statistics: Some(Arc::new(CompactOutputStatistics::new(&statistics(p)))),
        });
    }
}

fn producer_statistics(_: usize) -> TaskOutputStatistics {
    TaskOutputStatistics {
        rows: vec![100; 8],
        physical_bytes: Some(vec![MIB / 4; 8]),
        logical_bytes: vec![MIB; 8],
    }
}

fn job(graph: JobGraph) -> JobDescriptor {
    JobDescriptor::try_new(graph, JobState::Draining, Arc::new(TaskContext::default())).unwrap()
}

fn adapt_job(job: &mut JobDescriptor, options: &JobSchedulerOptions) -> bool {
    JobScheduler::update_task_regions(job, options);
    let replanned = JobScheduler::adapt_ready_stages(
        JobId::from(1),
        job,
        options,
        &RemoteExecutionCodec,
        &reporter(),
        "test",
    );
    if replanned {
        JobScheduler::update_task_regions(job, options);
    }
    replanned
}

fn adaptive_options() -> JobSchedulerOptions {
    JobSchedulerOptions::new_for_test(celeborn(), options(), None)
}

#[test]
fn celeborn_stage_is_coalesced_after_the_shuffle_commits() {
    let graph = aggregate_graph();
    assert_eq!(graph.stages().len(), 3);
    assert_eq!(graph.stages()[1].inputs[0].layout, BucketLayout::Direct);
    let mut job = job(graph);
    let options = adaptive_options();

    // Nothing is decided while the producer runs.
    assert!(!adapt_job(&mut job, &options));
    assert!(!job.stages[1].adaptive.is_frozen());

    succeed(&mut job, 0, producer_statistics);
    // The producer succeeded, but Celeborn has not reported the committed attempts:
    // the consumer runs as planned rather than using statistics of unknown attempts.
    let mut uncommitted = self::job(aggregate_graph());
    succeed(&mut uncommitted, 0, producer_statistics);
    assert!(!adapt_job(&mut uncommitted, &options));
    assert_eq!(uncommitted.stages[1].adaptive, AdaptiveDecision::Static);

    job.stages[0].commit = Some(ShuffleCommit {
        attempts: vec![0; 4],
        succeeded: true,
    });
    assert!(adapt_job(&mut job, &options));
    assert_eq!(job.stages[1].adaptive, AdaptiveDecision::Replanned);
    // 8 buckets of 1 MiB (4 x 0.25 MiB) coalesce into one partition.
    assert_eq!(job.stages[1].tasks.len(), 1);
    assert_eq!(count(&job.graph.stages()[1].plan), 1);
    let adaptive = job.graph.stages()[1].inputs[0].adaptive.clone().unwrap();
    assert_eq!(spec_ranges(&adaptive), vec![0..8]);
    // The merge consumer's placeholder was updated with the new count.
    assert!(
        job.graph.stages()[2]
            .plan
            .exists(|node| Ok(node
                .downcast_ref::<StageInputExec<usize>>()
                .is_some_and(|x| x.properties().output_partitioning().partition_count() == 1)))
            .unwrap()
    );

    // Task definitions resolve the spec into Celeborn channels at launch.
    struct NoAssignments;
    impl TaskAssignmentGetter for NoAssignments {
        fn get(&self, _: &TaskKey) -> Option<&TaskAssignment> {
            None
        }
    }
    let input = &job.graph.stages()[1].inputs[0];
    let locator = crate::driver::job_scheduler::core::tests_support::build_input_locator(
        &job,
        &TaskKey {
            job_id: JobId::from(1),
            stage: 1,
            partition: 0,
            attempt: 0,
        },
        input,
        &NoAssignments,
    );
    assert!(matches!(
        locator.as_ref(),
        TaskInputLocator::ShuffleService { channels } if channels == &vec![(0..8).collect::<Vec<_>>()]
    ));

    // The decision is frozen.
    assert!(!adapt_job(&mut job, &options));
}

#[test]
fn celeborn_statistics_come_from_the_committed_attempt() {
    let mut job = job(aggregate_graph());
    let options = adaptive_options();
    // Attempt 0 of partition 0 reached `mapper_end` and is served by Celeborn, but its
    // worker was lost before reporting success. Attempt 1 succeeded with statistics.
    for task in &mut job.stages[0].tasks {
        task.attempts.push(TaskAttemptDescriptor {
            state: TaskState::Failed,
            messages: vec![],
            cause: None,
            job_output_fetched: false,
            statistics: None,
        });
    }
    succeed(&mut job, 0, producer_statistics);
    job.stages[0].commit = Some(ShuffleCommit {
        attempts: vec![0, 1, 1, 1],
        succeeded: true,
    });
    assert!(!adapt_job(&mut job, &options));
    assert_eq!(job.stages[1].adaptive, AdaptiveDecision::Static);
    assert_eq!(job.stages[1].tasks.len(), 8);
}

#[test]
fn failed_celeborn_commit_is_not_adapted() {
    let mut job = job(aggregate_graph());
    let options = adaptive_options();
    succeed(&mut job, 0, producer_statistics);
    job.stages[0].commit = Some(ShuffleCommit {
        attempts: vec![0; 4],
        succeeded: false,
    });
    assert!(!adapt_job(&mut job, &options));
    assert_eq!(job.stages[1].adaptive, AdaptiveDecision::Static);
}

#[test]
fn slot_cap_failure_keeps_the_static_plan() {
    let mut job = job(aggregate_graph());
    // The static plan already needs 8 slots for its widest region, so any validated
    // rewrite would be rejected under a cap of 0 slots.
    let options = JobSchedulerOptions::new_for_test(celeborn(), options(), Some(0));
    succeed(&mut job, 0, producer_statistics);
    job.stages[0].commit = Some(ShuffleCommit {
        attempts: vec![0; 4],
        succeeded: true,
    });
    let before = job.graph.to_string();
    assert!(!adapt_job(&mut job, &options));
    assert_eq!(job.stages[1].adaptive, AdaptiveDecision::Static);
    assert_eq!(job.graph.to_string(), before);
    assert_eq!(job.stages[1].tasks.len(), 8);
}

#[test]
fn adaptive_execution_disabled_leaves_the_graph_unchanged() {
    let mut job = job(aggregate_graph());
    let options = JobSchedulerOptions::new_for_test(celeborn(), AdaptiveOptions::default(), None);
    succeed(&mut job, 0, producer_statistics);
    job.stages[0].commit = Some(ShuffleCommit {
        attempts: vec![0; 4],
        succeeded: true,
    });
    let before = job.graph.to_string();
    JobScheduler::update_task_regions(&mut job, &options);
    // `refresh_job` only calls the planner when adaptive execution is enabled.
    assert!(!options.adaptive.enabled);
    assert_eq!(job.graph.to_string(), before);
    assert_eq!(job.stages[1].tasks.len(), 8);
}

#[test]
fn output_is_retained_until_consumer_regions_succeed() {
    let mut job = job(aggregate_graph());
    let options = JobSchedulerOptions::new_for_test(celeborn(), AdaptiveOptions::default(), None);
    succeed(&mut job, 0, producer_statistics);
    // Stage 1 succeeds, but stage 2 in the same region is still running, so a failure
    // of stage 2 would rerun stage 1, which reads stage 0 again.
    succeed(&mut job, 1, |_| TaskOutputStatistics::default());
    job.stages[2].tasks[0].attempts.push(TaskAttemptDescriptor {
        state: TaskState::Running,
        messages: vec![],
        cause: None,
        job_output_fetched: false,
        statistics: None,
    });
    JobScheduler::update_task_regions(&mut job, &options);
    let actions =
        JobScheduler::clean_up_job_by_stage(JobId::from(1), &mut job, &reporter(), "test");
    assert!(actions.is_empty());
    assert!(matches!(job.stages[0].state, StageState::Active));

    job.stages[2].tasks[0].attempts[0].state = TaskState::Succeeded;
    JobScheduler::update_task_regions(&mut job, &options);
    let actions =
        JobScheduler::clean_up_job_by_stage(JobId::from(1), &mut job, &reporter(), "test");
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, JobAction::CleanUpJob { stage: Some(0), .. }))
    );
}

#[test]
fn statistics_are_folded_from_committed_attempts() {
    let mut job = job(aggregate_graph());
    succeed(&mut job, 0, producer_statistics);
    assert!(JobScheduler::committed_statistics(&job, 0).is_none());
    job.stages[0].commit = Some(ShuffleCommit {
        attempts: vec![0; 4],
        succeeded: true,
    });
    let statistics = JobScheduler::committed_statistics(&job, 0).unwrap();
    assert_eq!(statistics.total_rows, 3200);
    assert_eq!(statistics.total_physical_bytes, Some(8 * MIB));
    assert_eq!(statistics.total_logical_bytes, 32 * MIB);
}

#[test]
fn pipelined_output_is_released_when_its_region_reruns_the_producer() {
    // Under Flight, every stage of this job is in one region, so a retry reruns the
    // producer and recreates its output. Cleanup timing stays as before.
    let input = MemorySourceConfig::try_new_exec(&vec![vec![]; 4], schema(), None).unwrap();
    let shuffle =
        Arc::new(RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(4)).unwrap());
    let ordering = datafusion::physical_expr::LexOrdering::new(vec![
        datafusion::physical_expr::PhysicalSortExpr::new_default(Arc::new(Column::new("k", 0))),
    ])
    .unwrap();
    let sort = Arc::new(SortExec::new(ordering.clone(), shuffle).with_preserve_partitioning(true));
    let merge = Arc::new(SortPreservingMergeExec::new(ordering, sort));
    let flight = ShuffleBackendKind::Flight {
        compression: crate::shuffle::ShuffleCompression::None,
        connection_count: std::num::NonZeroUsize::MIN,
        initial_stream_window_size: None,
        initial_connection_window_size: None,
    };
    let graph = JobGraph::try_new(
        merge,
        JobGraphOptions {
            shuffle_backend: flight.clone(),
        },
    )
    .unwrap();
    let mut job = job(graph);
    assert_eq!(job.topology.regions.len(), 1);
    let options = JobSchedulerOptions::new_for_test(flight, AdaptiveOptions::default(), None);
    succeed(&mut job, 0, |_| TaskOutputStatistics::default());
    succeed(&mut job, 1, |_| TaskOutputStatistics::default());
    job.stages[2].tasks[0].attempts.push(TaskAttemptDescriptor {
        state: TaskState::Running,
        messages: vec![],
        cause: None,
        job_output_fetched: false,
        statistics: None,
    });
    JobScheduler::update_task_regions(&mut job, &options);
    let actions =
        JobScheduler::clean_up_job_by_stage(JobId::from(1), &mut job, &reporter(), "test");
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, JobAction::CleanUpJob { stage: Some(0), .. }))
    );
}
