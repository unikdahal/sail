#![expect(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{JoinType, NullEquality};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, lit};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::union::{InterleaveExec, UnionExec};
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, PlanProperties, StatisticsArgs, StatisticsContext,
};

use super::*;
use crate::driver::job_scheduler::JobSchedulerOptions;
use crate::driver::job_scheduler::state::{
    JobDescriptor, ShuffleCommit, TaskAttemptDescriptor, TaskState,
};
use crate::driver::job_scheduler::test_utils::{
    MIB, aggregate_graph, commit, final_aggregate, job, key, producer_statistics,
    remote_shuffle_service, reporter, schema, succeed,
};
use crate::id::TaskKey;
use crate::job_graph::{JobGraph, JobGraphOptions};
use crate::proto::RemoteExecutionCodec;
use crate::statistics::{ChannelCounts, ShuffleStatistics, TaskOutputStatistics};
use crate::task::definition::{ShuffleServiceRead, TaskInputLocator};
use crate::task::scheduling::{TaskAssignment, TaskAssignmentGetter};

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
        buckets: statistics.logical_bytes.len(),
        statistics: Some(statistics),
        producers: 1,
        committed: None,
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
        slot_cap: None,
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

fn filter(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let predicate = Arc::new(BinaryExpr::new(key(), Operator::Gt, lit(0i64)));
    Arc::new(FilterExec::try_new(predicate, input).unwrap())
}

fn drop_value_column(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(ProjectionExec::try_new(vec![(key(), "k".to_string())], input).unwrap())
}

/// A projection that computes a value, so its output may be wider than its input.
fn computed_projection(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let value = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("v", 1)),
        Operator::Plus,
        lit(1i64),
    ));
    Arc::new(
        ProjectionExec::try_new(
            vec![(key(), "k".to_string()), (value, "v".to_string())],
            input,
        )
        .unwrap(),
    )
}

/// Returns the stage input that the first hash join of the plan builds from.
fn build_input(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    let mut result = None;
    plan.apply(|node| {
        if let Some(join) = node.downcast_ref::<HashJoinExec>() {
            result = bounds::input_bound(join.left()).map(|x| x.input);
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    result
}

#[test]
fn build_side_without_a_bound_is_not_coalesced() {
    // The outer join builds from the output of the inner join, which can be far
    // larger than its inputs. A projection that computes values has no bound either.
    let nested = hash_join(
        hash_join(
            placeholder(0, 4),
            placeholder(1, 4),
            PartitionMode::Partitioned,
            JoinType::Inner,
        ),
        placeholder(2, 4),
        PartitionMode::Partitioned,
        JoinType::Inner,
    );
    let computed = hash_join(
        computed_projection(placeholder(0, 4)),
        placeholder(1, 4),
        PartitionMode::Partitioned,
        JoinType::Inner,
    );
    for (plan, inputs) in [(nested, 3), (computed, 2)] {
        let mut view = view(plan, vec![shuffle_input(statistics(&[MIB; 4])); inputs]);
        view.options.join_enabled = false;
        let rewrite = adapt(&view).unwrap();
        assert!(rewrite.is_none(), "{:?}", rewrite.map(|x| x.notes));
    }
}

#[test]
fn filtered_build_side_is_not_coalesced_without_a_byte_bound() {
    let plan = hash_join(
        filter(placeholder(0, 4)),
        placeholder(1, 4),
        PartitionMode::Partitioned,
        JoinType::Inner,
    );
    let mut view = view(
        plan,
        vec![
            shuffle_input(statistics(&[10 * MIB; 4])),
            shuffle_input(statistics(&[MIB; 4])),
        ],
    );
    view.options.join_max_build_partition_size = 50 * MIB;
    view.options.join_enabled = false;
    // FilterExec can repeat dictionaries while rebatching, so its input does
    // not bound the bytes of a coalesced build partition.
    assert!(adapt(&view).unwrap().is_none());
}

#[test]
fn logical_size_cap_bounds_coalesced_partitions() {
    // Logical bytes are twice the physical bytes: 2 MiB per bucket.
    let plan = final_aggregate(placeholder(0, 8));
    let mut view = view(plan, vec![shuffle_input(statistics(&[MIB; 8]))]);
    view.options.coalesce_max_logical_size = 5 * MIB;
    let rewrite = adapt(&view).unwrap().expect("a rewrite");
    assert_eq!(
        spec_ranges(rewrite.inputs[0].as_ref().unwrap()),
        vec![0..2, 2..4, 4..6, 6..8]
    );
}

#[test]
fn filtered_build_side_is_not_swapped() {
    // The filter may remove most of the build side, so its input size does not show
    // that the build side is larger than the probe side.
    let plan = hash_join(
        filter(placeholder(0, 4)),
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
    assert!(adapt(&view).unwrap().is_none());
}

#[test]
fn exact_build_side_is_not_swapped_over_an_unbounded_filtered_probe_side() {
    let plan = hash_join(
        placeholder(0, 4),
        filter(placeholder(1, 4)),
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
    // A filter can repeat dictionary bytes, so even this smaller probe input
    // does not prove its filtered output is smaller than the build side.
    assert!(adapt(&view).unwrap().is_none());
}

#[test]
fn exact_build_side_is_swapped_over_an_upper_bounded_probe_side() {
    let plan = hash_join(
        placeholder(0, 4),
        drop_value_column(placeholder(1, 4)),
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
    // Dropping a column preserves batch boundaries and gives a sound upper
    // bound, so the exact build side can still be proved larger than the probe.
    let rewrite = adapt(&view).unwrap().expect("a rewrite");
    assert_eq!(rewrite.rules, vec![joins::RULE_SWAP_JOIN_INPUTS]);
    assert_eq!(build_input(&rewrite.plan), Some(1));
    validate::validate_plan(&rewrite.plan).unwrap();
}

#[test]
fn upper_bounded_build_side_keeps_its_hash_join() {
    let plan = hash_join(
        drop_value_column(placeholder(0, 4)),
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
    assert!(adapt(&view).unwrap().is_none());
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
// Scheduler-level tests over a remote shuffle service job graph.
// ---------------------------------------------------------------------------

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
    JobSchedulerOptions::new_for_test(remote_shuffle_service(), options(), None)
}

#[test]
fn stage_is_coalesced_after_the_shuffle_commits() {
    let graph = aggregate_graph();
    assert_eq!(graph.stages().len(), 3);
    assert_eq!(graph.stages()[1].inputs[0].layout, BucketLayout::Direct);
    let mut job = job(graph);
    let options = adaptive_options();

    // Nothing is decided while the producer runs.
    assert!(!adapt_job(&mut job, &options));
    assert!(!job.stages[1].adaptive.is_frozen());

    succeed(&mut job, 0, producer_statistics);
    // The producer succeeded, but the shuffle service has not reported the committed attempts:
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

    // Task definitions resolve the spec into shuffle service channels at launch.
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
        TaskInputLocator::ShuffleService { reads, map_attempts: Some(attempts) }
            if reads == &vec![ShuffleServiceRead::channels((0..8).collect())]
                && attempts.as_ref() == [0, 0, 0, 0]
    ));

    // The decision is frozen.
    assert!(!adapt_job(&mut job, &options));
}

#[test]
fn statistics_come_from_the_committed_attempt() {
    let mut job = job(aggregate_graph());
    let options = adaptive_options();
    // Attempt 0 of partition 0 reached `mapper_end` and is served by the shuffle service, but its
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
fn failed_shuffle_commit_is_not_adapted() {
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
    let options = JobSchedulerOptions::new_for_test(remote_shuffle_service(), options(), Some(0));
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
    let options = JobSchedulerOptions::new_for_test(
        remote_shuffle_service(),
        AdaptiveOptions::default(),
        None,
    );
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

// ---------------------------------------------------------------------------
// Skew splits.
// ---------------------------------------------------------------------------

/// An input whose producer `m` wrote `matrix[m][b]` physical bytes to bucket `b`.
fn producer_input(matrix: &[Vec<u64>]) -> InputView {
    let committed = matrix
        .iter()
        .map(|physical| {
            Arc::new(
                TaskOutputStatistics::new(&ChannelCounts {
                    rows: physical.iter().map(|x| x / 8).collect(),
                    physical_bytes: Some(physical.clone()),
                    logical_bytes: physical.iter().map(|x| x * 2).collect(),
                })
                .unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let buckets = matrix[0].len();
    let statistics =
        Arc::new(ShuffleStatistics::fold(buckets, committed.iter().map(|x| x.as_ref())).unwrap());
    InputView {
        producers: matrix.len(),
        committed: Some(Arc::new(committed)),
        ..shuffle_input(statistics)
    }
}

/// `producers` producers that each write `size` bytes to every bucket, and
/// `skewed_size` bytes to bucket `skewed`.
fn skewed_matrix(
    producers: usize,
    buckets: usize,
    size: u64,
    skewed: usize,
    skewed_size: u64,
) -> Vec<Vec<u64>> {
    (0..producers)
        .map(|_| {
            (0..buckets)
                .map(|b| if b == skewed { skewed_size } else { size })
                .collect()
        })
        .collect()
}

fn skew_view(plan: Arc<dyn ExecutionPlan>, inputs: Vec<InputView>) -> StageView {
    let mut view = view(plan, inputs);
    view.options.skew_threshold = 64 * MIB;
    view.options.coalesce_target_size = 64 * MIB;
    view.options.join_enabled = false;
    view
}

fn check_specs(rewrite: &StageRewrite, view: &StageView, group: &[usize]) {
    let specs = group
        .iter()
        .map(|&i| spec::SpecInput {
            spec: rewrite.inputs[i].as_deref().unwrap(),
            buckets: view.inputs[i].buckets,
            producers: view.inputs[i].producers,
        })
        .collect::<Vec<_>>();
    spec::check_group(&specs).unwrap();
}

#[test]
fn skewed_bucket_of_the_preserved_side_is_split() {
    let plan = hash_join(
        placeholder(0, 8),
        placeholder(1, 8),
        PartitionMode::Partitioned,
        JoinType::Left,
    );
    // Bucket 3 of the left side holds 8 x 40 MiB; the other buckets 8 x 1 MiB.
    let view = skew_view(
        plan,
        vec![
            producer_input(&skewed_matrix(8, 8, MIB, 3, 40 * MIB)),
            producer_input(&skewed_matrix(8, 8, MIB, 3, MIB)),
        ],
    );
    let rewrite = adapt(&view).unwrap().expect("a rewrite");
    assert!(rewrite.rules.contains(&skew::RULE_SKEW_SPLIT));
    check_specs(&rewrite, &view, &[0, 1]);
    let left = rewrite.inputs[0].as_ref().unwrap();
    let right = rewrite.inputs[1].as_ref().unwrap();
    assert_eq!(left.property, AdaptiveProperty::Unknown);
    // The left bucket is split by producer; the right bucket is read whole per slice.
    let split = left
        .spec
        .iter()
        .filter(|slices| slices[0].part.is_some())
        .count();
    assert!(split > 1);
    assert!(right.spec.iter().all(|slices| slices[0].part.is_none()));
    assert_eq!(count(&rewrite.plan), left.partitions());
    validate::validate_plan(&rewrite.plan).unwrap();
}

#[test]
fn inner_join_splits_both_skewed_sides_into_pairs() {
    let plan = hash_join(
        placeholder(0, 4),
        placeholder(1, 4),
        PartitionMode::Partitioned,
        JoinType::Inner,
    );
    let view = skew_view(
        plan,
        vec![
            producer_input(&skewed_matrix(4, 4, MIB, 2, 50 * MIB)),
            producer_input(&skewed_matrix(6, 4, MIB, 2, 50 * MIB)),
        ],
    );
    let rewrite = adapt(&view).unwrap().expect("a rewrite");
    check_specs(&rewrite, &view, &[0, 1]);
    let left = rewrite.inputs[0].as_ref().unwrap();
    let right = rewrite.inputs[1].as_ref().unwrap();
    let pairs = left
        .spec
        .iter()
        .zip(&right.spec)
        .filter(|(l, r)| l[0].part.is_some() && r[0].part.is_some())
        .count();
    assert!(pairs >= 4, "{pairs} pairs");

    // Both sides repeat producer ranges across the cross product. The original
    // shuffle totals must not be advertised as exact statistics of these reads.
    let _ = rewrite
        .plan
        .apply(|node| {
            if node.is::<StageInputExec<usize>>() {
                let statistics = StatisticsContext::new()
                    .compute(node.as_ref(), &StatisticsArgs::default())
                    .unwrap();
                assert_eq!(
                    statistics.num_rows,
                    datafusion::common::stats::Precision::Absent
                );
                assert_eq!(
                    statistics.total_byte_size,
                    datafusion::common::stats::Precision::Absent
                );
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
}

#[test]
fn full_outer_joins_are_not_split() {
    let plan = hash_join(
        placeholder(0, 4),
        placeholder(1, 4),
        PartitionMode::Partitioned,
        JoinType::Full,
    );
    let view = skew_view(
        plan,
        vec![
            producer_input(&skewed_matrix(4, 4, MIB, 2, 100 * MIB)),
            producer_input(&skewed_matrix(4, 4, MIB, 2, 100 * MIB)),
        ],
    );
    let rules = adapt(&view).unwrap().map(|x| x.rules).unwrap_or_default();
    assert!(!rules.contains(&skew::RULE_SKEW_SPLIT));
}

#[test]
fn joins_below_a_hash_distributed_operator_are_not_split() {
    // The final aggregate needs every key in one partition.
    let plan = final_aggregate(hash_join(
        placeholder(0, 4),
        placeholder(1, 4),
        PartitionMode::Partitioned,
        JoinType::Inner,
    ));
    let view = skew_view(
        plan,
        vec![
            producer_input(&skewed_matrix(4, 4, MIB, 2, 100 * MIB)),
            producer_input(&skewed_matrix(4, 4, MIB, 2, MIB)),
        ],
    );
    let rules = adapt(&view).unwrap().map(|x| x.rules).unwrap_or_default();
    assert!(!rules.contains(&skew::RULE_SKEW_SPLIT));
}

#[test]
fn inputs_below_an_aggregate_are_not_split() {
    // A partial aggregate below the join is not split-safe: a split would produce
    // two partial groups for one key on the build side.
    let partial = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(vec![(key(), "k".to_string())]),
            vec![],
            vec![],
            placeholder(0, 4),
            schema(),
        )
        .unwrap(),
    ) as Arc<dyn ExecutionPlan>;
    let probe = Arc::new(
        datafusion::physical_plan::projection::ProjectionExec::try_new(
            vec![(key(), "k".to_string())],
            placeholder(1, 4),
        )
        .unwrap(),
    ) as Arc<dyn ExecutionPlan>;
    let plan = hash_join(partial, probe, PartitionMode::Partitioned, JoinType::Inner);
    let view = skew_view(
        plan,
        vec![
            producer_input(&skewed_matrix(4, 4, MIB, 2, 100 * MIB)),
            producer_input(&skewed_matrix(4, 4, MIB, 2, MIB)),
        ],
    );
    let rules = adapt(&view).unwrap().map(|x| x.rules).unwrap_or_default();
    assert!(!rules.contains(&skew::RULE_SKEW_SPLIT));
}

#[test]
fn broadcast_probe_is_split_without_replication() {
    let plan = hash_join(
        broadcast_placeholder(0),
        placeholder(1, 4),
        PartitionMode::CollectLeft,
        JoinType::Inner,
    );
    let view = skew_view(
        plan,
        vec![
            broadcast_input(statistics(&[MIB])),
            producer_input(&skewed_matrix(4, 4, MIB, 1, 100 * MIB)),
        ],
    );
    let rewrite = adapt(&view).unwrap().expect("a rewrite");
    assert!(rewrite.rules.contains(&skew::RULE_SKEW_SPLIT));
    assert!(rewrite.inputs[0].is_none());
    check_specs(&rewrite, &view, &[1]);
    validate::validate_plan(&rewrite.plan).unwrap();
}

#[test]
fn skew_splits_respect_the_slot_cap() {
    let plan = hash_join(
        placeholder(0, 4),
        placeholder(1, 4),
        PartitionMode::Partitioned,
        JoinType::Inner,
    );
    let mut view = skew_view(
        plan,
        vec![
            producer_input(&skewed_matrix(8, 4, MIB, 2, 100 * MIB)),
            producer_input(&skewed_matrix(8, 4, MIB, 2, MIB)),
        ],
    );
    view.slot_cap = Some(3);
    let rules = adapt(&view).unwrap().map(|x| x.rules).unwrap_or_default();
    assert!(!rules.contains(&skew::RULE_SKEW_SPLIT));
}

#[test]
fn uniform_inputs_are_not_split() {
    let plan = hash_join(
        placeholder(0, 4),
        placeholder(1, 4),
        PartitionMode::Partitioned,
        JoinType::Inner,
    );
    let view = skew_view(
        plan,
        vec![
            producer_input(&skewed_matrix(4, 4, 30 * MIB, 0, 30 * MIB)),
            producer_input(&skewed_matrix(4, 4, 30 * MIB, 0, 30 * MIB)),
        ],
    );
    let rules = adapt(&view).unwrap().map(|x| x.rules).unwrap_or_default();
    assert!(!rules.contains(&skew::RULE_SKEW_SPLIT));
}

/// `SELECT * FROM l JOIN r ON l.k = r.k` with both sides hash-shuffled into 4 buckets:
///   stages 0 and 1: scan and hash shuffle (blocking on a remote shuffle service)
///   stage 2: partitioned hash join (final stage)
fn join_graph(join_type: JoinType) -> JobGraph {
    let side = || {
        let input = MemorySourceConfig::try_new_exec(&vec![vec![]; 4], schema(), None).unwrap();
        Arc::new(RepartitionExec::try_new(input, Partitioning::Hash(vec![key()], 4)).unwrap())
            as Arc<dyn ExecutionPlan>
    };
    JobGraph::try_new(
        hash_join(side(), side(), PartitionMode::Partitioned, join_type),
        JobGraphOptions {
            shuffle_backend: remote_shuffle_service(),
        },
    )
    .unwrap()
}

fn skew_options() -> JobSchedulerOptions {
    let mut adaptive = options();
    adaptive.skew_threshold = 64 * MIB;
    adaptive.join_enabled = false;
    JobSchedulerOptions::new_for_test(remote_shuffle_service(), adaptive, None)
}

#[test]
fn join_stage_is_split_and_reads_mapper_ranges() {
    let mut job = job(join_graph(JoinType::Inner));
    assert_eq!(job.graph.stages().len(), 3);
    // Every producer writes 50 MiB to bucket 1 of the left side and 1 MiB elsewhere.
    succeed(&mut job, 0, |_| ChannelCounts {
        rows: vec![100; 4],
        physical_bytes: Some(vec![MIB, 50 * MIB, MIB, MIB]),
        logical_bytes: vec![2 * MIB; 4],
    });
    succeed(&mut job, 1, |_| ChannelCounts {
        rows: vec![100; 4],
        physical_bytes: Some(vec![MIB; 4]),
        logical_bytes: vec![2 * MIB; 4],
    });
    commit(&mut job, 0);
    commit(&mut job, 1);
    assert!(adapt_job(&mut job, &skew_options()));

    let stage = &job.graph.stages()[2];
    let left = stage.inputs[0].adaptive.clone().unwrap();
    let right = stage.inputs[1].adaptive.clone().unwrap();
    assert_eq!(left.partitions(), job.stages[2].tasks.len());
    let split = left
        .spec
        .iter()
        .position(|slices| slices[0].part.is_some())
        .expect("a split partition");

    struct NoAssignments;
    impl TaskAssignmentGetter for NoAssignments {
        fn get(&self, _: &TaskKey) -> Option<&TaskAssignment> {
            None
        }
    }
    let key = TaskKey {
        job_id: JobId::from(1),
        stage: 2,
        partition: split,
        attempt: 0,
    };
    let left_locator = crate::driver::job_scheduler::core::tests_support::build_input_locator(
        &job,
        &key,
        &stage.inputs[0],
        &NoAssignments,
    );
    let TaskInputLocator::ShuffleService {
        reads,
        map_attempts: Some(attempts),
    } = left_locator.as_ref()
    else {
        unreachable!("expected a shuffle service locator with committed attempts");
    };
    assert_eq!(attempts.as_ref(), [0, 0, 0, 0]);
    assert_eq!(reads[split].channels, vec![1]);
    assert!(reads[split].mappers.is_some());
    // The other side reads bucket 1 whole for the same partition.
    let right_locator = crate::driver::job_scheduler::core::tests_support::build_input_locator(
        &job,
        &key,
        &stage.inputs[1],
        &NoAssignments,
    );
    let TaskInputLocator::ShuffleService { reads, .. } = right_locator.as_ref() else {
        unreachable!("expected a shuffle service locator");
    };
    assert_eq!(reads[split], ShuffleServiceRead::channels(vec![1]));
    assert_eq!(right.spec[split][0].part, None);
}

#[test]
fn rule_failure_keeps_the_static_plan_and_freezes_the_decision() {
    // A union of two shuffles paired with a third input cannot be rewritten.
    let side = || {
        let input = MemorySourceConfig::try_new_exec(&vec![vec![]; 2], schema(), None).unwrap();
        Arc::new(RepartitionExec::try_new(input, Partitioning::Hash(vec![key()], 2)).unwrap())
            as Arc<dyn ExecutionPlan>
    };
    let union = UnionExec::try_new(vec![side(), side()]).unwrap();
    let plan = hash_join(union, side(), PartitionMode::Partitioned, JoinType::Inner);
    let graph = JobGraph::try_new(
        plan,
        JobGraphOptions {
            shuffle_backend: remote_shuffle_service(),
        },
    )
    .unwrap();
    let mut job = job(graph);
    let options = adaptive_options();
    let consumer = job.graph.stages().len() - 1;
    for s in 0..consumer {
        succeed(&mut job, s, |_| ChannelCounts {
            rows: vec![1; 2],
            physical_bytes: Some(vec![10; 2]),
            logical_bytes: vec![10; 2],
        });
        commit(&mut job, s);
    }
    let before = job.graph.to_string();
    assert!(!adapt_job(&mut job, &options));
    assert_eq!(job.stages[consumer].adaptive, AdaptiveDecision::Static);
    assert_eq!(job.graph.to_string(), before);
}

#[test]
fn retried_tasks_reuse_the_adapted_plan() {
    let mut job = job(aggregate_graph());
    let options = adaptive_options();
    succeed(&mut job, 0, producer_statistics);
    commit(&mut job, 0);
    assert!(adapt_job(&mut job, &options));
    let adapted = job.graph.to_string();
    // The consumer region runs, fails, and is retried.
    for task in &mut job.stages[1].tasks {
        task.attempts.push(TaskAttemptDescriptor {
            state: TaskState::Failed,
            messages: vec![],
            cause: None,
            job_output_fetched: false,
            statistics: None,
        });
    }
    assert!(!adapt_job(&mut job, &options));
    assert_eq!(job.graph.to_string(), adapted);
    assert_eq!(job.stages[1].adaptive, AdaptiveDecision::Replanned);
}

#[test]
fn coalescing_remains_available_after_join_dynamic_filter_pushdown() {
    use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;

    let filter = Arc::new(DynamicFilterPhysicalExpr::new(vec![key()], lit(true)));
    let join = HashJoinExec::try_new(
        placeholder(0, 4),
        placeholder(1, 4),
        vec![(key(), key())],
        None,
        &JoinType::Inner,
        None,
        PartitionMode::Partitioned,
        NullEquality::NullEqualsNothing,
        false,
    )
    .unwrap()
    .with_dynamic_filter_expr(filter)
    .unwrap();
    let view = view(
        Arc::new(join),
        vec![
            shuffle_input(statistics(&[8 * MIB; 4])),
            shuffle_input(statistics(&[MIB; 4])),
        ],
    );
    let rewrite = adapt(&view).unwrap().expect("a coalescing rewrite");
    assert_eq!(rewrite.rules, vec![RULE_COALESCE]);
    assert_eq!(count(&rewrite.plan), 1);
    assert_eq!(build_input(&rewrite.plan), Some(0));
    assert_eq!(
        rewrite
            .plan
            .downcast_ref::<HashJoinExec>()
            .unwrap()
            .dynamic_expressions_produced()
            .len(),
        1
    );
    validate::validate_plan(&rewrite.plan).unwrap();
}

#[test]
fn coalesced_inputs_keep_exact_rows_and_estimated_bytes() {
    use datafusion::common::stats::Precision;

    let statistics = statistics(&[MIB; 4]);
    let view = view(placeholder(0, 4), vec![shuffle_input(statistics.clone())]);
    let rewrite = adapt(&view).unwrap().expect("a coalescing rewrite");
    let actual = StatisticsContext::new()
        .compute(rewrite.plan.as_ref(), &StatisticsArgs::default())
        .unwrap();
    assert_eq!(
        actual.num_rows,
        Precision::Exact(statistics.total_rows as usize)
    );
    assert_eq!(
        actual.total_byte_size,
        Precision::Inexact(statistics.total_logical_bytes as usize)
    );
}

#[test]
fn coalescing_sorted_shuffle_buckets_keeps_the_static_sort_merge_join() {
    use datafusion::arrow::compute::SortOptions;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::sorts::sort::SortExec;

    let ordering = LexOrdering::new([PhysicalSortExpr::new_default(key())]).unwrap();
    let side = || {
        let source = MemorySourceConfig::try_new_exec(&[vec![]], schema(), None).unwrap();
        let sorted = Arc::new(SortExec::new(ordering.clone(), source));
        let shuffled =
            Arc::new(RepartitionExec::try_new(sorted, Partitioning::Hash(vec![key()], 4)).unwrap());
        // With one producer, each original hash bucket is still sorted even
        // without RepartitionExec's explicit preserve-order option.
        assert!(!shuffled.preserve_order());
        let shuffled: Arc<dyn ExecutionPlan> = shuffled;
        assert!(shuffled.output_ordering().is_some());
        shuffled
    };
    let join = Arc::new(
        SortMergeJoinExec::try_new(
            side(),
            side(),
            vec![(key(), key())],
            None,
            JoinType::Inner,
            vec![SortOptions::default()],
            NullEquality::NullEqualsNothing,
        )
        .unwrap(),
    );
    let graph = JobGraph::try_new(
        join,
        JobGraphOptions {
            shuffle_backend: remote_shuffle_service(),
        },
    )
    .unwrap();
    assert_eq!(graph.stages().len(), 3);
    let mut job = job(graph);
    let options = adaptive_options();
    for producer in 0..2 {
        succeed(&mut job, producer, |_| ChannelCounts {
            rows: vec![10; 4],
            physical_bytes: Some(vec![100; 4]),
            logical_bytes: vec![200; 4],
        });
        commit(&mut job, producer);
    }
    let before = job.graph.stages()[2].plan.clone();
    validate::validate_plan(&before).unwrap();
    let view = JobScheduler::stage_view(&mut job, 2, &options.adaptive, None).unwrap();
    let rewrite = adapt(&view).unwrap().expect("a coalescing proposal");
    assert_eq!(count(&rewrite.plan), 1);
    // The remote reader interleaves buckets without merging their sort order.
    // A consumer that needs sorted rows must reject this proposal.
    let reason = validate::validate_plan(&rewrite.plan).unwrap_err();
    assert!(
        reason.contains("input ordering is not satisfied"),
        "{reason}"
    );
    assert!(!adapt_job(&mut job, &options));
    assert!(Arc::ptr_eq(&before, &job.graph.stages()[2].plan));
    assert_eq!(job.stages[2].tasks.len(), 4);
    assert_eq!(job.stages[2].adaptive, AdaptiveDecision::Static);
    assert!(
        job.graph.stages()[2]
            .inputs
            .iter()
            .all(|input| input.adaptive.is_none())
    );
}

#[test]
fn coalescing_partition_constants_cannot_satisfy_sort_merge_ordering() {
    use datafusion::arrow::compute::SortOptions;
    use datafusion::physical_expr::equivalence::{AcrossPartitions, ConstExpr};

    let side = |input: usize| {
        let mut equivalence = EquivalenceProperties::new(schema());
        equivalence
            .add_constants([ConstExpr::new(key(), AcrossPartitions::Heterogeneous)])
            .unwrap();
        Arc::new(StageInputExec::new(
            input,
            Arc::new(PlanProperties::new(
                equivalence,
                Partitioning::Hash(vec![key()], 4),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        )) as Arc<dyn ExecutionPlan>
    };
    let join: Arc<dyn ExecutionPlan> = Arc::new(
        SortMergeJoinExec::try_new(
            side(0),
            side(1),
            vec![(key(), key())],
            None,
            JoinType::Inner,
            vec![SortOptions::default()],
            NullEquality::NullEqualsNothing,
        )
        .unwrap(),
    );
    // A key that is constant within each original bucket trivially satisfies
    // that bucket's ordering requirement, even without an explicit ordering.
    validate::validate_plan(&join).unwrap();
    let view = view(
        join,
        vec![
            shuffle_input(statistics(&[MIB; 4])),
            shuffle_input(statistics(&[MIB; 4])),
        ],
    );
    let rewrite = adapt(&view).unwrap().expect("a coalescing proposal");
    assert_eq!(count(&rewrite.plan), 1);
    // Different buckets may have different constant values. Merging them must
    // not let those old constants prove that the merged stream is sorted.
    let reason = validate::validate_plan(&rewrite.plan).unwrap_err();
    assert!(
        reason.contains("input ordering is not satisfied"),
        "{reason}"
    );
}

#[test]
fn checkpoint_writer_keeps_hash_buckets_when_skew_and_coalescing_keep_the_count() {
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use object_store::path::Path;
    use sail_physical_plan::remote_checkpoint::{
        RemoteCheckpointCommitExec, RemoteCheckpointWriteExec, checkpoint_storage_schema,
    };

    let side = || {
        let input = MemorySourceConfig::try_new_exec(&vec![vec![]; 3], schema(), None).unwrap();
        Arc::new(RepartitionExec::try_new(input, Partitioning::Hash(vec![key()], 4)).unwrap())
            as Arc<dyn ExecutionPlan>
    };
    let joined = hash_join(side(), side(), PartitionMode::Partitioned, JoinType::Inner);
    let logical_schema = joined.schema();
    let storage_schema = checkpoint_storage_schema(&logical_schema);
    let object_store = ObjectStoreUrl::parse("memory://").unwrap();
    let prefix = Path::from("checkpoint/adaptive-layout");
    let writer: Arc<dyn ExecutionPlan> = Arc::new(
        RemoteCheckpointWriteExec::try_new(
            joined,
            object_store.clone(),
            prefix.clone(),
            storage_schema.clone(),
            None,
        )
        .unwrap(),
    );
    let plan = Arc::new(RemoteCheckpointCommitExec::new(
        Arc::new(CoalescePartitionsExec::new(writer)),
        "adaptive-layout".to_string(),
        object_store,
        prefix,
        logical_schema,
        storage_schema,
        Partitioning::Hash(vec![Arc::new(Column::new("_c0", 0))], 4),
        None,
    ));
    let graph = JobGraph::try_new(
        plan,
        JobGraphOptions {
            shuffle_backend: remote_shuffle_service(),
        },
    )
    .unwrap();
    let writer_stage = graph
        .stages()
        .iter()
        .position(|stage| stage.plan.is::<RemoteCheckpointWriteExec>())
        .expect("checkpoint writer stage");
    let commit_stage = graph
        .stages()
        .iter()
        .position(|stage| stage.plan.is::<RemoteCheckpointCommitExec>())
        .expect("checkpoint commit stage");
    let mut job = job(graph);
    // Three large mapper slices of bucket 0 and one coalesced range of buckets
    // 1..4 still give four tasks. Count-only checkpoint metadata validation
    // cannot distinguish that layout from the original four hash buckets.
    succeed(&mut job, 0, |_| ChannelCounts {
        rows: vec![100; 4],
        physical_bytes: Some(vec![80 * MIB, MIB, MIB, MIB]),
        logical_bytes: vec![160 * MIB, 2 * MIB, 2 * MIB, 2 * MIB],
    });
    succeed(&mut job, 1, |_| ChannelCounts {
        rows: vec![100; 4],
        physical_bytes: Some(vec![MIB; 4]),
        logical_bytes: vec![2 * MIB; 4],
    });
    commit(&mut job, 0);
    commit(&mut job, 1);
    assert_eq!(count(&job.graph.stages()[writer_stage].plan), 4);
    let before = job.graph.to_string();
    let adapted = adapt_job(&mut job, &skew_options());
    assert_eq!(count(&job.graph.stages()[writer_stage].plan), 4);
    let checkpoint = job.graph.stages()[commit_stage]
        .plan
        .downcast_ref::<RemoteCheckpointCommitExec>()
        .unwrap();
    assert!(matches!(
        checkpoint.checkpoint_partitioning(),
        Partitioning::Hash(_, 4)
    ));
    if adapted {
        let spec = job.graph.stages()[writer_stage].inputs[0]
            .adaptive
            .as_ref()
            .unwrap();
        assert_eq!(spec.partitions(), 4);
        // The same key in bucket 0 can now appear in three saved partitions,
        // contradicting the static Hash(..., 4) checkpoint descriptor.
        assert_eq!(
            spec.spec
                .iter()
                .filter(|slices| slices[0].buckets == (0..1))
                .count(),
            3
        );
        assert_eq!(spec.spec[3][0].buckets, 1..4);
        assert_eq!(spec.property, AdaptiveProperty::Unknown);
    }
    assert!(
        !adapted,
        "checkpoint writer changed hash bucket boundaries while the commit retained Hash(..., 4)"
    );
    assert_eq!(job.graph.to_string(), before);
    assert_eq!(job.stages[writer_stage].adaptive, AdaptiveDecision::Static);
}

#[tokio::test]
async fn checkpoint_hash_metadata_cannot_describe_skew_slices_with_the_same_count() {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::TaskContext;
    use sail_physical_plan::remote_checkpoint::CheckpointDataSource;

    let row = |value| {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(vec![7])),
                Arc::new(Int64Array::from(vec![value])),
            ],
        )
        .unwrap()
    };
    let saved = MemorySourceConfig::try_new(
        &[vec![row(10)], vec![row(20)], vec![row(30)], vec![]],
        schema(),
        None,
    )
    .unwrap();
    // The real checkpoint scan trusts this saved descriptor instead of the
    // underlying file layout. The three slices of one key violate that claim.
    let checkpointed: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
        CheckpointDataSource::new(Arc::new(saved), Partitioning::Hash(vec![key()], 4)),
    )));
    let probe = || {
        let source = MemorySourceConfig::try_new_exec(&[vec![row(99)]], schema(), None).unwrap();
        Arc::new(RepartitionExec::try_new(source, Partitioning::Hash(vec![key()], 4)).unwrap())
            as Arc<dyn ExecutionPlan>
    };
    let joined = hash_join(
        checkpointed.clone(),
        probe(),
        PartitionMode::Partitioned,
        JoinType::Inner,
    );
    let rows = datafusion::physical_plan::collect(joined, Arc::new(TaskContext::default()))
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    assert!(
        rows <= 1,
        "one hash bucket cannot see the other saved slices"
    );

    // Shuffling the saved rows restores key co-location and all three matches.
    let restored: Arc<dyn ExecutionPlan> = Arc::new(
        RepartitionExec::try_new(checkpointed, Partitioning::Hash(vec![key()], 4)).unwrap(),
    );
    let joined = hash_join(
        restored,
        probe(),
        PartitionMode::Partitioned,
        JoinType::Inner,
    );
    let rows = datafusion::physical_plan::collect(joined, Arc::new(TaskContext::default()))
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    assert_eq!(rows, 3);
}

#[test]
fn global_sorts_keep_single_partition_skew_domains() {
    use datafusion::arrow::compute::SortOptions;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::sorts::sort::SortExec;

    let ordering = LexOrdering::new([PhysicalSortExpr::new_default(key())]).unwrap();
    for sort_merge in [false, true] {
        // Each sort is initially valid: one hash bucket contains both producers.
        for global_side in [None, Some(0), Some(1), Some(2)] {
            let sorted = |i| {
                Arc::new(
                    SortExec::new(ordering.clone(), placeholder(i, 1)).with_preserve_partitioning(
                        global_side != Some(i) && global_side != Some(2),
                    ),
                ) as Arc<dyn ExecutionPlan>
            };
            let plan = if sort_merge {
                Arc::new(
                    SortMergeJoinExec::try_new(
                        sorted(0),
                        sorted(1),
                        vec![(key(), key())],
                        None,
                        JoinType::Inner,
                        vec![SortOptions::default()],
                        NullEquality::NullEqualsNothing,
                    )
                    .unwrap(),
                ) as Arc<dyn ExecutionPlan>
            } else {
                hash_join(
                    sorted(0),
                    sorted(1),
                    PartitionMode::Partitioned,
                    JoinType::Inner,
                )
            };
            validate::validate_plan(&plan).unwrap();
            let mut view = skew_view(plan, vec![producer_input(&[vec![100], vec![100]]); 2]);
            view.options.coalesce_enabled = false;
            view.options.skew_factor = 0.5;
            view.options.skew_threshold = 1;
            view.options.coalesce_target_size = 1;
            let rewrite = adapt(&view).unwrap();
            if global_side.is_some() {
                assert!(
                    rewrite.is_none(),
                    "global sort on side {global_side:?} was split (sort-merge={sort_merge})"
                );
            } else {
                let rewrite = rewrite.expect("partition-preserving sorts remain splittable");
                assert!(rewrite.rules.contains(&skew::RULE_SKEW_SPLIT));
                assert_eq!(count(&rewrite.plan), 4);
                check_specs(&rewrite, &view, &[0, 1]);
                validate::validate_plan(&rewrite.plan).unwrap();
            }
        }
    }
}

#[test]
fn global_sorts_keep_the_static_job_graph_when_the_input_bucket_is_skewed() {
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::sorts::sort::SortExec;

    let ordering = LexOrdering::new([PhysicalSortExpr::new_default(key())]).unwrap();
    let side = || {
        let source = MemorySourceConfig::try_new_exec(&[vec![], vec![]], schema(), None).unwrap();
        let shuffled =
            Arc::new(RepartitionExec::try_new(source, Partitioning::Hash(vec![key()], 1)).unwrap());
        Arc::new(SortExec::new(ordering.clone(), shuffled)) as Arc<dyn ExecutionPlan>
    };
    let graph = JobGraph::try_new(
        hash_join(side(), side(), PartitionMode::Partitioned, JoinType::Inner),
        JobGraphOptions {
            shuffle_backend: remote_shuffle_service(),
        },
    )
    .unwrap();
    let mut job = job(graph);
    assert_eq!(job.graph.stages().len(), 3);
    for producer in 0..2 {
        succeed(&mut job, producer, |_| ChannelCounts {
            rows: vec![1],
            physical_bytes: Some(vec![100]),
            logical_bytes: vec![100],
        });
        commit(&mut job, producer);
    }
    let mut adaptive = options();
    adaptive.join_enabled = false;
    adaptive.coalesce_enabled = false;
    adaptive.skew_factor = 0.5;
    adaptive.skew_threshold = 1;
    adaptive.coalesce_target_size = 1;
    let options = JobSchedulerOptions::new_for_test(remote_shuffle_service(), adaptive, None);
    let before = job.graph.to_string();
    assert!(
        !adapt_job(&mut job, &options),
        "global sorts hid multiple input slices behind one output partition"
    );
    assert_eq!(job.graph.to_string(), before);
    assert_eq!(job.stages[2].tasks.len(), 1);
    assert!(
        job.graph.stages()[2]
            .inputs
            .iter()
            .all(|input| input.adaptive.is_none())
    );
    assert_eq!(job.stages[2].adaptive, AdaptiveDecision::Static);
}

#[tokio::test]
async fn single_partition_sort_validation_prevents_skew_slices_from_losing_join_rows() {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::execution::TaskContext;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::sorts::sort::SortExec;

    let row = |k| {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(vec![k])),
                Arc::new(Int64Array::from(vec![k * 10])),
            ],
        )
        .unwrap()
    };
    let joined =
        |left: Vec<Vec<RecordBatch>>, right: Vec<Vec<RecordBatch>>, preserve_partitioning| {
            let ordering = LexOrdering::new([PhysicalSortExpr::new_default(key())]).unwrap();
            let sorted = |partitions: Vec<Vec<RecordBatch>>| {
                let source = MemorySourceConfig::try_new_exec(&partitions, schema(), None).unwrap();
                Arc::new(
                    SortExec::new(ordering.clone(), source)
                        .with_preserve_partitioning(preserve_partitioning),
                ) as Arc<dyn ExecutionPlan>
            };
            hash_join(
                sorted(left),
                sorted(right),
                PartitionMode::Partitioned,
                JoinType::Inner,
            )
        };
    async fn rows(plan: Arc<dyn ExecutionPlan>) -> usize {
        datafusion::physical_plan::collect(plan, Arc::new(TaskContext::default()))
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum()
    }

    let static_plan = joined(
        vec![vec![row(1), row(2)]],
        vec![vec![row(1), row(2)]],
        false,
    );
    validate::validate_plan(&static_plan).unwrap();
    assert_eq!(rows(static_plan).await, 2);

    // The exact-coverage contract permits these four slice pairs. A global sort
    // above either input still advertises one partition and reads only pair 0.
    let sliced_left = vec![vec![row(1)], vec![row(1)], vec![row(2)], vec![row(2)]];
    let sliced_right = vec![vec![row(1)], vec![row(2)], vec![row(1)], vec![row(2)]];
    let unsafe_plan = joined(sliced_left.clone(), sliced_right.clone(), false);
    assert_eq!(count(&unsafe_plan), 1);
    assert_eq!(rows(unsafe_plan.clone()).await, 1);
    let preserving_plan = joined(sliced_left, sliced_right, true);
    validate::validate_plan(&preserving_plan).unwrap();
    assert_eq!(rows(preserving_plan).await, 2);

    let error = validate::validate_plan(&unsafe_plan)
        .expect_err("a global sort must reject multiple input partitions");
    assert!(error.contains("SortExec"), "{error}");
    assert!(error.contains("single partition"), "{error}");
}

#[test]
fn piecewise_build_output_requires_single_probe_partition() {
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::joins::PiecewiseMergeJoinExec;
    use datafusion::physical_plan::sorts::sort::SortExec;

    for join_type in [
        JoinType::Inner,
        JoinType::Right,
        JoinType::Left,
        JoinType::Full,
    ] {
        let left = Arc::new(
            SortExec::new(
                LexOrdering::new([PhysicalSortExpr::new_default(key())]).unwrap(),
                placeholder(0, 1),
            )
            .with_preserve_partitioning(true),
        );
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            PiecewiseMergeJoinExec::try_new(
                left,
                placeholder(1, 2),
                (key(), key()),
                Operator::Gt,
                join_type,
                2,
            )
            .unwrap(),
        );
        let result = validate::validate_plan(&plan);
        if matches!(join_type, JoinType::Left | JoinType::Full) {
            assert!(result.unwrap_err().contains("probe"));
        } else {
            result.unwrap();
        }
    }
}

fn whole_probe_join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_type: JoinType,
    null_aware: bool,
) -> Arc<dyn ExecutionPlan> {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::JoinSide;
    use datafusion::physical_plan::joins::NestedLoopJoinExec;
    use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};

    if null_aware {
        return Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                vec![(key(), key())],
                None,
                &join_type,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                true,
            )
            .unwrap(),
        );
    }
    // A real non-equi predicate, rather than a constant false expression that a
    // logical optimizer could eliminate before building this physical plan.
    let filter = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("build", 0)),
            Operator::Gt,
            Arc::new(Column::new("probe", 1)),
        )),
        vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
        ],
        Arc::new(Schema::new(vec![
            Field::new("build", DataType::Int64, true),
            Field::new("probe", DataType::Int64, true),
        ])),
    );
    Arc::new(NestedLoopJoinExec::try_new(left, right, Some(filter), &join_type, None).unwrap())
}

#[test]
fn nested_whole_probe_joins_keep_single_partition_skew_domains() {
    for null_aware in [false, true] {
        let types = if null_aware {
            vec![JoinType::LeftAnti]
        } else {
            vec![
                JoinType::Left,
                JoinType::LeftSemi,
                JoinType::LeftAnti,
                JoinType::LeftMark,
                JoinType::Full,
                JoinType::Inner,
                JoinType::Right,
                JoinType::RightSemi,
                JoinType::RightAnti,
            ]
        };
        for join_type in types {
            let nested = hash_join(
                placeholder(0, 1),
                placeholder(1, 1),
                PartitionMode::Partitioned,
                JoinType::Inner,
            );
            let plan = whole_probe_join(broadcast_placeholder(2), nested, join_type, null_aware);
            validate::validate_plan(&plan).unwrap();
            let mut view = skew_view(
                plan,
                vec![
                    producer_input(&[vec![100], vec![100]]),
                    producer_input(&[vec![100], vec![100]]),
                    broadcast_input(statistics(&[100])),
                ],
            );
            view.options.coalesce_enabled = false;
            view.options.skew_factor = 0.5;
            view.options.skew_threshold = 1;
            view.options.coalesce_target_size = 1;
            let rewritten = adapt(&view).unwrap();
            let whole_probe = null_aware
                || matches!(
                    join_type,
                    JoinType::Left
                        | JoinType::LeftSemi
                        | JoinType::LeftAnti
                        | JoinType::LeftMark
                        | JoinType::Full
                );
            if whole_probe {
                assert!(
                    rewritten.is_none(),
                    "{join_type:?}, null-aware={null_aware}"
                );
            } else {
                let rewritten = rewritten.expect("independent probe partitions remain splittable");
                assert_eq!(count(&rewritten.plan), 4);
                check_specs(&rewritten, &view, &[0, 1]);
                validate::validate_plan(&rewritten.plan).unwrap();
            }
        }
    }
}

#[test]
fn nested_whole_probe_joins_keep_the_static_job_graph() {
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;

    for null_aware in [false, true] {
        let side = || {
            let source =
                MemorySourceConfig::try_new_exec(&[vec![], vec![]], schema(), None).unwrap();
            Arc::new(RepartitionExec::try_new(source, Partitioning::Hash(vec![key()], 1)).unwrap())
                as Arc<dyn ExecutionPlan>
        };
        let build = MemorySourceConfig::try_new_exec(&[vec![]], schema(), None).unwrap();
        let build = Arc::new(CoalescePartitionsExec::new(build));
        let nested = hash_join(side(), side(), PartitionMode::Partitioned, JoinType::Inner);
        let graph = JobGraph::try_new(
            whole_probe_join(build, nested, JoinType::LeftAnti, null_aware),
            JobGraphOptions {
                shuffle_backend: remote_shuffle_service(),
            },
        )
        .unwrap();
        let mut job = job(graph);
        assert_eq!(job.graph.stages().len(), 4);
        assert_eq!(count(&job.graph.stages()[3].plan), 1);
        for producer in 0..3 {
            succeed(&mut job, producer, |_| ChannelCounts {
                rows: vec![1],
                physical_bytes: Some(vec![100]),
                logical_bytes: vec![100],
            });
            commit(&mut job, producer);
        }
        let mut adaptive = options();
        adaptive.join_enabled = false;
        adaptive.coalesce_enabled = false;
        adaptive.skew_factor = 0.5;
        adaptive.skew_threshold = 1;
        adaptive.coalesce_target_size = 1;
        let options = JobSchedulerOptions::new_for_test(remote_shuffle_service(), adaptive, None);
        let before = job.graph.to_string();
        assert!(
            !adapt_job(&mut job, &options),
            "whole-probe stage expanded, null-aware={null_aware}"
        );
        assert_eq!(job.graph.to_string(), before);
        assert_eq!(job.stages[3].tasks.len(), 1);
        assert_eq!(job.stages[3].adaptive, AdaptiveDecision::Static);
        assert!(
            job.graph.stages()[3]
                .inputs
                .iter()
                .all(|input| input.adaptive.is_none())
        );
    }
}

#[tokio::test]
async fn isolated_whole_probe_join_tasks_lose_unmatched_build_rows() {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::execution::TaskContext;
    use futures::StreamExt;

    let row = |k| {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(vec![k])),
                Arc::new(Int64Array::from(vec![k * 10])),
            ],
        )
        .unwrap()
    };
    let join = |left: Vec<Vec<RecordBatch>>, right: Vec<Vec<RecordBatch>>, null_aware| {
        let left = MemorySourceConfig::try_new_exec(&left, schema(), None).unwrap();
        let right = MemorySourceConfig::try_new_exec(&right, schema(), None).unwrap();
        let probe = hash_join(left, right, PartitionMode::Partitioned, JoinType::Inner);
        let build = MemorySourceConfig::try_new_exec(&[vec![row(0)]], schema(), None).unwrap();
        whole_probe_join(build, probe, JoinType::LeftAnti, null_aware)
    };
    let sliced_left = vec![vec![row(1)], vec![row(1)], vec![row(2)], vec![row(2)]];
    let sliced_right = vec![vec![row(1)], vec![row(2)], vec![row(1)], vec![row(2)]];
    for null_aware in [false, true] {
        let static_plan = join(
            vec![vec![row(1), row(2)]],
            vec![vec![row(1), row(2)]],
            null_aware,
        );
        validate::validate_plan(&static_plan).unwrap();
        let batches =
            datafusion::physical_plan::collect(static_plan, Arc::new(TaskContext::default()))
                .await
                .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

        // Distributed tasks deserialize independent plans and execute only their
        // own partition. They cannot share the build bitmap/probe completion
        // counter that makes a multi-partition join correct on one process.
        let mut rows = 0;
        for partition in 0..4 {
            let plan = join(sliced_left.clone(), sliced_right.clone(), null_aware);
            let mut stream = plan
                .execute(partition, Arc::new(TaskContext::default()))
                .unwrap();
            while let Some(batch) = stream.next().await {
                rows += batch.unwrap().num_rows();
            }
        }
        assert_eq!(
            rows, 0,
            "unmatched build row disappeared, null-aware={null_aware}"
        );
        let unsafe_plan = join(sliced_left.clone(), sliced_right.clone(), null_aware);
        let error = validate::validate_plan(&unsafe_plan)
            .expect_err("distributed build-side output requires the whole probe in one task");
        assert!(error.contains("probe"), "{error}");
    }
}
