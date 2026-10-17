//! Skew splitting for joins over remote shuffle service inputs.
//!
//! A skewed bucket of a join input is split by producer partition: each slice is read
//! with a map-range read from the shuffle service, and is paired with the whole bucket of the other
//! input (or with each slice of the other input when both sides are split). This
//! follows Spark's `OptimizeSkewedJoin`:
//!
//!   - a bucket is skewed when it is larger than both `factor` times the median bucket
//!     size of its input and the absolute threshold;
//!   - slices target the larger of the advisory size and the mean of the non-skewed
//!     buckets, and are cut over the committed per-producer sizes with Spark's
//!     `splitSizeListByTargetSize`.
//!
//! Splitting breaks the guarantee that all rows of a key are in one partition, so it
//! is applied only where that is safe:
//!   - only to the sides that the join type allows (the replicated side must not emit
//!     rows that depend on seeing all of the other side);
//!   - only when every operator between the input and the join gives the same rows on
//!     any subset of its input (projection, filter, sort);
//!   - only when no ancestor of the join in the stage requires a distribution.

use std::sync::Arc;

use datafusion::common::JoinType;
use datafusion::common::tree_node::TreeNode;
use datafusion::physical_expr::{Distribution, PhysicalExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;

use crate::driver::job_scheduler::adaptive::spec::{ranges_from_starts, split_by_target_size};
use crate::driver::job_scheduler::adaptive::view::StageView;
use crate::job_graph::{AdaptiveInput, AdaptiveProperty, BucketSlice};
use crate::plan::StageInputExec;
use crate::statistics::TaskOutputStatistics;

pub(crate) const RULE_SKEW_SPLIT: &str = "skew_split";

/// A join whose inputs may be split, with whether each input may be split.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SkewJoin {
    /// `(input, splittable)` for each paired stage input: the build and probe inputs
    /// of a partitioned join, or the probe input of a broadcast join.
    pub sides: Vec<(usize, bool)>,
}

/// Returns which sides of a join may be split: `(left, right)`.
/// In DataFusion the left input is the build side.
fn splittable_sides(join_type: JoinType) -> (bool, bool) {
    match join_type {
        JoinType::Inner => (true, true),
        JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
            (true, false)
        }
        JoinType::Right | JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {
            (false, true)
        }
        JoinType::Full => (false, false),
    }
}

/// Finds the joins of a stage plan whose inputs may be split.
pub(crate) fn skew_joins(plan: &Arc<dyn ExecutionPlan>) -> Vec<SkewJoin> {
    let mut result = vec![];
    walk(plan, true, &mut result);
    result
}

fn walk(node: &Arc<dyn ExecutionPlan>, safe: bool, result: &mut Vec<SkewJoin>) {
    if safe && let Some(join) = candidate(node) {
        result.push(join);
    }
    let requirements = node.input_distribution_requirements();
    for (i, child) in node.children().into_iter().enumerate() {
        // An operator that requires its input to be distributed in a particular way
        // relies on every key being in one partition.
        let unspecified = matches!(
            requirements.child_distribution(i),
            None | Some(Distribution::UnspecifiedDistribution)
        );
        walk(child, safe && unspecified, result);
    }
}

fn candidate(node: &Arc<dyn ExecutionPlan>) -> Option<SkewJoin> {
    if let Some(join) = node.downcast_ref::<HashJoinExec>() {
        if join.null_aware || join.fetch().is_some() {
            return None;
        }
        let (left_ok, right_ok) = splittable_sides(join.join_type);
        return match join.mode {
            PartitionMode::Partitioned => {
                let left = split_safe_input(join.left())?;
                let right = split_safe_input(join.right())?;
                (left_ok || right_ok).then(|| SkewJoin {
                    sides: vec![(left, left_ok), (right, right_ok)],
                })
            }
            // Every task reads the whole broadcast build side, so a probe slice needs
            // no replication.
            PartitionMode::CollectLeft => {
                let right = split_safe_input(join.right())?;
                right_ok.then(|| SkewJoin {
                    sides: vec![(right, true)],
                })
            }
            PartitionMode::Auto => None,
        };
    }
    if let Some(join) = node.downcast_ref::<SortMergeJoinExec>() {
        let (left_ok, right_ok) = splittable_sides(join.join_type());
        let left = split_safe_input(join.left())?;
        let right = split_safe_input(join.right())?;
        return (left_ok || right_ok).then(|| SkewJoin {
            sides: vec![(left, left_ok), (right, right_ok)],
        });
    }
    None
}

/// Returns the stage input that a join child reads through operators that give the
/// same rows on any subset of their input.
fn split_safe_input(node: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    let mut node = node;
    loop {
        if let Some(placeholder) = node.downcast_ref::<StageInputExec<usize>>() {
            return Some(*placeholder.input());
        }
        // A replicated slice re-evaluates these expressions. Volatile expressions
        // could then produce different values or select different rows for each
        // copy of the same input row.
        let safe = if let Some(projection) = node.downcast_ref::<ProjectionExec>() {
            projection
                .expr()
                .iter()
                .all(|expr| deterministic(&expr.expr))
        } else if let Some(filter) = node.downcast_ref::<FilterExec>() {
            // An embedded limit restarts for every slice, admitting rows that
            // the original partition's filter would not have emitted.
            filter.fetch().is_none() && deterministic(filter.predicate())
        } else {
            node.is::<CooperativeExec>()
                || node
                    .downcast_ref::<SortExec>()
                    .is_some_and(|sort| sort.fetch().is_none())
        };
        match node.children().as_slice() {
            [child] if safe => node = child,
            _ => return None,
        }
    }
}

fn deterministic(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.exists(|expr| Ok(expr.is_volatile_node()))
        .is_ok_and(|volatile| !volatile)
}

/// Splits the skewed single-bucket ranges of a join's inputs.
///
/// `ranges` are the bucket ranges after coalescing, one per partition of the
/// unsplit inputs. Returns one spec per side, in the order of `join.sides`, or
/// `None` if no bucket is split.
pub(crate) fn split_skewed_buckets(
    view: &StageView,
    join: &SkewJoin,
    ranges: &[std::ops::Range<usize>],
    notes: &mut Vec<String>,
) -> Result<Option<Vec<AdaptiveInput>>, String> {
    let options = &view.options;
    let mut sides = vec![];
    for &(input, splittable) in &join.sides {
        let view_input = view
            .inputs
            .get(input)
            .ok_or_else(|| format!("input {input} not found"))?;
        let statistics = view_input
            .statistics
            .as_deref()
            .ok_or_else(|| format!("input {input} has no statistics"))?;
        let sizes = statistics
            .physical_bytes
            .as_deref()
            .ok_or_else(|| format!("input {input} has unknown physical sizes"))?;
        let committed = view_input
            .committed
            .clone()
            .ok_or_else(|| format!("input {input} has no committed attempt statistics"))?;
        let mut sorted = sizes.to_vec();
        sorted.sort_unstable();
        let median = sorted.get(sorted.len() / 2).copied().unwrap_or(0);
        let threshold = options
            .skew_threshold
            .max((options.skew_factor * median as f64) as u64);
        let skewed = sizes
            .iter()
            .map(|&size| size > threshold)
            .collect::<Vec<_>>();
        let (sum, count) = sizes
            .iter()
            .zip(&skewed)
            .filter(|(_, skewed)| !**skewed)
            .fold((0u64, 0u64), |(s, c), (&size, _)| {
                (s.saturating_add(size), c + 1)
            });
        let mean = sum.checked_div(count).unwrap_or(0);
        let target = options.coalesce_target_size.max(mean).max(1);
        sides.push(Side {
            splittable,
            skewed,
            target,
            committed,
            producers: view_input.producers,
        });
    }

    let mut specs: Vec<Vec<Vec<BucketSlice>>> = vec![vec![]; sides.len()];
    let mut split_any = false;
    for range in ranges {
        let parts = if range.len() == 1 {
            sides
                .iter()
                .map(|side| side.parts(range.start))
                .collect::<Vec<_>>()
        } else {
            vec![None; sides.len()]
        };
        if parts.iter().all(Option::is_none) {
            for spec in &mut specs {
                spec.push(vec![BucketSlice::whole(range.clone())]);
            }
            continue;
        }
        // Count the Cartesian product before constructing it. The slot cap is
        // also an allocation guard: two skewed inputs can each have thousands of
        // producer slices even when only a handful of tasks are allowed.
        let partitions = parts.iter().try_fold(1usize, |count, part| {
            count.checked_mul(part.as_ref().map_or(1, Vec::len))
        });
        let total = partitions.and_then(|count| specs[0].len().checked_add(count));
        let Some(total) = total else {
            notes.push("skew split skipped: partition count overflow".to_string());
            return Ok(None);
        };
        if let Some(cap) = view.slot_cap
            && total > cap
        {
            notes.push(format!(
                "skew split skipped: at least {total} partitions exceed the limit of {cap} slots"
            ));
            return Ok(None);
        }
        split_any = true;
        let slices = parts
            .iter()
            .map(|part| match part {
                Some(producers) => producers
                    .iter()
                    .map(|p| BucketSlice::producers(range.start, p.clone()))
                    .collect::<Vec<_>>(),
                None => vec![BucketSlice::whole(range.clone())],
            })
            .collect::<Vec<_>>();
        notes.push(format!(
            "skew split bucket {}: [{}] slices",
            range.start,
            slices
                .iter()
                .map(|x| x.len().to_string())
                .collect::<Vec<_>>()
                .join(" x ")
        ));
        // Each combination of slices, one per side, becomes one partition.
        let mut combinations: Vec<Vec<BucketSlice>> = vec![vec![]];
        for side_slices in &slices {
            combinations = combinations
                .into_iter()
                .flat_map(|prefix| {
                    side_slices.iter().map(move |slice| {
                        let mut combination = prefix.clone();
                        combination.push(slice.clone());
                        combination
                    })
                })
                .collect();
        }
        for combination in combinations {
            for (spec, slice) in specs.iter_mut().zip(combination) {
                spec.push(vec![slice]);
            }
        }
    }
    if !split_any {
        return Ok(None);
    }
    if let Some(cap) = view.slot_cap
        && specs.first().is_some_and(|spec| spec.len() > cap)
    {
        notes.push(format!(
            "skew split skipped: {} partitions exceed the limit of {cap} slots",
            specs[0].len()
        ));
        return Ok(None);
    }
    Ok(Some(
        specs
            .into_iter()
            .map(|spec| AdaptiveInput {
                spec,
                property: AdaptiveProperty::Unknown,
            })
            .collect(),
    ))
}

struct Side {
    splittable: bool,
    skewed: Vec<bool>,
    target: u64,
    committed: Arc<Vec<Arc<TaskOutputStatistics>>>,
    producers: usize,
}

impl Side {
    /// Returns the producer ranges to split a bucket into, or `None` to read it whole.
    fn parts(&self, bucket: usize) -> Option<Vec<std::ops::Range<usize>>> {
        if !self.splittable || !self.skewed.get(bucket).copied().unwrap_or(false) {
            return None;
        }
        let sizes = self
            .committed
            .iter()
            .map(|statistics| statistics.physical_bytes_of(bucket))
            .collect::<Option<Vec<_>>>()?;
        if sizes.len() != self.producers || sizes.len() < 2 {
            return None;
        }
        let ranges = ranges_from_starts(&split_by_target_size(&sizes, self.target), sizes.len());
        (ranges.len() > 1).then_some(ranges)
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::NullEquality;
    use datafusion::common::config::ConfigOptions;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, lit};
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, ScalarFunctionExpr};
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::limit::LocalLimitExec;
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::physical_plan::{Partitioning, PlanProperties};

    use super::*;
    use crate::driver::job_scheduler::AdaptiveOptions;
    use crate::driver::job_scheduler::adaptive::view::InputView;
    use crate::driver::job_scheduler::test_utils::{key, schema};
    use crate::job_graph::{BucketLayout, InputMode, ShuffleOrigin};
    use crate::statistics::{ChannelCounts, ShuffleStatistics};

    fn input(i: usize) -> Arc<dyn ExecutionPlan> {
        input_with_buckets(i, 4)
    }

    fn input_with_buckets(i: usize, buckets: usize) -> Arc<dyn ExecutionPlan> {
        Arc::new(StageInputExec::new(
            i,
            Arc::new(PlanProperties::new(
                datafusion::physical_expr::EquivalenceProperties::new(schema()),
                Partitioning::UnknownPartitioning(buckets),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        ))
    }

    fn random() -> Arc<dyn PhysicalExpr> {
        Arc::new(
            ScalarFunctionExpr::try_new(
                datafusion::functions::math::random(),
                vec![],
                schema().as_ref(),
                Arc::new(ConfigOptions::default()),
            )
            .unwrap(),
        )
    }

    #[test]
    fn replicated_inputs_must_not_re_evaluate_volatile_expressions() {
        let projection: Arc<dyn ExecutionPlan> = Arc::new(
            ProjectionExec::try_new(
                vec![(key(), "k".to_string()), (random(), "r".to_string())],
                input(0),
            )
            .unwrap(),
        );
        assert_eq!(split_safe_input(&projection), None);

        let predicate = Arc::new(BinaryExpr::new(random(), Operator::Gt, lit(0.5f64)));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input(0)).unwrap());
        assert_eq!(split_safe_input(&filter), None);

        let deterministic: Arc<dyn ExecutionPlan> =
            Arc::new(ProjectionExec::try_new(vec![(key(), "k".to_string())], input(0)).unwrap());
        assert_eq!(split_safe_input(&deterministic), Some(0));
    }

    fn skewed_input(producers: usize) -> InputView {
        let output = Arc::new(
            TaskOutputStatistics::new(&ChannelCounts {
                rows: vec![0, 1, 0, 0],
                physical_bytes: Some(vec![0, 100, 0, 0]),
                logical_bytes: vec![0, 100, 0, 0],
            })
            .unwrap(),
        );
        let committed = vec![output; producers];
        let statistics =
            Arc::new(ShuffleStatistics::fold(4, committed.iter().map(Arc::as_ref)).unwrap());
        InputView {
            mode: InputMode::Shuffle,
            origin: ShuffleOrigin::Engine,
            layout: BucketLayout::Direct,
            hash_partitioned: true,
            buckets: 4,
            statistics: Some(statistics),
            producers,
            committed: Some(Arc::new(committed)),
        }
    }

    fn positive_filter(
        input: Arc<dyn ExecutionPlan>,
        fetch: Option<usize>,
    ) -> Arc<dyn ExecutionPlan> {
        let predicate = Arc::new(BinaryExpr::new(key(), Operator::Gt, lit(0i64)));
        FilterExec::try_new(predicate, input)
            .unwrap()
            .with_fetch(fetch)
            .unwrap()
    }

    fn join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        mode: PartitionMode,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                vec![(key(), key())],
                None,
                &JoinType::Inner,
                None,
                mode,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        )
    }

    fn small_input() -> InputView {
        let output = Arc::new(
            TaskOutputStatistics::new(&ChannelCounts {
                rows: vec![1; 4],
                physical_bytes: Some(vec![100; 4]),
                logical_bytes: vec![100; 4],
            })
            .unwrap(),
        );
        let statistics = Arc::new(ShuffleStatistics::fold(4, [output.as_ref()]).unwrap());
        InputView {
            statistics: Some(statistics),
            producers: 1,
            committed: Some(Arc::new(vec![output])),
            ..skewed_input(1)
        }
    }

    fn coalescing_view(plan: Arc<dyn ExecutionPlan>, inputs: Vec<InputView>) -> StageView {
        StageView {
            plan,
            inputs,
            options: AdaptiveOptions {
                join_enabled: false,
                skew_enabled: false,
                coalesce_target_size: 1024,
                coalesce_min_partition_size: 1,
                ..AdaptiveOptions::default()
            },
            slot_cap: None,
        }
    }

    #[test]
    fn limited_filters_are_not_skew_split() {
        for limited_side in [0, 1, 2] {
            for fetch in [None, Some(1), Some(0)] {
                let mut children = [input(0), input(1)];
                if limited_side < 2 {
                    children[limited_side] = positive_filter(children[limited_side].clone(), fetch);
                }
                let mut plan = join(
                    children[0].clone(),
                    children[1].clone(),
                    PartitionMode::Partitioned,
                );
                if limited_side == 2 {
                    plan = positive_filter(plan, fetch);
                }
                let view = StageView {
                    plan,
                    inputs: vec![skewed_input(2), skewed_input(2)],
                    options: AdaptiveOptions {
                        join_enabled: false,
                        coalesce_enabled: false,
                        skew_threshold: 1,
                        coalesce_target_size: 1,
                        ..AdaptiveOptions::default()
                    },
                    slot_cap: None,
                };
                let rewrite = crate::driver::job_scheduler::adaptive::adapt(&view).unwrap();
                let split = rewrite
                    .as_ref()
                    .is_some_and(|rewrite| rewrite.rules.contains(&RULE_SKEW_SPLIT));
                assert_eq!(
                    split,
                    fetch.is_none(),
                    "side {limited_side}, fetch {fetch:?}"
                );
            }
        }
    }

    #[test]
    fn limited_filters_are_not_coalesced() {
        for fetch in [None, Some(1)] {
            let view = coalescing_view(positive_filter(input(0), fetch), vec![small_input()]);
            let rewrite = crate::driver::job_scheduler::adaptive::adapt(&view).unwrap();
            if let Some(rewrite) = &rewrite {
                crate::driver::job_scheduler::adaptive::validate::validate_plan(&rewrite.plan)
                    .unwrap();
            }
            let coalesced = rewrite.as_ref().is_some_and(|rewrite| {
                rewrite
                    .rules
                    .contains(&crate::driver::job_scheduler::adaptive::RULE_COALESCE)
            });
            // Four original buckets may each emit one row. A single coalesced
            // partition would apply the embedded limit once and emit only one.
            assert_eq!(coalesced, fetch.is_none(), "fetch {fetch:?}");
        }
    }

    #[test]
    fn partition_fetch_limits_preserve_coalescing_domains() {
        let ordering = LexOrdering::new([PhysicalSortExpr::new_default(key())]).unwrap();
        let limited: Vec<Arc<dyn ExecutionPlan>> = vec![
            Arc::new(LocalLimitExec::new(input(0), 1)),
            Arc::new(
                SortExec::new(ordering, input(0))
                    .with_preserve_partitioning(true)
                    .with_fetch(Some(1)),
            ),
            join(input(0), input(1), PartitionMode::Partitioned)
                .with_fetch(Some(1))
                .unwrap(),
            positive_filter(
                join(input(0), input(1), PartitionMode::Partitioned),
                Some(1),
            ),
        ];
        for plan in limited {
            let name = plan.name().to_string();
            let view = coalescing_view(plan, vec![small_input(); 2]);
            assert!(
                crate::driver::job_scheduler::adaptive::adapt(&view)
                    .unwrap()
                    .is_none(),
                "{name}"
            );
        }
    }

    #[test]
    fn partition_fetch_limits_keep_independent_union_and_broadcast_domains_available() {
        let plan = UnionExec::try_new(vec![positive_filter(input(0), Some(1)), input(1)]).unwrap();
        let view = coalescing_view(plan, vec![small_input(); 2]);
        let rewrite = crate::driver::job_scheduler::adaptive::adapt(&view)
            .unwrap()
            .unwrap();
        assert!(rewrite.inputs[0].is_none());
        assert_eq!(rewrite.inputs[1].as_ref().unwrap().partitions(), 1);

        let plan = positive_filter(
            UnionExec::try_new(vec![input(0), input(1)]).unwrap(),
            Some(1),
        );
        let view = coalescing_view(plan, vec![small_input(); 2]);
        assert!(
            crate::driver::job_scheduler::adaptive::adapt(&view)
                .unwrap()
                .is_none()
        );

        let plan = join(
            positive_filter(input_with_buckets(0, 1), Some(1)),
            input(1),
            PartitionMode::CollectLeft,
        );
        let mut broadcast = small_input();
        broadcast.mode = InputMode::Broadcast;
        broadcast.layout = BucketLayout::Opaque;
        let view = coalescing_view(plan, vec![broadcast, small_input()]);
        let rewrite = crate::driver::job_scheduler::adaptive::adapt(&view)
            .unwrap()
            .unwrap();
        assert!(rewrite.inputs[0].is_none());
        assert_eq!(rewrite.inputs[1].as_ref().unwrap().partitions(), 1);
    }

    #[test]
    fn a_fetch_above_a_single_bucket_join_prevents_skew_splitting() {
        let output = Arc::new(
            TaskOutputStatistics::new(&ChannelCounts {
                rows: vec![1],
                physical_bytes: Some(vec![100]),
                logical_bytes: vec![100],
            })
            .unwrap(),
        );
        let committed = vec![output; 2];
        let statistics =
            Arc::new(ShuffleStatistics::fold(1, committed.iter().map(Arc::as_ref)).unwrap());
        let source = InputView {
            buckets: 1,
            statistics: Some(statistics),
            committed: Some(Arc::new(committed)),
            ..skewed_input(2)
        };
        for fetch in [None, Some(1)] {
            let plan = positive_filter(
                join(
                    input_with_buckets(0, 1),
                    input_with_buckets(1, 1),
                    PartitionMode::Partitioned,
                ),
                fetch,
            );
            let view = StageView {
                plan,
                inputs: vec![source.clone(); 2],
                options: AdaptiveOptions {
                    join_enabled: false,
                    coalesce_enabled: false,
                    skew_threshold: 1,
                    // A one-bucket input equals its median; this supported
                    // setting makes the unlimited control eligible for a split.
                    skew_factor: 0.5,
                    coalesce_target_size: 1,
                    ..AdaptiveOptions::default()
                },
                slot_cap: None,
            };
            let rewrite = crate::driver::job_scheduler::adaptive::adapt(&view).unwrap();
            assert_eq!(rewrite.is_some(), fetch.is_none(), "fetch {fetch:?}");
        }
    }

    #[tokio::test]
    async fn splitting_below_a_limited_filter_changes_join_rows() {
        fn batch(key_value: i64, value: i64) -> RecordBatch {
            RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(Int64Array::from(vec![key_value])),
                    Arc::new(Int64Array::from(vec![value])),
                ],
            )
            .unwrap()
        }

        async fn joined_rows(
            partitions: Vec<Vec<RecordBatch>>,
            probe_partitions: Vec<Vec<RecordBatch>>,
        ) -> usize {
            let left = MemorySourceConfig::try_new_exec(&partitions, schema(), None).unwrap();
            let right =
                MemorySourceConfig::try_new_exec(&probe_partitions, schema(), None).unwrap();
            let join = Arc::new(
                HashJoinExec::try_new(
                    positive_filter(left, Some(1)),
                    right,
                    vec![(key(), key())],
                    None,
                    &JoinType::Inner,
                    None,
                    PartitionMode::Partitioned,
                    NullEquality::NullEqualsNothing,
                    false,
                )
                .unwrap(),
            );
            datafusion::physical_plan::collect(join, Arc::new(TaskContext::default()))
                .await
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum()
        }

        // One bucket contains two producer batches. Splitting it into two map
        // ranges resets the embedded filter limit for each range, admitting a
        // row that the original join never saw. The other side is replicated.
        assert_eq!(
            joined_rows(
                vec![vec![batch(1, 11), batch(1, 22)]],
                vec![vec![batch(1, 99)]]
            )
            .await,
            1
        );
        assert_eq!(
            joined_rows(
                vec![vec![batch(1, 11)], vec![batch(1, 22)]],
                vec![vec![batch(1, 99)]; 2],
            )
            .await,
            2
        );

        // Coalescing has the opposite effect: four buckets each admit their own
        // matching key, whereas merging the buckets applies the limit only once.
        let build = (1..=4).map(|k| batch(k, k * 10)).collect::<Vec<_>>();
        let probe = (1..=4).map(|k| batch(k, k * 100)).collect::<Vec<_>>();
        assert_eq!(
            joined_rows(
                build.iter().map(|batch| vec![batch.clone()]).collect(),
                probe.iter().map(|batch| vec![batch.clone()]).collect(),
            )
            .await,
            4
        );
        assert_eq!(joined_rows(vec![build], vec![probe]).await, 1);
    }

    #[test]
    fn slot_cap_is_checked_before_allocating_slice_combinations() {
        let view = StageView {
            plan: input(0),
            inputs: vec![skewed_input(512), skewed_input(512)],
            options: AdaptiveOptions {
                skew_threshold: 1,
                coalesce_target_size: 1,
                ..AdaptiveOptions::default()
            },
            slot_cap: Some(8),
        };
        let join = SkewJoin {
            sides: vec![(0, true), (1, true)],
        };
        let mut notes = vec![];
        assert!(
            split_skewed_buckets(&view, &join, &[0..1, 1..2, 2..3, 3..4], &mut notes)
                .unwrap()
                .is_none()
        );
        assert_eq!(notes.len(), 1);
        assert!(notes[0].contains("262145 partitions"));
    }
}
