//! Join decisions from committed statistics.
//!
//!   - Empty-input pruning: a join or grouped aggregate whose result must be empty
//!     because of an input with exactly zero committed rows becomes `EmptyExec`.
//!   - Build-side swap: a partitioned hash join whose build side is certainly larger
//!     than its probe side swaps its inputs, keeping the partitioned mode.
//!   - Hash join to sort-merge join: a partitioned hash join whose largest build
//!     partition certainly exceeds the build cap becomes a sort-merge join with
//!     explicit sorts, since no physical optimizer runs after the job graph is built.
//!
//! "Certainly" means that the sizes come from [`input_bound`]: the build side must
//! have an exact size, and the probe side at least an upper bound. Otherwise the
//! join keeps its static strategy.

use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{JoinType, Result, Statistics, internal_datafusion_err};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode, SortMergeJoinExec,
};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use crate::driver::job_scheduler::adaptive::bounds::{Bound, input_bound};
use crate::driver::job_scheduler::adaptive::view::StageView;
use crate::plan::StageInputExec;
use crate::statistics::ShuffleStatistics;

/// The result of applying the join rules to a stage plan.
pub(crate) struct JoinRuleOutcome {
    pub plan: Arc<dyn ExecutionPlan>,
    pub rules: Vec<&'static str>,
    pub notes: Vec<String>,
}

pub(crate) const RULE_EMPTY_INPUT: &str = "empty_input";
pub(crate) const RULE_SWAP_JOIN_INPUTS: &str = "swap_join_inputs";
pub(crate) const RULE_SORT_MERGE_JOIN: &str = "sort_merge_join";

pub(crate) fn apply_join_rules(
    plan: Arc<dyn ExecutionPlan>,
    view: &StageView,
) -> Result<JoinRuleOutcome> {
    let mut rules = vec![];
    let mut notes = vec![];
    let plan = plan
        .transform_up(|node| {
            if is_prunable(&node) && output_is_empty(&node, view) {
                rules.push(RULE_EMPTY_INPUT);
                notes.push(format!("{} has an empty input", node.name()));
                let partitions = node.output_partitioning().partition_count();
                let empty = EmptyExec::new(node.schema()).with_partitions(partitions);
                return Ok(Transformed::yes(Arc::new(empty) as Arc<dyn ExecutionPlan>));
            }
            if let Some(join) = node.downcast_ref::<HashJoinExec>()
                && join.mode == PartitionMode::Partitioned
                && let Some(plan) =
                    rewrite_partitioned_hash_join(join, view, &mut rules, &mut notes)?
            {
                return Ok(Transformed::yes(plan));
            }
            Ok(Transformed::no(node))
        })?
        .data;
    rules.dedup();
    Ok(JoinRuleOutcome { plan, rules, notes })
}

/// Whether the node is an operator that the empty-input rule may replace.
fn is_prunable(node: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(aggregate) = node.downcast_ref::<AggregateExec>() {
        // A global aggregate, or a grouping set with an empty grouping (`()`),
        // returns a row even for an empty input.
        let group_by = aggregate.group_expr();
        return !group_by.expr().is_empty()
            && !group_by.groups().iter().any(|g| g.iter().all(|null| *null));
    }
    node.is::<HashJoinExec>()
        || node.is::<SortMergeJoinExec>()
        || node.is::<NestedLoopJoinExec>()
        || node.is::<CrossJoinExec>()
}

/// Whether the output of a prunable operator is empty given its inputs.
fn output_is_empty(node: &Arc<dyn ExecutionPlan>, view: &StageView) -> bool {
    let children = node.children();
    if node.is::<AggregateExec>() {
        return children.first().is_some_and(|x| is_empty(x, view));
    }
    let [left, right] = children.as_slice() else {
        return false;
    };
    let join_type = if let Some(join) = node.downcast_ref::<HashJoinExec>() {
        join.join_type
    } else if let Some(join) = node.downcast_ref::<SortMergeJoinExec>() {
        join.join_type()
    } else if let Some(join) = node.downcast_ref::<NestedLoopJoinExec>() {
        *join.join_type()
    } else if node.is::<CrossJoinExec>() {
        JoinType::Inner
    } else {
        return false;
    };
    let (l, r) = (is_empty(left, view), is_empty(right, view));
    match join_type {
        JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi => l || r,
        JoinType::Left | JoinType::LeftAnti | JoinType::LeftMark => l,
        JoinType::Right | JoinType::RightAnti | JoinType::RightMark => r,
        JoinType::Full => l && r,
    }
}

/// Follows operators that emit at most one row per input row, partition by
/// partition, so that an empty input means an empty output. This is about rows
/// only: a projection may still widen each row.
fn row_subset_base(node: &Arc<dyn ExecutionPlan>) -> &Arc<dyn ExecutionPlan> {
    let mut node = node;
    loop {
        let transparent = node.is::<ProjectionExec>()
            || node.is::<FilterExec>()
            || node.is::<CooperativeExec>()
            || node.is::<SortExec>()
            || node.is::<LocalLimitExec>()
            || node.is::<GlobalLimitExec>();
        match node.children().as_slice() {
            [child] if transparent => node = child,
            _ => return node,
        }
    }
}

/// Whether the node certainly produces no rows: it reads, through row-subset
/// operators, an input with exactly zero committed rows, or an empty plan.
fn is_empty(node: &Arc<dyn ExecutionPlan>, view: &StageView) -> bool {
    let base = row_subset_base(node);
    if base.is::<EmptyExec>() {
        return true;
    }
    base.downcast_ref::<StageInputExec<usize>>()
        .and_then(|placeholder| view.statistics(*placeholder.input()))
        .is_some_and(|statistics| statistics.total_rows == 0)
}

/// Returns the committed statistics of the stage input that bounds what a join
/// child produces, if the bound is at least as strong as `required`.
fn bounding_statistics<'a>(
    node: &Arc<dyn ExecutionPlan>,
    view: &'a StageView,
    required: Bound,
) -> Option<(usize, &'a ShuffleStatistics)> {
    let bound = input_bound(node)?;
    if required == Bound::Exact && bound.bound != Bound::Exact {
        return None;
    }
    Some((bound.input, view.statistics(bound.input)?))
}

fn rewrite_partitioned_hash_join(
    join: &HashJoinExec,
    view: &StageView,
    rules: &mut Vec<&'static str>,
    notes: &mut Vec<String>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if let (Some((_, build)), Some((_, probe))) = (
        bounding_statistics(join.left(), view, Bound::Exact),
        bounding_statistics(join.right(), view, Bound::Upper),
    ) && should_swap(join, build, probe)
    {
        notes.push(format!(
            "swap join inputs: build {} bytes, probe at most {} bytes",
            build.total_logical_bytes, probe.total_logical_bytes
        ));
        rules.push(RULE_SWAP_JOIN_INPUTS);
        return Ok(Some(join.swap_inputs(PartitionMode::Partitioned)?));
    }
    Ok(None)
}

/// Converts partitioned hash joins whose largest build partition exceeds the build
/// cap into sort-merge joins. This runs after the partition specs are decided, so
/// `build_sizes` returns the logical size of each partition of a stage input's own
/// domain, after coalescing. Only a build side whose size is exact
/// is converted; an upper bound above the cap does not show that the cap is exceeded.
pub(crate) fn apply_sort_merge_joins(
    plan: Arc<dyn ExecutionPlan>,
    view: &StageView,
    build_sizes: &dyn Fn(usize) -> Option<Vec<u64>>,
) -> Result<JoinRuleOutcome> {
    let mut rules = vec![];
    let mut notes = vec![];
    let cap = view.options.join_max_build_partition_size;
    let plan = plan
        .transform_up(|node| {
            let Some(join) = node.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            if join.mode != PartitionMode::Partitioned || !supports_sort_merge(join) {
                return Ok(Transformed::no(node));
            }
            let Some((input, _)) = bounding_statistics(join.left(), view, Bound::Exact) else {
                return Ok(Transformed::no(node));
            };
            let Some(largest) = build_sizes(input).and_then(|x| x.into_iter().max()) else {
                return Ok(Transformed::no(node));
            };
            if largest <= cap {
                return Ok(Transformed::no(node));
            }
            // A join that cannot be expressed as a sort-merge join keeps its hash join,
            // without giving up the other rules for the stage.
            match to_sort_merge_join(join) {
                Ok(smj) => {
                    notes.push(format!(
                        "sort-merge join: largest build partition {largest} bytes exceeds {cap} bytes"
                    ));
                    rules.push(RULE_SORT_MERGE_JOIN);
                    Ok(Transformed::yes(smj))
                }
                Err(error) => {
                    notes.push(format!("sort-merge join not applicable: {error}"));
                    Ok(Transformed::no(node))
                }
            }
        })?
        .data;
    rules.dedup();
    Ok(JoinRuleOutcome { plan, rules, notes })
}

/// Swaps only when the exact build size exceeds the probe's upper bound by a margin
/// that absorbs measurement differences, and only for joins whose semantics allow
/// swapping.
fn should_swap(join: &HashJoinExec, build: &ShuffleStatistics, probe: &ShuffleStatistics) -> bool {
    !join.null_aware
        && join.fetch().is_none()
        && join.join_type.supports_swap()
        && build.total_logical_bytes
            > probe
                .total_logical_bytes
                .saturating_add(probe.total_logical_bytes / 10)
}

fn supports_sort_merge(join: &HashJoinExec) -> bool {
    // Null-aware anti joins need the whole probe side, and a fetch limit has no
    // sort-merge equivalent.
    !join.null_aware && join.fetch().is_none() && !join.on().is_empty()
}

fn to_sort_merge_join(join: &HashJoinExec) -> Result<Arc<dyn ExecutionPlan>> {
    let options = SortOptions::default();
    let sort = |child: &Arc<dyn ExecutionPlan>,
                keys: Vec<Arc<dyn PhysicalExpr>>|
     -> Result<Arc<dyn ExecutionPlan>> {
        let ordering = LexOrdering::new(
            keys.into_iter()
                .map(|expr| PhysicalSortExpr::new(expr, options)),
        )
        .ok_or_else(|| internal_datafusion_err!("empty join keys"))?;
        Ok(Arc::new(
            SortExec::new(ordering, Arc::clone(child)).with_preserve_partitioning(true),
        ))
    };
    let (left_keys, right_keys): (Vec<_>, Vec<_>) = join
        .on()
        .iter()
        .map(|(l, r)| (Arc::clone(l), Arc::clone(r)))
        .unzip();
    let smj: Arc<dyn ExecutionPlan> = Arc::new(SortMergeJoinExec::try_new(
        sort(join.left(), left_keys)?,
        sort(join.right(), right_keys)?,
        join.on().to_vec(),
        join.filter().cloned(),
        join.join_type,
        vec![options; join.on().len()],
        join.null_equality(),
    )?);
    match &join.projection {
        None => Ok(smj),
        Some(projection) => {
            let schema = smj.schema();
            let exprs = projection
                .iter()
                .map(|&i| {
                    let name = schema.field(i).name().to_string();
                    (
                        Arc::new(Column::new(&name, i)) as Arc<dyn PhysicalExpr>,
                        name,
                    )
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(ProjectionExec::try_new(exprs, smj)?))
        }
    }
}

/// Converts committed statistics into DataFusion statistics for a stage input.
pub(crate) fn exact_statistics(
    statistics: &ShuffleStatistics,
    schema: &datafusion::arrow::datatypes::Schema,
) -> Statistics {
    use datafusion::common::stats::Precision;
    Statistics {
        num_rows: usize::try_from(statistics.total_rows)
            .map(Precision::Exact)
            .unwrap_or(Precision::Absent),
        total_byte_size: usize::try_from(statistics.total_logical_bytes)
            .map(Precision::Exact)
            .unwrap_or(Precision::Absent),
        column_statistics: Statistics::unknown_column(schema),
    }
}
