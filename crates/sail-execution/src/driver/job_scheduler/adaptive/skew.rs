//! Skew splitting for joins over Celeborn shuffles.
//!
//! A skewed bucket of a join input is split by producer partition: each slice is read
//! with a Celeborn map-range read, and is paired with the whole bucket of the other
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
use datafusion::physical_expr::Distribution;
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
        let safe = node.is::<ProjectionExec>()
            || node.is::<FilterExec>()
            || node.is::<CooperativeExec>()
            || node
                .downcast_ref::<SortExec>()
                .is_some_and(|sort| sort.fetch().is_none());
        match node.children().as_slice() {
            [child] if safe => node = child,
            _ => return None,
        }
    }
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
    committed: Arc<Vec<Arc<crate::statistics::CompactOutputStatistics>>>,
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
            .map(|statistics| statistics.physical_bytes(bucket).unwrap_or(0))
            .collect::<Vec<_>>();
        if sizes.len() != self.producers || sizes.len() < 2 {
            return None;
        }
        let ranges = ranges_from_starts(&split_by_target_size(&sizes, self.target), sizes.len());
        (ranges.len() > 1).then_some(ranges)
    }
}
