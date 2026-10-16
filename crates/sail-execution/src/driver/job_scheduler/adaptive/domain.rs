//! Partition domains of the inputs of a stage.
//!
//! Each stage input has its own domain: the partitions of the operator that consumes
//! it. Inputs that an operator pairs partition by partition must keep identical
//! bucket boundaries, so they form one alignment group. The grouping follows Spark's
//! `CoalesceShufflePartitions`: children of broadcast joins and plain unions are
//! grouped independently.
//!
//! Each group also records the build side of every partitioned hash join in it,
//! since coalescing the group enlarges the hash table that each task builds.

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode, PiecewiseMergeJoinExec,
    SortMergeJoinExec, SymmetricHashJoinExec,
};
use datafusion::physical_plan::union::{InterleaveExec, UnionExec};

use crate::driver::job_scheduler::adaptive::bounds::input_bound;
use crate::plan::StageInputExec;

/// An alignment group of stage inputs.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct AlignmentGroup {
    /// The stage inputs in the group, by placeholder index.
    pub inputs: BTreeSet<usize>,
    /// The build side of each partitioned hash join over the group: the stage
    /// input whose committed logical bytes bound the build side in each partition,
    /// or `None` if no input bounds it (e.g. the build side is itself a join).
    pub builds: Vec<Option<usize>>,
    /// Whether the group contains a leaf that is not a stage input (e.g. a scan),
    /// whose partitioning cannot be rewritten.
    pub fixed: bool,
}

/// Returns the alignment groups of a stage plan, or the reason why the stage
/// must not be rewritten.
pub(crate) fn alignment_groups(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Vec<AlignmentGroup>, String> {
    let mut groups = vec![AlignmentGroup::default()];
    walk(plan, Some(0), false, &mut groups)?;
    Ok(groups
        .into_iter()
        .filter(|group| !group.inputs.is_empty() || group.fixed)
        .collect())
}

/// Walks the plan from the root.
///   - `group` is the group of the current domain, or `None` on a broadcast side,
///     whose partitions are read in full by every task.
///   - `aligned` is whether the current domain is paired with another domain by an
///     ancestor, in which case the domain's partition count must not change on its own.
fn walk(
    node: &Arc<dyn ExecutionPlan>,
    group: Option<usize>,
    aligned: bool,
    groups: &mut Vec<AlignmentGroup>,
) -> Result<(), String> {
    if let Some(placeholder) = node.downcast_ref::<StageInputExec<usize>>() {
        if let Some(g) = group {
            groups[g].inputs.insert(*placeholder.input());
        }
        return Ok(());
    }
    if node.is::<InterleaveExec>() {
        // Its constructor demands hash or range partitioned inputs.
        return Err("the stage contains InterleaveExec".to_string());
    }
    if let Some(join) = node.downcast_ref::<HashJoinExec>() {
        return match join.mode {
            PartitionMode::Partitioned => {
                if let Some(g) = group {
                    groups[g]
                        .builds
                        .push(input_bound(join.left()).map(|x| x.input));
                }
                walk(join.left(), group, true, groups)?;
                walk(join.right(), group, true, groups)
            }
            PartitionMode::CollectLeft => {
                walk(join.left(), None, false, groups)?;
                walk(join.right(), group, aligned, groups)
            }
            PartitionMode::Auto => Err("the stage has a hash join in auto mode".to_string()),
        };
    }
    if let Some(join) = node.downcast_ref::<SortMergeJoinExec>() {
        walk(join.left(), group, true, groups)?;
        return walk(join.right(), group, true, groups);
    }
    if node.is::<SymmetricHashJoinExec>() {
        let children = node.children();
        for child in children {
            walk(child, group, true, groups)?;
        }
        return Ok(());
    }
    if node.is::<NestedLoopJoinExec>()
        || node.is::<CrossJoinExec>()
        || node.is::<PiecewiseMergeJoinExec>()
    {
        let children = node.children();
        let [left, right] = children.as_slice() else {
            return Err(format!("unexpected children of {}", node.name()));
        };
        walk(left, None, false, groups)?;
        return walk(right, group, aligned, groups);
    }
    if node.is::<UnionExec>() {
        if aligned {
            return Err("a union is paired with another input".to_string());
        }
        for child in node.children() {
            let child_group = match group {
                Some(_) => {
                    groups.push(AlignmentGroup::default());
                    Some(groups.len() - 1)
                }
                None => None,
            };
            walk(child, child_group, false, groups)?;
        }
        return Ok(());
    }
    match node.children().as_slice() {
        [] => {
            if let Some(g) = group {
                groups[g].fixed = true;
            }
            Ok(())
        }
        [child] => walk(child, group, aligned, groups),
        _ => Err(format!(
            "the stage contains {} with multiple inputs",
            node.name()
        )),
    }
}
