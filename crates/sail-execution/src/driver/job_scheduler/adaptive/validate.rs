//! Validation of an adaptive rewrite before it is committed.
//!
//! DataFusion checks join partition counts only at execution, so construction and
//! serialization cannot catch a broken rewrite. Every check here is necessary for
//! the rewritten job to produce the same rows as the static plan.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{OrderingRequirements, Partitioning};
use datafusion::physical_plan::joins::{
    HashJoinExec, PartitionMode, SortMergeJoinExec, SymmetricHashJoinExec,
};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::Message;

use crate::driver::job_scheduler::topology::JobTopology;
use crate::job_graph::{AdaptiveProperty, BucketLayout, InputMode, JobGraph, TaskPlacement};
use crate::plan::StageInputExec;
use crate::proto::{encode_remote_physical_plan, proto_to_physical_plan};

pub(crate) struct ValidationContext<'a> {
    pub old: &'a JobGraph,
    pub new: &'a JobGraph,
    pub topology: &'a JobTopology,
    /// The stages whose plans changed.
    pub changed: &'a [usize],
    /// The re-planned stage.
    pub stage: usize,
    /// Groups of inputs of the re-planned stage that must share bucket boundaries.
    pub groups: &'a [Vec<usize>],
    pub slot_cap: Option<usize>,
    pub codec: &'a dyn PhysicalExtensionCodec,
    pub context: &'a TaskContext,
}

pub(crate) fn validate(ctx: &ValidationContext) -> Result<(), String> {
    for &s in ctx.changed {
        let old = &ctx.old.stages()[s];
        let new = &ctx.new.stages()[s];
        if old.plan.schema().fields() != new.plan.schema().fields() {
            return Err(format!("stage {s} output schema changed"));
        }
        validate_inputs(ctx, s)?;
        validate_plan(&new.plan).map_err(|e| format!("stage {s}: {e}"))?;
        validate_codec(ctx, s)?;
    }
    validate_groups(ctx)?;
    validate_consumers(ctx)?;
    validate_schedulable(ctx)?;
    Ok(())
}

/// Each input's spec covers every bucket of its producer exactly once, in order,
/// and its length equals the partition count the placeholder declares.
fn validate_inputs(ctx: &ValidationContext, s: usize) -> Result<(), String> {
    let stage = &ctx.new.stages()[s];
    let declared = placeholder_partitions(&stage.plan);
    for (i, input) in stage.inputs.iter().enumerate() {
        let Some(adaptive) = &input.adaptive else {
            continue;
        };
        if !matches!(input.mode, InputMode::Shuffle) {
            return Err(format!(
                "stage {s} input {i} is adapted but not a shuffle input"
            ));
        }
        if input.layout != BucketLayout::Direct {
            return Err(format!(
                "stage {s} input {i} does not have a direct bucket layout"
            ));
        }
        let buckets = ctx.new.stages()[input.stage].distribution.channels();
        let mut next = 0;
        for slices in &adaptive.spec {
            for slice in slices {
                if slice.buckets.start != next || slice.buckets.is_empty() {
                    return Err(format!(
                        "stage {s} input {i} does not read buckets contiguously: {adaptive}"
                    ));
                }
                next = slice.buckets.end;
            }
        }
        if next != buckets {
            return Err(format!(
                "stage {s} input {i} reads {next} of {buckets} buckets"
            ));
        }
        let AdaptiveProperty::CoalescedHash { from, boundaries } = &adaptive.property;
        {
            let expected = adaptive
                .spec
                .iter()
                .filter_map(|slices| slices.first().map(|slice| slice.buckets.start))
                .chain([buckets])
                .collect::<Vec<_>>();
            if *from != buckets || *boundaries != expected {
                return Err(format!("stage {s} input {i} has inconsistent boundaries"));
            }
        }
        // An input whose placeholder was pruned from the plan is never read.
        if let Some(&partitions) = declared.get(&i) {
            if partitions != adaptive.partitions() {
                return Err(format!(
                    "stage {s} input {i} declares {partitions} partitions but its spec has {}",
                    adaptive.partitions()
                ));
            }
            if !declares_unknown_partitioning(&stage.plan, i) {
                return Err(format!(
                    "stage {s} input {i} declares a partitioning that the spec does not provide"
                ));
            }
        }
    }
    Ok(())
}

/// Join partition modes and ordering requirements hold for every operator.
pub(crate) fn validate_plan(plan: &Arc<dyn ExecutionPlan>) -> Result<(), String> {
    let mut error = None;
    let _ = plan.apply(|node| {
        if let Err(e) = validate_node(node) {
            error = Some(e);
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    match error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

fn validate_node(node: &Arc<dyn ExecutionPlan>) -> Result<(), String> {
    let count = |plan: &Arc<dyn ExecutionPlan>| plan.output_partitioning().partition_count();
    if let Some(join) = node.downcast_ref::<HashJoinExec>() {
        match join.mode {
            PartitionMode::Partitioned if count(join.left()) != count(join.right()) => {
                return Err(format!(
                    "partitioned hash join has {} build and {} probe partitions",
                    count(join.left()),
                    count(join.right())
                ));
            }
            PartitionMode::CollectLeft if count(join.left()) != 1 => {
                return Err("collect-left hash join has a multi-partition build side".to_string());
            }
            _ => {}
        }
    }
    if let Some(join) = node.downcast_ref::<SortMergeJoinExec>()
        && count(join.left()) != count(join.right())
    {
        return Err("sort-merge join has unequal input partitions".to_string());
    }
    if let Some(join) = node.downcast_ref::<SymmetricHashJoinExec>()
        && count(join.left()) != count(join.right())
    {
        return Err("symmetric hash join has unequal input partitions".to_string());
    }
    let children = node.children();
    for (child, requirement) in children.iter().zip(node.required_input_ordering()) {
        let Some(requirement) = requirement else {
            continue;
        };
        let alternatives = match requirement {
            OrderingRequirements::Hard(x) => x,
            // A soft requirement is an optimization, not a correctness condition.
            OrderingRequirements::Soft(_) => continue,
        };
        let properties = child.equivalence_properties();
        let satisfied = alternatives.into_iter().any(|requirement| {
            properties
                .ordering_satisfy_requirement(requirement)
                .unwrap_or(false)
        });
        if !satisfied {
            return Err(format!("{} input ordering is not satisfied", node.name()));
        }
    }
    Ok(())
}

/// The changed plan survives a round trip through the remote codec, which runs the
/// checked constructors that workers use.
fn validate_codec(ctx: &ValidationContext, s: usize) -> Result<(), String> {
    let plan = &ctx.new.stages()[s].plan;
    let encoded = encode_remote_physical_plan(ctx.codec, plan.clone())
        .map_err(|e| format!("stage {s} cannot be encoded: {e}"))?;
    let proto = PhysicalPlanNode::decode(encoded.as_slice())
        .map_err(|e| format!("stage {s} cannot be decoded: {e}"))?;
    let decoded = proto_to_physical_plan(ctx.context, ctx.codec, &proto)
        .map_err(|e| format!("stage {s} cannot be decoded: {e}"))?;
    if decoded.schema().fields() != plan.schema().fields() {
        return Err(format!("stage {s} schema changed in the codec round trip"));
    }
    if decoded.output_partitioning().partition_count()
        != plan.output_partitioning().partition_count()
    {
        return Err(format!(
            "stage {s} partition count changed in the codec round trip"
        ));
    }
    validate_plan(&decoded).map_err(|e| format!("stage {s} after the codec round trip: {e}"))
}

/// Inputs that an operator pairs partition by partition share bucket boundaries.
fn validate_groups(ctx: &ValidationContext) -> Result<(), String> {
    let stage = &ctx.new.stages()[ctx.stage];
    for group in ctx.groups {
        let mut specs = group
            .iter()
            .filter_map(|&i| stage.inputs.get(i))
            .map(|input| input.adaptive.as_ref().map(|x| &x.spec));
        if let Some(first) = specs.next()
            && specs.any(|spec| spec != first)
        {
            return Err(format!(
                "stage {} inputs {group:?} have different bucket boundaries",
                ctx.stage
            ));
        }
    }
    Ok(())
}

/// Every consumer that embeds the producer's partition count was updated with it,
/// and no consumer depends on the producer's partition count partition by partition.
fn validate_consumers(ctx: &ValidationContext) -> Result<(), String> {
    let s = ctx.stage;
    let partitions = ctx.new.stages()[s]
        .plan
        .output_partitioning()
        .partition_count();
    for &c in &ctx.topology.stages[s].consumers {
        let consumer = &ctx.new.stages()[c];
        let declared = placeholder_partitions(&consumer.plan);
        for (i, input) in consumer.inputs.iter().enumerate() {
            if input.stage != s {
                continue;
            }
            match input.mode {
                InputMode::Forward => {
                    return Err(format!("stage {c} reads stage {s} by forward input"));
                }
                InputMode::Merge => {
                    if declared.get(&i).is_some_and(|&p| p != partitions) {
                        return Err(format!("stage {c} merge input {i} is stale for stage {s}"));
                    }
                }
                InputMode::Shuffle | InputMode::Broadcast | InputMode::Rescale { .. } => {}
            }
        }
    }
    Ok(())
}

/// No region of the rebuilt topology needs more worker slots than the cluster has.
fn validate_schedulable(ctx: &ValidationContext) -> Result<(), String> {
    let Some(cap) = ctx.slot_cap else {
        return Ok(());
    };
    for (r, region) in ctx.topology.regions.iter().enumerate() {
        let slots = region_slots(ctx.new, &region.tasks);
        if slots > cap {
            return Err(format!(
                "region {r} needs {slots} slots, more than the limit of {cap}"
            ));
        }
    }
    Ok(())
}

/// Returns the worker slots that a region needs: stages in one slot sharing group
/// share slots, so the group needs as many slots as its widest stage in the region.
pub(crate) fn region_slots(
    graph: &JobGraph,
    tasks: &[crate::driver::job_scheduler::topology::TaskTopology],
) -> usize {
    let mut groups: HashMap<&str, HashMap<usize, usize>> = HashMap::new();
    for task in tasks {
        let stage = &graph.stages()[task.stage];
        if stage.placement != TaskPlacement::Worker {
            continue;
        }
        *groups
            .entry(stage.group.as_str())
            .or_default()
            .entry(task.stage)
            .or_default() += 1;
    }
    groups
        .values()
        .map(|stages| stages.values().copied().max().unwrap_or(0))
        .sum()
}

/// Returns the declared partition count of each stage input placeholder in a plan.
fn placeholder_partitions(plan: &Arc<dyn ExecutionPlan>) -> HashMap<usize, usize> {
    let mut result = HashMap::new();
    let _ = plan.apply(|node| {
        if let Some(placeholder) = node.downcast_ref::<StageInputExec<usize>>() {
            result.insert(
                *placeholder.input(),
                placeholder
                    .properties()
                    .output_partitioning()
                    .partition_count(),
            );
        }
        Ok(TreeNodeRecursion::Continue)
    });
    result
}

/// Whether every placeholder of the input declares unknown partitioning, so that no
/// DataFusion operator relies on a hash or range partitioning that a coalesced or
/// split input does not provide.
fn declares_unknown_partitioning(plan: &Arc<dyn ExecutionPlan>, input: usize) -> bool {
    let mut result = true;
    let _ = plan.apply(|node| {
        if let Some(placeholder) = node.downcast_ref::<StageInputExec<usize>>()
            && *placeholder.input() == input
            && !matches!(
                placeholder.properties().output_partitioning(),
                Partitioning::UnknownPartitioning(_)
            )
        {
            result = false;
        }
        Ok(TreeNodeRecursion::Continue)
    });
    result
}
