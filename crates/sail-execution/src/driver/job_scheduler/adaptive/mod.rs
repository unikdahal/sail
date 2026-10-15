//! Adaptive query execution.
//!
//! A stage is re-planned just before it first runs, at blocking region boundaries,
//! from the committed statistics of its producers. The pipeline for each ready stage:
//!
//!   1. **Snapshot**: build an immutable [`StageView`] of the stage plan, its inputs
//!      and their committed statistics.
//!   2. **Rules**: pure functions of the view propose a rewrite or nothing.
//!   3. **Validate**: domains, coverage, join modes, consumers, slot cap and a codec
//!      round trip.
//!   4. **Commit**: the stage, every consumer placeholder that copies its partition
//!      count and the rebuilt topology are swapped in together, or not at all.
//!
//! Any failure leaves the stage as statically planned. Adaptive execution never
//! fails a job by itself.

mod coalesce;
mod domain;
mod joins;
mod validate;
mod view;

use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, PlanProperties};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use indexmap::IndexSet;
use log::{debug, info};
use sail_system_store::SystemEvent;
use sail_telemetry::events::SystemEventReporter;

use crate::driver::job_scheduler::adaptive::coalesce::{
    CoalesceParams, coalesce, within_build_cap,
};
use crate::driver::job_scheduler::adaptive::domain::{AlignmentGroup, alignment_groups};
use crate::driver::job_scheduler::adaptive::joins::{apply_join_rules, exact_statistics};
use crate::driver::job_scheduler::adaptive::validate::{ValidationContext, validate};
pub(crate) use crate::driver::job_scheduler::adaptive::view::{InputView, StageRewrite, StageView};
use crate::driver::job_scheduler::state::{
    AdaptiveDecision, JobDescriptor, TaskDescriptor, TaskRegionDescriptor, TaskRegionState,
};
use crate::driver::job_scheduler::topology::{JobTopology, TaskTopology};
use crate::driver::job_scheduler::{AdaptiveOptions, JobScheduler, JobSchedulerOptions};
use crate::id::JobId;
use crate::job_graph::{
    AdaptiveInput, AdaptiveProperty, BucketLayout, BucketSlice, InputMode, OutputDistribution,
    OutputMode, ShuffleOrigin, StageKind, TaskPlacement,
};
use crate::plan::StageInputExec;
use crate::shuffle::ShuffleBackendKind;

pub(crate) const RULE_COALESCE: &str = "coalesce";

/// Whether a stage can be decided now.
enum Readiness {
    /// Some dependency of the stage's regions has not succeeded yet.
    Waiting,
    /// No region of the stage has an attempt, and all dependencies have succeeded.
    Ready,
    /// A region of the stage already has an attempt, so the stage runs as planned.
    Started,
}

impl JobScheduler {
    /// Makes the adaptive decision for every stage that became ready.
    /// Returns whether any stage was re-planned.
    pub(super) fn adapt_ready_stages(
        job_id: JobId,
        job: &mut JobDescriptor,
        options: &JobSchedulerOptions,
        codec: &dyn PhysicalExtensionCodec,
        event_reporter: &SystemEventReporter,
        session_id: &str,
    ) -> bool {
        let started = Instant::now();
        let mut replanned = false;
        // Only blocking shuffle backends have materialized outputs whose committed
        // sizes are known before the consumer starts. Storage merge stages share a
        // region with the next shuffle's merge stage, so they are not adapted yet.
        let supported = matches!(
            job.graph.shuffle_backend(),
            ShuffleBackendKind::Celeborn { .. }
        );
        for s in 0..job.stages.len() {
            if job.stages[s].adaptive.is_frozen() {
                continue;
            }
            if !supported {
                job.stages[s].adaptive = AdaptiveDecision::Static;
                continue;
            }
            match Self::stage_readiness(job, s) {
                Readiness::Waiting => continue,
                Readiness::Started => {
                    job.stages[s].adaptive = AdaptiveDecision::Static;
                    continue;
                }
                Readiness::Ready => {}
            }
            job.stages[s].adaptive = match Self::replan_stage(job_id, job, s, options, codec) {
                Ok(Some(applied)) => {
                    info!(
                        "job {job_id} stage {s} re-planned by [{}]: {} -> {} partitions; {}",
                        applied.rules.join(", "),
                        applied.old_partitions,
                        applied.new_partitions,
                        applied.summary
                    );
                    event_reporter.report(SystemEvent::StageReplanned {
                        session_id: session_id.to_string(),
                        job_id: u64::from(job_id),
                        stage: s as u64,
                        old_partitions: applied.old_partitions as u64,
                        new_partitions: applied.new_partitions as u64,
                        rules: applied.rules.iter().map(|x| x.to_string()).collect(),
                        updated_consumers: applied.consumers.iter().map(|&c| c as u64).collect(),
                        summary: applied.summary,
                        replanned_at: Utc::now(),
                    });
                    job.adaptive.replanned_stages += 1;
                    replanned = true;
                    AdaptiveDecision::Replanned
                }
                Ok(None) => {
                    debug!("job {job_id} stage {s} runs as planned: no adaptive rule applies");
                    AdaptiveDecision::Static
                }
                Err(reason) => {
                    info!("job {job_id} stage {s} runs as planned: {reason}");
                    AdaptiveDecision::Static
                }
            };
        }
        job.adaptive.replanning_time += started.elapsed();
        replanned
    }

    /// A stage is ready when none of its regions has an attempt and all their
    /// dependencies have succeeded. Under Celeborn a stage between blocking shuffles
    /// is sliced into one region per partition, and these regions become ready
    /// together, so one decision covers all of them.
    fn stage_readiness(job: &JobDescriptor, stage: usize) -> Readiness {
        let tasks = &job.stages[stage].tasks;
        if tasks.iter().any(|task| !task.attempts.is_empty()) {
            return Readiness::Started;
        }
        let regions = (0..tasks.len())
            .filter_map(|partition| {
                job.topology
                    .task_regions
                    .get(&TaskTopology { stage, partition })
                    .copied()
            })
            .collect::<IndexSet<_>>();
        for &r in &regions {
            let region = &job.topology.regions[r];
            if region
                .tasks
                .iter()
                .any(|t| !job.stages[t.stage].tasks[t.partition].attempts.is_empty())
            {
                return Readiness::Started;
            }
            if !region
                .dependencies
                .iter()
                .all(|d| matches!(job.regions[*d].state, TaskRegionState::Succeeded))
            {
                return Readiness::Waiting;
            }
        }
        Readiness::Ready
    }

    fn replan_stage(
        job_id: JobId,
        job: &mut JobDescriptor,
        s: usize,
        options: &JobSchedulerOptions,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Option<AppliedRewrite>, String> {
        let view = Self::stage_view(job, s, &options.adaptive)?;
        let Some(rewrite) = adapt(&view)? else {
            return Ok(None);
        };
        debug!(
            "job {job_id} stage {s} adaptive rewrite: {}",
            rewrite.notes.join("; ")
        );
        Self::commit_rewrite(job, s, rewrite, options, codec).map(Some)
    }

    /// Builds the snapshot of a stage, or returns why the stage is not eligible.
    fn stage_view(
        job: &JobDescriptor,
        s: usize,
        options: &AdaptiveOptions,
    ) -> Result<StageView, String> {
        let stage = &job.graph.stages()[s];
        if stage.placement != TaskPlacement::Worker || stage.kind != StageKind::Compute {
            return Err("the stage is not a worker compute stage".to_string());
        }
        // A consumer that reads this stage partition by partition depends on its count.
        for &c in &job.topology.stages[s].consumers {
            for input in &job.graph.stages()[c].inputs {
                if input.stage == s && matches!(input.mode, InputMode::Forward) {
                    return Err(format!("stage {c} reads the stage by forward input"));
                }
            }
        }
        if !stage
            .inputs
            .iter()
            .any(|input| matches!(input.mode, InputMode::Shuffle))
        {
            return Err("the stage has no shuffle input".to_string());
        }
        let mut inputs = vec![];
        for (i, input) in stage.inputs.iter().enumerate() {
            let producer = &job.graph.stages()[input.stage];
            // Only blocking producers have committed outputs whose sizes are final.
            if !matches!(producer.mode, OutputMode::Blocking) {
                return Err(format!("input {i} is not produced by a blocking stage"));
            }
            if !matches!(input.mode, InputMode::Shuffle | InputMode::Broadcast) {
                return Err(format!(
                    "input {i} is a partition-wise {} input",
                    input.mode
                ));
            }
            let statistics = Self::committed_statistics(job, input.stage).map(Arc::new);
            if statistics.is_none() && matches!(input.mode, InputMode::Shuffle) {
                return Err(format!(
                    "input {i} has no committed statistics from stage {}",
                    input.stage
                ));
            }
            inputs.push(InputView {
                mode: input.mode,
                origin: input.origin,
                layout: input.layout,
                hash_partitioned: matches!(producer.distribution, OutputDistribution::Hash { .. }),
                buckets: producer.distribution.channels(),
                statistics,
            });
        }
        Ok(StageView {
            plan: stage.plan.clone(),
            inputs,
            options: options.clone(),
        })
    }

    /// Validates the rewrite and commits it atomically.
    fn commit_rewrite(
        job: &mut JobDescriptor,
        s: usize,
        rewrite: StageRewrite,
        options: &JobSchedulerOptions,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<AppliedRewrite, String> {
        let old_partitions = job.graph.stages()[s]
            .plan
            .output_partitioning()
            .partition_count();
        let mut stages = job.graph.stages().to_vec();
        stages[s].plan = rewrite.plan.clone();
        for (input, adaptive) in stages[s].inputs.iter_mut().zip(&rewrite.inputs) {
            input.adaptive = adaptive.clone();
        }
        let new_partitions = stages[s].plan.output_partitioning().partition_count();
        let properties = stages[s].plan.properties().clone();

        // A merge consumer copies the producer's plan properties into its placeholder.
        let mut changed = vec![s];
        let mut consumers = vec![];
        for &c in &job.topology.stages[s].consumers {
            let merge_inputs = stages[c]
                .inputs
                .iter()
                .enumerate()
                .filter(|(_, input)| input.stage == s && matches!(input.mode, InputMode::Merge))
                .map(|(i, _)| i)
                .collect::<Vec<_>>();
            if merge_inputs.is_empty() {
                continue;
            }
            stages[c].plan =
                replace_placeholder_properties(stages[c].plan.clone(), &merge_inputs, &properties)
                    .map_err(|e| format!("failed to update stage {c}: {e}"))?;
            changed.push(c);
            consumers.push(c);
        }

        let graph = job.graph.with_stages(stages);
        let topology = JobTopology::try_new(&graph).map_err(|e| e.to_string())?;
        // A stage that has started cannot change its partition count.
        for &c in &changed {
            let partitions = graph.stages()[c]
                .plan
                .output_partitioning()
                .partition_count();
            if partitions != job.stages[c].tasks.len()
                && job.stages[c].tasks.iter().any(|t| !t.attempts.is_empty())
            {
                return Err(format!("stage {c} has started and cannot be re-planned"));
            }
        }
        let context: Arc<TaskContext> = job.context.clone();
        validate(&ValidationContext {
            old: &job.graph,
            new: &graph,
            topology: &topology,
            changed: &changed,
            stage: s,
            groups: &rewrite.groups,
            slot_cap: options.slot_cap,
            codec,
            context: &context,
        })?;

        // Commit: everything below is infallible.
        for &c in &changed {
            let partitions = graph.stages()[c]
                .plan
                .output_partitioning()
                .partition_count();
            job.stages[c]
                .tasks
                .resize_with(partitions, || TaskDescriptor { attempts: vec![] });
        }
        job.regions = (0..topology.regions.len())
            .map(|_| TaskRegionDescriptor {
                state: TaskRegionState::Running,
            })
            .collect();
        job.graph = graph;
        job.topology = topology;
        // Region states are derived from task attempts, so they are recomputed for the
        // new topology right away. Later stages in the same pass see succeeded
        // dependencies as succeeded.
        Self::update_task_regions(job, options);
        Ok(AppliedRewrite {
            old_partitions,
            new_partitions,
            rules: rewrite.rules,
            consumers,
            summary: rewrite.notes.join("; "),
        })
    }
}

/// A committed rewrite, for logging and events.
struct AppliedRewrite {
    old_partitions: usize,
    new_partitions: usize,
    rules: Vec<&'static str>,
    consumers: Vec<usize>,
    summary: String,
}

/// Runs the adaptive rules over a stage view.
/// Returns a rewrite proposal, nothing if no rule applies, or why the stage is skipped.
pub(crate) fn adapt(view: &StageView) -> Result<Option<StageRewrite>, String> {
    let mut rules = vec![];
    let mut notes = vec![];
    let mut plan = view.plan.clone();
    if view.options.join_enabled {
        let outcome = apply_join_rules(plan, view).map_err(|e| e.to_string())?;
        plan = outcome.plan;
        rules.extend(outcome.rules);
        notes.extend(outcome.notes);
    }

    let groups = alignment_groups(&plan)?;
    let mut inputs: Vec<Option<Arc<AdaptiveInput>>> = vec![None; view.inputs.len()];
    if view.options.coalesce_enabled {
        let mut coalesced = false;
        for group in &groups {
            match coalesce_group(view, group) {
                Ok(Some((buckets, ranges))) => {
                    let boundaries = ranges
                        .iter()
                        .map(|r| r.start)
                        .chain([buckets])
                        .collect::<Vec<_>>();
                    let adaptive = Arc::new(AdaptiveInput {
                        spec: ranges
                            .iter()
                            .map(|r| vec![BucketSlice::whole(r.clone())])
                            .collect(),
                        property: AdaptiveProperty::CoalescedHash {
                            from: buckets,
                            boundaries,
                        },
                    });
                    notes.push(format!(
                        "coalesce inputs {:?}: {buckets} -> {} partitions",
                        group.inputs,
                        ranges.len()
                    ));
                    for &i in &group.inputs {
                        inputs[i] = Some(adaptive.clone());
                    }
                    coalesced = true;
                }
                Ok(None) => {}
                Err(reason) => {
                    notes.push(format!("inputs {:?} not coalesced: {reason}", group.inputs))
                }
            }
        }
        if coalesced {
            rules.push(RULE_COALESCE);
        }
    }
    if rules.is_empty() {
        return Ok(None);
    }
    let plan = rewrite_placeholders(plan, view, &inputs).map_err(|e| e.to_string())?;
    Ok(Some(StageRewrite {
        plan,
        inputs,
        groups: groups
            .iter()
            .map(|group| group.inputs.iter().copied().collect())
            .collect(),
        rules,
        notes,
    }))
}

/// Coalesces one alignment group, returning the original bucket count and the new
/// bucket ranges, nothing if coalescing does not reduce the partition count, or why
/// the group is not eligible.
/// The original bucket count of a group and the coalesced bucket ranges.
type CoalescedGroup = (usize, Vec<std::ops::Range<usize>>);

fn coalesce_group(
    view: &StageView,
    group: &AlignmentGroup,
) -> Result<Option<CoalescedGroup>, String> {
    if group.fixed {
        return Err("the group reads a source that is not a stage input".to_string());
    }
    let mut buckets = None;
    let mut sizes: Vec<u64> = vec![];
    let mut build_sizes: Option<Vec<u64>> = None;
    for &i in &group.inputs {
        let input = view
            .inputs
            .get(i)
            .ok_or_else(|| format!("input {i} not found"))?;
        if !matches!(input.mode, InputMode::Shuffle) || input.layout != BucketLayout::Direct {
            return Err(format!("input {i} is not addressable by bucket"));
        }
        if input.origin != ShuffleOrigin::Engine {
            return Err(format!("input {i} has a user-chosen partition count"));
        }
        if !input.hash_partitioned {
            return Err(format!("input {i} is not hash partitioned"));
        }
        if *buckets.get_or_insert(input.buckets) != input.buckets {
            return Err("inputs have different bucket counts".to_string());
        }
        let statistics = input
            .statistics
            .as_deref()
            .ok_or_else(|| format!("input {i} has no statistics"))?;
        // Unknown sizes are never treated as zero.
        let physical = statistics
            .physical_bytes
            .as_ref()
            .ok_or_else(|| format!("input {i} has unknown physical sizes"))?;
        if physical.len() != input.buckets {
            return Err(format!("input {i} statistics do not match its buckets"));
        }
        if sizes.is_empty() {
            sizes = vec![0; input.buckets];
        }
        for (total, value) in sizes.iter_mut().zip(physical) {
            *total = total.saturating_add(*value);
        }
        if group.build_inputs.contains(&i) {
            let build = build_sizes.get_or_insert_with(|| vec![0; input.buckets]);
            for (total, value) in build.iter_mut().zip(&statistics.logical_bytes) {
                *total = total.saturating_add(*value);
            }
        }
    }
    let Some(buckets) = buckets else {
        return Ok(None);
    };
    let options = &view.options;
    let params = CoalesceParams {
        target_size: options.coalesce_target_size,
        min_partition_size: options.coalesce_min_partition_size,
        min_partitions: options.coalesce_min_partitions,
        build_cap: build_sizes
            .as_ref()
            .map(|_| options.join_max_build_partition_size),
    };
    let ranges = coalesce(&sizes, build_sizes.as_deref(), &params);
    if let Some(build) = &build_sizes
        && !within_build_cap(&ranges, build, options.join_max_build_partition_size)
    {
        return Err("a coalesced build partition would exceed the build cap".to_string());
    }
    if ranges.len() >= buckets {
        return Ok(None);
    }
    Ok(Some((buckets, ranges)))
}

/// Rewrites the stage input placeholders: an adapted input declares
/// `UnknownPartitioning` with its new partition count, and every input with
/// committed statistics exposes them. Ancestors are rebuilt, so their properties
/// follow from their inputs.
fn rewrite_placeholders(
    plan: Arc<dyn ExecutionPlan>,
    view: &StageView,
    inputs: &[Option<Arc<AdaptiveInput>>],
) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    Ok(plan
        .transform_up(|node| {
            let Some(placeholder) = node.downcast_ref::<StageInputExec<usize>>() else {
                return Ok(Transformed::no(node));
            };
            let i = *placeholder.input();
            let mut properties = placeholder.properties().clone();
            if let Some(Some(adaptive)) = inputs.get(i) {
                properties =
                    Arc::new(properties.as_ref().clone().with_partitioning(
                        Partitioning::UnknownPartitioning(adaptive.partitions()),
                    ));
            }
            let mut rewritten = StageInputExec::new(i, properties);
            if let Some(statistics) = view.statistics(i) {
                rewritten = rewritten
                    .with_statistics(Arc::new(exact_statistics(statistics, &node.schema())));
            }
            Ok(Transformed::yes(
                Arc::new(rewritten) as Arc<dyn ExecutionPlan>
            ))
        })?
        .data)
}

/// Replaces the properties of the given placeholders and rebuilds their ancestors.
fn replace_placeholder_properties(
    plan: Arc<dyn ExecutionPlan>,
    inputs: &[usize],
    properties: &Arc<PlanProperties>,
) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    let targets = inputs
        .iter()
        .copied()
        .collect::<std::collections::HashSet<_>>();
    Ok(plan
        .transform_up(|node| match node.downcast_ref::<StageInputExec<usize>>() {
            Some(placeholder) if targets.contains(placeholder.input()) => Ok(Transformed::yes(
                Arc::new(StageInputExec::new(
                    *placeholder.input(),
                    properties.clone(),
                )) as Arc<dyn ExecutionPlan>,
            )),
            _ => Ok(Transformed::no(node)),
        })?
        .data)
}

#[cfg(test)]
mod tests;
