use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use indexmap::{IndexMap, IndexSet};
use log::{debug, info, warn};
use sail_common::actor::ActorContext;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use sail_system_store::SystemEvent;
use sail_telemetry::events::SystemEventReporter;

use crate::driver::DriverActor;
use crate::driver::job_scheduler::state::{
    JobDescriptor, ShuffleCommit, StageState, TaskAttemptDescriptor, TaskRegionState, TaskState,
};
use crate::driver::job_scheduler::topology::{TaskRegionTopology, TaskTopology};
use crate::driver::job_scheduler::{JobAction, JobScheduler, JobSchedulerOptions, JobState};
use crate::driver::output::{JobOutputOutcome, build_job_output};
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskKey, TaskKeyDisplay, TaskStreamKey};
use crate::job_graph::{
    InputMode, JobGraph, OutputDistribution, OutputMode, Stage, StageInput, SubRange, TaskPlacement,
};
use crate::proto::{
    encode_remote_partitioning, encode_remote_physical_expr, encode_remote_physical_plan,
};
use crate::shuffle::ShuffleBackendKind;
use crate::statistics::{ShuffleStatistics, TaskOutputStatistics};
use crate::task::definition::{
    ShuffleServiceRead, TaskDefinition, TaskInput, TaskInputKey, TaskInputLocator, TaskOutput,
    TaskOutputDistribution, TaskOutputLocator,
};
use crate::task::scheduling::{
    TaskAssignment, TaskAssignmentGetter, TaskOutputKind, TaskRegion, TaskSet, TaskSetEntry,
};

impl JobScheduler {
    fn next_job_id(&mut self) -> ExecutionResult<JobId> {
        self.job_id_generator.generate()
    }

    pub fn accept_job(
        &mut self,
        ctx: &mut ActorContext<DriverActor>,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> ExecutionResult<(JobId, SendableRecordBatchStream)> {
        let job_id = self.next_job_id()?;

        debug!(
            "job {job_id} execution plan\n{}",
            DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
        );
        let graph = JobGraph::try_new(
            plan,
            crate::job_graph::JobGraphOptions {
                shuffle_backend: self.options.shuffle_backend.clone(),
            },
        )?;
        debug!("job {job_id} job graph \n{graph}");

        let (output, stream) = build_job_output(ctx, job_id, graph.schema().clone());
        let descriptor = JobDescriptor::try_new(graph, JobState::Running { output }, context)?;
        self.jobs.insert(job_id, descriptor);

        if let Some(job) = self.jobs.get(&job_id) {
            let event_reporter = &self.event_reporter;
            let session_id = &self.options.session_id;
            event_reporter.report(SystemEvent::JobCreated {
                session_id: session_id.clone(),
                job_id: u64::from(job_id),
                status: job.state.status().to_string(),
                created_at: Utc::now(),
            });
            for (s, (stage, descriptor)) in job.graph.stages().iter().zip(&job.stages).enumerate() {
                event_reporter.report(SystemEvent::StageCreated {
                    session_id: session_id.clone(),
                    job_id: u64::from(job_id),
                    stage: s as u64,
                    partitions: stage.plan.output_partitioning().partition_count() as u64,
                    inputs: stage
                        .inputs
                        .iter()
                        .map(|input| sail_system_store::types::StageInput {
                            stage: input.stage as u64,
                            mode: input.mode.to_string(),
                        })
                        .collect(),
                    group: stage.group.clone(),
                    mode: stage.mode.to_string(),
                    distribution: stage.distribution.to_string(),
                    placement: stage.placement.to_string(),
                    status: descriptor.state.status().to_string(),
                    created_at: Utc::now(),
                });
            }
        }

        Ok((job_id, stream))
    }

    pub fn update_task(
        &mut self,
        key: &TaskKey,
        state: TaskState,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
        statistics: Option<TaskOutputStatistics>,
    ) {
        let event_reporter = self.event_reporter.clone();
        let session_id = self.options.session_id.clone();
        let Some(job) = self.jobs.get_mut(&key.job_id) else {
            warn!("{} not found", TaskKeyDisplay(key));
            return;
        };
        // Only blocking outputs have a committed form that later stages can rely on.
        let channels = job
            .graph
            .stages()
            .get(key.stage)
            .filter(|stage| matches!(stage.mode, OutputMode::Blocking))
            .map(|stage| stage.distribution.channels());
        let Some((stage, attempt)) = job.stages.get_mut(key.stage).and_then(|stage| {
            let attempt = stage
                .tasks
                .get_mut(key.partition)
                .and_then(|task| task.attempts.get_mut(key.attempt))?;
            Some((&mut stage.retained_statistics_bytes, attempt))
        }) else {
            warn!("{} not found", TaskKeyDisplay(key));
            return;
        };
        let previous = attempt.state;
        attempt.state = attempt.state.consolidate(state);
        attempt.messages.extend(message);
        if let Some(cause) = cause {
            attempt.cause = Some(cause);
        }
        // Statistics are bound to the attempt that reported them, and are kept only
        // when this report is the one that moved the attempt to the succeeded state.
        if let Some(statistics) = statistics
            && !previous.is_terminal()
            && matches!(attempt.state, TaskState::Succeeded)
        {
            if channels.is_none() {
                // Statistics of pipelined outputs are not used.
            } else if Some(statistics.channels()) == channels {
                // Statistics are retained in the form they are reported in.
                let bytes = statistics.retained_bytes() as u64;
                job.adaptive.statistics_payload_bytes =
                    job.adaptive.statistics_payload_bytes.saturating_add(bytes);
                *stage = stage.saturating_add(bytes);
                job.adaptive.retained_statistics_bytes =
                    job.adaptive.retained_statistics_bytes.saturating_add(bytes);
                job.adaptive.peak_retained_statistics_bytes = job
                    .adaptive
                    .peak_retained_statistics_bytes
                    .max(job.adaptive.retained_statistics_bytes);
                attempt.statistics = Some(Arc::new(statistics));
            } else {
                warn!(
                    "{} reported statistics for {} channels, expected {channels:?}",
                    TaskKeyDisplay(key),
                    statistics.channels()
                );
            }
        }
        event_reporter.report(SystemEvent::TaskUpdated {
            session_id,
            job_id: u64::from(key.job_id),
            stage: key.stage as u64,
            partition: key.partition as u64,
            attempt: key.attempt as u64,
            status: attempt.state.status().to_string(),
            updated_at: Utc::now(),
        });
    }

    pub fn get_task_state(&self, key: &TaskKey) -> Option<TaskState> {
        let attempt = self
            .jobs
            .get(&key.job_id)
            .and_then(|job| job.stages.get(key.stage))
            .and_then(|stage| stage.tasks.get(key.partition))
            .and_then(|task| task.attempts.get(key.attempt));
        if attempt.is_none() {
            warn!("{} not found", TaskKeyDisplay(key));
        };
        attempt.map(|x| x.state)
    }

    /// Determine the actions needed in the driver for the job whose
    /// task states may have changed.
    ///
    /// The method first determines the task regions and then decides
    /// the actions to take.
    ///   1. For each task region, if any task attempt fails, all existing task attempts
    ///      in the region are canceled if not already.
    ///   2. If any task in the final stages is running or has succeeded, all its channels are
    ///      added as job output streams if not already.
    ///   3. For each stage, if all the stages that consume it have succeeded, remove
    ///      the output streams of the stage if not already.
    ///   4. If any task exceeds the maximum allowed attempts, the task region and the job
    ///      are marked as failed.
    ///   5. If all the tasks in the final stages have succeeded, the job is marked as succeeded.
    ///   6. For each task region, schedule the tasks of the region if all the dependency
    ///      regions have succeeded.
    pub fn refresh_job(&mut self, job_id: JobId) -> Vec<JobAction> {
        let event_reporter = self.event_reporter.clone();
        let session_id = self.options.session_id.clone();
        let Some(job) = self.jobs.get_mut(&job_id) else {
            warn!("job {job_id} not found");
            return vec![];
        };
        if !matches!(job.state, JobState::Running { .. }) {
            return vec![];
        }

        let mut actions = vec![];

        actions.extend(Self::cascade_cancel_task_attempts(
            job_id,
            job,
            &event_reporter,
            &session_id,
        ));
        actions.extend(Self::extend_job_output(job_id, job));

        Self::update_task_regions(job, &self.options);

        actions.extend(Self::clean_up_job_by_stage(
            job_id,
            job,
            &event_reporter,
            &session_id,
        ));
        Self::report_stage_statistics(job_id, job, &event_reporter, &session_id);

        if job
            .regions
            .iter()
            .any(|x| matches!(x.state, TaskRegionState::Failed))
        {
            let cause = Self::infer_job_failure_cause(job);
            if let JobState::Running { output, .. } = &job.state {
                actions.push(JobAction::FailJobOutput {
                    handle: output.handle(),
                    cause,
                })
            }
            job.state = JobState::Failed;
            event_reporter.report(SystemEvent::JobUpdated {
                session_id,
                job_id: u64::from(job_id),
                status: job.state.status().to_string(),
                updated_at: Utc::now(),
            });
            return actions;
        }

        if job
            .regions
            .iter()
            .all(|x| matches!(x.state, TaskRegionState::Succeeded))
        {
            // This drops `JobOutputManager` in the job state,
            // so that `JobOutputStream` turns to the draining state as well.
            job.state = JobState::Draining;
            event_reporter.report(SystemEvent::JobUpdated {
                session_id,
                job_id: u64::from(job_id),
                status: job.state.status().to_string(),
                updated_at: Utc::now(),
            });
            Self::log_adaptive_metrics(job_id, job, &self.options);
            return actions;
        }

        if self.options.adaptive.enabled
            && Self::adapt_ready_stages(
                job_id,
                job,
                &self.options,
                self.codec.as_ref(),
                &event_reporter,
                &session_id,
            )
        {
            // A re-planned stage changes the topology, so region states are recomputed
            // from the task attempts before scheduling.
            Self::update_task_regions(job, &self.options);
        }
        Self::release_statistics(job, &self.options);

        actions.extend(Self::schedule_task_regions(
            job_id,
            job,
            &event_reporter,
            &session_id,
        ));

        actions
    }

    pub(super) fn update_task_regions(job: &mut JobDescriptor, options: &JobSchedulerOptions) {
        for (r, region) in job.topology.regions.iter().enumerate() {
            let failed = region.tasks.iter().any(|t| {
                let attempts = &job.stages[t.stage].tasks[t.partition].attempts;
                if let Some(attempt) = attempts.last()
                    && matches!(attempt.state, TaskState::Failed | TaskState::Canceled)
                    && attempts.len() >= options.task_max_attempts
                {
                    return true;
                }
                false
            });
            if failed {
                job.regions[r].state = TaskRegionState::Failed;
                continue;
            }
            let succeeded = region.tasks.iter().all(|t| {
                job.stages[t.stage].tasks[t.partition]
                    .attempts
                    .last()
                    .is_some_and(|x| matches!(x.state, TaskState::Succeeded))
            });
            if succeeded {
                job.regions[r].state = TaskRegionState::Succeeded;
            } else {
                job.regions[r].state = TaskRegionState::Running;
            }
        }
    }

    fn cascade_cancel_task_attempts(
        job_id: JobId,
        job: &mut JobDescriptor,
        event_reporter: &SystemEventReporter,
        session_id: &str,
    ) -> Vec<JobAction> {
        let mut actions = vec![];

        for region in &job.topology.regions {
            let mut failed = false;

            for t in &region.tasks {
                let attempts = &job.stages[t.stage].tasks[t.partition].attempts;
                if let Some(attempt) = attempts.last()
                    && matches!(attempt.state, TaskState::Failed)
                {
                    failed = true;
                }
            }

            if failed {
                // cancel all other tasks in this region
                for t in &region.tasks {
                    let task = &mut job.stages[t.stage].tasks[t.partition];
                    for (a, attempt) in task.attempts.iter_mut().enumerate() {
                        if !attempt.state.is_terminal() {
                            attempt.state = TaskState::Canceled;
                            event_reporter.report(SystemEvent::TaskUpdated {
                                session_id: session_id.to_string(),
                                job_id: u64::from(job_id),
                                stage: t.stage as u64,
                                partition: t.partition as u64,
                                attempt: a as u64,
                                status: attempt.state.status().to_string(),
                                updated_at: Utc::now(),
                            });
                            actions.push(JobAction::CancelTask {
                                key: TaskKey {
                                    job_id,
                                    stage: t.stage,
                                    partition: t.partition,
                                    attempt: a,
                                },
                            });
                        }
                    }
                }
            }
        }

        actions
    }

    pub(super) fn clean_up_job_by_stage(
        job_id: JobId,
        job: &mut JobDescriptor,
        event_reporter: &SystemEventReporter,
        session_id: &str,
    ) -> Vec<JobAction> {
        let mut actions = vec![];

        for (s, stage) in job.topology.stages.iter().enumerate() {
            if matches!(job.stages[s].state, StageState::Inactive) {
                continue;
            }
            // The output must stay readable while any consumer task can still be retried.
            // A failed task reschedules its whole region, including consumer tasks that
            // have succeeded. A retried region that also holds producer tasks recreates
            // the output, so only the consumer tasks must have succeeded. Otherwise the
            // output is released only once the consumer region has succeeded, since
            // succeeded regions never run again.
            // The producer regions are computed only once every consumer task has
            // succeeded, since this runs on every task status update.
            let producer_regions = std::cell::OnceCell::new();
            let producer_regions = || {
                producer_regions.get_or_init(|| {
                    (0..job.stages[s].tasks.len())
                        .filter_map(|p| {
                            job.topology
                                .task_regions
                                .get(&TaskTopology {
                                    stage: s,
                                    partition: p,
                                })
                                .copied()
                        })
                        .collect::<IndexSet<_>>()
                })
            };
            let all_consumer_regions_succeeded = stage.consumers.iter().all(|&c| {
                let partitions = job.graph.stages()[c]
                    .plan
                    .output_partitioning()
                    .partition_count();
                (0..partitions).all(|p| {
                    let task_succeeded = job.stages[c].tasks[p]
                        .attempts
                        .last()
                        .is_some_and(|a| matches!(a.state, TaskState::Succeeded));
                    let region = job.topology.task_regions.get(&TaskTopology {
                        stage: c,
                        partition: p,
                    });
                    task_succeeded
                        && region.is_some_and(|r| {
                            matches!(job.regions[*r].state, TaskRegionState::Succeeded)
                                || producer_regions().contains(r)
                        })
                })
            });

            if all_consumer_regions_succeeded && !stage.consumers.is_empty() {
                job.stages[s].state = StageState::Inactive;
                event_reporter.report(SystemEvent::StageUpdated {
                    session_id: session_id.to_string(),
                    job_id: u64::from(job_id),
                    stage: s as u64,
                    status: job.stages[s].state.status().to_string(),
                    updated_at: Utc::now(),
                });
                actions.push(JobAction::CleanUpJob {
                    job_id,
                    stage: Some(s),
                    context: job.context.clone(),
                });
            }
        }

        actions
    }

    fn schedule_task_regions(
        job_id: JobId,
        job: &mut JobDescriptor,
        event_reporter: &SystemEventReporter,
        session_id: &str,
    ) -> Vec<JobAction> {
        let mut actions = vec![];

        for (r, region) in job.topology.regions.iter().enumerate() {
            if matches!(job.regions[r].state, TaskRegionState::Succeeded) {
                continue;
            }

            if !region
                .dependencies
                .iter()
                .all(|d| matches!(job.regions[*d].state, TaskRegionState::Succeeded))
            {
                // The region has dependencies that are not yet succeeded.
                continue;
            }

            if region.tasks.iter().any(|t| {
                job.stages[t.stage].tasks[t.partition]
                    .attempts
                    .last()
                    .is_some_and(|x| !x.state.is_terminal())
            }) {
                // The latest task attempt is still active, so the region must have been
                // scheduled already.
                continue;
            }

            for t in &region.tasks {
                let attempts = &mut job.stages[t.stage].tasks[t.partition].attempts;
                attempts.push(TaskAttemptDescriptor {
                    state: TaskState::Created,
                    messages: vec![],
                    cause: None,
                    job_output_fetched: false,
                    statistics: None,
                });
                event_reporter.report(SystemEvent::TaskCreated {
                    session_id: session_id.to_string(),
                    job_id: u64::from(job_id),
                    stage: t.stage as u64,
                    partition: t.partition as u64,
                    attempt: (attempts.len() - 1) as u64,
                    status: TaskState::Created.status().to_string(),
                    created_at: Utc::now(),
                });
            }

            actions.push(JobAction::ScheduleTaskRegion {
                region: Self::build_task_region(job_id, job, region),
            });
        }

        actions
    }

    fn build_task_region(
        job_id: JobId,
        job: &JobDescriptor,
        region: &TaskRegionTopology,
    ) -> TaskRegion {
        let stages = region
            .tasks
            .iter()
            .map(|t| t.stage)
            .collect::<IndexSet<_>>();
        let mut stage_partitions: IndexMap<StageGroupKey, IndexMap<usize, usize>> = IndexMap::new();
        for s in stages {
            let stage = &job.graph.stages()[s];
            let key = StageGroupKey {
                placement: stage.placement,
                group: stage.group.clone(),
            };
            let p = stage.plan.output_partitioning().partition_count();
            stage_partitions.entry(key).or_default().insert(s, p);
        }
        let mut stage_groups = stage_partitions
            .into_iter()
            .map(|(key, partitions)| (key, StageGroup::new(partitions)))
            .collect::<IndexMap<_, _>>();

        for t in &region.tasks {
            if let Some(attempt) = Self::get_latest_task_attempt(job, t.stage, t.partition) {
                let stage = &job.graph.stages()[t.stage];
                let output = match stage.mode {
                    OutputMode::Pipelined => TaskOutputKind::Local,
                    OutputMode::Blocking => match job.graph.shuffle_backend() {
                        ShuffleBackendKind::Storage { .. } => TaskOutputKind::Storage,
                        ShuffleBackendKind::Celeborn { .. } => TaskOutputKind::External,
                        ShuffleBackendKind::Flight { .. } => unreachable!(),
                    },
                };
                let key = StageGroupKey {
                    placement: stage.placement,
                    group: stage.group.clone(),
                };
                if let Some(group) = stage_groups.get_mut(&key) {
                    group.add_task(TaskSetEntry {
                        key: TaskKey {
                            job_id,
                            stage: t.stage,
                            partition: t.partition,
                            attempt,
                        },
                        output,
                    });
                }
            }
        }

        let mut tasks: Vec<(TaskPlacement, TaskSet)> = vec![];
        for (key, value) in stage_groups {
            // A region may contain only some stage partitions. Empty buckets need no task slots.
            for entries in value
                .buckets
                .into_iter()
                .filter(|entries| !entries.is_empty())
            {
                tasks.push((key.placement, TaskSet { entries }));
            }
        }

        TaskRegion { tasks }
    }

    fn extend_job_output(job_id: JobId, job: &mut JobDescriptor) -> Vec<JobAction> {
        let JobState::Running { output, .. } = &mut job.state else {
            return vec![];
        };
        let mut actions = vec![];
        let schema = job.graph.schema().clone();
        for (s, stage) in job.graph.stages().iter().enumerate() {
            if !job.topology.stages[s].consumers.is_empty() {
                // The stage is not a final stage.
                continue;
            }
            let partitions = stage.plan.output_partitioning().partition_count();
            let channels = stage.distribution.channels();
            for p in 0..partitions {
                let Some((attempt, head)) = job.stages[s].tasks[p].attempts.split_last_mut() else {
                    continue;
                };
                if !matches!(attempt.state, TaskState::Running | TaskState::Succeeded)
                    || attempt.job_output_fetched
                {
                    continue;
                }
                attempt.job_output_fetched = true;
                for c in 0..channels {
                    let key = TaskStreamKey {
                        job_id,
                        stage: s,
                        partition: p,
                        attempt: head.len(),
                        channel: c,
                    };
                    actions.push(JobAction::ExtendJobOutput {
                        handle: output.handle(),
                        key,
                        schema: schema.clone(),
                    });
                }
            }
        }

        actions
    }

    fn infer_job_failure_cause(job: &JobDescriptor) -> CommonErrorCause {
        // a map from (stage, partition) to a list of causes
        let mut causes: HashMap<(usize, usize), Vec<&CommonErrorCause>> = HashMap::new();
        for (s, stage) in job.stages.iter().enumerate() {
            for (t, task) in stage.tasks.iter().enumerate() {
                for attempt in task.attempts.iter() {
                    if matches!(attempt.state, TaskState::Failed)
                        && let Some(cause) = &attempt.cause
                    {
                        causes.entry((s, t)).or_default().push(cause);
                    }
                }
            }
        }
        // Get the most recent cause from the most likely failed task.
        if let Some((_, &[.., cause])) = causes.iter().map(|(k, v)| (k, v.as_slice())).min_by(
            |((s1, p1), v1), ((s2, p2), v2)| {
                // Find the task that fails the most number of times,
                // and if there is a tie, choose the one with the smallest
                // stage and partition.
                v2.len().cmp(&v1.len()).then(s1.cmp(s2)).then(p1.cmp(p2))
            },
        ) {
            cause.clone()
        } else {
            CommonErrorCause::new::<PyErrExtractor>(&ExecutionError::InternalError(
                "job failed for unknown reason".to_string(),
            ))
        }
    }

    /// Determine the actions needed in the driver to clean up the job.
    /// The method cancels all the task attempts that are not in terminal states
    /// and removes all the job output streams.
    pub fn clean_up_job(&mut self, job_id: JobId, outcome: JobOutputOutcome) -> Vec<JobAction> {
        let event_reporter = self.event_reporter.clone();
        let session_id = self.options.session_id.clone();
        let Some(job) = self.jobs.get_mut(&job_id) else {
            warn!("job {job_id} not found");
            return vec![];
        };
        let mut actions = vec![];
        for (s, stage) in job.stages.iter().enumerate() {
            for (t, task) in stage.tasks.iter().enumerate() {
                for (a, attempt) in task.attempts.iter().enumerate() {
                    if !attempt.state.is_terminal() {
                        actions.push(JobAction::CancelTask {
                            key: TaskKey {
                                job_id,
                                stage: s,
                                partition: t,
                                attempt: a,
                            },
                        });
                    }
                }
            }
        }
        for (s, stage) in job.stages.iter_mut().enumerate() {
            match stage.state {
                StageState::Active => {
                    stage.state = StageState::Inactive;
                    event_reporter.report(SystemEvent::StageUpdated {
                        session_id: session_id.clone(),
                        job_id: u64::from(job_id),
                        stage: s as u64,
                        status: stage.state.status().to_string(),
                        updated_at: Utc::now(),
                    });
                }
                StageState::Inactive => {}
            }
        }
        actions.push(JobAction::CleanUpJob {
            job_id,
            stage: None,
            context: job.context.clone(),
        });
        job.state.finish_output(outcome);
        event_reporter.report(SystemEvent::JobUpdated {
            session_id,
            job_id: u64::from(job_id),
            status: job.state.status().to_string(),
            updated_at: Utc::now(),
        });
        actions
    }

    /// Returns the retry region containing a task partition.
    pub fn task_region(&self, key: &TaskKey) -> Option<usize> {
        let job = self.jobs.get(&key.job_id)?;
        job.topology
            .task_regions
            .get(&super::topology::TaskTopology {
                stage: key.stage,
                partition: key.partition,
            })
            .copied()
    }

    /// Builds the stage definition against the current assignment snapshot.
    pub fn get_task_definition(
        &self,
        key: &TaskKey,
        assignments: &dyn TaskAssignmentGetter,
    ) -> ExecutionResult<(TaskDefinition, Arc<TaskContext>)> {
        let Some(job) = self.jobs.get(&key.job_id) else {
            return Err(ExecutionError::InvalidArgument(format!(
                "job {} not found",
                key.job_id
            )));
        };
        let Some(stage) = job.graph.stages().get(key.stage) else {
            return Err(ExecutionError::InvalidArgument(format!(
                "stage {} not found in job {}",
                key.stage, key.job_id
            )));
        };

        let plan = encode_remote_physical_plan(self.codec.as_ref(), stage.plan.clone())?;
        let inputs = stage
            .inputs
            .iter()
            .map(|input| TaskInputBuilder::try_new(job, key, input, assignments)?.build())
            .collect::<ExecutionResult<Vec<_>>>()?;
        let collect_statistics = Self::collects_statistics(&self.options, job);
        let output =
            TaskOutputBuilder::new(job, key, stage, self.codec.as_ref(), collect_statistics)
                .build()?;
        let definition = TaskDefinition {
            plan: Arc::from(plan),
            inputs,
            output,
        };
        Ok((definition, job.context.clone()))
    }

    pub fn stop(&mut self) {
        let event_reporter = self.event_reporter.clone();
        let session_id = self.options.session_id.clone();
        for (job_id, job) in self.jobs.iter_mut() {
            let job_id = u64::from(*job_id);
            if matches!(job.state, JobState::Running { .. } | JobState::Draining) {
                // For running jobs, the job output is dropped here.
                // Internally, the job output manages the receiving end of the output stream.
                // So once the stream receiver is no longer available, all the running tasks
                // will ultimately find that the sink (owned by the shuffle write node) is closed.
                // The task will stop running even if the task produces an infinite stream.
                // Once the tasks are stopped, the inputs (gRPC streaming responses owned by
                // shuffle read nodes) are dropped. So the worker gRPC server will have no active
                // clients subscribing to local streams, and the server can proceed with shutdown.
                job.state = JobState::Canceled;
                event_reporter.report(SystemEvent::JobUpdated {
                    session_id: session_id.to_string(),
                    job_id,
                    status: job.state.status().to_string(),
                    updated_at: Utc::now(),
                });
            }
            for (s, stage) in job.stages.iter_mut().enumerate() {
                if matches!(stage.state, StageState::Active) {
                    stage.state = StageState::Inactive;
                    event_reporter.report(SystemEvent::StageUpdated {
                        session_id: session_id.to_string(),
                        job_id,
                        stage: s as u64,
                        status: stage.state.status().to_string(),
                        updated_at: Utc::now(),
                    });
                }
                for (partition, task) in stage.tasks.iter_mut().enumerate() {
                    for (a, attempt) in task.attempts.iter_mut().enumerate() {
                        if !attempt.state.is_terminal() {
                            attempt.state = TaskState::Canceled;
                            event_reporter.report(SystemEvent::TaskUpdated {
                                session_id: session_id.to_string(),
                                job_id,
                                stage: s as u64,
                                partition: partition as u64,
                                attempt: a as u64,
                                status: attempt.state.status().to_string(),
                                updated_at: Utc::now(),
                            });
                        }
                    }
                }
            }
        }
    }

    /// Whether the blocking outputs of a job count their committed statistics.
    /// Only adaptive execution reads them, and only on a remote shuffle service, so
    /// execution without it does not pay for counting.
    pub(super) fn collects_statistics(options: &JobSchedulerOptions, job: &JobDescriptor) -> bool {
        options.adaptive.enabled && job.graph.shuffle_backend().is_remote_shuffle_service()
    }

    /// Records the output that the remote shuffle service committed for a stage.
    /// A shuffle is committed once, successfully or not, and the commit never changes.
    pub fn record_shuffle_commit(
        &mut self,
        job_id: JobId,
        stage: usize,
        map_attempts: Vec<i32>,
        result: Result<(), String>,
    ) {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            warn!("job {job_id} not found for shuffle commit of stage {stage}");
            return;
        };
        let Some(descriptor) = job.stages.get_mut(stage) else {
            warn!("job {job_id} stage {stage} not found for shuffle commit");
            return;
        };
        if let Some(commit) = &descriptor.commit {
            warn!("job {job_id} stage {stage} shuffle is already committed: {commit:?}");
            return;
        }
        let attempts = map_attempts
            .iter()
            .enumerate()
            .map(|(p, &a)| {
                let a = usize::try_from(a).ok()?;
                // The selected attempt must be one that the scheduler created.
                (a < descriptor.tasks.get(p)?.attempts.len()).then_some(a)
            })
            .collect::<Option<Vec<_>>>();
        let commit = match (attempts, &result) {
            (Some(attempts), Ok(())) if attempts.len() == descriptor.tasks.len() => ShuffleCommit {
                attempts,
                succeeded: true,
            },
            (attempts, _) => {
                if let Err(error) = &result {
                    warn!("job {job_id} stage {stage} shuffle commit failed: {error}");
                } else {
                    warn!(
                        "job {job_id} stage {stage} shuffle commit has unexpected attempts: {map_attempts:?}"
                    );
                }
                ShuffleCommit {
                    attempts: attempts.unwrap_or_default(),
                    succeeded: false,
                }
            }
        };
        debug!("job {job_id} stage {stage} shuffle commit: {commit:?}");
        descriptor.commit = Some(commit);
    }

    /// Returns the attempt whose output the shuffle backend committed, for each
    /// partition of a blocking stage, or `None` if the committed output is not known.
    /// Statistics are collected only on a remote shuffle service, which reports the
    /// committed attempt of each map task. It need not be the scheduler's latest
    /// successful attempt (Celeborn keeps the first attempt that reached `mapper_end`).
    pub(super) fn committed_attempts(job: &JobDescriptor, stage: usize) -> Option<&[usize]> {
        if !matches!(job.graph.stages().get(stage)?.mode, OutputMode::Blocking)
            || !job.graph.shuffle_backend().is_remote_shuffle_service()
        {
            return None;
        }
        job.stages[stage]
            .commit
            .as_ref()
            .filter(|commit| commit.succeeded)
            .map(|commit| commit.attempts.as_slice())
    }

    /// Returns the statistics of the committed attempt of each partition of a stage,
    /// or `None` if the committed output is not known or any attempt lacks statistics.
    pub(super) fn committed_attempt_statistics(
        job: &JobDescriptor,
        stage: usize,
    ) -> Option<Vec<Arc<TaskOutputStatistics>>> {
        Self::committed_attempts(job, stage)?
            .iter()
            .enumerate()
            .map(|(p, &a)| {
                job.stages[stage]
                    .tasks
                    .get(p)?
                    .attempts
                    .get(a)?
                    .statistics
                    .clone()
            })
            .collect()
    }

    /// Returns the statistics of the committed attempts of a stage folded per bucket,
    /// or `None` if they are not all known. The fold is cached for the stage.
    pub(super) fn committed_statistics(
        job: &mut JobDescriptor,
        stage: usize,
    ) -> Option<Arc<ShuffleStatistics>> {
        if let Some(statistics) = &job.stages.get(stage)?.shuffle_statistics {
            return Some(statistics.clone());
        }
        let attempts = Self::committed_attempt_statistics(job, stage)?;
        let channels = job.graph.stages()[stage].distribution.channels();
        let statistics = ShuffleStatistics::fold(channels, attempts.iter().map(|x| x.as_ref()))
            .inspect_err(|e| warn!("failed to fold statistics of stage {stage}: {e}"))
            .ok()
            .map(Arc::new)?;
        let bytes = statistics.retained_bytes() as u64;
        job.stages[stage].retained_statistics_bytes = job.stages[stage]
            .retained_statistics_bytes
            .saturating_add(bytes);
        job.adaptive.retained_statistics_bytes =
            job.adaptive.retained_statistics_bytes.saturating_add(bytes);
        job.adaptive.peak_retained_statistics_bytes = job
            .adaptive
            .peak_retained_statistics_bytes
            .max(job.adaptive.retained_statistics_bytes);
        job.stages[stage].shuffle_statistics = Some(statistics.clone());
        Some(statistics)
    }

    fn report_stage_statistics(
        job_id: JobId,
        job: &mut JobDescriptor,
        event_reporter: &SystemEventReporter,
        session_id: &str,
    ) {
        for s in 0..job.stages.len() {
            if job.stages[s].statistics_reported {
                continue;
            }
            let Some(attempts) = Self::committed_attempts(job, s) else {
                continue;
            };
            let all_reported = attempts.iter().enumerate().all(|(p, &a)| {
                let attempt = &job.stages[s].tasks[p].attempts[a];
                attempt.statistics.is_some() || attempt.state.is_terminal()
            });
            if !all_reported {
                continue;
            }
            job.stages[s].statistics_reported = true;
            let Some(statistics) = Self::committed_statistics(job, s) else {
                debug!("job {job_id} stage {s} has committed output without statistics");
                continue;
            };
            let (max, median) = statistics.max_and_median_bucket_bytes();
            debug!(
                "job {job_id} stage {s} committed {} rows, {:?} physical bytes, {} logical bytes, max bucket {max} bytes, median bucket {median} bytes",
                statistics.total_rows,
                statistics.total_physical_bytes,
                statistics.total_logical_bytes
            );
            event_reporter.report(SystemEvent::StageStatisticsReported {
                session_id: session_id.to_string(),
                job_id: u64::from(job_id),
                stage: s as u64,
                rows: statistics.total_rows,
                physical_bytes: statistics.total_physical_bytes,
                logical_bytes: statistics.total_logical_bytes,
                max_partition_bytes: max,
                median_partition_bytes: median,
                reported_at: Utc::now(),
            });
        }
    }

    /// Releases the statistics of every stage whose statistics are no longer needed:
    /// they have been reported, and every consumer has made its adaptive decision.
    fn release_statistics(job: &mut JobDescriptor, options: &JobSchedulerOptions) {
        if job.adaptive.retained_statistics_bytes == 0 {
            return;
        }
        for s in 0..job.stages.len() {
            if job.stages[s].retained_statistics_bytes == 0 {
                continue;
            }
            let consumers_frozen = !options.adaptive.enabled
                || job.topology.stages[s]
                    .consumers
                    .iter()
                    .all(|&c| job.stages[c].adaptive.is_frozen());
            let finished = job.stages[s].statistics_reported
                || matches!(job.stages[s].state, StageState::Inactive);
            if consumers_frozen && finished {
                for task in &mut job.stages[s].tasks {
                    for attempt in &mut task.attempts {
                        attempt.statistics = None;
                    }
                }
                job.stages[s].shuffle_statistics = None;
                job.adaptive.retained_statistics_bytes = job
                    .adaptive
                    .retained_statistics_bytes
                    .saturating_sub(job.stages[s].retained_statistics_bytes);
                job.stages[s].retained_statistics_bytes = 0;
            }
        }
    }

    fn log_adaptive_metrics(job_id: JobId, job: &JobDescriptor, options: &JobSchedulerOptions) {
        if !options.adaptive.enabled {
            return;
        }
        info!(
            "job {job_id} adaptive execution: {} stage(s) re-planned in {:?}, {} bytes of statistics received, peak {} bytes of statistics retained",
            job.adaptive.replanned_stages,
            job.adaptive.replanning_time,
            job.adaptive.statistics_payload_bytes,
            job.adaptive.peak_retained_statistics_bytes
        );
    }

    fn get_latest_task_attempt(
        job: &JobDescriptor,
        stage: usize,
        partition: usize,
    ) -> Option<usize> {
        job.stages
            .get(stage)
            .and_then(|stage| stage.tasks.get(partition))
            .and_then(|task| task.attempts.split_last().map(|(_, head)| head.len()))
    }
}

struct TaskInputBuilder<'a> {
    job: &'a JobDescriptor,
    key: &'a TaskKey,
    input: &'a StageInput,
    producer: &'a Stage,
    consumer: &'a Stage,
    assignments: &'a dyn TaskAssignmentGetter,
}

impl<'a> TaskInputBuilder<'a> {
    fn try_new(
        job: &'a JobDescriptor,
        key: &'a TaskKey,
        input: &'a StageInput,
        assignments: &'a dyn TaskAssignmentGetter,
    ) -> ExecutionResult<Self> {
        let producer = job.graph.stages().get(input.stage).ok_or_else(|| {
            ExecutionError::InvalidArgument(format!(
                "job {} input stage {} not found",
                key.job_id, input.stage
            ))
        })?;
        let consumer = job.graph.stages().get(key.stage).ok_or_else(|| {
            ExecutionError::InvalidArgument(format!(
                "stage {} not found in job {}",
                key.stage, key.job_id
            ))
        })?;
        Ok(Self {
            job,
            key,
            input,
            producer,
            consumer,
            assignments,
        })
    }

    fn build(&self) -> ExecutionResult<TaskInput> {
        let locator = match self.producer.mode {
            OutputMode::Pipelined => match self.producer.placement {
                TaskPlacement::Driver => self.build_driver_locator()?,
                TaskPlacement::Worker => self.build_worker_locator()?,
            },
            OutputMode::Blocking => match self.job.graph.shuffle_backend() {
                ShuffleBackendKind::Storage { .. } => self.build_storage_locator()?,
                ShuffleBackendKind::Celeborn { .. } => self.build_shuffle_service_locator()?,
                ShuffleBackendKind::Flight { .. } => self.build_storage_locator()?,
            },
        };
        Ok(TaskInput {
            stage: self.input.stage,
            locator: Arc::new(locator),
        })
    }

    fn build_driver_locator(&self) -> ExecutionResult<TaskInputLocator> {
        let keys = self.build_task_input_keys()?;
        keys.iter()
            .flatten()
            .try_for_each(|key| match self.assignment(key) {
                Some(TaskAssignment::Driver) => Ok(()),
                _ => Err(ExecutionError::InternalError(format!(
                    "job {} input stage {} partition {} attempt {} is not assigned to driver",
                    self.key.job_id, self.input.stage, key.partition, key.attempt
                ))),
            })?;
        Ok(TaskInputLocator::Driver { keys })
    }

    fn build_worker_locator(&self) -> ExecutionResult<TaskInputLocator> {
        let keys = self
            .build_task_input_keys()?
            .into_iter()
            .map(|keys| {
                keys.into_iter()
                    .map(|key| {
                        let Some(TaskAssignment::Worker { worker_id, slot: _ }) =
                            self.assignment(&key)
                        else {
                            return Err(ExecutionError::InternalError(format!(
                                "job {} input stage {} partition {} attempt {} is not assigned to worker",
                                self.key.job_id,
                                self.input.stage,
                                key.partition,
                                key.attempt
                            )));
                        };
                        Ok((*worker_id, key))
                    })
                    .collect::<ExecutionResult<Vec<_>>>()
            })
            .collect::<ExecutionResult<Vec<Vec<_>>>>()?;
        Ok(TaskInputLocator::Worker { keys })
    }

    fn build_storage_locator(&self) -> ExecutionResult<TaskInputLocator> {
        Ok(TaskInputLocator::Storage {
            keys: self.build_task_input_keys()?,
        })
    }

    fn build_shuffle_service_locator(&self) -> ExecutionResult<TaskInputLocator> {
        let input_channels = self.producer.distribution.channels();
        let output_partitions = self.consumer.plan.output_partitioning().partition_count();
        let map_attempts = self.committed_map_attempts()?;
        let reads = if let Some(adaptive) = &self.input.adaptive {
            // Partition `i` of this input's own domain reads the bucket slices of its
            // spec entry: whole channels, or one channel restricted to a range of
            // producer partitions for a skew split.
            adaptive
                .spec
                .iter()
                .map(|slices| {
                    let mut read = ShuffleServiceRead::channels(vec![]);
                    for slice in slices {
                        if slice.buckets.end > input_channels {
                            return Err(ExecutionError::InternalError(format!(
                                "invalid shuffle service bucket slice {slice} for {input_channels} channels"
                            )));
                        }
                        read.channels.extend(slice.buckets.clone());
                        if let Some(SubRange::Producers(mappers)) = &slice.part {
                            if slices.len() != 1 || slice.buckets.len() != 1 {
                                return Err(ExecutionError::InternalError(format!(
                                    "a partial bucket must be read alone: {slice}"
                                )));
                            }
                            read.mappers = Some(mappers.clone());
                        }
                    }
                    if read.mappers.is_some() && map_attempts.is_none() {
                        return Err(ExecutionError::InternalError(
                            "a partial bucket read requires the committed map attempts"
                                .to_string(),
                        ));
                    }
                    Ok(read)
                })
                .collect::<ExecutionResult<Vec<_>>>()?
        } else {
            match self.input.mode {
                InputMode::Shuffle => (0..output_partitions)
                    .map(|partition| ShuffleServiceRead::channels(vec![partition]))
                    .collect(),
                InputMode::Broadcast => {
                    let channels = (0..input_channels).collect::<Vec<_>>();
                    vec![ShuffleServiceRead::channels(channels); output_partitions]
                }
                mode => {
                    return Err(ExecutionError::InvalidArgument(format!(
                        "Celeborn shuffle service does not support {mode} input"
                    )));
                }
            }
        };
        Ok(TaskInputLocator::ShuffleService {
            reads,
            map_attempts,
        })
    }

    /// Returns the attempt that the shuffle service committed for each producer partition, so
    /// that readers skip output of other attempts (e.g. partial data of a failed
    /// attempt). Reading a shuffle whose global commit failed is an error, since its
    /// files are not verified.
    fn committed_map_attempts(&self) -> ExecutionResult<Option<Arc<[i32]>>> {
        match &self.job.stages[self.input.stage].commit {
            Some(commit) if commit.succeeded => commit
                .attempts
                .iter()
                .map(|&a| {
                    i32::try_from(a).map_err(|e| ExecutionError::InternalError(e.to_string()))
                })
                .collect::<ExecutionResult<Vec<_>>>()
                .map(|x| Some(Arc::from(x))),
            Some(_) => Err(ExecutionError::InternalError(format!(
                "job {} stage {} shuffle commit failed",
                self.key.job_id, self.input.stage
            ))),
            // A producer without partitions never commits a shuffle.
            None if self.job.stages[self.input.stage].tasks.is_empty() => Ok(None),
            // The commit is reported before any producer task succeeds, so it is known
            // whenever a consumer runs. Reading without it would not skip the output of
            // attempts that the shuffle service did not commit.
            None => Err(ExecutionError::InternalError(format!(
                "job {} stage {} shuffle commit is not known",
                self.key.job_id, self.input.stage
            ))),
        }
    }

    fn assignment(&self, key: &TaskInputKey) -> Option<&TaskAssignment> {
        self.assignments.get(&TaskKey {
            job_id: self.key.job_id,
            stage: self.input.stage,
            partition: key.partition,
            attempt: key.attempt,
        })
    }

    fn build_task_input_keys(&self) -> ExecutionResult<Vec<Vec<TaskInputKey>>> {
        let input_partitions = self.producer.plan.output_partitioning().partition_count();
        let input_channels = self.producer.distribution.channels();

        match self.input.mode {
            InputMode::Forward | InputMode::Merge => {
                let mut groups = Vec::with_capacity(input_partitions);
                for partition in 0..input_partitions {
                    // Forward regions are assigned independently. Preserve input-local indices,
                    // but do not resolve producers for consumer partitions outside this snapshot.
                    if matches!(self.input.mode, InputMode::Forward)
                        && input_partitions
                            == self.consumer.plan.output_partitioning().partition_count()
                        && !JobScheduler::get_latest_task_attempt(
                            self.job,
                            self.key.stage,
                            partition,
                        )
                        .is_some_and(|attempt| {
                            self.assignments
                                .get(&TaskKey {
                                    job_id: self.key.job_id,
                                    stage: self.key.stage,
                                    partition,
                                    attempt,
                                })
                                .is_some()
                        })
                    {
                        groups.push(vec![]);
                        continue;
                    }
                    let attempt = self.latest_attempt(partition)?;
                    let mut group = Vec::with_capacity(input_channels);
                    for channel in 0..input_channels {
                        group.push(TaskInputKey {
                            partition,
                            attempt,
                            channel,
                        });
                    }
                    groups.push(group);
                }
                Ok(groups)
            }
            // Enumerate channels in the outer loop and partitions in the inner loop.
            // This is the whole point of shuffle!
            InputMode::Shuffle => {
                let mut groups = Vec::with_capacity(input_channels);
                for channel in 0..input_channels {
                    let mut group = Vec::with_capacity(input_partitions);
                    for partition in 0..input_partitions {
                        group.push(TaskInputKey {
                            partition,
                            attempt: self.latest_attempt(partition)?,
                            channel,
                        });
                    }
                    groups.push(group);
                }
                Ok(groups)
            }
            InputMode::Broadcast => {
                let mut keys = Vec::with_capacity(input_partitions * input_channels);
                for partition in 0..input_partitions {
                    let attempt = self.latest_attempt(partition)?;
                    for channel in 0..input_channels {
                        keys.push(TaskInputKey {
                            partition,
                            attempt,
                            channel,
                        });
                    }
                }
                Ok(vec![keys])
            }
            InputMode::Rescale {
                partitions: output_partitions,
            } => {
                // Keep rescale input expansion aligned with CoalesceExec's contiguous partition
                // grouping. A parent such as UnionExec can change the containing stage's
                // partition count, so use the target count recorded on this input edge.
                let mut groups = Vec::with_capacity(output_partitions);
                for output_partition in 0..output_partitions {
                    let start = output_partition * input_partitions / output_partitions;
                    let end = (output_partition + 1) * input_partitions / output_partitions;
                    let mut group =
                        Vec::with_capacity((end.saturating_sub(start)) * input_channels);
                    for partition in start..end {
                        let attempt = self.latest_attempt(partition)?;
                        for channel in 0..input_channels {
                            group.push(TaskInputKey {
                                partition,
                                attempt,
                                channel,
                            });
                        }
                    }
                    groups.push(group);
                }
                Ok(groups)
            }
        }
    }

    fn latest_attempt(&self, partition: usize) -> ExecutionResult<usize> {
        if let Some(attempt) =
            JobScheduler::get_latest_task_attempt(self.job, self.input.stage, partition)
        {
            Ok(attempt)
        } else {
            Err(ExecutionError::InvalidArgument(format!(
                "no latest task attempt found for job {} stage {} partition {}",
                self.key.job_id, self.input.stage, partition
            )))
        }
    }
}

struct TaskOutputBuilder<'a> {
    job: &'a JobDescriptor,
    key: &'a TaskKey,
    stage: &'a Stage,
    codec: &'a dyn PhysicalExtensionCodec,
    collect_statistics: bool,
}

impl<'a> TaskOutputBuilder<'a> {
    fn new(
        job: &'a JobDescriptor,
        key: &'a TaskKey,
        stage: &'a Stage,
        codec: &'a dyn PhysicalExtensionCodec,
        collect_statistics: bool,
    ) -> Self {
        Self {
            job,
            key,
            stage,
            codec,
            collect_statistics,
        }
    }

    fn build(&self) -> ExecutionResult<TaskOutput> {
        let distribution = match &self.stage.distribution {
            OutputDistribution::Hash { keys, channels } => {
                let keys = keys
                    .iter()
                    .map(|expr| {
                        let expr = encode_remote_physical_expr(self.codec, expr)?;
                        Ok(Arc::from(expr))
                    })
                    .collect::<ExecutionResult<Vec<Arc<[u8]>>>>()?;
                TaskOutputDistribution::Hash {
                    keys,
                    channels: *channels,
                }
            }
            OutputDistribution::Range { partitioning } => {
                let channels = partitioning.partition_count();
                let partitioning = encode_remote_partitioning(
                    self.codec,
                    &Partitioning::Range(partitioning.clone()),
                )?;
                TaskOutputDistribution::Range {
                    partitioning: Arc::from(partitioning),
                    channels,
                }
            }
            OutputDistribution::RoundRobinBatch { channels } => {
                TaskOutputDistribution::RoundRobinBatch {
                    channels: *channels,
                }
            }
            OutputDistribution::RoundRobinRow { channels } => {
                TaskOutputDistribution::RoundRobinRow {
                    channels: *channels,
                }
            }
        };
        let locator = match self.stage.mode {
            OutputMode::Pipelined => TaskOutputLocator::Pipelined {
                replicas: self.job.graph.replicas(self.key.stage),
            },
            OutputMode::Blocking => TaskOutputLocator::Blocking {
                collect_statistics: self.collect_statistics,
            },
        };
        Ok(TaskOutput {
            distribution,
            locator,
        })
    }
}

#[derive(PartialEq, Eq, Hash)]
struct StageGroupKey {
    placement: TaskPlacement,
    group: String,
}

struct StageGroup {
    partition_offsets: HashMap<usize, usize>,
    buckets: Vec<Vec<TaskSetEntry>>,
}

impl StageGroup {
    fn new(stage_partitions: IndexMap<usize, usize>) -> Self {
        let count = stage_partitions.values().copied().max().unwrap_or(0);
        let mut partition_offsets = HashMap::new();
        let mut offset = 0;
        for (stage, partitions) in stage_partitions {
            if partitions > 0 {
                partition_offsets.insert(stage, offset);
                offset = (offset + partitions) % count;
            }
        }
        Self {
            partition_offsets,
            buckets: vec![vec![]; count],
        }
    }

    fn add_task(&mut self, entry: TaskSetEntry) {
        // Place each stage consecutively, continuing from the preceding stage's offset.
        // Full regions have bucket sizes differing by at most one, with no two partitions
        // of a stage sharing a bucket. For partition-sliced forward regions, all stages
        // have the same partition count, so offsets are zero and matching partitions
        // continue to share a bucket.
        let bucket =
            (self.partition_offsets[&entry.key.stage] + entry.key.partition) % self.buckets.len();
        self.buckets[bucket].push(entry);
    }
}

#[cfg(test)]
pub(crate) mod tests_support {
    use std::sync::Arc;

    use super::{JobDescriptor, TaskInputBuilder};
    use crate::id::TaskKey;
    use crate::job_graph::StageInput;
    use crate::task::definition::TaskInputLocator;
    use crate::task::scheduling::TaskAssignmentGetter;

    /// Builds the input locator of a task, as the scheduler does at launch.
    pub(crate) fn build_input_locator(
        job: &JobDescriptor,
        key: &TaskKey,
        input: &StageInput,
        assignments: &dyn TaskAssignmentGetter,
    ) -> Arc<TaskInputLocator> {
        #[expect(clippy::unwrap_used)]
        TaskInputBuilder::try_new(job, key, input, assignments)
            .and_then(|builder| builder.build())
            .unwrap()
            .locator
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::StageGroup;
    use crate::id::{JobId, TaskKey};
    use crate::task::scheduling::{TaskOutputKind, TaskSetEntry};

    #[test]
    fn forward_routing_uses_only_assigned_consumers_and_refreshes_producer_attempts()
    -> crate::error::ExecutionResult<()> {
        use std::collections::HashMap;
        use std::sync::Arc;

        use datafusion::arrow::datatypes::Schema;
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::execution::TaskContext;
        use datafusion::physical_expr::Partitioning;
        use datafusion::physical_plan::repartition::RepartitionExec;

        use super::{JobDescriptor, JobState, TaskAttemptDescriptor, TaskInputBuilder, TaskState};
        use crate::id::WorkerId;
        use crate::job_graph::{InputMode, JobGraph, JobGraphOptions, StageInput};
        use crate::shuffle::{ShuffleBackendKind, ShuffleCompression};
        use crate::task::definition::TaskInputLocator;
        use crate::task::scheduling::{TaskAssignment, TaskAssignmentGetter};

        struct Assignments(HashMap<TaskKey, TaskAssignment>);
        impl TaskAssignmentGetter for Assignments {
            fn get(&self, key: &TaskKey) -> Option<&TaskAssignment> {
                self.0.get(key)
            }
        }
        let input =
            MemorySourceConfig::try_new_exec(&vec![vec![]; 4], Arc::new(Schema::empty()), None)?;
        let graph = JobGraph::try_new(
            Arc::new(RepartitionExec::try_new(
                input,
                Partitioning::RoundRobinBatch(4),
            )?),
            JobGraphOptions {
                shuffle_backend: ShuffleBackendKind::Flight {
                    compression: ShuffleCompression::None,
                    connection_count: std::num::NonZeroUsize::MIN,
                    initial_stream_window_size: None,
                    initial_connection_window_size: None,
                },
            },
        )?;
        let mut job =
            JobDescriptor::try_new(graph, JobState::Draining, Arc::new(TaskContext::default()))?;
        let attempt = || TaskAttemptDescriptor {
            state: TaskState::Created,
            messages: vec![],
            cause: None,
            job_output_fetched: false,
            statistics: None,
        };
        for stage in &mut job.stages {
            for task in &mut stage.tasks {
                task.attempts.push(attempt());
            }
        }
        let key = |stage, partition, attempt| TaskKey {
            job_id: JobId::from(1),
            stage,
            partition,
            attempt,
        };
        let worker = |id| TaskAssignment::Worker {
            worker_id: WorkerId::from(id),
            slot: 0,
        };
        let mut assignments = Assignments(HashMap::new());
        for stage in 0..2 {
            for partition in 0..2 {
                assignments.0.insert(key(stage, partition, 0), worker(1));
            }
        }
        // Exercise the routing builder with a forward edge of the same partition shape.
        // Partitions 2 and 3 have attempts, but their regions have no assignments yet.
        let edge = StageInput::new(0, InputMode::Forward);
        let consumer = key(1, 0, 0);
        let original = TaskInputBuilder::try_new(&job, &consumer, &edge, &assignments)?.build()?;
        let TaskInputLocator::Worker { keys } = original.locator.as_ref() else {
            return Err(crate::error::ExecutionError::InternalError(
                "expected worker routing".into(),
            ));
        };
        assert_eq!(keys.len(), 4);
        assert!(!keys[0].is_empty());
        assert!(!keys[1].is_empty());
        assert!(keys[2].is_empty());
        assert!(keys[3].is_empty());

        job.stages[0].tasks[0].attempts.push(attempt());
        assignments.0.insert(key(0, 0, 1), worker(2));
        let retried = TaskInputBuilder::try_new(&job, &consumer, &edge, &assignments)?.build()?;
        let TaskInputLocator::Worker { keys: updated } = retried.locator.as_ref() else {
            return Err(crate::error::ExecutionError::InternalError(
                "expected worker routing".into(),
            ));
        };
        assert!(
            updated[0]
                .iter()
                .all(|(worker, key)| *worker == WorkerId::from(2) && key.attempt == 1)
        );
        assert!(
            keys[0]
                .iter()
                .all(|(worker, key)| *worker == WorkerId::from(1) && key.attempt == 0)
        );

        // Missing routing for an assigned consumer is still an error, not an empty input.
        assignments.0.remove(&key(0, 0, 1));
        assert!(
            TaskInputBuilder::try_new(&job, &consumer, &edge, &assignments)?
                .build()
                .is_err()
        );
        Ok(())
    }

    fn task(stage: usize, partition: usize) -> TaskSetEntry {
        TaskSetEntry {
            key: TaskKey {
                job_id: JobId::from(1),
                stage,
                partition,
                attempt: 0,
            },
            output: TaskOutputKind::Local,
        }
    }

    #[test]
    fn stage_group_balances_buckets_without_sharing_stage_partitions() {
        for partitions in [
            vec![4, 4, 8],
            vec![256, 256, 256, 768],
            vec![5, 3, 7],
            vec![8, 4, 4],
            vec![4, 8, 4],
        ] {
            let mut group = StageGroup::new(partitions.iter().copied().enumerate().collect());
            // Task insertion order must not affect the bucket offsets.
            for (stage, count) in partitions.iter().copied().enumerate().rev() {
                for partition in (0..count).rev() {
                    group.add_task(task(stage, partition));
                }
            }

            let count = partitions.iter().copied().max().unwrap_or(0);
            let total = partitions.iter().sum::<usize>();
            assert_eq!(group.buckets.len(), count);
            let mut tasks = HashSet::new();
            for bucket in &group.buckets {
                assert!((total / count..=total.div_ceil(count)).contains(&bucket.len()));
                let stages = bucket
                    .iter()
                    .map(|entry| entry.key.stage)
                    .collect::<HashSet<_>>();
                assert_eq!(stages.len(), bucket.len());
                for entry in bucket {
                    assert!(tasks.insert((entry.key.stage, entry.key.partition)));
                }
            }
            assert_eq!(tasks.len(), total);
        }
    }

    #[test]
    fn stage_group_preserves_partition_sliced_forward_regions() {
        for partition in 0..4 {
            let mut group = StageGroup::new([(0, 4), (1, 4), (2, 4)].into());
            for stage in 0..3 {
                group.add_task(task(stage, partition));
            }
            assert_eq!(group.buckets[partition].len(), 3);
            assert_eq!(
                group
                    .buckets
                    .iter()
                    .filter(|bucket| !bucket.is_empty())
                    .count(),
                1
            );
        }
    }
}
