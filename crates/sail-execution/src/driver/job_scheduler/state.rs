use std::sync::Arc;
use std::time::Duration;

use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlanProperties;
use sail_common_datafusion::error::CommonErrorCause;

use crate::driver::job_scheduler::topology::JobTopology;
use crate::driver::output::{JobOutputManager, JobOutputOutcome};
use crate::error::ExecutionResult;
use crate::job_graph::JobGraph;
use crate::statistics::{ShuffleStatistics, TaskOutputStatistics};

/// Tracks graph/topology and runtime state for a single job.
pub struct JobDescriptor {
    pub context: Arc<TaskContext>,
    pub graph: JobGraph,
    pub topology: JobTopology,
    pub stages: Vec<StageDescriptor>,
    pub regions: Vec<TaskRegionDescriptor>,
    pub state: JobState,
    pub adaptive: AdaptiveMetrics,
}

/// Overhead of adaptive query execution for one job.
#[derive(Debug, Default)]
pub struct AdaptiveMetrics {
    /// Time spent re-planning stages.
    pub replanning_time: Duration,
    /// The number of stages whose plans were rewritten.
    pub replanned_stages: usize,
    /// Bytes of compact statistics received with task reports.
    pub statistics_payload_bytes: u64,
    /// The number of bytes currently retained for statistics.
    pub retained_statistics_bytes: u64,
    /// The largest number of bytes retained for statistics at any time.
    pub peak_retained_statistics_bytes: u64,
}

pub enum JobState {
    Running { output: JobOutputManager },
    Draining,
    Succeeded,
    Failed,
    Canceled,
}

impl JobState {
    pub fn finish_output(&mut self, outcome: JobOutputOutcome) {
        match self {
            JobState::Running { .. } | JobState::Draining => {
                *self = match outcome {
                    JobOutputOutcome::Completed => JobState::Succeeded,
                    JobOutputOutcome::Failed => JobState::Failed,
                    JobOutputOutcome::Canceled => JobState::Canceled,
                };
            }
            JobState::Succeeded | JobState::Failed | JobState::Canceled => {}
        }
    }

    pub fn status(&self) -> &'static str {
        match self {
            JobState::Running { .. } => "RUNNING",
            JobState::Draining => "DRAINING",
            JobState::Succeeded => "SUCCEEDED",
            JobState::Failed => "FAILED",
            JobState::Canceled => "CANCELED",
        }
    }
}

impl JobDescriptor {
    pub fn try_new(
        graph: JobGraph,
        state: JobState,
        context: Arc<TaskContext>,
    ) -> ExecutionResult<Self> {
        let mut stages = vec![];
        for stage in graph.stages().iter() {
            let mut descriptor = StageDescriptor {
                tasks: vec![],
                state: StageState::Active,
                commit: None,
                adaptive: AdaptiveDecision::Pending,
                statistics_reported: false,
                retained_statistics_bytes: 0,
                shuffle_statistics: None,
            };
            for _ in 0..stage.plan.output_partitioning().partition_count() {
                descriptor.tasks.push(TaskDescriptor { attempts: vec![] });
            }
            stages.push(descriptor);
        }
        let topology = JobTopology::try_new(&graph)?;
        let regions = (0..topology.regions.len())
            .map(|_| TaskRegionDescriptor {
                state: TaskRegionState::Running,
            })
            .collect();
        Ok(Self {
            context,
            graph,
            topology,
            stages,
            regions,
            state,
            adaptive: AdaptiveMetrics::default(),
        })
    }
}

#[derive(Debug)]
pub struct StageDescriptor {
    /// A list of tasks for each partition of the stage.
    pub tasks: Vec<TaskDescriptor>,
    pub state: StageState,
    /// The output that the shuffle backend committed for this stage, as reported by
    /// the backend itself. This is set only for backends whose committed attempt can
    /// differ from the scheduler's latest successful attempt (remote shuffle services).
    pub commit: Option<ShuffleCommit>,
    /// The adaptive decision for the stage, made once before any of its tasks run.
    pub adaptive: AdaptiveDecision,
    /// Whether the committed output statistics of the stage have been reported.
    pub statistics_reported: bool,
    /// The number of bytes retained for the statistics of the stage's attempts.
    pub retained_statistics_bytes: u64,
    /// The statistics of the committed attempts folded per bucket, cached so that
    /// every consumer shares one fold. Cleared when the commit changes or the
    /// statistics are released.
    pub shuffle_statistics: Option<Arc<ShuffleStatistics>>,
}

/// The output that a shuffle backend committed for a stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShuffleCommit {
    /// The attempt whose output the backend serves, for each producer partition.
    pub attempts: Vec<usize>,
    /// Whether the global commit succeeded. The output of a failed commit is lost:
    /// reading it fails, and it is never used for adaptive decisions.
    pub succeeded: bool,
}

/// The adaptive decision for a stage.
/// A decision is made before any region of the stage has an attempt,
/// and it never changes afterwards, so that retries reuse it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveDecision {
    /// No decision has been made yet.
    Pending,
    /// The stage runs as planned, either because it was not eligible or because
    /// no rule applied.
    Static,
    /// The stage was re-planned.
    Replanned,
}

impl AdaptiveDecision {
    pub fn is_frozen(&self) -> bool {
        !matches!(self, AdaptiveDecision::Pending)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum StageState {
    /// The tasks in the stage are not yet completed,
    /// or the task streams are still being consumed.
    Active,
    /// The tasks in the stage will not be scheduled anymore,
    /// and the task streams are no longer being consumed.
    Inactive,
}

impl StageState {
    pub fn status(&self) -> &'static str {
        match self {
            StageState::Active => "ACTIVE",
            StageState::Inactive => "INACTIVE",
        }
    }
}

#[derive(Debug)]
pub struct TaskRegionDescriptor {
    pub state: TaskRegionState,
}

#[derive(Debug, Clone, Copy)]
pub enum TaskRegionState {
    Running,
    Failed,
    Succeeded,
}

#[derive(Debug)]
pub struct TaskDescriptor {
    pub attempts: Vec<TaskAttemptDescriptor>,
}

#[derive(Debug)]
pub struct TaskAttemptDescriptor {
    pub state: TaskState,
    pub messages: Vec<String>,
    pub cause: Option<CommonErrorCause>,
    /// Whether the task streams are fetched for the job output.
    /// This will always be false if the task does not belong to
    /// the final stages of the job.
    pub job_output_fetched: bool,
    /// The committed output statistics reported with the succeeded status.
    /// They are released once every consumer of the stage has made its
    /// adaptive decision.
    pub statistics: Option<Arc<TaskOutputStatistics>>,
}

#[derive(Debug, Clone, Copy)]
pub enum TaskState {
    /// The task attempt has been created, but is not assigned to any worker.
    /// A task attempt is only created when the task region is eligible for scheduling.
    Created,
    /// The task attempt is scheduled to a worker, but its status is unknown.
    Scheduled,
    /// The task attempt is running on a worker.
    Running,
    /// The task attempt has succeeded.
    Succeeded,
    /// The task attempt has failed.
    Failed,
    /// The task attempt has been canceled.
    Canceled,
}

impl TaskState {
    pub fn status(&self) -> &'static str {
        match self {
            TaskState::Created => "CREATED",
            TaskState::Scheduled => "SCHEDULED",
            TaskState::Running => "RUNNING",
            TaskState::Succeeded => "SUCCEEDED",
            TaskState::Failed => "FAILED",
            TaskState::Canceled => "CANCELED",
        }
    }

    pub fn consolidate(&self, next: Self) -> Self {
        match (self, next) {
            (TaskState::Created, x) => x,
            (TaskState::Scheduled, TaskState::Created) => *self,
            (TaskState::Scheduled, x) => x,
            (TaskState::Running, TaskState::Created | TaskState::Scheduled) => *self,
            (TaskState::Running, x) => x,
            (TaskState::Succeeded | TaskState::Failed | TaskState::Canceled, _) => *self,
        }
    }

    pub fn is_terminal(&self) -> bool {
        match self {
            TaskState::Succeeded | TaskState::Failed | TaskState::Canceled => true,
            TaskState::Created | TaskState::Scheduled | TaskState::Running => false,
        }
    }
}
