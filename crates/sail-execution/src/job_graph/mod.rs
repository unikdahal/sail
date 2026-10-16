mod planner;

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::physical_expr::{PhysicalExpr, RangePartitioning};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use crate::shuffle::ShuffleBackendKind;

#[derive(Debug, Clone)]
pub struct JobGraphOptions {
    pub shuffle_backend: ShuffleBackendKind,
}

/// A job graph represents a distributed execution plan for a job.
/// A job consists of multiple *stages*, where each stage has one or more
/// *partitions*. There are *tasks* which each corresponds to the execution of a single partition
/// of a stage and can have multiple *attempts*.
/// Each task produces output split into multiple *channels*.
#[derive(Debug)]
pub struct JobGraph {
    /// A list of stages sorted in topological order.
    /// For any stage, all its input stages are guaranteed to
    /// appear before it in the list.
    stages: Vec<Stage>,
    /// The output schema of the job.
    schema: SchemaRef,
    options: JobGraphOptions,
}

impl JobGraph {
    pub fn stages(&self) -> &[Stage] {
        &self.stages
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub(crate) fn shuffle_backend(&self) -> &ShuffleBackendKind {
        &self.options.shuffle_backend
    }

    /// Returns a copy of the graph with the given stages, for an adaptive rewrite
    /// that is validated before it replaces the current graph.
    pub(crate) fn with_stages(&self, stages: Vec<Stage>) -> Self {
        Self {
            stages,
            schema: self.schema.clone(),
            options: self.options.clone(),
        }
    }

    /// Get the required number of output replicas for the given stage.
    pub fn replicas(&self, stage: usize) -> usize {
        let replicas = self
            .stages
            .iter()
            .flat_map(|x| {
                x.inputs
                    .iter()
                    .filter(|input| input.stage == stage)
                    .map(|input| match input.mode {
                        InputMode::Forward | InputMode::Shuffle | InputMode::Rescale { .. } => 1,
                        InputMode::Merge | InputMode::Broadcast => {
                            x.plan.output_partitioning().partition_count()
                        }
                    })
            })
            .sum::<usize>();
        // ensure one replica for final stages for the job output
        replicas.max(1)
    }
}

impl fmt::Display for JobGraph {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (i, stage) in self.stages.iter().enumerate() {
            let displayable = DisplayableExecutionPlan::new(stage.plan.as_ref());
            writeln!(f, "=== stage {i} ===")?;
            writeln!(
                f,
                "inputs=[{}]",
                stage
                    .inputs
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
            if !stage.group.is_empty() {
                writeln!(f, "group={}", stage.group)?;
            }
            writeln!(f, "mode={}", stage.mode)?;
            writeln!(
                f,
                "partitions={}",
                stage.plan.output_partitioning().partition_count()
            )?;
            writeln!(f, "distribution={}", stage.distribution)?;
            writeln!(f, "placement={}", stage.placement)?;
            writeln!(f, "{}", displayable.indent(true))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Stage {
    pub inputs: Vec<StageInput>,
    pub plan: Arc<dyn ExecutionPlan>,
    /// The name of the "slot sharing group" for the stage.
    pub group: String,
    pub mode: OutputMode,
    pub distribution: OutputDistribution,
    pub placement: TaskPlacement,
    pub kind: StageKind,
}

/// The role of a stage in the job graph, so that later rewrites never need to
/// infer it from the plan shape.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum StageKind {
    /// A stage that runs a part of the query plan.
    Compute,
    /// A stage that collects one shuffle partition into storage files
    /// (the `storage` shuffle backend).
    ShuffleMerge,
    /// A stage that must run on the driver.
    Driver,
}

/// Specifies whether a task must run on the driver or on any available worker node.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum TaskPlacement {
    Driver,
    Worker,
}

impl fmt::Display for TaskPlacement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TaskPlacement::Driver => write!(f, "Driver"),
            TaskPlacement::Worker => write!(f, "Worker"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StageInput {
    pub stage: usize,
    pub mode: InputMode,
    /// Who chose the partitioning of the shuffle feeding this input.
    pub origin: ShuffleOrigin,
    /// How the buckets of this input are addressed.
    pub layout: BucketLayout,
    /// The partition spec of this input, set only by a committed adaptive rewrite.
    /// Without it, the input is read exactly as its mode describes.
    pub adaptive: Option<Arc<AdaptiveInput>>,
}

impl StageInput {
    pub fn new(stage: usize, mode: InputMode) -> Self {
        Self {
            stage,
            mode,
            origin: ShuffleOrigin::Engine,
            layout: BucketLayout::Opaque,
            adaptive: None,
        }
    }

    pub fn with_origin(mut self, origin: ShuffleOrigin) -> Self {
        self.origin = origin;
        self
    }

    pub fn with_layout(mut self, layout: BucketLayout) -> Self {
        self.layout = layout;
        self
    }
}

impl fmt::Display for StageInput {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StageInput(stage={}, mode={}", self.stage, self.mode)?;
        if let Some(adaptive) = &self.adaptive {
            write!(f, ", adaptive={adaptive}")?;
        }
        write!(f, ")")
    }
}

/// Who chose the partitioning of a shuffle.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ShuffleOrigin {
    /// The shuffle was inserted by the engine to satisfy a distribution requirement.
    Engine,
    /// The user chose the partitioning explicitly, e.g. with `repartition(n, ...)`.
    /// Adaptive rules keep such partition counts.
    User,
}

/// How the buckets of a stage input are addressed.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum BucketLayout {
    /// Bucket `b` is channel `b` of every producer partition,
    /// e.g. a shuffle stored by a remote shuffle service.
    Direct,
    /// Bucket `b` is channel 0 of storage merge partition `b`.
    Merged,
    /// The input is not addressable by bucket: broadcast, forward, rescale or merge inputs.
    Opaque,
}

/// The adaptive partition spec of one stage input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveInput {
    /// One entry per partition of this input's own domain. Each entry lists the
    /// bucket slices that partition reads.
    pub spec: Vec<Vec<BucketSlice>>,
    pub property: AdaptiveProperty,
}

impl AdaptiveInput {
    pub fn partitions(&self) -> usize {
        self.spec.len()
    }
}

impl fmt::Display for AdaptiveInput {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ranges = self
            .spec
            .iter()
            .map(|slices| {
                slices
                    .iter()
                    .map(|slice| slice.to_string())
                    .collect::<Vec<_>>()
                    .join("+")
            })
            .collect::<Vec<_>>();
        write!(f, "{}[{}]", self.property, ranges.join(", "))
    }
}

/// A contiguous range of buckets, or a part of one bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketSlice {
    pub buckets: Range<usize>,
    /// A part of a single bucket, used by skew splits. Whole buckets have none.
    pub part: Option<SubRange>,
}

impl BucketSlice {
    pub fn whole(buckets: Range<usize>) -> Self {
        Self {
            buckets,
            part: None,
        }
    }

    pub fn producers(bucket: usize, producers: Range<usize>) -> Self {
        Self {
            buckets: bucket..bucket + 1,
            part: Some(SubRange::Producers(producers)),
        }
    }
}

impl fmt::Display for BucketSlice {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.buckets.len() == 1 {
            write!(f, "{}", self.buckets.start)?;
        } else {
            write!(f, "{}..{}", self.buckets.start, self.buckets.end)?;
        }
        match &self.part {
            None => Ok(()),
            Some(SubRange::Producers(r)) => write!(f, "@producers({}..{})", r.start, r.end),
        }
    }
}

/// A part of a single bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubRange {
    /// The output of a range of producer partitions (`Direct` layout), read with a
    /// map-range read from the shuffle service.
    Producers(Range<usize>),
}

/// The partitioning property an adaptive input actually provides.
/// The stage input itself declares `UnknownPartitioning` to DataFusion,
/// so that no DataFusion operator relies on a partitioning that is false.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdaptiveProperty {
    /// Each partition holds the rows of a contiguous range of hash buckets of the
    /// original hash partitioning, so every key is in exactly one partition.
    /// `boundaries` has one start bucket per partition, then the end bucket.
    CoalescedHash {
        /// The original number of hash buckets.
        from: usize,
        boundaries: Vec<usize>,
    },
    /// No partitioning property is provided, e.g. after a skew split, where a bucket
    /// is split across partitions or read by several partitions.
    Unknown,
}

impl fmt::Display for AdaptiveProperty {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AdaptiveProperty::CoalescedHash { from, boundaries } => write!(
                f,
                "CoalescedHash(from={from}, partitions={})",
                boundaries.len().saturating_sub(1)
            ),
            AdaptiveProperty::Unknown => write!(f, "Unknown"),
        }
    }
}

/// Usually, when partition `p` is executed for the stage, all the child physical plan nodes
/// are executed recursively with the same partition `p`, but this is not always the case.
/// So we introduce input mode to describe the expectation how stage inputs (which are
/// leaf nodes of the physical plan) are executed and which partitions and channels are read
/// from the dependent stages.
#[derive(Debug, Clone, Copy)]
pub enum InputMode {
    /// For each partition in the current stage, execute the same partition to fetch the input
    /// which reads all channels from the corresponding partition in the input stage.
    Forward,
    /// For each partition in the current stage, execute all partitions to fetch the input
    /// which each reads all channels from the corresponding partition in the input stage.
    Merge,
    /// For each partition in the current stage, execute the same partition to fetch the input
    /// which reads data from the corresponding channel of all partitions in the input stage.
    Shuffle,
    /// For each partition in the current stage, execute a single partition to fetch the input
    /// which reads all channels from all partitions in the input stage.
    Broadcast,
    /// For each partition of this stage input, execute a contiguous subset of producer
    /// partitions and read all channels from each. The target partition count belongs to
    /// this input, which may differ from the containing stage (e.g. under a union).
    Rescale { partitions: usize },
}

impl fmt::Display for InputMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            InputMode::Forward => write!(f, "Forward"),
            InputMode::Merge => write!(f, "Merge"),
            InputMode::Shuffle => write!(f, "Shuffle"),
            InputMode::Broadcast => write!(f, "Broadcast"),
            InputMode::Rescale { partitions } => write!(f, "Rescale(partitions={partitions})"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum OutputMode {
    Pipelined,
    Blocking,
}

impl fmt::Display for OutputMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OutputMode::Pipelined => write!(f, "Pipelined"),
            OutputMode::Blocking => write!(f, "Blocking"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum OutputDistribution {
    Hash {
        keys: Vec<Arc<dyn PhysicalExpr>>,
        channels: usize,
    },
    Range {
        partitioning: RangePartitioning,
    },
    RoundRobinBatch {
        channels: usize,
    },
    /// Row-level round-robin distribution for explicit user repartition calls.
    /// Unlike `RoundRobinBatch`, this distributes individual rows across
    /// output partitions to ensure even data distribution.
    RoundRobinRow {
        channels: usize,
    },
}

impl OutputDistribution {
    pub fn channels(&self) -> usize {
        match self {
            OutputDistribution::Hash { channels, .. } => *channels,
            OutputDistribution::Range { partitioning } => partitioning.partition_count(),
            OutputDistribution::RoundRobinBatch { channels } => *channels,
            OutputDistribution::RoundRobinRow { channels } => *channels,
        }
    }
}

impl fmt::Display for OutputDistribution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutputDistribution::Hash { keys, channels } => {
                let keys = keys.iter().map(|k| k.to_string()).collect::<Vec<_>>();
                write!(f, "Hash(keys=[{}], channels={})", keys.join(", "), channels)
            }
            OutputDistribution::Range { partitioning } => write!(f, "{partitioning}"),
            OutputDistribution::RoundRobinBatch { channels } => {
                write!(f, "RoundRobinBatch(channels={})", channels)
            }
            OutputDistribution::RoundRobinRow { channels } => {
                write!(f, "RoundRobinRow(channels={})", channels)
            }
        }
    }
}
