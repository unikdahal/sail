use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;

use crate::driver::job_scheduler::AdaptiveOptions;
use crate::job_graph::{AdaptiveInput, BucketLayout, InputMode, ShuffleOrigin};
use crate::statistics::ShuffleStatistics;

/// An immutable snapshot of everything the adaptive rules may read about one stage.
/// Rules are pure functions of this view, so they never touch scheduler state and
/// return the same answer for the same view.
#[derive(Debug, Clone)]
pub(crate) struct StageView {
    pub plan: Arc<dyn ExecutionPlan>,
    /// One entry per stage input, indexed like the placeholders in the plan.
    pub inputs: Vec<InputView>,
    pub options: AdaptiveOptions,
}

impl StageView {
    pub fn statistics(&self, input: usize) -> Option<&ShuffleStatistics> {
        self.inputs.get(input)?.statistics.as_deref()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct InputView {
    pub mode: InputMode,
    pub origin: ShuffleOrigin,
    pub layout: BucketLayout,
    /// Whether the producer writes a hash distribution.
    pub hash_partitioned: bool,
    /// The number of buckets (output channels) of the producer.
    pub buckets: usize,
    /// The committed statistics of the producer, folded per bucket.
    pub statistics: Option<Arc<ShuffleStatistics>>,
}

/// A rewrite proposal for one stage.
#[derive(Debug, Clone)]
pub(crate) struct StageRewrite {
    /// The new stage plan.
    pub plan: Arc<dyn ExecutionPlan>,
    /// The new adaptive spec for each input, or `None` to keep the input as planned.
    pub inputs: Vec<Option<Arc<AdaptiveInput>>>,
    /// Groups of inputs that the plan pairs partition by partition.
    /// Inputs in one group must share bucket boundaries.
    pub groups: Vec<Vec<usize>>,
    /// The names of the rules that fired.
    pub rules: Vec<&'static str>,
    /// Human-readable notes, one per decision.
    pub notes: Vec<String>,
}
