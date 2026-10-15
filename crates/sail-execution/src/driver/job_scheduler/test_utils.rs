//! Helpers for the job scheduler tests.
#![expect(clippy::unwrap_used)]

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use opentelemetry::logs::LoggerProvider;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use sail_celeborn::common::PartitionSplitMode;
use sail_telemetry::events::SystemEventReporter;

use crate::driver::job_scheduler::state::{
    JobDescriptor, JobState, ShuffleCommit, TaskAttemptDescriptor, TaskState,
};
use crate::id::TaskKey;
use crate::job_graph::{JobGraph, JobGraphOptions};
use crate::shuffle::ShuffleBackendKind;
use crate::statistics::{ChannelCounts, TaskOutputStatistics};
use crate::task::scheduling::{TaskAssignment, TaskAssignmentGetter};

pub(crate) const MIB: u64 = 1024 * 1024;

pub(crate) fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]))
}

pub(crate) fn key() -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new("k", 0))
}

pub(crate) fn final_aggregate(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        AggregateExec::try_new(
            AggregateMode::FinalPartitioned,
            PhysicalGroupBy::new_single(vec![(key(), "k".to_string())]),
            vec![],
            vec![],
            input,
            schema(),
        )
        .unwrap(),
    )
}

pub(crate) fn celeborn() -> ShuffleBackendKind {
    ShuffleBackendKind::Celeborn {
        master_endpoints: vec!["localhost:1".to_string()],
        compression: sail_celeborn::common::CompressionCodec::Lz4,
        endpoint_overrides: vec![],
        heartbeat_interval_secs: 10,
        partition_split_threshold: 1_i64 << 30,
        partition_split_mode: PartitionSplitMode::Soft,
    }
}

pub(crate) fn reporter() -> SystemEventReporter {
    SystemEventReporter::new(SdkLoggerProvider::builder().build().logger("test"))
}

/// `SELECT k FROM t GROUP BY k ORDER BY k` shaped as a job graph:
///   stage 0: scan, hash shuffle on `k` into 8 buckets (blocking under Celeborn)
///   stage 1: final aggregate and sort (pipelined)
///   stage 2: sort-preserving merge of stage 1 (merge input)
pub(crate) fn aggregate_graph() -> JobGraph {
    let input = MemorySourceConfig::try_new_exec(&vec![vec![]; 4], schema(), None).unwrap();
    let shuffle =
        Arc::new(RepartitionExec::try_new(input, Partitioning::Hash(vec![key()], 8)).unwrap());
    let aggregate = final_aggregate(shuffle);
    let ordering = datafusion::physical_expr::LexOrdering::new(vec![
        datafusion::physical_expr::PhysicalSortExpr::new_default(Arc::new(Column::new("k", 0))),
    ])
    .unwrap();
    let sort =
        Arc::new(SortExec::new(ordering.clone(), aggregate).with_preserve_partitioning(true));
    let merge = Arc::new(SortPreservingMergeExec::new(ordering, sort));
    JobGraph::try_new(
        merge,
        JobGraphOptions {
            shuffle_backend: celeborn(),
        },
    )
    .unwrap()
}

pub(crate) fn succeed(
    job: &mut JobDescriptor,
    stage: usize,
    statistics: impl Fn(usize) -> ChannelCounts,
) {
    for (p, task) in job.stages[stage].tasks.iter_mut().enumerate() {
        task.attempts.push(TaskAttemptDescriptor {
            state: TaskState::Succeeded,
            messages: vec![],
            cause: None,
            job_output_fetched: false,
            statistics: Some(Arc::new(TaskOutputStatistics::new(&statistics(p)).unwrap())),
        });
    }
}

pub(crate) fn producer_statistics(_: usize) -> ChannelCounts {
    ChannelCounts {
        rows: vec![100; 8],
        physical_bytes: Some(vec![MIB / 4; 8]),
        logical_bytes: vec![MIB; 8],
    }
}

pub(crate) fn job(graph: JobGraph) -> JobDescriptor {
    JobDescriptor::try_new(graph, JobState::Draining, Arc::new(TaskContext::default())).unwrap()
}

pub(crate) fn commit(job: &mut JobDescriptor, stage: usize) {
    let partitions = job.stages[stage].tasks.len();
    job.stages[stage].commit = Some(ShuffleCommit {
        attempts: vec![0; partitions],
        succeeded: true,
    });
}

/// A task assignment getter for tasks that have not been assigned.
pub(crate) struct NoAssignments;

impl TaskAssignmentGetter for NoAssignments {
    fn get(&self, _: &TaskKey) -> Option<&TaskAssignment> {
        None
    }
}
