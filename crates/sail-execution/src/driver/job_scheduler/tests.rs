//! Tests of the job scheduler over Celeborn job graphs that are not about adaptive
//! rewrites: committed output, retention and statistics.
#![expect(clippy::unwrap_used)]

use std::sync::Arc;

use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;

use crate::driver::job_scheduler::state::{
    ShuffleCommit, StageState, TaskAttemptDescriptor, TaskState,
};
use crate::driver::job_scheduler::test_utils::{
    MIB, aggregate_graph, celeborn, commit, job, producer_statistics, reporter, schema, succeed,
};
use crate::driver::job_scheduler::{AdaptiveOptions, JobAction, JobScheduler, JobSchedulerOptions};
use crate::id::{JobId, TaskKey};
use crate::job_graph::{JobGraph, JobGraphOptions};
use crate::shuffle::ShuffleBackendKind;
use crate::statistics::ChannelCounts;
use crate::task::scheduling::{TaskAssignment, TaskAssignmentGetter};

#[test]
fn output_is_retained_until_consumer_regions_succeed() {
    let mut job = job(aggregate_graph());
    let options = JobSchedulerOptions::new_for_test(celeborn(), AdaptiveOptions::default(), None);
    succeed(&mut job, 0, producer_statistics);
    // Stage 1 succeeds, but stage 2 in the same region is still running, so a failure
    // of stage 2 would rerun stage 1, which reads stage 0 again.
    succeed(&mut job, 1, |_| ChannelCounts::default());
    job.stages[2].tasks[0].attempts.push(TaskAttemptDescriptor {
        state: TaskState::Running,
        messages: vec![],
        cause: None,
        job_output_fetched: false,
        statistics: None,
    });
    JobScheduler::update_task_regions(&mut job, &options);
    let actions =
        JobScheduler::clean_up_job_by_stage(JobId::from(1), &mut job, &reporter(), "test");
    assert!(actions.is_empty());
    assert!(matches!(job.stages[0].state, StageState::Active));

    job.stages[2].tasks[0].attempts[0].state = TaskState::Succeeded;
    JobScheduler::update_task_regions(&mut job, &options);
    let actions =
        JobScheduler::clean_up_job_by_stage(JobId::from(1), &mut job, &reporter(), "test");
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, JobAction::CleanUpJob { stage: Some(0), .. }))
    );
}

#[test]
fn statistics_are_collected_only_for_adaptive_execution_on_celeborn() {
    let job = job(aggregate_graph());
    let enabled = JobSchedulerOptions::new_for_test(
        celeborn(),
        AdaptiveOptions {
            enabled: true,
            ..AdaptiveOptions::default()
        },
        None,
    );
    assert!(JobScheduler::collects_statistics(&enabled, &job));
    let disabled = JobSchedulerOptions::new_for_test(celeborn(), AdaptiveOptions::default(), None);
    assert!(!JobScheduler::collects_statistics(&disabled, &job));
}

#[test]
fn statistics_are_folded_from_committed_attempts() {
    let mut job = job(aggregate_graph());
    succeed(&mut job, 0, producer_statistics);
    assert!(JobScheduler::committed_statistics(&mut job, 0).is_none());
    commit(&mut job, 0);
    let statistics = JobScheduler::committed_statistics(&mut job, 0).unwrap();
    assert_eq!(statistics.total_rows, 3200);
    assert_eq!(statistics.total_physical_bytes, Some(8 * MIB));
    assert_eq!(statistics.total_logical_bytes, 32 * MIB);
    // The fold is computed once and shared by every consumer.
    let again = JobScheduler::committed_statistics(&mut job, 0).unwrap();
    assert!(Arc::ptr_eq(&statistics, &again));
    assert!(job.stages[0].retained_statistics_bytes >= statistics.retained_bytes() as u64);
}

#[test]
fn pipelined_output_is_released_when_its_region_reruns_the_producer() {
    // Under Flight, every stage of this job is in one region, so a retry reruns the
    // producer and recreates its output. Cleanup timing stays as before.
    let input = MemorySourceConfig::try_new_exec(&vec![vec![]; 4], schema(), None).unwrap();
    let shuffle =
        Arc::new(RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(4)).unwrap());
    let ordering = datafusion::physical_expr::LexOrdering::new(vec![
        datafusion::physical_expr::PhysicalSortExpr::new_default(Arc::new(Column::new("k", 0))),
    ])
    .unwrap();
    let sort = Arc::new(SortExec::new(ordering.clone(), shuffle).with_preserve_partitioning(true));
    let merge = Arc::new(SortPreservingMergeExec::new(ordering, sort));
    let flight = ShuffleBackendKind::Flight {
        compression: crate::shuffle::ShuffleCompression::None,
        connection_count: std::num::NonZeroUsize::MIN,
        initial_stream_window_size: None,
        initial_connection_window_size: None,
    };
    let graph = JobGraph::try_new(
        merge,
        JobGraphOptions {
            shuffle_backend: flight.clone(),
        },
    )
    .unwrap();
    let mut job = job(graph);
    assert_eq!(job.topology.regions.len(), 1);
    let options = JobSchedulerOptions::new_for_test(flight, AdaptiveOptions::default(), None);
    succeed(&mut job, 0, |_| ChannelCounts::default());
    succeed(&mut job, 1, |_| ChannelCounts::default());
    job.stages[2].tasks[0].attempts.push(TaskAttemptDescriptor {
        state: TaskState::Running,
        messages: vec![],
        cause: None,
        job_output_fetched: false,
        statistics: None,
    });
    JobScheduler::update_task_regions(&mut job, &options);
    let actions =
        JobScheduler::clean_up_job_by_stage(JobId::from(1), &mut job, &reporter(), "test");
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, JobAction::CleanUpJob { stage: Some(0), .. }))
    );
}

#[test]
fn failed_celeborn_commit_fails_the_consumer_definition() {
    let mut job = job(aggregate_graph());
    succeed(&mut job, 0, producer_statistics);
    job.stages[0].commit = Some(ShuffleCommit {
        attempts: vec![0; 4],
        succeeded: false,
    });
    struct NoAssignments;
    impl TaskAssignmentGetter for NoAssignments {
        fn get(&self, _: &TaskKey) -> Option<&TaskAssignment> {
            None
        }
    }
    let input = job.graph.stages()[1].inputs[0].clone();
    let key = TaskKey {
        job_id: JobId::from(1),
        stage: 1,
        partition: 0,
        attempt: 0,
    };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        crate::driver::job_scheduler::core::tests_support::build_input_locator(
            &job,
            &key,
            &input,
            &NoAssignments,
        )
    }));
    assert!(result.is_err(), "reading an uncommitted shuffle must fail");
}
