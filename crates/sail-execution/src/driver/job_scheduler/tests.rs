//! Tests of the job scheduler over remote shuffle service job graphs: committed output and retention.
#![expect(clippy::unwrap_used)]

use std::sync::Arc;

use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;

use crate::driver::job_scheduler::core::tests_support::build_input_locator;
use crate::driver::job_scheduler::state::{
    ShuffleCommit, StageState, TaskAttemptDescriptor, TaskState,
};
use crate::driver::job_scheduler::test_utils::{
    NoAssignments, aggregate_graph, commit, job, remote_shuffle_service, reporter, schema, succeed,
};
use crate::driver::job_scheduler::{JobAction, JobScheduler, JobSchedulerOptions};
use crate::id::{JobId, TaskKey};
use crate::job_graph::{JobGraph, JobGraphOptions};
use crate::shuffle::ShuffleBackendKind;
use crate::task::definition::TaskInputLocator;

#[test]
fn output_is_retained_until_consumer_regions_succeed() {
    let mut job = job(aggregate_graph());
    let options = JobSchedulerOptions::new_for_test(remote_shuffle_service());
    succeed(&mut job, 0);
    // Stage 1 succeeds, but stage 2 in the same region is still running, so a failure
    // of stage 2 would rerun stage 1, which reads stage 0 again.
    succeed(&mut job, 1);
    job.stages[2].tasks[0].attempts.push(TaskAttemptDescriptor {
        state: TaskState::Running,
        messages: vec![],
        cause: None,
        job_output_fetched: false,
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
    let options = JobSchedulerOptions::new_for_test(flight);
    succeed(&mut job, 0);
    succeed(&mut job, 1);
    job.stages[2].tasks[0].attempts.push(TaskAttemptDescriptor {
        state: TaskState::Running,
        messages: vec![],
        cause: None,
        job_output_fetched: false,
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
fn committed_map_attempts_are_passed_to_readers() {
    let mut job = job(aggregate_graph());
    succeed(&mut job, 0);
    commit(&mut job, 0);
    let input = job.graph.stages()[1].inputs[0].clone();
    let key = TaskKey {
        job_id: JobId::from(1),
        stage: 1,
        partition: 0,
        attempt: 0,
    };
    let locator = build_input_locator(&job, &key, &input, &NoAssignments);
    // Readers skip the output of any other attempt, e.g. partial data of a failed one.
    assert!(matches!(
        locator.as_ref(),
        TaskInputLocator::ShuffleService {
            map_attempts: Some(attempts),
            ..
        } if attempts.as_ref() == [0, 0, 0, 0]
    ));
}

#[test]
fn failed_shuffle_commit_fails_the_consumer_definition() {
    let mut job = job(aggregate_graph());
    succeed(&mut job, 0);
    job.stages[0].commit = Some(ShuffleCommit {
        attempts: vec![0; 4],
        succeeded: false,
    });
    let input = job.graph.stages()[1].inputs[0].clone();
    let key = TaskKey {
        job_id: JobId::from(1),
        stage: 1,
        partition: 0,
        attempt: 0,
    };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        build_input_locator(&job, &key, &input, &NoAssignments)
    }));
    assert!(result.is_err(), "reading an uncommitted shuffle must fail");
}

#[test]
fn shuffle_commit_is_recorded_once() {
    let options = JobSchedulerOptions::new_for_test(remote_shuffle_service());
    let mut scheduler = JobScheduler::new(options, reporter());
    let job_id = JobId::from(1);
    let mut descriptor = job(aggregate_graph());
    succeed(&mut descriptor, 0);
    scheduler.jobs.insert(job_id, descriptor);

    scheduler.record_shuffle_commit(job_id, 0, vec![0; 4], Err("worker failed".to_string()));
    // A shuffle service never revises a commit. A later report must not turn a lost
    // shuffle into a readable one.
    scheduler.record_shuffle_commit(job_id, 0, vec![0; 4], Ok(()));
    assert_eq!(
        scheduler.jobs[&job_id].stages[0].commit,
        Some(ShuffleCommit {
            attempts: vec![0; 4],
            succeeded: false,
        })
    );
}

#[test]
fn unknown_shuffle_commit_fails_the_consumer_definition() {
    let mut job = job(aggregate_graph());
    succeed(&mut job, 0);
    job.stages[0].commit = None;
    let input = job.graph.stages()[1].inputs[0].clone();
    let key = TaskKey {
        job_id: JobId::from(1),
        stage: 1,
        partition: 0,
        attempt: 0,
    };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        build_input_locator(&job, &key, &input, &NoAssignments)
    }));
    assert!(
        result.is_err(),
        "reading without the committed attempts must fail"
    );
}
