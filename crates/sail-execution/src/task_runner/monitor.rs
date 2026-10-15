use std::any::Any;

use datafusion::execution::SendableRecordBatchStream;
use fastrace::Span;
use fastrace::future::FutureExt;
use futures::StreamExt;
use log::warn;
use sail_common::actor::ActorHandle;
use sail_common::telemetry::SpanAttribute;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use tokio::sync::oneshot;
use tokio_util::task::AbortOnDropHandle;

use crate::driver::TaskStatus;
use crate::id::{TaskKey, TaskKeyDisplay};
use crate::statistics::TaskOutputStatistics;
use crate::task_runner::{TaskRunnerActor, TaskRunnerMessage};

pub struct TaskMonitor {
    handle: ActorHandle<TaskRunnerActor>,
    key: TaskKey,
    stream: SendableRecordBatchStream,
    signal: oneshot::Receiver<()>,
}

impl TaskMonitor {
    pub fn new(
        handle: ActorHandle<TaskRunnerActor>,
        key: TaskKey,
        stream: SendableRecordBatchStream,
        signal: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            handle,
            key,
            stream,
            signal,
        }
    }

    pub async fn run(self) {
        let Self {
            handle,
            key,
            stream,
            signal,
        } = self;
        let _ = handle.send(Self::running(key.clone())).await;
        let message = tokio::select! {
            x = Self::execute(key.clone(), stream) => x,
            x = Self::cancel(key.clone(), signal) => x,
        };
        let _ = handle.send(message).await;
    }

    pub async fn supervise(self) {
        let handle = self.handle.clone();
        let key = self.key.clone();
        // Admission now has one span per batch. Preserve task identity on the monitor
        // so preparation and execution spans can still be attributed to each attempt.
        let span = Span::enter_with_local_parent("TaskMonitor::run").with_properties(|| {
            [
                (SpanAttribute::EXECUTION_JOB_ID, key.job_id.to_string()),
                (SpanAttribute::EXECUTION_STAGE, key.stage.to_string()),
                (
                    SpanAttribute::EXECUTION_PARTITION,
                    key.partition.to_string(),
                ),
                (SpanAttribute::EXECUTION_ATTEMPT, key.attempt.to_string()),
            ]
        });
        let monitor = AbortOnDropHandle::new(tokio::spawn(self.run().in_span(span)));
        if let Some(message) = Self::monitor_failure(key, monitor).await {
            let _ = handle.send(message).await;
        }
    }

    async fn monitor_failure(
        key: TaskKey,
        monitor: AbortOnDropHandle<()>,
    ) -> Option<TaskRunnerMessage> {
        match monitor.await {
            Ok(()) => None,
            Err(error) if error.is_cancelled() => None,
            Err(error) if error.is_panic() => {
                let message = format!("task panicked: {}", Self::panic_message(error.into_panic()));
                Some(Self::status(
                    key,
                    TaskStatus::Failed,
                    Some(message.clone()),
                    Some(CommonErrorCause::Internal(message)),
                ))
            }
            Err(error) => {
                let message = format!("task monitor failed: {error}");
                Some(Self::status(
                    key,
                    TaskStatus::Failed,
                    Some(message.clone()),
                    Some(CommonErrorCause::Internal(message)),
                ))
            }
        }
    }

    /// Builds a "task is running" status message.
    fn running(key: TaskKey) -> TaskRunnerMessage {
        Self::status(key, TaskStatus::Running, None, None)
    }

    fn status(
        key: TaskKey,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
    ) -> TaskRunnerMessage {
        TaskRunnerMessage::ReportTaskStatus {
            key,
            status,
            message,
            cause,
            statistics: None,
        }
    }

    fn succeeded(key: TaskKey, statistics: Option<TaskOutputStatistics>) -> TaskRunnerMessage {
        TaskRunnerMessage::ReportTaskStatus {
            key,
            status: TaskStatus::Succeeded,
            message: None,
            cause: None,
            statistics,
        }
    }

    fn panic_message(payload: Box<dyn Any + Send>) -> String {
        if let Some(message) = payload.downcast_ref::<&str>() {
            (*message).to_string()
        } else if let Some(message) = payload.downcast_ref::<String>() {
            message.clone()
        } else {
            "unknown panic".to_string()
        }
    }

    /// Waits for a cancellation signal and builds a canceled status message.
    async fn cancel(key: TaskKey, signal: oneshot::Receiver<()>) -> TaskRunnerMessage {
        let _ = signal.await;
        Self::status(
            key.clone(),
            TaskStatus::Canceled,
            Some(format!("{} canceled", TaskKeyDisplay(&key))),
            None,
        )
    }

    /// Drains the task stream, whose only data is the completion batch of the
    /// shuffle writer, and attaches the committed output statistics to the status.
    async fn execute(key: TaskKey, mut stream: SendableRecordBatchStream) -> TaskRunnerMessage {
        let mut statistics = None;
        loop {
            let batch = match stream.next().await {
                None => break Self::succeeded(key, statistics),
                Some(Ok(batch)) => batch,
                Some(Err(error)) => {
                    break Self::status(
                        key,
                        TaskStatus::Failed,
                        Some(format!("task error: {error}")),
                        Some(CommonErrorCause::new::<PyErrExtractor>(&error)),
                    );
                }
            };
            // Statistics are advisory, so a malformed completion batch drops them
            // instead of failing a task whose output has been committed.
            match TaskOutputStatistics::try_from_record_batch(&batch) {
                Ok(Some(x)) => statistics = Some(x),
                Ok(None) => {}
                Err(error) => {
                    warn!(
                        "{} ignoring task output statistics: {error}",
                        TaskKeyDisplay(&key)
                    );
                    statistics = None;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::future;

    use super::*;
    use crate::id::JobId;

    fn task_key() -> TaskKey {
        TaskKey {
            job_id: JobId::from(1),
            stage: 2,
            partition: 3,
            attempt: 4,
        }
    }

    #[tokio::test]
    async fn ignores_successful_monitor_completion() {
        let monitor = AbortOnDropHandle::new(tokio::spawn(async {}));

        let message = TaskMonitor::monitor_failure(task_key(), monitor).await;

        assert!(message.is_none());
    }

    #[tokio::test]
    async fn ignores_cancelled_monitor() {
        let monitor = AbortOnDropHandle::new(tokio::spawn(future::pending()));
        monitor.abort();

        let message = TaskMonitor::monitor_failure(task_key(), monitor).await;

        assert!(message.is_none());
    }

    #[tokio::test]
    async fn reports_monitor_panic_as_task_failure() -> Result<(), String> {
        let monitor = AbortOnDropHandle::new(tokio::spawn(async {
            std::panic::resume_unwind(Box::new("test monitor panic"));
        }));

        let message = TaskMonitor::monitor_failure(task_key(), monitor)
            .await
            .ok_or_else(|| "expected a failed task status".to_string())?;

        let TaskRunnerMessage::ReportTaskStatus {
            key,
            status: TaskStatus::Failed,
            message: Some(message),
            cause: Some(CommonErrorCause::Internal(cause)),
            statistics: None,
        } = message
        else {
            return Err("expected a failed task status".to_string());
        };
        assert_eq!(key, task_key());
        assert_eq!(message, "task panicked: test monitor panic");
        assert_eq!(cause, message);
        Ok(())
    }

    #[tokio::test]
    async fn attaches_statistics_from_the_completion_batch() -> Result<(), String> {
        use datafusion::arrow::array::RecordBatch;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

        let statistics = TaskOutputStatistics {
            rows: vec![3, 4],
            physical_bytes: None,
            logical_bytes: vec![30, 40],
        };
        let batch = statistics.to_record_batch().map_err(|e| e.to_string())?;
        let empty = RecordBatch::new_empty(TaskOutputStatistics::schema());
        for (batches, expected) in [(vec![batch], Some(statistics.clone())), (vec![empty], None)] {
            let stream = Box::pin(RecordBatchStreamAdapter::new(
                TaskOutputStatistics::schema(),
                futures::stream::iter(batches.into_iter().map(Ok)),
            ));
            let TaskRunnerMessage::ReportTaskStatus {
                status: TaskStatus::Succeeded,
                statistics,
                ..
            } = TaskMonitor::execute(task_key(), stream).await
            else {
                return Err("expected a succeeded task status".to_string());
            };
            assert_eq!(statistics, expected);
        }
        Ok(())
    }

    #[test]
    fn extracts_panic_messages() {
        assert_eq!(
            TaskMonitor::panic_message(Box::new("literal panic")),
            "literal panic"
        );
        assert_eq!(
            TaskMonitor::panic_message(Box::new("owned panic".to_string())),
            "owned panic"
        );
        assert_eq!(TaskMonitor::panic_message(Box::new(42)), "unknown panic");
    }
}
