use std::fmt;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use futures::future::try_join_all;

use crate::statistics::{ChannelCounter, ChannelCounts};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStreamWriteState {
    Active,
    Closed,
}

#[tonic::async_trait]
pub trait TaskStreamWriter: fmt::Debug + Send + Sync {
    async fn open(&self, partition: usize) -> Result<Box<dyn TaskStreamSink>>;
}

/// A sink for one shuffle task partition and all of its output channels.
#[tonic::async_trait]
pub trait TaskStreamSink: Send {
    /// Write at most one batch per output channel concurrently. Calls are ordered
    /// so that each channel preserves its input batch order.
    async fn write(&mut self, batches: Vec<Option<RecordBatch>>) -> Result<TaskStreamWriteState>;
    /// Commits the output and returns the counts of the committed output, if the
    /// sink counts. Counts must describe only what the backend has committed.
    async fn commit(self: Box<Self>) -> Result<Option<ChannelCounts>>;
    async fn abort(self: Box<Self>) -> Result<()>;
}

/// The result of committing one channel.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TaskStreamChannelCommit {
    /// The bytes the backend stores for the channel after its own compression,
    /// or `None` if the backend cannot tell.
    pub physical_bytes: Option<u64>,
}

/// A physical sink for exactly one channel of the task stream.
#[tonic::async_trait]
pub trait TaskStreamChannelSink: Send {
    async fn write(&mut self, batch: RecordBatch) -> Result<TaskStreamWriteState>;
    async fn commit(self: Box<Self>) -> Result<TaskStreamChannelCommit>;
    async fn abort(self: Box<Self>) -> Result<()>;
}

pub(crate) struct MultiChannelTaskStreamSink {
    sinks: Vec<Option<Box<dyn TaskStreamChannelSink>>>,
    /// Per-channel counters, present only when the output's statistics are used.
    counters: Option<Vec<ChannelCounter>>,
}

impl MultiChannelTaskStreamSink {
    /// Creates a sink that counts what each channel accepts if `count` is set.
    pub(crate) fn new(sinks: Vec<Box<dyn TaskStreamChannelSink>>, count: bool) -> Self {
        let counters = count.then(|| vec![ChannelCounter::default(); sinks.len()]);
        Self {
            sinks: sinks.into_iter().map(Some).collect(),
            counters,
        }
    }
}

#[tonic::async_trait]
impl TaskStreamSink for MultiChannelTaskStreamSink {
    async fn write(&mut self, batches: Vec<Option<RecordBatch>>) -> Result<TaskStreamWriteState> {
        if batches.len() != self.sinks.len() {
            return Err(DataFusionError::Internal(format!(
                "expected {} shuffle output channels, got {}",
                self.sinks.len(),
                batches.len()
            )));
        }
        // Borrow each channel independently so slow I/O on one partition does
        // not prevent writes to other partitions from making progress. Retain
        // the sinks so the caller can abort them if any write fails.
        let channels = self.sinks.len();
        let counters = self
            .counters
            .as_mut()
            .map(|x| x.iter_mut().map(Some).collect::<Vec<_>>())
            .unwrap_or_else(|| (0..channels).map(|_| None).collect());
        try_join_all(self.sinks.iter_mut().zip(counters).zip(batches).map(
            |((sink, counter), batch)| async move {
                if let (Some(writer), Some(batch)) = (sink.as_mut(), batch) {
                    // Count before the batch is moved into the backend. The counts are
                    // published only if the whole output is committed.
                    if let Some(counter) = counter {
                        counter.add(&batch);
                    }
                    if writer.write(batch).await? == TaskStreamWriteState::Closed {
                        *sink = None;
                    }
                }
                Ok::<_, DataFusionError>(())
            },
        ))
        .await?;
        Ok(if self.sinks.iter().any(Option::is_some) {
            TaskStreamWriteState::Active
        } else {
            TaskStreamWriteState::Closed
        })
    }

    async fn commit(self: Box<Self>) -> Result<Option<ChannelCounts>> {
        let Self { sinks, counters } = *self;
        // A consumer may finish without reading one channel (for example, an
        // empty hash-join build partition). Do not let that channel's buffered
        // data prevent other channels from reaching end-of-stream. Once the
        // consumers finish, stage cleanup releases any unread channels.
        let commits = try_join_all(sinks.into_iter().map(|sink| async move {
            match sink {
                Some(sink) => sink.commit().await.map(|x| x.physical_bytes),
                // A closed channel has no committed size.
                None => Ok(None),
            }
        }))
        .await?;
        Ok(counters.map(|counters| ChannelCounts {
            rows: counters.iter().map(|x| x.rows).collect(),
            physical_bytes: commits.into_iter().collect::<Option<Vec<_>>>(),
            logical_bytes: counters.iter().map(|x| x.logical_bytes).collect(),
        }))
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        for sink in self.sinks.into_iter().flatten() {
            sink.abort().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use datafusion::arrow::datatypes::Schema;
    use tokio::sync::Barrier;

    use super::*;

    struct TestSink {
        barrier: Option<Arc<Barrier>>,
        state: TaskStreamWriteState,
        fail: bool,
        aborted: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl TaskStreamChannelSink for TestSink {
        async fn write(&mut self, _batch: RecordBatch) -> Result<TaskStreamWriteState> {
            if let Some(barrier) = &self.barrier {
                barrier.wait().await;
            }
            if self.fail {
                return Err(DataFusionError::Execution("write failed".to_string()));
            }
            Ok(self.state)
        }

        async fn commit(self: Box<Self>) -> Result<TaskStreamChannelCommit> {
            Ok(TaskStreamChannelCommit {
                physical_bytes: Some(7),
            })
        }

        async fn abort(self: Box<Self>) -> Result<()> {
            self.aborted.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn batch() -> Option<RecordBatch> {
        Some(RecordBatch::new_empty(Arc::new(Schema::empty())))
    }

    #[tokio::test]
    async fn writes_channels_concurrently() -> Result<()> {
        let barrier = Arc::new(Barrier::new(2));
        let mut sink = MultiChannelTaskStreamSink::new(
            (0..2)
                .map(|_| {
                    Box::new(TestSink {
                        barrier: Some(barrier.clone()),
                        state: TaskStreamWriteState::Active,
                        fail: false,
                        aborted: Arc::new(AtomicUsize::new(0)),
                    }) as Box<dyn TaskStreamChannelSink>
                })
                .collect(),
            true,
        );
        // A sequential writer cannot get past the first channel's barrier.
        for _ in 0..2 {
            let state =
                tokio::time::timeout(Duration::from_secs(5), sink.write(vec![batch(), batch()]))
                    .await
                    .map_err(|error| DataFusionError::External(Box::new(error)))??;
            assert_eq!(state, TaskStreamWriteState::Active);
        }
        let statistics = Box::new(sink).commit().await?.unwrap_or_default();
        assert_eq!(statistics.rows, vec![0, 0]);
        assert_eq!(statistics.physical_bytes, Some(vec![7, 7]));
        Ok(())
    }

    #[tokio::test]
    async fn stops_only_after_all_channels_close() -> Result<()> {
        let mut sink = MultiChannelTaskStreamSink::new(
            (0..2)
                .map(|_| {
                    Box::new(TestSink {
                        barrier: None,
                        state: TaskStreamWriteState::Closed,
                        fail: false,
                        aborted: Arc::new(AtomicUsize::new(0)),
                    }) as Box<dyn TaskStreamChannelSink>
                })
                .collect(),
            true,
        );
        assert_eq!(
            sink.write(vec![batch(), None]).await?,
            TaskStreamWriteState::Active
        );
        assert_eq!(
            sink.write(vec![batch(), batch()]).await?,
            TaskStreamWriteState::Closed
        );
        Ok(())
    }

    #[tokio::test]
    async fn commit_returns_counts_of_accepted_batches() -> Result<()> {
        use datafusion::arrow::array::Int32Array;

        let mut sink = MultiChannelTaskStreamSink::new(
            (0..3)
                .map(|i| {
                    Box::new(TestSink {
                        barrier: None,
                        // The last channel closes after its first batch.
                        state: if i == 2 {
                            TaskStreamWriteState::Closed
                        } else {
                            TaskStreamWriteState::Active
                        },
                        fail: false,
                        aborted: Arc::new(AtomicUsize::new(0)),
                    }) as Box<dyn TaskStreamChannelSink>
                })
                .collect(),
            true,
        );
        let rows = |n: i32| -> Result<Option<RecordBatch>> {
            Ok(Some(RecordBatch::try_from_iter(vec![(
                "a",
                Arc::new(Int32Array::from_iter_values(0..n)) as _,
            )])?))
        };
        sink.write(vec![rows(3)?, None, rows(1)?]).await?;
        sink.write(vec![rows(2)?, None, rows(5)?]).await?;
        let statistics = Box::new(sink).commit().await?.unwrap_or_default();
        assert_eq!(statistics.rows, vec![5, 0, 1]);
        assert_eq!(statistics.logical_bytes, vec![20, 0, 4]);
        // A closed channel has no committed size, so the physical size is unknown.
        assert_eq!(statistics.physical_bytes, None);
        Ok(())
    }

    #[tokio::test]
    async fn commit_without_counting_returns_no_counts() -> Result<()> {
        let mut sink = MultiChannelTaskStreamSink::new(
            vec![Box::new(TestSink {
                barrier: None,
                state: TaskStreamWriteState::Active,
                fail: false,
                aborted: Arc::new(AtomicUsize::new(0)),
            }) as Box<dyn TaskStreamChannelSink>],
            false,
        );
        sink.write(vec![batch()]).await?;
        assert_eq!(Box::new(sink).commit().await?, None);
        Ok(())
    }

    #[tokio::test]
    async fn retains_channels_for_abort_after_write_error() -> Result<()> {
        let aborted = Arc::new(AtomicUsize::new(0));
        let mut sink = MultiChannelTaskStreamSink::new(
            (0..2)
                .map(|i| {
                    Box::new(TestSink {
                        // This channel remains pending when the other channel fails.
                        barrier: (i == 0).then(|| Arc::new(Barrier::new(2))),
                        state: TaskStreamWriteState::Active,
                        fail: i == 1,
                        aborted: aborted.clone(),
                    }) as Box<dyn TaskStreamChannelSink>
                })
                .collect(),
            true,
        );
        let result =
            tokio::time::timeout(Duration::from_secs(5), sink.write(vec![batch(), batch()]))
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
        assert!(result.is_err());
        Box::new(sink).abort().await?;
        assert_eq!(aborted.load(Ordering::SeqCst), 2);
        Ok(())
    }
}
