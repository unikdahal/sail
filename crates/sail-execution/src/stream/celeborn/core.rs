use std::io::Cursor;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::common::{DataFusionError, Result};
use futures::{StreamExt, TryStreamExt};
use sail_celeborn::shuffle::{PartitionReadOptions, ShuffleClient};

use crate::id::{JobId, TaskKey};
use crate::statistics::ChannelCounts;
use crate::stream::error::TaskStreamError;
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{
    MultiChannelTaskStreamSink, TaskStreamChannelCommit, TaskStreamChannelSink, TaskStreamSink,
    TaskStreamWriteState,
};
use crate::task::definition::ShuffleServiceRead;

/// The largest number of Celeborn partitions that one task reads at a time.
const MAX_CONCURRENT_PARTITION_READS: usize = 8;

/// The shape of one map task's output to a Celeborn shuffle.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CelebornOutput {
    /// The number of map tasks of the producer stage.
    pub mappers: usize,
    /// The number of reduce channels.
    pub channels: usize,
    /// Whether the sinks count the output for committed statistics.
    pub collect_statistics: bool,
}

#[derive(Clone)]
pub(crate) struct CelebornStreamManager {
    client: ShuffleClient,
}

impl CelebornStreamManager {
    pub(crate) fn new(client: ShuffleClient) -> Self {
        Self { client }
    }

    pub(crate) async fn stop(&self) {
        let _ = self.client.stop().await;
    }

    pub(crate) async fn remove_streams(
        &self,
        job_id: JobId,
        stage: Option<usize>,
        unregister: bool,
    ) -> Result<()> {
        let shuffle_ids = self
            .client
            .get_job_shuffle_ids(job_id.into())
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        for (stream_stage, shuffle_id) in shuffle_ids {
            if stage.is_none_or(|stage| stream_stage == stage as u64) {
                if unregister {
                    self.client
                        .unregister_shuffle(shuffle_id)
                        .await
                        .map_err(|error| DataFusionError::External(Box::new(error)))?;
                }
                self.client
                    .clean_up_shuffle(shuffle_id)
                    .await
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
            }
        }
        Ok(())
    }

    pub(crate) async fn create_stream(
        &self,
        key: TaskKey,
        output: CelebornOutput,
        schema: SchemaRef,
    ) -> Result<Box<dyn TaskStreamSink>> {
        let CelebornOutput {
            mappers,
            channels,
            collect_statistics,
        } = output;
        // A Celeborn shuffle spans every map task and reduce channel in one producer stage.
        let shuffle_id = self
            .client
            .get_shuffle_id(key.job_id.into(), key.stage as u64)
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        self.client
            .register_shuffle(
                shuffle_id,
                (0..channels)
                    .map(|channel| {
                        i32::try_from(channel)
                            .map_err(|error| DataFusionError::External(Box::new(error)))
                    })
                    .collect::<Result<Vec<_>>>()?,
                false,
                0,
            )
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let map_id = i32::try_from(key.partition)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let attempt_id = i32::try_from(key.attempt)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let sinks = (0..channels)
            .map(|channel| {
                Ok(Box::new(CelebornStreamSink {
                    client: self.client.clone(),
                    shuffle_id,
                    partition_id: i32::try_from(channel)
                        .map_err(|error| DataFusionError::External(Box::new(error)))?,
                    map_id,
                    attempt_id,
                    schema: schema.clone(),
                    pushed_bytes: 0,
                }) as Box<dyn TaskStreamChannelSink>)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Box::new(CelebornTaskStreamSink {
            channels: MultiChannelTaskStreamSink::new(sinks, collect_statistics),
            client: self.client.clone(),
            shuffle_id,
            map_id,
            attempt_id,
            num_mappers: i32::try_from(mappers)
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
        }))
    }

    pub(crate) async fn fetch_stream(
        &self,
        job_id: JobId,
        stage: usize,
        read: ShuffleServiceRead,
        map_attempts: Option<Arc<[i32]>>,
        schema: SchemaRef,
    ) -> Result<TaskStreamSource> {
        let ShuffleServiceRead { channels, mappers } = read;
        let map_range = mappers
            .map(|range| {
                Ok::<_, DataFusionError>(
                    i32::try_from(range.start)
                        .map_err(|error| DataFusionError::External(Box::new(error)))?
                        ..i32::try_from(range.end)
                            .map_err(|error| DataFusionError::External(Box::new(error)))?,
                )
            })
            .transpose()?;
        if map_range.is_some() && map_attempts.is_none() {
            // A partial read is planned only from committed statistics, so the commit
            // must be known. Without it, a range could include output of failed attempts.
            return Err(DataFusionError::Internal(
                "a Celeborn map-range read requires the committed map attempts".to_string(),
            ));
        }
        let options = PartitionReadOptions {
            map_range,
            map_attempts,
        };
        let shuffle_id = self
            .client
            .get_shuffle_id(job_id.into(), stage as u64)
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let partition_ids = channels
            .iter()
            .map(|channel| {
                i32::try_from(*channel).map_err(|error| DataFusionError::External(Box::new(error)))
            })
            .collect::<Result<Vec<_>>>()?;
        // This initializes routing information in this task runner's client. The
        // lifecycle manager returns the existing reservation for this shuffle.
        self.client
            .register_shuffle(shuffle_id, partition_ids.clone(), false, 0)
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        // Partition streams are opened lazily and at most a few at a time, so a task
        // that reads many channels (a coalesced range or a broadcast input) does not
        // open all of them on the shuffle service at once.
        let client = self.client.clone();
        let streams = futures::stream::iter(partition_ids).map(move |partition_id| {
            let client = client.clone();
            let options = options.clone();
            let schema = schema.clone();
            Box::pin(futures::stream::once(async move {
                client
                    .read_partition_stream_with_options(shuffle_id, partition_id, options)
                    .await
            }))
            .flatten()
            .map(move |result| {
                result
                    .map_err(|error| TaskStreamError::External(Arc::new(error)))
                    .and_then(|data| {
                        decode_batches(data, &schema)
                            .map_err(|error| TaskStreamError::Unknown(error.to_string()))
                    })
            })
            .map_ok(|batches| futures::stream::iter(batches.into_iter().map(Ok)))
            .try_flatten()
        });
        Ok(Box::pin(
            streams.flatten_unordered(MAX_CONCURRENT_PARTITION_READS),
        ))
    }
}

struct CelebornStreamSink {
    client: ShuffleClient,
    shuffle_id: i32,
    partition_id: i32,
    map_id: i32,
    attempt_id: i32,
    schema: SchemaRef,
    /// The compressed payload bytes pushed for this channel.
    pushed_bytes: u64,
}

#[tonic::async_trait]
impl TaskStreamChannelSink for CelebornStreamSink {
    async fn write(&mut self, batch: RecordBatch) -> Result<TaskStreamWriteState> {
        // Reserve the frame length prefix before serializing Arrow IPC so the payload can be
        // passed to Celeborn without copying it into a second framing buffer.
        let mut writer = StreamWriter::try_new(vec![0; size_of::<u32>()], self.schema.as_ref())
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        writer
            .write(&batch)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        writer
            .finish()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let mut data = writer
            .into_inner()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let length = data.len().checked_sub(size_of::<u32>()).ok_or_else(|| {
            DataFusionError::Execution("invalid Celeborn frame length".to_string())
        })?;
        let length =
            u32::try_from(length).map_err(|error| DataFusionError::External(Box::new(error)))?;
        data[..size_of::<u32>()].copy_from_slice(&length.to_be_bytes());
        let size = self
            .client
            .push_data(
                self.shuffle_id,
                self.partition_id,
                self.map_id,
                self.attempt_id,
                Bytes::from(data),
            )
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        self.pushed_bytes = self.pushed_bytes.saturating_add(size.compressed);
        Ok(TaskStreamWriteState::Active)
    }

    async fn commit(self: Box<Self>) -> Result<TaskStreamChannelCommit> {
        // The data is committed by `mapper_end` for the whole task, which happens only
        // after every channel has been committed here.
        Ok(TaskStreamChannelCommit {
            physical_bytes: Some(self.pushed_bytes),
        })
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

struct CelebornTaskStreamSink {
    channels: MultiChannelTaskStreamSink,
    client: ShuffleClient,
    shuffle_id: i32,
    map_id: i32,
    attempt_id: i32,
    num_mappers: i32,
}

#[tonic::async_trait]
impl TaskStreamSink for CelebornTaskStreamSink {
    async fn write(&mut self, batches: Vec<Option<RecordBatch>>) -> Result<TaskStreamWriteState> {
        self.channels.write(batches).await
    }

    async fn commit(self: Box<Self>) -> Result<Option<ChannelCounts>> {
        let counts = Box::new(self.channels).commit().await?;
        // The counts become visible only once `mapper_end` succeeds. Whether this
        // attempt's output is the one Celeborn serves is decided by the lifecycle manager,
        // which reports the selected attempt of every mapper to the driver.
        self.client
            .mapper_end(
                self.shuffle_id,
                self.map_id,
                self.attempt_id,
                self.num_mappers,
            )
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        Ok(counts)
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        Box::new(self.channels).abort().await
    }
}

fn decode_batches(data: Bytes, _schema: &SchemaRef) -> Result<Vec<RecordBatch>> {
    let mut offset = 0;
    let mut batches = vec![];
    while offset < data.len() {
        let Some(header) = data.get(offset..offset + size_of::<u32>()) else {
            return Err(DataFusionError::Execution(
                "truncated Celeborn shuffle frame header".to_string(),
            ));
        };
        let length =
            u32::from_be_bytes(header.try_into().map_err(|_| {
                DataFusionError::Execution("invalid Celeborn frame header".to_string())
            })?) as usize;
        offset += size_of::<u32>();
        let Some(frame) = data.get(offset..offset + length) else {
            return Err(DataFusionError::Execution(
                "truncated Celeborn shuffle frame".to_string(),
            ));
        };
        let reader = StreamReader::try_new(Cursor::new(frame), None)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        for batch in reader {
            batches.push(batch.map_err(|error| DataFusionError::External(Box::new(error)))?);
        }
        offset += length;
    }
    Ok(batches)
}
