use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::Partitioning;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskStreamKey, WorkerId};
use crate::plan::ShufflePartitioning;
use crate::proto::{decode_remote_partitioning, decode_remote_physical_expr};
use crate::task::r#gen;

#[derive(Debug, Clone)]
pub struct TaskDefinition {
    pub plan: Arc<[u8]>,
    pub inputs: Vec<TaskInput>,
    pub output: TaskOutput,
}

#[derive(Debug, Clone)]
pub struct TaskInput {
    pub stage: usize,
    pub locator: Arc<TaskInputLocator>,
}

#[derive(Debug, Clone)]
pub enum TaskInputLocator {
    Driver {
        keys: Vec<Vec<TaskInputKey>>,
    },
    Worker {
        keys: Vec<Vec<(WorkerId, TaskInputKey)>>,
    },
    Storage {
        keys: Vec<Vec<TaskInputKey>>,
    },
    ShuffleService {
        /// For each partition of the stage input, the shuffle partitions (channels)
        /// to read.
        reads: Vec<ShuffleServiceRead>,
        /// The committed attempt of each producer partition, so that output of other
        /// attempts is never read. `None` if the commit is not known.
        map_attempts: Option<Arc<[i32]>>,
    },
}

/// The shuffle service data read by one partition of a stage input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShuffleServiceRead {
    pub channels: Vec<usize>,
    /// Read only the output of producer partitions in this range. This is set only
    /// for a single channel split by an adaptive skew rewrite.
    pub mappers: Option<std::ops::Range<usize>>,
}

impl ShuffleServiceRead {
    pub fn channels(channels: Vec<usize>) -> Self {
        Self {
            channels,
            mappers: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TaskInputKey {
    pub partition: usize,
    pub attempt: usize,
    pub channel: usize,
}

impl TaskInputKey {
    pub fn task_stream_key(&self, job_id: JobId, stage: usize) -> TaskStreamKey {
        TaskStreamKey {
            job_id,
            stage,
            partition: self.partition,
            attempt: self.attempt,
            channel: self.channel,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TaskOutput {
    pub distribution: TaskOutputDistribution,
    pub locator: TaskOutputLocator,
}

#[derive(Debug, Clone)]
pub enum TaskOutputDistribution {
    Hash {
        keys: Vec<Arc<[u8]>>,
        channels: usize,
    },
    Range {
        partitioning: Arc<[u8]>,
        channels: usize,
    },
    RoundRobinBatch {
        channels: usize,
    },
    RoundRobinRow {
        channels: usize,
    },
}

#[derive(Debug, Clone)]
pub enum TaskOutputLocator {
    Pipelined {
        replicas: usize,
    },
    Blocking {
        /// Whether the sinks count the output and the task reports its committed
        /// statistics.
        collect_statistics: bool,
    },
}

impl From<TaskDefinition> for r#gen::TaskDefinition {
    fn from(value: TaskDefinition) -> Self {
        let TaskDefinition {
            plan,
            inputs,
            output,
        } = value;
        r#gen::TaskDefinition {
            plan: plan.to_vec(),
            inputs: inputs.into_iter().map(|x| x.into()).collect(),
            output: Some(output.into()),
        }
    }
}

impl TryFrom<r#gen::TaskDefinition> for TaskDefinition {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskDefinition) -> Result<Self, Self::Error> {
        let inputs = value
            .inputs
            .into_iter()
            .map(|x| x.try_into())
            .collect::<ExecutionResult<Vec<_>>>()?;
        let output = match value.output {
            Some(x) => x.try_into()?,
            None => {
                return Err(ExecutionError::InvalidArgument(
                    "cannot decode empty task output".to_string(),
                ));
            }
        };
        Ok(TaskDefinition {
            plan: Arc::from(value.plan),
            inputs,
            output,
        })
    }
}

impl From<TaskInput> for r#gen::TaskInput {
    fn from(value: TaskInput) -> Self {
        let TaskInput { stage, locator } = value;
        r#gen::TaskInput {
            stage: stage as u64,
            locator: Some(locator.as_ref().clone().into()),
        }
    }
}

impl TryFrom<r#gen::TaskInput> for TaskInput {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskInput) -> Result<Self, Self::Error> {
        let locator = match value.locator {
            Some(x) => x.try_into()?,
            None => {
                return Err(ExecutionError::InvalidArgument(
                    "cannot decode empty task input locator".to_string(),
                ));
            }
        };
        Ok(TaskInput {
            stage: value.stage as usize,
            locator: Arc::new(locator),
        })
    }
}

impl From<TaskInputLocator> for r#gen::TaskInputLocator {
    fn from(value: TaskInputLocator) -> Self {
        let kind = match value {
            TaskInputLocator::Driver { keys } => {
                r#gen::task_input_locator::Kind::Driver(r#gen::TaskInputDriverLocator {
                    keys: keys.into_iter().map(|x| x.into()).collect(),
                })
            }
            TaskInputLocator::Worker { keys } => {
                r#gen::task_input_locator::Kind::Worker(r#gen::TaskInputWorkerLocator {
                    keys: keys.into_iter().map(|x| x.into()).collect(),
                })
            }
            TaskInputLocator::Storage { keys } => {
                r#gen::task_input_locator::Kind::Storage(r#gen::TaskInputStorageLocator {
                    keys: keys.into_iter().map(|x| x.into()).collect(),
                })
            }
            TaskInputLocator::ShuffleService {
                reads,
                map_attempts,
            } => r#gen::task_input_locator::Kind::ShuffleService(
                r#gen::TaskInputShuffleServiceLocator {
                    channels: reads
                        .into_iter()
                        .map(|read| r#gen::TaskInputChannelList {
                            channels: read.channels.into_iter().map(|x| x as u64).collect(),
                            mapper_start: read.mappers.as_ref().map(|x| x.start as u64),
                            mapper_end: read.mappers.as_ref().map(|x| x.end as u64),
                        })
                        .collect(),
                    map_attempts: map_attempts.map(|x| encode_map_attempts(&x)),
                },
            ),
        };
        r#gen::TaskInputLocator { kind: Some(kind) }
    }
}

impl TryFrom<r#gen::TaskInputLocator> for TaskInputLocator {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskInputLocator) -> Result<Self, Self::Error> {
        match value.kind {
            Some(r#gen::task_input_locator::Kind::Driver(r#gen::TaskInputDriverLocator {
                keys,
            })) => {
                let keys = keys
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<ExecutionResult<Vec<_>>>()?;
                Ok(TaskInputLocator::Driver { keys })
            }
            Some(r#gen::task_input_locator::Kind::Worker(r#gen::TaskInputWorkerLocator {
                keys,
            })) => {
                let keys = keys
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<ExecutionResult<Vec<_>>>()?;
                Ok(TaskInputLocator::Worker { keys })
            }
            Some(r#gen::task_input_locator::Kind::Storage(r#gen::TaskInputStorageLocator {
                keys,
            })) => {
                let keys = keys
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<ExecutionResult<Vec<_>>>()?;
                Ok(TaskInputLocator::Storage { keys })
            }
            Some(r#gen::task_input_locator::Kind::ShuffleService(
                r#gen::TaskInputShuffleServiceLocator {
                    channels,
                    map_attempts,
                },
            )) => Ok(TaskInputLocator::ShuffleService {
                reads: channels
                    .into_iter()
                    .map(|x| {
                        let mappers = match (x.mapper_start, x.mapper_end) {
                            (Some(start), Some(end)) => Some(start as usize..end as usize),
                            (None, None) => None,
                            _ => {
                                return Err(ExecutionError::InvalidArgument(
                                    "incomplete shuffle service mapper range".to_string(),
                                ));
                            }
                        };
                        Ok(ShuffleServiceRead {
                            channels: x.channels.into_iter().map(|x| x as usize).collect(),
                            mappers,
                        })
                    })
                    .collect::<ExecutionResult<Vec<_>>>()?,
                map_attempts: map_attempts.map(decode_map_attempts).transpose()?,
            }),
            None => Err(ExecutionError::InvalidArgument(
                "cannot decode empty task input locator".to_string(),
            )),
        }
    }
}

impl From<TaskInputKey> for r#gen::TaskInputDriverKey {
    fn from(value: TaskInputKey) -> Self {
        let TaskInputKey {
            partition,
            attempt,
            channel,
        } = value;
        r#gen::TaskInputDriverKey {
            partition: partition as u64,
            attempt: attempt as u64,
            channel: channel as u64,
        }
    }
}

impl TryFrom<r#gen::TaskInputDriverKey> for TaskInputKey {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskInputDriverKey) -> Result<Self, Self::Error> {
        Ok(TaskInputKey {
            partition: value.partition as usize,
            attempt: value.attempt as usize,
            channel: value.channel as usize,
        })
    }
}

impl From<Vec<TaskInputKey>> for r#gen::TaskInputDriverKeyList {
    fn from(value: Vec<TaskInputKey>) -> Self {
        r#gen::TaskInputDriverKeyList {
            keys: value.into_iter().map(|x| x.into()).collect(),
        }
    }
}

impl TryFrom<r#gen::TaskInputDriverKeyList> for Vec<TaskInputKey> {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskInputDriverKeyList) -> Result<Self, Self::Error> {
        value
            .keys
            .into_iter()
            .map(|x| x.try_into())
            .collect::<ExecutionResult<Vec<_>>>()
    }
}

impl From<(WorkerId, TaskInputKey)> for r#gen::TaskInputWorkerKey {
    fn from(value: (WorkerId, TaskInputKey)) -> Self {
        let (
            worker_id,
            TaskInputKey {
                partition,
                attempt,
                channel,
            },
        ) = value;
        r#gen::TaskInputWorkerKey {
            worker_id: worker_id.into(),
            partition: partition as u64,
            attempt: attempt as u64,
            channel: channel as u64,
        }
    }
}

impl TryFrom<r#gen::TaskInputWorkerKey> for (WorkerId, TaskInputKey) {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskInputWorkerKey) -> Result<Self, Self::Error> {
        Ok((
            value.worker_id.into(),
            TaskInputKey {
                partition: value.partition as usize,
                attempt: value.attempt as usize,
                channel: value.channel as usize,
            },
        ))
    }
}

impl From<Vec<(WorkerId, TaskInputKey)>> for r#gen::TaskInputWorkerKeyList {
    fn from(value: Vec<(WorkerId, TaskInputKey)>) -> Self {
        r#gen::TaskInputWorkerKeyList {
            keys: value.into_iter().map(|x| x.into()).collect(),
        }
    }
}

impl TryFrom<r#gen::TaskInputWorkerKeyList> for Vec<(WorkerId, TaskInputKey)> {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskInputWorkerKeyList) -> Result<Self, Self::Error> {
        value
            .keys
            .into_iter()
            .map(|x| x.try_into())
            .collect::<ExecutionResult<Vec<_>>>()
    }
}

impl From<TaskInputKey> for r#gen::TaskInputRemoteKey {
    fn from(value: TaskInputKey) -> Self {
        let TaskInputKey {
            partition,
            attempt,
            channel,
        } = value;
        r#gen::TaskInputRemoteKey {
            partition: partition as u64,
            attempt: attempt as u64,
            channel: channel as u64,
        }
    }
}

impl TryFrom<r#gen::TaskInputRemoteKey> for TaskInputKey {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskInputRemoteKey) -> Result<Self, Self::Error> {
        Ok(TaskInputKey {
            partition: value.partition as usize,
            attempt: value.attempt as usize,
            channel: value.channel as usize,
        })
    }
}

impl From<Vec<TaskInputKey>> for r#gen::TaskInputRemoteKeyList {
    fn from(value: Vec<TaskInputKey>) -> Self {
        r#gen::TaskInputRemoteKeyList {
            keys: value.into_iter().map(|x| x.into()).collect(),
        }
    }
}

impl TryFrom<r#gen::TaskInputRemoteKeyList> for Vec<TaskInputKey> {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskInputRemoteKeyList) -> Result<Self, Self::Error> {
        value
            .keys
            .into_iter()
            .map(|x| x.try_into())
            .collect::<ExecutionResult<Vec<_>>>()
    }
}

impl From<TaskOutput> for r#gen::TaskOutput {
    fn from(value: TaskOutput) -> Self {
        let TaskOutput {
            distribution,
            locator,
        } = value;
        r#gen::TaskOutput {
            distribution: Some(distribution.into()),
            locator: Some(locator.into()),
        }
    }
}

impl TryFrom<r#gen::TaskOutput> for TaskOutput {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskOutput) -> Result<Self, Self::Error> {
        let distribution = match value.distribution {
            Some(x) => x.try_into()?,
            None => {
                return Err(ExecutionError::InvalidArgument(
                    "cannot decode empty task output distribution".to_string(),
                ));
            }
        };
        let locator = match value.locator {
            Some(x) => x.try_into()?,
            None => {
                return Err(ExecutionError::InvalidArgument(
                    "cannot decode empty task output locator".to_string(),
                ));
            }
        };
        Ok(TaskOutput {
            distribution,
            locator,
        })
    }
}

impl From<TaskOutputDistribution> for r#gen::TaskOutputDistribution {
    fn from(value: TaskOutputDistribution) -> Self {
        let kind = match value {
            TaskOutputDistribution::Hash { keys, channels } => {
                r#gen::task_output_distribution::Kind::Hash(r#gen::TaskOutputHashDistribution {
                    keys: keys.into_iter().map(|k| k.to_vec()).collect(),
                    channels: channels as u64,
                })
            }
            TaskOutputDistribution::Range {
                partitioning,
                channels,
            } => r#gen::task_output_distribution::Kind::Range(r#gen::TaskOutputRangeDistribution {
                partitioning: partitioning.to_vec(),
                channels: channels as u64,
            }),
            TaskOutputDistribution::RoundRobinBatch { channels } => {
                r#gen::task_output_distribution::Kind::RoundRobin(
                    r#gen::TaskOutputRoundRobinDistribution {
                        channels: channels as u64,
                    },
                )
            }
            TaskOutputDistribution::RoundRobinRow { channels } => {
                r#gen::task_output_distribution::Kind::RoundRobinRow(
                    r#gen::TaskOutputRoundRobinRowDistribution {
                        channels: channels as u64,
                    },
                )
            }
        };
        r#gen::TaskOutputDistribution { kind: Some(kind) }
    }
}

impl TryFrom<r#gen::TaskOutputDistribution> for TaskOutputDistribution {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskOutputDistribution) -> Result<Self, Self::Error> {
        match value.kind {
            Some(r#gen::task_output_distribution::Kind::Hash(
                r#gen::TaskOutputHashDistribution { keys, channels },
            )) => Ok(TaskOutputDistribution::Hash {
                keys: keys.into_iter().map(Arc::from).collect(),
                channels: channels as usize,
            }),
            Some(r#gen::task_output_distribution::Kind::Range(
                r#gen::TaskOutputRangeDistribution {
                    partitioning,
                    channels,
                },
            )) => Ok(TaskOutputDistribution::Range {
                partitioning: Arc::from(partitioning),
                channels: channels as usize,
            }),
            Some(r#gen::task_output_distribution::Kind::RoundRobin(
                r#gen::TaskOutputRoundRobinDistribution { channels },
            )) => Ok(TaskOutputDistribution::RoundRobinBatch {
                channels: channels as usize,
            }),
            Some(r#gen::task_output_distribution::Kind::RoundRobinRow(
                r#gen::TaskOutputRoundRobinRowDistribution { channels },
            )) => Ok(TaskOutputDistribution::RoundRobinRow {
                channels: channels as usize,
            }),
            None => Err(ExecutionError::InvalidArgument(
                "cannot decode empty task output distribution".to_string(),
            )),
        }
    }
}

impl From<TaskOutputLocator> for r#gen::TaskOutputLocator {
    fn from(value: TaskOutputLocator) -> Self {
        let kind = match value {
            TaskOutputLocator::Pipelined { replicas } => {
                r#gen::task_output_locator::Kind::Pipelined(r#gen::TaskOutputPipelinedLocator {
                    replicas: replicas as u64,
                })
            }
            TaskOutputLocator::Blocking { collect_statistics } => {
                r#gen::task_output_locator::Kind::Blocking(r#gen::TaskOutputBlockingLocator {
                    collect_statistics,
                })
            }
        };
        r#gen::TaskOutputLocator { kind: Some(kind) }
    }
}

impl TryFrom<r#gen::TaskOutputLocator> for TaskOutputLocator {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskOutputLocator) -> Result<Self, Self::Error> {
        match value.kind {
            Some(r#gen::task_output_locator::Kind::Pipelined(
                r#gen::TaskOutputPipelinedLocator { replicas },
            )) => Ok(TaskOutputLocator::Pipelined {
                replicas: replicas as usize,
            }),
            Some(r#gen::task_output_locator::Kind::Blocking(
                r#gen::TaskOutputBlockingLocator { collect_statistics },
            )) => Ok(TaskOutputLocator::Blocking { collect_statistics }),
            None => Err(ExecutionError::InvalidArgument(
                "cannot decode empty task output locator".to_string(),
            )),
        }
    }
}

impl TaskOutput {
    pub fn channels(&self) -> usize {
        match self.distribution {
            TaskOutputDistribution::Hash { channels, .. } => channels,
            TaskOutputDistribution::Range { channels, .. } => channels,
            TaskOutputDistribution::RoundRobinBatch { channels, .. } => channels,
            TaskOutputDistribution::RoundRobinRow { channels, .. } => channels,
        }
    }

    pub fn shuffle_partitioning(
        &self,
        ctx: &TaskContext,
        schema: &Schema,
        codec: &dyn PhysicalExtensionCodec,
    ) -> ExecutionResult<ShufflePartitioning> {
        match &self.distribution {
            TaskOutputDistribution::Hash { keys, channels } => {
                let keys = keys
                    .iter()
                    .map(|k| {
                        decode_remote_physical_expr(ctx, codec, k.as_ref(), schema)
                            .map_err(|e| e.into())
                    })
                    .collect::<ExecutionResult<Vec<_>>>()?;
                Ok(ShufflePartitioning::Hash(keys, *channels))
            }
            TaskOutputDistribution::Range {
                partitioning,
                channels,
            } => {
                let partitioning =
                    match decode_remote_partitioning(ctx, codec, partitioning.as_ref(), schema)
                        .map_err(ExecutionError::from)?
                    {
                        Partitioning::Range(partitioning) => partitioning,
                        partitioning => {
                            return Err(ExecutionError::InvalidArgument(format!(
                                "expected range partitioning, got {partitioning}"
                            )));
                        }
                    };
                if partitioning.partition_count() != *channels {
                    return Err(ExecutionError::InvalidArgument(format!(
                        "range partition count {} does not match task output channel count {channels}",
                        partitioning.partition_count()
                    )));
                }
                Ok(ShufflePartitioning::Range(partitioning))
            }
            TaskOutputDistribution::RoundRobinBatch { channels } => {
                Ok(ShufflePartitioning::RoundRobinBatch(*channels))
            }
            TaskOutputDistribution::RoundRobinRow { channels } => {
                Ok(ShufflePartitioning::RoundRobinRow(*channels))
            }
        }
    }
}

fn encode_map_attempts(attempts: &[i32]) -> r#gen::TaskInputMapAttempts {
    let mut counts = HashMap::<i32, usize>::new();
    for &attempt in attempts {
        *counts.entry(attempt).or_default() += 1;
    }
    // The most common attempt, the smallest one among ties.
    let attempt = counts
        .into_iter()
        .max_by_key(|&(attempt, count)| (count, std::cmp::Reverse(attempt)))
        .map(|(attempt, _)| attempt)
        .unwrap_or_default();
    let (other_partitions, other_attempts) = attempts
        .iter()
        .enumerate()
        .filter(|&(_, &a)| a != attempt)
        .map(|(p, &a)| (p as u32, a))
        .unzip();
    r#gen::TaskInputMapAttempts {
        partitions: attempts.len() as u32,
        attempt,
        other_partitions,
        other_attempts,
    }
}

fn decode_map_attempts(value: r#gen::TaskInputMapAttempts) -> ExecutionResult<Arc<[i32]>> {
    let r#gen::TaskInputMapAttempts {
        partitions,
        attempt,
        other_partitions,
        other_attempts,
    } = value;
    if other_partitions.len() != other_attempts.len()
        || !other_partitions.is_sorted_by(|a, b| a < b)
        || other_partitions.last().is_some_and(|&p| p >= partitions)
    {
        return Err(ExecutionError::InvalidArgument(
            "invalid committed map attempts".to_string(),
        ));
    }
    let mut attempts = vec![attempt; partitions as usize];
    for (p, a) in other_partitions.into_iter().zip(other_attempts) {
        attempts[p as usize] = a;
    }
    Ok(Arc::from(attempts))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{decode_map_attempts, encode_map_attempts};

    #[test]
    fn uniform_map_attempts_are_encoded_without_a_list() {
        let encoded = encode_map_attempts(&[0; 1000]);
        assert_eq!((encoded.partitions, encoded.attempt), (1000, 0));
        assert!(encoded.other_partitions.is_empty());
    }

    #[test]
    fn invalid_map_attempts_are_rejected() {
        let mut encoded = encode_map_attempts(&[0, 1, 0, 2]);
        encoded.other_partitions.swap(0, 1);
        assert!(decode_map_attempts(encoded).is_err());
        let mut encoded = encode_map_attempts(&[0, 0, 1]);
        encoded.partitions = 2;
        assert!(decode_map_attempts(encoded).is_err());
    }

    proptest! {
        #[test]
        fn map_attempts_round_trip(attempts in prop::collection::vec(0..4_i32, 0..200)) {
            let decoded = decode_map_attempts(encode_map_attempts(&attempts)).ok();
            prop_assert_eq!(decoded.as_deref(), Some(attempts.as_slice()));
        }
    }
}
