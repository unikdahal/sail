use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use sail_common::actor::{ActorAction, ActorContext};
use tokio::sync::oneshot;

use crate::common::{
    CompressionCodec, PartitionLocation, PartitionReadOptions, PushDataSize, SlotReservation,
};
use crate::endpoint::EndpointResolver;
use crate::error::{CelebornError, CelebornResult};
use crate::lifecycle::{LifecycleManager, ReviveRequest};
use crate::protocol::StatusCode;
use crate::shuffle::ShuffleClientMessage;
use crate::shuffle::actor::ShuffleClientActor;
use crate::worker::{BatchFilter, WorkerClient, WorkerClientOptions, WorkerClientPool, compress};

impl ShuffleClientActor {
    pub(super) fn handle_get_shuffle_id(
        &mut self,
        ctx: &mut ActorContext<Self>,
        job_id: u64,
        stage: u64,
        reply: oneshot::Sender<CelebornResult<i32>>,
    ) -> ActorAction {
        if let Some(&shuffle_id) = self.shuffle_ids.get(&(job_id, stage)) {
            let _ = reply.send(Ok(shuffle_id));
            return ActorAction::Continue;
        }
        let lifecycle_manager = Arc::clone(&self.options.lifecycle_manager);
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let result = lifecycle_manager.get_shuffle_id(job_id, stage).await;
            let _ = handle
                .send(ShuffleClientMessage::GetShuffleIdComplete {
                    job_id,
                    stage,
                    result,
                    reply,
                })
                .await;
        });
        ActorAction::Continue
    }

    pub(super) fn handle_get_shuffle_id_complete(
        &mut self,
        job_id: u64,
        stage: u64,
        result: CelebornResult<i32>,
        reply: oneshot::Sender<CelebornResult<i32>>,
    ) -> ActorAction {
        if let Ok(shuffle_id) = result {
            self.shuffle_ids.insert((job_id, stage), shuffle_id);
            let _ = reply.send(Ok(shuffle_id));
        } else {
            let _ = reply.send(result);
        }
        ActorAction::Continue
    }

    pub(super) fn handle_get_job_shuffle_ids(
        &mut self,
        ctx: &mut ActorContext<Self>,
        job_id: u64,
        reply: oneshot::Sender<CelebornResult<Vec<(u64, i32)>>>,
    ) -> ActorAction {
        let lifecycle_manager = Arc::clone(&self.options.lifecycle_manager);
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let result = lifecycle_manager.get_job_shuffle_ids(job_id).await;
            let _ = handle
                .send(ShuffleClientMessage::GetJobShuffleIdsComplete {
                    job_id,
                    result,
                    reply,
                })
                .await;
        });
        ActorAction::Continue
    }

    pub(super) fn handle_get_job_shuffle_ids_complete(
        &mut self,
        job_id: u64,
        result: CelebornResult<Vec<(u64, i32)>>,
        reply: oneshot::Sender<CelebornResult<Vec<(u64, i32)>>>,
    ) -> ActorAction {
        if let Ok(shuffle_ids) = result {
            self.shuffle_ids.extend(
                shuffle_ids
                    .iter()
                    .map(|&(stage, shuffle_id)| ((job_id, stage), shuffle_id)),
            );
            let _ = reply.send(Ok(self
                .shuffle_ids
                .iter()
                .filter_map(|(&(cached_job_id, stage), &shuffle_id)| {
                    (cached_job_id == job_id).then_some((stage, shuffle_id))
                })
                .collect()));
        } else {
            let _ = reply.send(result);
        }
        ActorAction::Continue
    }

    pub(super) fn handle_register_shuffle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        shuffle_id: i32,
        partition_ids: Vec<i32>,
        should_replicate: bool,
        max_workers: i32,
        reply: oneshot::Sender<CelebornResult<SlotReservation>>,
    ) -> ActorAction {
        let lifecycle_manager = Arc::clone(&self.options.lifecycle_manager);
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let result = lifecycle_manager
                .register_shuffle(shuffle_id, partition_ids, should_replicate, max_workers)
                .await;
            let _ = handle
                .send(ShuffleClientMessage::RegisterShuffleComplete {
                    shuffle_id,
                    result,
                    reply,
                })
                .await;
        });
        ActorAction::Continue
    }

    pub(super) fn handle_register_shuffle_complete(
        &mut self,
        shuffle_id: i32,
        result: CelebornResult<SlotReservation>,
        reply: oneshot::Sender<CelebornResult<SlotReservation>>,
    ) -> ActorAction {
        if let Ok(reservation) = &result {
            let mut locations = reservation
                .worker_locations
                .values()
                .flat_map(|locations| locations.primary_locations.iter().cloned())
                .collect::<Vec<_>>();
            if locations.is_empty() {
                locations.extend(reservation.primary_locations.values().cloned());
            }
            for location in locations {
                self.update_partition_location(shuffle_id, location);
            }
        }
        let _ = reply.send(result);
        ActorAction::Continue
    }

    pub(super) fn handle_push_data(
        &mut self,
        ctx: &mut ActorContext<Self>,
        shuffle_id: i32,
        partition_id: i32,
        map_id: i32,
        attempt_id: i32,
        data: Bytes,
        reply: oneshot::Sender<CelebornResult<PushDataSize>>,
    ) -> ActorAction {
        let Some(location) = self.locations.get(&(shuffle_id, partition_id)).cloned() else {
            let _ = reply.send(Err(CelebornError::Application(format!(
                "shuffle {shuffle_id} partition {partition_id} is not registered"
            ))));
            return ActorAction::Continue;
        };
        let client = self.worker_clients.client(
            WorkerClientOptions::new(location.clone())
                .with_endpoint_resolver(self.options.endpoint_resolver.clone()),
        );
        let batch_id = self
            .batch_ids
            .entry((shuffle_id, map_id, attempt_id))
            .or_default();
        let current_batch_id = *batch_id;
        *batch_id += 1;
        let shuffle_key = self.shuffle_key(shuffle_id);
        let lifecycle_manager = Arc::clone(&self.options.lifecycle_manager);
        let endpoint_resolver = self.options.endpoint_resolver.clone();
        let worker_clients = self.worker_clients.clone();
        let compression = self.options.compression;
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            // Compress once, so that a retry after a revive sends the same payload and
            // the compressed size is known whichever path accepts the batch.
            let uncompressed = data.len() as u64;
            let (location, result) = match compress(data, compression) {
                Ok(data) => {
                    let size = PushDataSize {
                        uncompressed,
                        compressed: data.len() as u64,
                    };
                    push_compressed(
                        client,
                        &shuffle_key,
                        PushTarget {
                            shuffle_id,
                            partition_id,
                            map_id,
                            attempt_id,
                            batch_id: current_batch_id,
                        },
                        location,
                        data,
                        lifecycle_manager,
                        worker_clients,
                        endpoint_resolver,
                    )
                    .await
                    .map_or_else(
                        |(location, error)| (location, Err(error)),
                        |location| (location, Ok(size)),
                    )
                }
                Err(error) => (None, Err(error)),
            };
            let _ = handle
                .send(ShuffleClientMessage::PushDataComplete {
                    shuffle_id,
                    partition_id,
                    location,
                    result,
                    reply,
                })
                .await;
        });
        ActorAction::Continue
    }

    pub(super) fn handle_push_data_complete(
        &mut self,
        shuffle_id: i32,
        partition_id: i32,
        location: Option<PartitionLocation>,
        result: CelebornResult<PushDataSize>,
        reply: oneshot::Sender<CelebornResult<PushDataSize>>,
    ) -> ActorAction {
        if let Some(location) = location {
            debug_assert_eq!(location.id, partition_id);
            self.update_partition_location(shuffle_id, location);
        }
        let _ = reply.send(result);
        ActorAction::Continue
    }

    pub(super) fn handle_mapper_end(
        &mut self,
        ctx: &mut ActorContext<Self>,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        num_mappers: i32,
        reply: oneshot::Sender<CelebornResult<()>>,
    ) -> ActorAction {
        let lifecycle_manager = Arc::clone(&self.options.lifecycle_manager);
        ctx.spawn(async move {
            let _ = reply.send(
                lifecycle_manager
                    .mapper_end(shuffle_id, map_id, attempt_id, num_mappers)
                    .await,
            );
        });
        ActorAction::Continue
    }

    pub(super) fn handle_unregister_shuffle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        shuffle_id: i32,
        reply: oneshot::Sender<CelebornResult<()>>,
    ) -> ActorAction {
        let lifecycle_manager = Arc::clone(&self.options.lifecycle_manager);
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let result = lifecycle_manager.unregister_shuffle(shuffle_id).await;
            let _ = handle
                .send(ShuffleClientMessage::UnregisterShuffleComplete {
                    shuffle_id,
                    result,
                    reply,
                })
                .await;
        });
        ActorAction::Continue
    }

    pub(super) fn handle_unregister_shuffle_complete(
        &mut self,
        shuffle_id: i32,
        result: CelebornResult<()>,
        reply: oneshot::Sender<CelebornResult<()>>,
    ) -> ActorAction {
        if result.is_ok() {
            self.shuffle_ids.retain(|_, id| *id != shuffle_id);
            self.locations.retain(|(id, _), _| *id != shuffle_id);
            self.location_history.retain(|(id, _), _| *id != shuffle_id);
            self.batch_ids.retain(|(id, _, _), _| *id != shuffle_id);
        }
        let _ = reply.send(result);
        ActorAction::Continue
    }

    pub(super) fn handle_clean_up_shuffle(
        &mut self,
        shuffle_id: i32,
        reply: oneshot::Sender<CelebornResult<()>>,
    ) -> ActorAction {
        self.locations.retain(|(id, _), _| *id != shuffle_id);
        self.location_history.retain(|(id, _), _| *id != shuffle_id);
        self.batch_ids.retain(|(id, _, _), _| *id != shuffle_id);
        self.shuffle_ids.retain(|_, id| *id != shuffle_id);
        let _ = reply.send(Ok(()));
        ActorAction::Continue
    }

    pub(super) fn handle_read_partition_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        shuffle_id: i32,
        partition_id: i32,
        options: PartitionReadOptions,
        reply: oneshot::Sender<BoxStream<'static, CelebornResult<Bytes>>>,
    ) -> ActorAction {
        if !self
            .location_history
            .contains_key(&(shuffle_id, partition_id))
        {
            let _ = reply.send(Box::pin(stream::once(async move {
                Err(CelebornError::Application(format!(
                    "shuffle {shuffle_id} partition {partition_id} is not registered"
                )))
            })));
            return ActorAction::Continue;
        }
        let lifecycle_manager = Arc::clone(&self.options.lifecycle_manager);
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let result = lifecycle_manager
                .register_shuffle(shuffle_id, vec![partition_id], false, 0)
                .await;
            let _ = handle
                .send(ShuffleClientMessage::ReadPartitionStreamComplete {
                    shuffle_id,
                    partition_id,
                    options,
                    result,
                    reply,
                })
                .await;
        });
        ActorAction::Continue
    }

    pub(super) fn handle_read_partition_stream_complete(
        &mut self,
        ctx: &mut ActorContext<Self>,
        shuffle_id: i32,
        partition_id: i32,
        options: PartitionReadOptions,
        result: CelebornResult<SlotReservation>,
        reply: oneshot::Sender<BoxStream<'static, CelebornResult<Bytes>>>,
    ) -> ActorAction {
        let reservation = match result {
            Ok(reservation) => reservation,
            Err(error) => {
                let _ = reply.send(Box::pin(stream::once(async move { Err(error) })));
                return ActorAction::Continue;
            }
        };
        for locations in reservation.worker_locations.values() {
            for location in locations
                .primary_locations
                .iter()
                .filter(|location| location.id == partition_id)
                .cloned()
            {
                self.update_partition_location(shuffle_id, location);
            }
        }
        let Some(locations) = self
            .location_history
            .get(&(shuffle_id, partition_id))
            .cloned()
        else {
            let _ = reply.send(Box::pin(stream::once(async move {
                Err(CelebornError::Application(format!(
                    "shuffle {shuffle_id} partition {partition_id} is not registered"
                )))
            })));
            return ActorAction::Continue;
        };
        let shuffle_key = self.shuffle_key(shuffle_id);
        let compression = self.options.compression;
        let clients = locations
            .into_iter()
            .map(|location| {
                self.worker_clients.client(
                    WorkerClientOptions::new(location)
                        .with_endpoint_resolver(self.options.endpoint_resolver.clone()),
                )
            })
            .collect::<Vec<_>>();
        // Every location (one per split or revived epoch) is read with the same map
        // range, and one filter spans all of them so that a batch stored in two
        // locations is returned once.
        let filter = BatchFilter::new(&options, compression);
        let map_range = options.map_range;
        ctx.spawn(async move {
            let stream = stream::iter(clients)
                .then(move |client| {
                    let shuffle_key = shuffle_key.clone();
                    let map_range = map_range.clone();
                    async move { client.read_partition_stream(&shuffle_key, map_range).await }
                })
                .flatten()
                .scan(filter, |filter, item| {
                    let item = match item {
                        Ok((header, payload)) => filter.accept(header, payload).transpose(),
                        Err(error) => Some(Err(error)),
                    };
                    std::future::ready(Some(item))
                })
                .filter_map(std::future::ready);
            let _ = reply.send(Box::pin(stream));
        });
        ActorAction::Continue
    }

    pub(super) fn handle_stop(&mut self, reply: oneshot::Sender<()>) -> ActorAction {
        let _ = reply.send(());
        ActorAction::Stop
    }

    fn shuffle_key(&self, shuffle_id: i32) -> String {
        format!("{}-{shuffle_id}", self.options.application_id)
    }

    fn update_partition_location(&mut self, shuffle_id: i32, location: PartitionLocation) {
        let key = (shuffle_id, location.id);
        let history = self.location_history.entry(key).or_default();
        if !history
            .iter()
            .any(|existing| existing.unique_id() == location.unique_id())
        {
            history.push(location.clone());
            history.sort_by_key(|location| location.epoch);
        }
        let update_latest = self
            .locations
            .get(&key)
            .is_none_or(|current| current.epoch <= location.epoch);
        if update_latest {
            self.locations.insert(key, location.clone());
        }
    }
}

fn push_failure_cause(error: &CelebornError) -> Option<i32> {
    match error {
        CelebornError::Worker { status } => Some(*status),
        CelebornError::Io(_) => Some(StatusCode::PushDataCreateConnectionFailPrimary as i32),
        CelebornError::Timeout => Some(StatusCode::PushDataTimeoutPrimary as i32),
        _ => None,
    }
}

/// The batch that a push writes.
struct PushTarget {
    shuffle_id: i32,
    partition_id: i32,
    map_id: i32,
    attempt_id: i32,
    batch_id: i32,
}

/// Pushes an already compressed payload, reviving the partition location once when
/// the worker asks for a split or the push fails. Returns the new location, if any.
async fn push_compressed(
    client: WorkerClient,
    shuffle_key: &str,
    target: PushTarget,
    location: PartitionLocation,
    data: Bytes,
    lifecycle_manager: Arc<dyn LifecycleManager>,
    worker_clients: WorkerClientPool,
    endpoint_resolver: Option<Arc<dyn EndpointResolver>>,
) -> Result<Option<PartitionLocation>, (Option<PartitionLocation>, CelebornError)> {
    let PushTarget {
        shuffle_id,
        partition_id,
        map_id,
        attempt_id,
        batch_id,
    } = target;
    let push = |client: WorkerClient, data: Bytes| async move {
        client
            .push_data(
                shuffle_key,
                map_id,
                attempt_id,
                batch_id,
                data,
                CompressionCodec::None,
            )
            .await
            .map(|_| ())
    };
    let revive = |cause: i32, old_location: PartitionLocation| {
        let lifecycle_manager = Arc::clone(&lifecycle_manager);
        async move {
            lifecycle_manager
                .revive(ReviveRequest {
                    shuffle_id,
                    partition_id,
                    map_id,
                    attempt_id,
                    old_location,
                    cause,
                })
                .await
        }
    };
    match push(client, data.clone()).await {
        Ok(()) => Ok(None),
        // The worker accepted the batch before asking for a split.
        Err(CelebornError::Worker { status }) if status == StatusCode::SoftSplit as i32 => {
            match revive(status, location).await {
                Ok(location) => Ok(Some(location)),
                Err(error) => Err((None, error)),
            }
        }
        Err(error) => match push_failure_cause(&error) {
            Some(cause) => match revive(cause, location).await {
                Ok(location) => {
                    let client = worker_clients.client(
                        WorkerClientOptions::new(location.clone())
                            .with_endpoint_resolver(endpoint_resolver),
                    );
                    match push(client, data).await {
                        Ok(()) => Ok(Some(location)),
                        Err(error) => Err((Some(location), error)),
                    }
                }
                Err(error) => Err((None, error)),
            },
            None => Err((None, error)),
        },
    }
}
