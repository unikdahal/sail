use futures::future::join_all;
use log::warn;
use sail_common::actor::{ActorAction, ActorContext};
use tokio::sync::oneshot;

use crate::common::{
    ApplicationMetrics, PartitionLocation, SlotReservation, UserIdentifier, WorkerSlotLocations,
};
use crate::error::{CelebornError, CelebornResult};
use crate::lifecycle::actor::{
    LifecycleManagerActor, PendingUnregister, ShuffleCommitState, ShuffleKey,
};
use crate::lifecycle::{LifecycleManagerMessage, ReviveRequest, ShuffleCommitReport};
use crate::protocol::StatusCode;
use crate::worker::WorkerClientOptions;

impl LifecycleManagerActor {
    pub(super) fn handle_report_metrics(
        &mut self,
        metrics: ApplicationMetrics,
        reply: oneshot::Sender<CelebornResult<()>>,
    ) -> ActorAction {
        self.application_metrics.add_assign(metrics);
        let _ = reply.send(Ok(()));
        ActorAction::Continue
    }

    pub(super) fn handle_heartbeat(&mut self, ctx: &mut ActorContext<Self>) -> ActorAction {
        ctx.send_with_delay(
            LifecycleManagerMessage::Heartbeat,
            self.options.heartbeat_interval,
        );
        if self.heartbeat_metrics.is_some() {
            return ActorAction::Continue;
        }
        let client = self.client.clone();
        let application_id = self.options.application_id.clone();
        let metrics = std::mem::take(&mut self.application_metrics);
        self.heartbeat_metrics = Some(metrics.clone());
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let result = client.heartbeat_application(application_id, metrics).await;
            let _ = handle
                .send(LifecycleManagerMessage::HeartbeatComplete { result })
                .await;
        });
        ActorAction::Continue
    }

    pub(super) fn handle_heartbeat_complete(&mut self, result: CelebornResult<()>) -> ActorAction {
        if let Err(error) = result {
            if let Some(metrics) = self.heartbeat_metrics.take() {
                self.application_metrics.add_assign(metrics);
            }
            warn!("failed to send Celeborn application heartbeat: {error}");
        } else {
            self.heartbeat_metrics = None;
        }
        ActorAction::Continue
    }

    pub(super) fn handle_get_shuffle_id(
        &mut self,
        job_id: u64,
        stage: u64,
        reply: oneshot::Sender<CelebornResult<i32>>,
    ) -> ActorAction {
        let key = ShuffleKey { job_id, stage };
        let id = match self.shuffle_ids.get(&key) {
            Some(id) => Ok(*id),
            None => match self.next_shuffle_id.checked_add(1) {
                Some(next) => {
                    self.next_shuffle_id = next;
                    self.shuffle_ids.insert(key, next);
                    self.shuffle_keys.insert(next, key);
                    Ok(next)
                }
                None => Err(CelebornError::Application(
                    "shuffle ID space is exhausted".to_string(),
                )),
            },
        };
        let _ = reply.send(id);
        ActorAction::Continue
    }

    pub(super) fn handle_get_job_shuffle_ids(
        &mut self,
        job_id: u64,
        reply: oneshot::Sender<CelebornResult<Vec<(u64, i32)>>>,
    ) -> ActorAction {
        let shuffle_ids = self
            .shuffle_ids
            .iter()
            .filter_map(|(key, &shuffle_id)| {
                (key.job_id == job_id).then_some((key.stage, shuffle_id))
            })
            .collect();
        let _ = reply.send(Ok(shuffle_ids));
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
        if self.pending_unregisters.contains_key(&shuffle_id) {
            let _ = reply.send(Err(unregistering(shuffle_id)));
            return ActorAction::Continue;
        }
        // Readers refresh locations through registration, so an old reservation
        // must not let them read files after the global commit has failed.
        if let Some(ShuffleCommitState::Lost { reason }) = self.shuffle_commits.get(&shuffle_id) {
            let _ = reply.send(Err(shuffle_lost(shuffle_id, reason)));
            return ActorAction::Continue;
        }
        if let Some(reservation) = self.reservations.get(&shuffle_id) {
            let _ = reply.send(Ok(reservation.clone()));
            return ActorAction::Continue;
        }
        if let Some(replies) = self.pending_slot_requests.get_mut(&shuffle_id) {
            replies.push(reply);
            return ActorAction::Continue;
        }
        self.pending_slot_requests.insert(shuffle_id, vec![reply]);
        if let Some(error) = self.application_registration.error() {
            if let Some(replies) = self.pending_slot_requests.remove(&shuffle_id) {
                for reply in replies {
                    let _ = reply.send(Err(CelebornError::Application(error.to_string())));
                }
            }
            return ActorAction::Continue;
        }
        let client = self.client.clone();
        let application_id = self.options.application_id.clone();
        let hostname = self.options.hostname.clone();
        let user_identifier = self.user_identifier();
        let endpoint_resolver = self.options.endpoint_resolver.clone();
        let worker_clients = self.worker_clients.clone();
        let partition_split_threshold = self.options.partition_split_threshold;
        let partition_split_mode = self.options.partition_split_mode;
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let result = async {
                let reservation = client
                    .request_slots(
                        application_id.clone(),
                        shuffle_id,
                        partition_ids,
                        hostname,
                        should_replicate,
                        max_workers,
                        user_identifier.clone(),
                        Vec::new(),
                    )
                    .await?;
                for locations in reservation.worker_locations.values() {
                    let Some(location) = locations
                        .primary_locations
                        .first()
                        .or_else(|| locations.replica_locations.first())
                        .cloned()
                    else {
                        continue;
                    };
                    worker_clients
                        .client(
                            WorkerClientOptions::new(location)
                                .with_endpoint_resolver(endpoint_resolver.clone()),
                        )
                        .reserve_slots(
                            application_id.clone(),
                            shuffle_id,
                            locations.primary_locations.clone(),
                            locations.replica_locations.clone(),
                            user_identifier.clone(),
                            partition_split_threshold,
                            partition_split_mode,
                        )
                        .await?;
                }
                Ok(reservation)
            }
            .await;
            let _ = handle
                .send(LifecycleManagerMessage::RegisterShuffleComplete { shuffle_id, result })
                .await;
        });
        ActorAction::Continue
    }

    pub(super) fn handle_register_shuffle_complete(
        &mut self,
        ctx: &mut ActorContext<Self>,
        shuffle_id: i32,
        result: CelebornResult<SlotReservation>,
    ) -> ActorAction {
        let replies = self
            .pending_slot_requests
            .remove(&shuffle_id)
            .unwrap_or_default();
        if self.pending_unregisters.contains_key(&shuffle_id) {
            self.advance_unregister(ctx, shuffle_id);
            return ActorAction::Continue;
        }
        if let Ok(reservation) = &result {
            self.registered_shuffles
                .insert(shuffle_id, reservation.worker_locations.clone());
            self.reservations.insert(shuffle_id, reservation.clone());
            self.application_metrics.shuffle_count =
                self.application_metrics.shuffle_count.saturating_add(1);
            for reply in replies {
                let _ = reply.send(Ok(reservation.clone()));
            }
        } else if let Err(error) = result {
            for reply in replies {
                let _ = reply.send(Err(CelebornError::Application(error.to_string())));
            }
        }
        ActorAction::Continue
    }

    pub(super) fn handle_revive(
        &mut self,
        ctx: &mut ActorContext<Self>,
        request: ReviveRequest,
        reply: oneshot::Sender<CelebornResult<PartitionLocation>>,
    ) -> ActorAction {
        if self.pending_unregisters.contains_key(&request.shuffle_id) {
            let _ = reply.send(Err(unregistering(request.shuffle_id)));
            return ActorAction::Continue;
        }
        if let Some(error) = self.application_registration.error() {
            let _ = reply.send(Err(error));
            return ActorAction::Continue;
        }
        if self
            .shuffle_commits
            .get(&request.shuffle_id)
            .is_some_and(|state| !matches!(state, ShuffleCommitState::Collecting { .. }))
        {
            let _ = reply.send(Err(CelebornError::Application(format!(
                "shuffle {} has already ended",
                request.shuffle_id
            ))));
            return ActorAction::Continue;
        }
        let Some(current) = self
            .reservations
            .get(&request.shuffle_id)
            .and_then(|reservation| reservation.primary_locations.get(&request.partition_id))
            .cloned()
        else {
            let _ = reply.send(Err(CelebornError::Application(format!(
                "shuffle {} partition {} is not registered",
                request.shuffle_id, request.partition_id
            ))));
            return ActorAction::Continue;
        };
        if current.epoch > request.old_location.epoch {
            let _ = reply.send(Ok(current));
            return ActorAction::Continue;
        }
        if current.epoch != request.old_location.epoch {
            let _ = reply.send(Err(CelebornError::Application(format!(
                "shuffle {} partition {} has an unexpected epoch",
                request.shuffle_id, request.partition_id
            ))));
            return ActorAction::Continue;
        }
        self.exclude_failed_workers(&request);
        let key = (request.shuffle_id, request.partition_id);
        if let Some(replies) = self.pending_revives.get_mut(&key) {
            replies.push(reply);
            return ActorAction::Continue;
        }
        self.pending_revives.insert(key, vec![reply]);

        let client = self.client.clone();
        let application_id = self.options.application_id.clone();
        let hostname = self.options.hostname.clone();
        let user_identifier = self.user_identifier();
        let endpoint_resolver = self.options.endpoint_resolver.clone();
        let worker_clients = self.worker_clients.clone();
        let partition_split_threshold = self.options.partition_split_threshold;
        let partition_split_mode = self.options.partition_split_mode;
        let excluded_workers = self.excluded_workers.values().cloned().collect();
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let result = async {
                let reservation = client
                    .request_slots(
                        application_id.clone(),
                        request.shuffle_id,
                        vec![request.partition_id],
                        hostname,
                        current.peer.is_some(),
                        1,
                        user_identifier.clone(),
                        excluded_workers,
                    )
                    .await?;
                let reservation =
                    reservation.with_epoch(current.epoch.checked_add(1).ok_or_else(|| {
                        CelebornError::Application("partition epoch is exhausted".to_string())
                    })?);
                for locations in reservation.worker_locations.values() {
                    let Some(location) = locations
                        .primary_locations
                        .first()
                        .or_else(|| locations.replica_locations.first())
                        .cloned()
                    else {
                        continue;
                    };
                    worker_clients
                        .client(
                            WorkerClientOptions::new(location)
                                .with_endpoint_resolver(endpoint_resolver.clone()),
                        )
                        .reserve_slots(
                            application_id.clone(),
                            request.shuffle_id,
                            locations.primary_locations.clone(),
                            locations.replica_locations.clone(),
                            user_identifier.clone(),
                            partition_split_threshold,
                            partition_split_mode,
                        )
                        .await?;
                }
                Ok(reservation)
            }
            .await;
            let _ = handle
                .send(LifecycleManagerMessage::ReviveComplete {
                    shuffle_id: request.shuffle_id,
                    partition_id: request.partition_id,
                    result,
                })
                .await;
        });
        ActorAction::Continue
    }

    pub(super) fn handle_revive_complete(
        &mut self,
        ctx: &mut ActorContext<Self>,
        shuffle_id: i32,
        partition_id: i32,
        result: CelebornResult<SlotReservation>,
    ) -> ActorAction {
        let replies = self
            .pending_revives
            .remove(&(shuffle_id, partition_id))
            .unwrap_or_default();
        if self.pending_unregisters.contains_key(&shuffle_id) {
            self.advance_unregister(ctx, shuffle_id);
            return ActorAction::Continue;
        }
        match result {
            Ok(replacement) => {
                let Some(location) = replacement.primary_locations.get(&partition_id).cloned()
                else {
                    let error = CelebornError::Application(format!(
                        "revive for shuffle {shuffle_id} partition {partition_id} returned no location"
                    ));
                    for reply in replies {
                        let _ = reply.send(Err(CelebornError::Application(error.to_string())));
                    }
                    return ActorAction::Continue;
                };
                let Some(reservation) = self.reservations.get_mut(&shuffle_id) else {
                    let client = self.client.clone();
                    let application_id = self.options.application_id.clone();
                    ctx.spawn(async move {
                        if let Err(error) = client.unregister_shuffle(application_id, shuffle_id).await {
                            warn!(
                                "failed to clean up Celeborn shuffle {shuffle_id} after a cancelled revive: {error}"
                            );
                        }
                    });
                    let error = CelebornError::Application(format!(
                        "revive for shuffle {shuffle_id} partition {partition_id} was cancelled"
                    ));
                    for reply in replies {
                        let _ = reply.send(Err(CelebornError::Application(error.to_string())));
                    }
                    return ActorAction::Continue;
                };
                reservation
                    .primary_locations
                    .insert(partition_id, location.clone());
                merge_worker_locations(
                    &mut reservation.worker_locations,
                    replacement.worker_locations.clone(),
                );
                reservation.worker_ids.extend(replacement.worker_ids);
                reservation.worker_ids.sort();
                reservation.worker_ids.dedup();
                if let Some(registered) = self.registered_shuffles.get_mut(&shuffle_id) {
                    merge_worker_locations(registered, replacement.worker_locations);
                }
                for reply in replies {
                    let _ = reply.send(Ok(location.clone()));
                }
            }
            Err(error) => {
                for reply in replies {
                    let _ = reply.send(Err(CelebornError::Application(error.to_string())));
                }
            }
        }
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
        if self.pending_unregisters.contains_key(&shuffle_id) {
            let _ = reply.send(Err(unregistering(shuffle_id)));
            return ActorAction::Continue;
        }
        let num_mappers = match usize::try_from(num_mappers) {
            Ok(num_mappers) if num_mappers > 0 => num_mappers,
            _ => {
                let _ = reply.send(Err(CelebornError::Application(
                    "number of mappers must be positive".to_string(),
                )));
                return ActorAction::Continue;
            }
        };
        let map_id = match usize::try_from(map_id) {
            Ok(map_id) if map_id < num_mappers => map_id,
            _ => {
                let _ = reply.send(Err(CelebornError::Application(format!(
                    "map ID {map_id} is outside the mapper range"
                ))));
                return ActorAction::Continue;
            }
        };
        let state = self.shuffle_commits.entry(shuffle_id).or_insert_with(|| {
            ShuffleCommitState::Collecting {
                attempts: vec![-1; num_mappers],
            }
        });
        let attempts = match state {
            ShuffleCommitState::Committed => {
                let _ = reply.send(Ok(()));
                return ActorAction::Continue;
            }
            ShuffleCommitState::Lost { reason } => {
                let _ = reply.send(Err(shuffle_lost(shuffle_id, reason)));
                return ActorAction::Continue;
            }
            ShuffleCommitState::Committing { waiters, .. }
            | ShuffleCommitState::Reporting { waiters } => {
                waiters.push(reply);
                return ActorAction::Continue;
            }
            ShuffleCommitState::Collecting { attempts } => {
                if attempts.len() != num_mappers {
                    let _ = reply.send(Err(CelebornError::Application(format!(
                        "shuffle {shuffle_id} was registered with {} mappers, not {num_mappers}",
                        attempts.len()
                    ))));
                    return ActorAction::Continue;
                }
                // Match Celeborn's speculative execution semantics: the first attempt of
                // each mapper to end is selected. Later attempts are acknowledged
                // without changing the selection.
                if attempts[map_id] == -1 {
                    attempts[map_id] = attempt_id;
                }
                if attempts.contains(&-1) {
                    let _ = reply.send(Ok(()));
                    return ActorAction::Continue;
                }
                std::mem::take(attempts)
            }
        };
        let Some(worker_locations) = self.registered_shuffles.get(&shuffle_id).cloned() else {
            let reason = "the shuffle is not registered".to_string();
            let _ = reply.send(Err(shuffle_lost(shuffle_id, &reason)));
            *state = ShuffleCommitState::Lost { reason };
            return ActorAction::Continue;
        };
        *state = ShuffleCommitState::Committing {
            attempts: attempts.clone(),
            waiters: vec![reply],
        };
        let application_id = self.options.application_id.clone();
        let endpoint_resolver = self.options.endpoint_resolver.clone();
        let worker_clients = self.worker_clients.clone();
        let max_retries = self.options.commit_max_retries;
        let retry_wait = self.options.commit_retry_wait;
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            // Workers commit independently, so commit them concurrently.
            let commits = worker_locations.into_values().filter_map(|locations| {
                let location = locations
                    .primary_locations
                    .first()
                    .or_else(|| locations.replica_locations.first())?
                    .clone();
                let host = location.host.clone();
                let client = worker_clients.client(
                    WorkerClientOptions::new(location)
                        .with_endpoint_resolver(endpoint_resolver.clone()),
                );
                let application_id = application_id.clone();
                let attempts = attempts.clone();
                Some(async move {
                    let mut retries = 0;
                    loop {
                        // The same request every time: a worker that already committed
                        // it returns its cached response.
                        match client
                            .commit_files(
                                application_id.clone(),
                                shuffle_id,
                                locations.primary_locations.clone(),
                                locations.replica_locations.clone(),
                                attempts.clone(),
                            )
                            .await
                        {
                            Ok(metrics) => break Ok(metrics),
                            Err(error) if retries < max_retries => {
                                retries += 1;
                                warn!(
                                    "retrying commit of shuffle {shuffle_id} on worker {host} \
                                     ({retries}/{max_retries}): {error}"
                                );
                                tokio::time::sleep(retry_wait).await;
                            }
                            Err(error) => break Err(error),
                        }
                    }
                })
            });
            // Keep every worker transaction alive until it finishes, including
            // after another worker fails. Unregister waits for this completion
            // before asking the master to remove worker resources.
            let result = join_all(commits)
                .await
                .into_iter()
                .collect::<CelebornResult<Vec<_>>>()
                .and_then(|commits| {
                    let mut metrics = ApplicationMetrics::default();
                    let mut unreadable = vec![];
                    for commit in commits {
                        metrics.total_written =
                            metrics.total_written.saturating_add(commit.total_written);
                        metrics.file_count = metrics.file_count.saturating_add(commit.file_count);
                        unreadable.extend(commit.unreadable_files);
                    }
                    // A commit is final, so a shuffle with files that cannot be read is lost.
                    // Reading them would return no data instead of failing.
                    match unreadable.first() {
                        None => Ok(metrics),
                        Some((id, storage)) => Err(CelebornError::Application(format!(
                            "{} committed partition files are on distributed storage, which is \
                         not supported (for example, location {id} on {storage})",
                            unreadable.len()
                        ))),
                    }
                });
            let _ = handle
                .send(LifecycleManagerMessage::CommitComplete { shuffle_id, result })
                .await;
        });
        ActorAction::Continue
    }

    pub(super) fn handle_commit_complete(
        &mut self,
        ctx: &mut ActorContext<Self>,
        shuffle_id: i32,
        result: CelebornResult<ApplicationMetrics>,
    ) -> ActorAction {
        if self.pending_unregisters.contains_key(&shuffle_id) {
            self.shuffle_commits.remove(&shuffle_id);
            self.advance_unregister(ctx, shuffle_id);
            return ActorAction::Continue;
        }
        // The shuffle may have been unregistered during the commit.
        let Some(state) = self.shuffle_commits.get_mut(&shuffle_id) else {
            return ActorAction::Continue;
        };
        let ShuffleCommitState::Committing { attempts, waiters } = state else {
            warn!("unexpected commit completion for shuffle {shuffle_id}");
            return ActorAction::Continue;
        };
        let attempts = std::mem::take(attempts);
        let waiters = std::mem::take(waiters);
        *state = ShuffleCommitState::Reporting { waiters };
        let reply = match result {
            Ok(metrics) => {
                self.application_metrics.add_assign(metrics);
                Ok(())
            }
            Err(error) => {
                warn!("shuffle {shuffle_id} is lost: commit failed: {error}");
                Err(error.to_string())
            }
        };
        let report = self.options.commit_observer.as_ref().and_then(|observer| {
            match self.shuffle_keys.get(&shuffle_id) {
                Some(key) => Some((
                    observer.clone(),
                    ShuffleCommitReport {
                        job_id: key.job_id,
                        stage: key.stage,
                        shuffle_id,
                        map_attempts: attempts,
                        result: reply.clone(),
                    },
                )),
                None => {
                    warn!("cannot report commit of unknown shuffle {shuffle_id}");
                    None
                }
            }
        });
        // Keep every mapper awaiting the global commit blocked until its report is
        // known, including attempts that end while the observer is running.
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            if let Some((observer, report)) = report {
                observer.shuffle_committed(report).await;
            }
            let _ = handle
                .send(LifecycleManagerMessage::CommitReported {
                    shuffle_id,
                    result: reply,
                })
                .await;
        });
        ActorAction::Continue
    }

    pub(super) fn handle_commit_reported(
        &mut self,
        ctx: &mut ActorContext<Self>,
        shuffle_id: i32,
        result: Result<(), String>,
    ) -> ActorAction {
        if self.pending_unregisters.contains_key(&shuffle_id) {
            self.shuffle_commits.remove(&shuffle_id);
            self.advance_unregister(ctx, shuffle_id);
            return ActorAction::Continue;
        }
        let Some(state) = self.shuffle_commits.get_mut(&shuffle_id) else {
            return ActorAction::Continue;
        };
        let ShuffleCommitState::Reporting { waiters } = state else {
            warn!("unexpected commit report completion for shuffle {shuffle_id}");
            return ActorAction::Continue;
        };
        let waiters = std::mem::take(waiters);
        let outcome = match &result {
            Ok(()) => ShuffleCommitState::Committed,
            Err(reason) => ShuffleCommitState::Lost {
                reason: reason.clone(),
            },
        };
        *state = outcome;
        for waiter in waiters {
            let _ = waiter.send(
                result
                    .clone()
                    .map_err(|reason| shuffle_lost(shuffle_id, &reason)),
            );
        }
        ActorAction::Continue
    }

    pub(super) fn handle_unregister_shuffle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        shuffle_id: i32,
        reply: oneshot::Sender<CelebornResult<()>>,
    ) -> ActorAction {
        if let Some(pending) = self.pending_unregisters.get_mut(&shuffle_id) {
            pending.waiters.push(reply);
            self.advance_unregister(ctx, shuffle_id);
            return ActorAction::Continue;
        }
        if let Some(error) = self.application_registration.error() {
            let _ = reply.send(Err(error));
            return ActorAction::Continue;
        }
        self.pending_unregisters.insert(
            shuffle_id,
            PendingUnregister {
                waiters: vec![reply],
                request_sent: false,
            },
        );
        // Close the shuffle immediately, but retain the empty pending entries
        // until their network operations finish. Final master cleanup must run
        // after every ReserveSlots/CommitFiles transaction, or a delayed request
        // could recreate worker resources after cleanup was acknowledged.
        if let Some(waiters) = self.pending_slot_requests.get_mut(&shuffle_id) {
            for waiter in waiters.drain(..) {
                let _ = waiter.send(Err(unregistering(shuffle_id)));
            }
        }
        for ((id, _), waiters) in &mut self.pending_revives {
            if *id == shuffle_id {
                for waiter in waiters.drain(..) {
                    let _ = waiter.send(Err(unregistering(shuffle_id)));
                }
            }
        }
        if let Some(
            ShuffleCommitState::Committing { waiters, .. }
            | ShuffleCommitState::Reporting { waiters },
        ) = self.shuffle_commits.get_mut(&shuffle_id)
        {
            for waiter in waiters.drain(..) {
                let _ = waiter.send(Err(unregistering(shuffle_id)));
            }
        }
        self.advance_unregister(ctx, shuffle_id);
        ActorAction::Continue
    }

    fn advance_unregister(&mut self, ctx: &mut ActorContext<Self>, shuffle_id: i32) {
        if self.pending_slot_requests.contains_key(&shuffle_id)
            || self.pending_revives.keys().any(|(id, _)| *id == shuffle_id)
            || matches!(
                self.shuffle_commits.get(&shuffle_id),
                Some(ShuffleCommitState::Committing { .. } | ShuffleCommitState::Reporting { .. })
            )
        {
            return;
        }
        let Some(pending) = self.pending_unregisters.get_mut(&shuffle_id) else {
            return;
        };
        if pending.request_sent {
            return;
        }
        pending.request_sent = true;
        let client = self.client.clone();
        let application_id = self.options.application_id.clone();
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let result = client.unregister_shuffle(application_id, shuffle_id).await;
            let _ = handle
                .send(LifecycleManagerMessage::UnregisterShuffleComplete { shuffle_id, result })
                .await;
        });
    }

    pub(super) fn handle_unregister_shuffle_complete(
        &mut self,
        shuffle_id: i32,
        result: CelebornResult<()>,
    ) -> ActorAction {
        let Some(pending) = self.pending_unregisters.get_mut(&shuffle_id) else {
            return ActorAction::Continue;
        };
        let waiters = std::mem::take(&mut pending.waiters);
        // A failed response cannot prove that cleanup did not happen. Keep the
        // shuffle closed, but permit another explicit unregister to retry it.
        pending.request_sent = false;
        if result.is_ok() {
            self.pending_unregisters.remove(&shuffle_id);
            if let Some(key) = self.shuffle_keys.remove(&shuffle_id) {
                self.shuffle_ids.remove(&key);
            }
            self.registered_shuffles.remove(&shuffle_id);
            self.reservations.remove(&shuffle_id);
            self.shuffle_commits.remove(&shuffle_id);
        }
        let error = result.err().map(|error| error.to_string());
        for waiter in waiters {
            let _ = waiter.send(match &error {
                None => Ok(()),
                Some(error) => Err(CelebornError::Application(error.clone())),
            });
        }
        ActorAction::Continue
    }

    pub(super) fn handle_stop(
        &mut self,
        ctx: &mut ActorContext<Self>,
        result: oneshot::Sender<()>,
    ) -> ActorAction {
        let client = self.client.clone();
        let application_id = self.options.application_id.clone();
        let shuffle_ids = self.registered_shuffles.keys().copied().collect::<Vec<_>>();
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            for shuffle_id in shuffle_ids {
                if let Err(error) = client
                    .unregister_shuffle(application_id.clone(), shuffle_id)
                    .await
                {
                    warn!(
                        "failed to unregister Celeborn shuffle {shuffle_id} while stopping: {error}"
                    );
                }
            }
            let _ = handle
                .send(LifecycleManagerMessage::StopComplete { result })
                .await;
        });
        ActorAction::Continue
    }

    pub(super) fn user_identifier(&self) -> UserIdentifier {
        UserIdentifier {
            tenant_id: self.options.tenant_id.clone(),
            name: self.options.user_name.clone(),
        }
    }

    fn exclude_failed_workers(&mut self, request: &ReviveRequest) {
        let failed_locations = match request.cause {
            status
                if status == StatusCode::PushDataWriteFailPrimary as i32
                    || status == StatusCode::PushDataCreateConnectionFailPrimary as i32
                    || status == StatusCode::PushDataConnectionExceptionPrimary as i32
                    || status == StatusCode::PushDataTimeoutPrimary as i32
                    || status == StatusCode::PushDataPrimaryWorkerExcluded as i32 =>
            {
                vec![request.old_location.clone()]
            }
            status
                if status == StatusCode::PushDataWriteFailReplica as i32
                    || status == StatusCode::PushDataCreateConnectionFailReplica as i32
                    || status == StatusCode::PushDataConnectionExceptionReplica as i32
                    || status == StatusCode::PushDataTimeoutReplica as i32
                    || status == StatusCode::PushDataReplicaWorkerExcluded as i32 =>
            {
                request
                    .old_location
                    .peer
                    .iter()
                    .map(|peer| (**peer).clone())
                    .collect()
            }
            _ => Vec::new(),
        };
        for location in failed_locations {
            self.excluded_workers
                .entry(location.worker_identity())
                .or_insert(location);
        }
    }
}

impl SlotReservation {
    fn with_epoch(mut self, epoch: i32) -> Self {
        for location in self.primary_locations.values_mut() {
            location.set_epoch(epoch);
        }
        for locations in self.worker_locations.values_mut() {
            for location in &mut locations.primary_locations {
                location.set_epoch(epoch);
            }
            for location in &mut locations.replica_locations {
                location.set_epoch(epoch);
            }
        }
        self
    }
}

fn merge_worker_locations(
    target: &mut std::collections::HashMap<crate::common::WorkerIdentity, WorkerSlotLocations>,
    source: std::collections::HashMap<crate::common::WorkerIdentity, WorkerSlotLocations>,
) {
    for (worker_identity, locations) in source {
        let target_locations =
            target
                .entry(worker_identity)
                .or_insert_with(|| WorkerSlotLocations {
                    primary_locations: Vec::new(),
                    replica_locations: Vec::new(),
                });
        append_locations(
            &mut target_locations.primary_locations,
            locations.primary_locations,
        );
        append_locations(
            &mut target_locations.replica_locations,
            locations.replica_locations,
        );
    }
}

fn append_locations(target: &mut Vec<PartitionLocation>, source: Vec<PartitionLocation>) {
    for location in source {
        if !target
            .iter()
            .any(|existing| existing.unique_id() == location.unique_id())
        {
            target.push(location);
        }
    }
}

fn unregistering(shuffle_id: i32) -> CelebornError {
    CelebornError::Application(format!("shuffle {shuffle_id} is being unregistered"))
}

fn shuffle_lost(shuffle_id: i32, reason: &str) -> CelebornError {
    CelebornError::Application(format!(
        "shuffle {shuffle_id} is lost: commit failed: {reason}"
    ))
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use prost::Message;
    use sail_common::actor::ActorSystem;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::{Notify, oneshot};

    use crate::common::{PartitionLocation, SlotReservation, WorkerSlotLocations};
    use crate::lifecycle::{
        LifecycleManager, LifecycleManagerActor, LifecycleManagerMessage, LifecycleManagerOptions,
        ShuffleCommitObserver, ShuffleCommitReport,
    };
    use crate::master::MasterClientOptions;
    use crate::protocol::proto::{
        MessageType, PbCommitFilesResponse, PbHeartbeatFromApplicationResponse,
        PbRequestSlotsResponse, PbReserveSlotsResponse, PbUnregisterShuffleResponse,
        PbWorkerResource,
    };
    use crate::protocol::transport::TransportMessage;

    async fn read_control_request(socket: &mut TcpStream) -> (i64, TransportMessage) {
        let header_length = socket.read_i32().await.unwrap() as usize;
        assert_eq!(socket.read_u8().await.unwrap(), 3); // RpcRequest
        let body_length = socket.read_i32().await.unwrap() as usize;
        let mut header = vec![0; header_length];
        let mut body = vec![0; body_length];
        socket.read_exact(&mut header).await.unwrap();
        socket.read_exact(&mut body).await.unwrap();
        let request_id = i64::from_be_bytes(header[..8].try_into().unwrap());
        // The control message follows the two addresses and endpoint name.
        let mut offset = 1;
        for address in 0..2 {
            let length = u16::from_be_bytes(body[offset..offset + 2].try_into().unwrap());
            offset += 2 + usize::from(length) + 4;
            if address == 0 {
                assert_eq!(body[offset], 1);
                offset += 1; // receiver address protocol version
            }
        }
        let length = u16::from_be_bytes(body[offset..offset + 2].try_into().unwrap());
        offset += 2 + usize::from(length);
        assert_eq!(body[offset], 0xff);
        (
            request_id,
            TransportMessage::decode(&body[offset + 1..]).unwrap(),
        )
    }

    async fn reply_control(
        socket: &mut TcpStream,
        request_id: i64,
        kind: MessageType,
        payload: Vec<u8>,
    ) {
        // Java serialization's stable TransportMessage layout, as used by the
        // real master and worker control endpoints.
        fn utf(output: &mut Vec<u8>, value: &[u8]) {
            output.extend_from_slice(&(value.len() as u16).to_be_bytes());
            output.extend_from_slice(value);
        }
        let mut body = vec![0xac, 0xed, 0, 5, 0x73, 0x72];
        utf(
            &mut body,
            b"org.apache.celeborn.common.network.protocol.TransportMessage",
        );
        body.extend_from_slice(&0_i64.to_be_bytes());
        body.extend_from_slice(&[2, 0, 2, b'I']);
        utf(&mut body, b"messageTypeValue");
        body.push(b'[');
        utf(&mut body, b"payload");
        body.push(0x74);
        utf(&mut body, b"[B");
        body.extend_from_slice(&[0x78, 0x70]);
        body.extend_from_slice(&(kind as i32).to_be_bytes());
        body.extend_from_slice(&[0x75, 0x72]);
        utf(&mut body, b"[B");
        body.extend_from_slice(&0_i64.to_be_bytes());
        body.extend_from_slice(&[2, 0, 0, 0x78, 0x70]);
        body.extend_from_slice(&(payload.len() as i32).to_be_bytes());
        body.extend_from_slice(&payload);
        socket.write_i32(12).await.unwrap();
        socket.write_u8(4).await.unwrap(); // RpcResponse
        socket.write_i32(body.len() as i32).await.unwrap();
        socket.write_i64(request_id).await.unwrap();
        socket.write_i32(body.len() as i32).await.unwrap();
        socket.write_all(&body).await.unwrap();
    }

    #[tokio::test]
    async fn unregister_cancels_pending_registration_without_resurrecting_reservation() {
        check_unregister_drains_reservation(false, false).await;
    }

    #[tokio::test]
    async fn unregister_drains_pending_revive_before_cleanup_and_id_reuse() {
        check_unregister_drains_reservation(true, false).await;
    }

    #[tokio::test]
    async fn failed_unregister_keeps_shuffle_closed_until_cleanup_is_retried() {
        check_unregister_drains_reservation(true, true).await;
    }

    #[expect(clippy::panic)]
    async fn check_unregister_drains_reservation(reviving: bool, fail_cleanup: bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            let master_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let worker_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let master_port = master_listener.local_addr().unwrap().port();
            let worker_port = worker_listener.local_addr().unwrap().port();
            let location = PartitionLocation {
                mode: 0,
                id: 0,
                epoch: 0,
                host: "127.0.0.1".into(),
                rpc_port: worker_port,
                push_port: worker_port,
                fetch_port: worker_port,
                replicate_port: worker_port,
                peer: None,
            };
            let slot_requests = Arc::new(AtomicUsize::new(0));
            let old_location = location.clone();
            let master_requests = slot_requests.clone();
            let unregisters = Arc::new(AtomicUsize::new(0));
            let master_unregisters = unregisters.clone();
            let master = tokio::spawn(async move {
                let (mut socket, _) = master_listener.accept().await.unwrap();
                loop {
                    let (request_id, message) = read_control_request(&mut socket).await;
                    let (kind, payload) = match message.message_type {
                        MessageType::RegisterApplicationInfo => {
                            (MessageType::OneWayMessageResponse, vec![])
                        }
                        MessageType::HeartbeatFromApplication => (
                            MessageType::HeartbeatFromApplicationResponse,
                            PbHeartbeatFromApplicationResponse::default().encode_to_vec(),
                        ),
                        MessageType::RequestSlots => {
                            master_requests.fetch_add(1, Ordering::SeqCst);
                            (
                                MessageType::RequestSlotsResponse,
                                PbRequestSlotsResponse {
                                    worker_resource: [(
                                        format!("127.0.0.1:{worker_port}:{worker_port}:{worker_port}:{worker_port}"),
                                        PbWorkerResource {
                                            primary_partitions: vec![location.clone().into()],
                                            ..Default::default()
                                        },
                                    )].into(),
                                    ..Default::default()
                                }.encode_to_vec(),
                            )
                        }
                        MessageType::UnregisterShuffle => {
                            let attempt = master_unregisters.fetch_add(1, Ordering::SeqCst);
                            (MessageType::UnregisterShuffleResponse,
                                PbUnregisterShuffleResponse {
                                    status: if fail_cleanup && attempt == 0 {
                                        crate::protocol::StatusCode::RequestFailed as i32
                                    } else {
                                        0
                                    },
                                }.encode_to_vec())
                        },
                        kind => panic!("unexpected master request: {kind:?}"),
                    };
                    reply_control(&mut socket, request_id, kind, payload).await;
                }
            });
            let (reserve_started, started) = oneshot::channel();
            let (release_reserve, resume) = oneshot::channel();
            let worker = tokio::spawn(async move {
                let (mut socket, _) = worker_listener.accept().await.unwrap();
                if reviving {
                    let (request_id, request) = read_control_request(&mut socket).await;
                    assert_eq!(request.message_type, MessageType::ReserveSlots);
                    reply_control(&mut socket, request_id, MessageType::ReserveSlotsResponse,
                        PbReserveSlotsResponse::default().encode_to_vec()).await;
                }
                let (request_id, request) = read_control_request(&mut socket).await;
                assert_eq!(request.message_type, MessageType::ReserveSlots);
                reserve_started.send(()).unwrap();
                resume.await.unwrap();
                reply_control(&mut socket, request_id, MessageType::ReserveSlotsResponse,
                    PbReserveSlotsResponse::default().encode_to_vec()).await;
                let (request_id, request) = read_control_request(&mut socket).await;
                assert_eq!(request.message_type, MessageType::ReserveSlots);
                reply_control(&mut socket, request_id, MessageType::ReserveSlotsResponse,
                    PbReserveSlotsResponse::default().encode_to_vec()).await;
            });
            let mut system = ActorSystem::new();
            let handle = system.spawn::<LifecycleManagerActor>(LifecycleManagerOptions::new(
                "test-unregister-pending",
                MasterClientOptions::new(vec![format!("127.0.0.1:{master_port}")]),
            ));
            let manager = crate::lifecycle::LocalLifecycleManager::new(handle.clone());
            let mut registration = None;
            let mut revive = None;
            if reviving {
                let reservation = manager.register_shuffle(1, vec![0], false, 1).await.unwrap();
                let (result, pending) = oneshot::channel();
                assert!(handle.send(LifecycleManagerMessage::Revive {
                    request: crate::lifecycle::ReviveRequest {
                        shuffle_id: 1, partition_id: 0, map_id: 0, attempt_id: 0,
                        old_location: reservation.primary_locations[&0].clone(),
                        cause: crate::protocol::StatusCode::HardSplit as i32,
                    }, result,
                }).await.is_ok());
                revive = Some(pending);
            } else {
                let (result, pending) = oneshot::channel();
                assert!(handle.send(LifecycleManagerMessage::RegisterShuffle {
                    shuffle_id: 1, partition_ids: vec![0], should_replicate: false,
                    max_workers: 1, result,
                }).await.is_ok());
                registration = Some(pending);
            }
            started.await.unwrap();
            // Registration is still waiting on the worker. Both unregister
            // callers must wait until it drains, while new operations fail.
            let (result, mut first_unregister) = oneshot::channel();
            assert!(handle.send(LifecycleManagerMessage::UnregisterShuffle {
                shuffle_id: 1, result,
            }).await.is_ok());
            let (result, mut second_unregister) = oneshot::channel();
            assert!(handle.send(LifecycleManagerMessage::UnregisterShuffle {
                shuffle_id: 1, result,
            }).await.is_ok());
            manager.get_job_shuffle_ids(0).await.unwrap(); // actor barrier
            if let Some(pending) = registration.as_mut() {
                assert!(matches!(pending.try_recv(), Ok(Err(_))),
                    "unregister must fail the pending registration caller");
            }
            if let Some(pending) = revive.as_mut() {
                assert!(matches!(pending.try_recv(), Ok(Err(_))),
                    "unregister must fail the pending revive caller");
            }
            assert!(matches!(first_unregister.try_recv(), Err(oneshot::error::TryRecvError::Empty)));
            assert!(matches!(second_unregister.try_recv(), Err(oneshot::error::TryRecvError::Empty)));
            assert_eq!(unregisters.load(Ordering::SeqCst), 0,
                "master cleanup ran before the delayed reservation finished");
            assert!(manager.register_shuffle(1, vec![0], false, 1).await.is_err());
            assert!(manager.mapper_end(1, 0, 0, 1).await.is_err());
            assert!(manager.revive(crate::lifecycle::ReviveRequest {
                shuffle_id: 1, partition_id: 0, map_id: 0, attempt_id: 0,
                old_location: old_location.clone(),
                cause: crate::protocol::StatusCode::HardSplit as i32,
            }).await.is_err());
            release_reserve.send(()).unwrap();
            let first_result = first_unregister.await.unwrap();
            let second_result = second_unregister.await.unwrap();
            if fail_cleanup {
                assert!(first_result.is_err());
                assert!(second_result.is_err());
                assert!(manager.register_shuffle(1, vec![0], false, 1).await.is_err(),
                    "a failed cleanup must not reopen a cached reservation");
                assert!(manager.mapper_end(1, 0, 0, 1).await.is_err());
                assert!(manager.revive(crate::lifecycle::ReviveRequest {
                    shuffle_id: 1, partition_id: 0, map_id: 0, attempt_id: 0,
                    old_location,
                    cause: crate::protocol::StatusCode::HardSplit as i32,
                }).await.is_err());
                manager.unregister_shuffle(1).await.unwrap();
            } else {
                first_result.unwrap();
                second_result.unwrap();
            }
            assert_eq!(unregisters.load(Ordering::SeqCst), if fail_cleanup { 2 } else { 1 },
                "concurrent unregisters must share one master transaction");
            // Reusing an explicit ID must allocate again; the old delayed
            // completion must not satisfy this new request or retain its cache.
            manager.register_shuffle(1, vec![0], false, 1).await.unwrap();
            assert_eq!(slot_requests.load(Ordering::SeqCst), if reviving { 3 } else { 2 },
                "late reservation resurrected the unregistered shuffle");
            worker.await.unwrap();
            let (result, stopped) = oneshot::channel();
            assert!(handle.send(LifecycleManagerMessage::StopComplete { result }).await.is_ok());
            stopped.await.unwrap();
            system.join().await;
            master.abort();
        }).await.unwrap();
    }

    #[tokio::test]
    async fn unregister_drains_worker_commit_before_master_cleanup() {
        check_unregister_drains_commit(false, false).await;
    }

    #[tokio::test]
    async fn unregister_drains_commit_observer_before_master_cleanup() {
        check_unregister_drains_commit(true, false).await;
    }

    #[tokio::test]
    async fn unregister_drains_other_worker_commits_after_one_worker_fails() {
        check_unregister_drains_commit(false, true).await;
    }

    #[expect(clippy::panic)]
    async fn check_unregister_drains_commit(reporting: bool, failing_worker: bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            let master_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let worker_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let master_port = master_listener.local_addr().unwrap().port();
            let worker_port = worker_listener.local_addr().unwrap().port();
            let failure_listener = if failing_worker {
                Some(TcpListener::bind(("127.0.0.1", 0)).await.unwrap())
            } else {
                None
            };
            let failure_port = failure_listener
                .as_ref()
                .map(|listener| listener.local_addr().unwrap().port());
            let failure = failure_listener.map(|listener| {
                tokio::spawn(async move {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let (request_id, request) = read_control_request(&mut socket).await;
                    assert_eq!(request.message_type, MessageType::CommitFiles);
                    reply_control(
                        &mut socket,
                        request_id,
                        MessageType::CommitFilesResponse,
                        PbCommitFilesResponse {
                            status: crate::protocol::StatusCode::RequestFailed as i32,
                            ..Default::default()
                        }
                        .encode_to_vec(),
                    )
                    .await;
                })
            });
            let unregisters = Arc::new(AtomicUsize::new(0));
            let master_unregisters = unregisters.clone();
            let master = tokio::spawn(async move {
                let (mut socket, _) = master_listener.accept().await.unwrap();
                loop {
                    let (request_id, message) = read_control_request(&mut socket).await;
                    let (kind, payload) = match message.message_type {
                        MessageType::RegisterApplicationInfo => {
                            (MessageType::OneWayMessageResponse, vec![])
                        }
                        MessageType::HeartbeatFromApplication => (
                            MessageType::HeartbeatFromApplicationResponse,
                            PbHeartbeatFromApplicationResponse::default().encode_to_vec(),
                        ),
                        MessageType::UnregisterShuffle => {
                            master_unregisters.fetch_add(1, Ordering::SeqCst);
                            (
                                MessageType::UnregisterShuffleResponse,
                                PbUnregisterShuffleResponse::default().encode_to_vec(),
                            )
                        }
                        kind => panic!("unexpected master request: {kind:?}"),
                    };
                    reply_control(&mut socket, request_id, kind, payload).await;
                }
            });
            let (commit_started, started) = oneshot::channel();
            let (release_commit, resume) = oneshot::channel();
            let worker = tokio::spawn(async move {
                let (mut socket, _) = worker_listener.accept().await.unwrap();
                let (request_id, request) = read_control_request(&mut socket).await;
                assert_eq!(request.message_type, MessageType::CommitFiles);
                commit_started.send(()).unwrap();
                if !reporting {
                    resume.await.unwrap();
                }
                reply_control(
                    &mut socket,
                    request_id,
                    MessageType::CommitFilesResponse,
                    PbCommitFilesResponse::default().encode_to_vec(),
                )
                .await;
            });
            let observer = Arc::new(BlockingCommitObserver::default());
            let options = LifecycleManagerOptions::new(
                "test-unregister-commit",
                MasterClientOptions::new(vec![format!("127.0.0.1:{master_port}")]),
            )
            .with_commit_retries(0, Duration::ZERO);
            let options = if reporting {
                options.with_commit_observer(observer.clone())
            } else {
                options
            };
            let mut system = ActorSystem::new();
            let handle = system.spawn::<LifecycleManagerActor>(options);
            let manager = crate::lifecycle::LocalLifecycleManager::new(handle.clone());
            let shuffle_id = manager.get_shuffle_id(9, 0).await.unwrap();
            let location = PartitionLocation {
                mode: 0,
                id: 0,
                epoch: 0,
                host: "127.0.0.1".into(),
                rpc_port: worker_port,
                push_port: worker_port,
                fetch_port: worker_port,
                replicate_port: worker_port,
                peer: None,
            };
            let mut locations = vec![location];
            if let Some(port) = failure_port {
                locations.push(PartitionLocation {
                    id: 1,
                    rpc_port: port,
                    push_port: port,
                    fetch_port: port,
                    replicate_port: port,
                    ..locations[0].clone()
                });
            }
            assert!(
                handle
                    .send(LifecycleManagerMessage::RegisterShuffleComplete {
                        shuffle_id,
                        result: Ok(SlotReservation {
                            worker_ids: locations
                                .iter()
                                .map(|location| location.worker_identity())
                                .collect(),
                            primary_locations: locations
                                .iter()
                                .map(|location| (location.id, location.clone()))
                                .collect(),
                            worker_locations: locations
                                .into_iter()
                                .map(|location| (
                                    location.worker_identity(),
                                    WorkerSlotLocations {
                                        primary_locations: vec![location],
                                        replica_locations: vec![],
                                    }
                                ))
                                .collect(),
                        }),
                    })
                    .await
                    .is_ok()
            );
            let (result, mut mapper) = oneshot::channel();
            assert!(
                handle
                    .send(LifecycleManagerMessage::MapperEnd {
                        shuffle_id,
                        map_id: 0,
                        attempt_id: 0,
                        num_mappers: 1,
                        result,
                    })
                    .await
                    .is_ok()
            );
            started.await.unwrap();
            if let Some(failure) = failure {
                failure.await.unwrap();
            }
            if reporting {
                observer.started.notified().await;
            }
            let (result, mut cleanup) = oneshot::channel();
            assert!(
                handle
                    .send(LifecycleManagerMessage::UnregisterShuffle { shuffle_id, result })
                    .await
                    .is_ok()
            );
            manager.get_job_shuffle_ids(9).await.unwrap(); // actor barrier
            assert!(matches!(mapper.try_recv(), Ok(Err(_))));
            if failing_worker {
                // The failed worker's response has been sent. Give its error
                // time to propagate while the other transaction remains gated;
                // try_join_all would cancel it and allow cleanup to finish.
                assert!(
                    tokio::time::timeout(Duration::from_millis(100), &mut cleanup)
                        .await
                        .is_err(),
                    "a failed worker cancelled the other worker's active commit"
                );
            }
            assert!(matches!(
                cleanup.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            assert_eq!(
                unregisters.load(Ordering::SeqCst),
                0,
                "master cleanup overtook an active commit/report"
            );
            assert!(manager.mapper_end(shuffle_id, 0, 0, 1).await.is_err());
            if reporting {
                observer.resume.notify_one();
            } else {
                release_commit.send(()).unwrap();
            }
            cleanup.await.unwrap().unwrap();
            assert_eq!(unregisters.load(Ordering::SeqCst), 1);
            assert!(manager.get_job_shuffle_ids(9).await.unwrap().is_empty());
            worker.await.unwrap();
            let (result, stopped) = oneshot::channel();
            assert!(
                handle
                    .send(LifecycleManagerMessage::StopComplete { result })
                    .await
                    .is_ok()
            );
            stopped.await.unwrap();
            system.join().await;
            master.abort();
        })
        .await
        .unwrap();
    }

    #[derive(Default)]
    struct BlockingCommitObserver {
        started: Notify,
        resume: Notify,
        reported: AtomicBool,
    }

    #[tonic::async_trait]
    impl ShuffleCommitObserver for BlockingCommitObserver {
        async fn shuffle_committed(&self, _report: ShuffleCommitReport) {
            self.started.notify_one();
            self.resume.notified().await;
            self.reported.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn lost_shuffle_cannot_reuse_its_cached_reservation() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
                .await
                .unwrap();
            let port = listener.local_addr().unwrap().port();
            // Closing the worker connection makes the real commit fail without
            // relying on an unused port or on a transport timeout.
            let worker = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                drop(socket);
            });
            let location = PartitionLocation {
                mode: 0,
                id: 0,
                epoch: 0,
                host: "127.0.0.1".into(),
                rpc_port: port,
                push_port: port,
                fetch_port: port,
                replicate_port: port,
                peer: None,
            };
            let identity = location.worker_identity();
            let reservation = SlotReservation {
                worker_ids: vec![identity.clone()],
                primary_locations: [(0, location.clone())].into(),
                worker_locations: [(
                    identity,
                    WorkerSlotLocations {
                        primary_locations: vec![location],
                        replica_locations: vec![],
                    },
                )]
                .into(),
            };
            let mut system = ActorSystem::new();
            let handle = system.spawn::<LifecycleManagerActor>(
                LifecycleManagerOptions::new("test", MasterClientOptions::new(vec![]))
                    .with_commit_retries(0, Duration::ZERO),
            );
            let manager = crate::lifecycle::LocalLifecycleManager::new(handle.clone());
            let shuffle_id = 1;
            assert!(
                handle
                    .send(LifecycleManagerMessage::RegisterShuffleComplete {
                        shuffle_id,
                        result: Ok(reservation.clone()),
                    })
                    .await
                    .is_ok()
            );
            assert_eq!(
                manager
                    .register_shuffle(shuffle_id, vec![0], false, 0)
                    .await
                    .unwrap(),
                reservation
            );
            let error = manager.mapper_end(shuffle_id, 0, 0, 1).await.unwrap_err();
            assert!(error.to_string().contains("is lost"), "{error}");
            worker.await.unwrap();

            // Reads refresh their locations through this same registration API.
            // An old reservation must not authorize reading a lost shuffle.
            let result = manager
                .register_shuffle(shuffle_id, vec![0], false, 0)
                .await;
            assert!(
                result.is_err(),
                "a lost shuffle returned its cached reservation: {result:?}"
            );
            let error = result.unwrap_err();
            assert!(error.to_string().contains("is lost"), "{error}");

            let (result, stopped) = oneshot::channel();
            assert!(
                handle
                    .send(LifecycleManagerMessage::StopComplete { result })
                    .await
                    .is_ok()
            );
            stopped.await.unwrap();
            system.join().await;
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn duplicate_mapper_end_waits_for_commit_observer() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let observer = Arc::new(BlockingCommitObserver::default());
            let mut system = ActorSystem::new();
            let handle = system.spawn::<LifecycleManagerActor>(
                LifecycleManagerOptions::new("test", MasterClientOptions::new(vec![]))
                    .with_commit_observer(observer.clone()),
            );
            let (result, shuffle_id) = oneshot::channel();
            assert!(
                handle
                    .send(LifecycleManagerMessage::GetShuffleId {
                        job_id: 1,
                        stage: 0,
                        result,
                    })
                    .await
                    .is_ok()
            );
            let shuffle_id = shuffle_id.await.unwrap().unwrap();
            // An empty worker reservation completes the worker phase without a service.
            assert!(
                handle
                    .send(LifecycleManagerMessage::RegisterShuffleComplete {
                        shuffle_id,
                        result: Ok(SlotReservation {
                            worker_ids: vec![],
                            primary_locations: Default::default(),
                            worker_locations: Default::default(),
                        }),
                    })
                    .await
                    .is_ok()
            );
            let (result, mut original) = oneshot::channel();
            assert!(
                handle
                    .send(LifecycleManagerMessage::MapperEnd {
                        shuffle_id,
                        map_id: 0,
                        attempt_id: 0,
                        num_mappers: 1,
                        result,
                    })
                    .await
                    .is_ok()
            );
            observer.started.notified().await;

            let (result, mut duplicate) = oneshot::channel();
            assert!(
                handle
                    .send(LifecycleManagerMessage::MapperEnd {
                        shuffle_id,
                        map_id: 0,
                        attempt_id: 1,
                        num_mappers: 1,
                        result,
                    })
                    .await
                    .is_ok()
            );
            // A reply to a later message proves the duplicate was processed while
            // the observer was blocked, without relying on timing or sleeps.
            let (result, barrier) = oneshot::channel();
            assert!(
                handle
                    .send(LifecycleManagerMessage::GetJobShuffleIds { job_id: 1, result })
                    .await
                    .is_ok()
            );
            barrier.await.unwrap().unwrap();
            assert!(matches!(
                original.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            assert!(matches!(
                duplicate.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));

            observer.resume.notify_one();
            original.await.unwrap().unwrap();
            duplicate.await.unwrap().unwrap();
            assert!(observer.reported.load(Ordering::SeqCst));

            let (result, stopped) = oneshot::channel();
            assert!(
                handle
                    .send(LifecycleManagerMessage::StopComplete { result })
                    .await
                    .is_ok()
            );
            stopped.await.unwrap();
            system.join().await;
        })
        .await
        .unwrap();
    }
}
