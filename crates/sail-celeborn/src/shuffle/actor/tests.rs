#![expect(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use sail_common::actor::ActorSystem;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

use crate::common::{ApplicationMetrics, CompressionCodec, PartitionLocation, SlotReservation};
use crate::error::{CelebornError, CelebornResult};
use crate::lifecycle::{LifecycleManager, ReviveRequest};
use crate::shuffle::{
    ShuffleClient, ShuffleClientActor, ShuffleClientMessage, ShuffleClientOptions,
};

type RegistrationReply = oneshot::Sender<CelebornResult<SlotReservation>>;
type ShuffleIdReply = oneshot::Sender<CelebornResult<i32>>;
type JobShuffleIdsReply = oneshot::Sender<CelebornResult<Vec<(u64, i32)>>>;
type ReviveReply = oneshot::Sender<CelebornResult<PartitionLocation>>;

struct ControlledLifecycle {
    registrations: mpsc::UnboundedSender<RegistrationReply>,
    unregisters: mpsc::UnboundedSender<oneshot::Sender<CelebornResult<()>>>,
    shuffle_ids: Option<mpsc::UnboundedSender<ShuffleIdReply>>,
    job_shuffle_ids: Option<mpsc::UnboundedSender<JobShuffleIdsReply>>,
    revives: Option<mpsc::UnboundedSender<ReviveReply>>,
}

#[tonic::async_trait]
impl LifecycleManager for ControlledLifecycle {
    async fn get_shuffle_id(&self, _: u64, _: u64) -> CelebornResult<i32> {
        if let Some(calls) = &self.shuffle_ids {
            let (reply, result) = oneshot::channel();
            calls.send(reply).unwrap();
            result.await.unwrap()
        } else {
            Ok(7)
        }
    }

    async fn get_job_shuffle_ids(&self, _: u64) -> CelebornResult<Vec<(u64, i32)>> {
        if let Some(calls) = &self.job_shuffle_ids {
            let (reply, result) = oneshot::channel();
            calls.send(reply).unwrap();
            result.await.unwrap()
        } else {
            Ok(vec![])
        }
    }

    async fn register_shuffle(
        &self,
        _: i32,
        _: Vec<i32>,
        _: bool,
        _: i32,
    ) -> CelebornResult<SlotReservation> {
        let (reply, result) = oneshot::channel();
        self.registrations.send(reply).unwrap();
        result.await.unwrap()
    }

    async fn revive(&self, _: ReviveRequest) -> CelebornResult<PartitionLocation> {
        if let Some(calls) = &self.revives {
            let (reply, result) = oneshot::channel();
            calls.send(reply).unwrap();
            result.await.unwrap()
        } else {
            Err(CelebornError::Application("unexpected revive".into()))
        }
    }

    async fn mapper_end(&self, _: i32, _: i32, _: i32, _: i32) -> CelebornResult<()> {
        Ok(())
    }

    async fn unregister_shuffle(&self, _: i32) -> CelebornResult<()> {
        let (reply, result) = oneshot::channel();
        self.unregisters.send(reply).unwrap();
        result.await.unwrap()
    }

    async fn report_metrics(&self, _: ApplicationMetrics) -> CelebornResult<()> {
        Ok(())
    }

    async fn stop(&self) -> CelebornResult<()> {
        Ok(())
    }
}

fn reservation() -> SlotReservation {
    let location = PartitionLocation {
        mode: 0,
        id: 0,
        epoch: 0,
        host: "127.0.0.1".into(),
        rpc_port: 0,
        push_port: 0,
        fetch_port: 0,
        replicate_port: 0,
        peer: None,
    };
    SlotReservation {
        worker_ids: vec![],
        primary_locations: [(0, location)].into(),
        worker_locations: Default::default(),
    }
}

#[tokio::test]
async fn client_cleanup_drains_registration_before_allowing_id_reuse() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let (registrations, mut registration_calls) = mpsc::unbounded_channel();
        let (unregisters, mut unregister_calls) = mpsc::unbounded_channel();
        let mut system = ActorSystem::new();
        let handle = system.spawn::<ShuffleClientActor>(ShuffleClientOptions::new(
            "cleanup-test".into(),
            Arc::new(ControlledLifecycle {
                registrations,
                unregisters,
                shuffle_ids: None,
                job_shuffle_ids: None,
                revives: None,
            }),
            None,
            CompressionCodec::None,
        ));
        let client = ShuffleClient::new(handle.clone());
        let registering = tokio::spawn({
            let client = client.clone();
            async move { client.register_shuffle(7, vec![0], false, 0).await }
        });
        let registration = registration_calls.recv().await.unwrap();
        let (reply, mut cleaned) = oneshot::channel();
        assert!(
            handle
                .send(ShuffleClientMessage::CleanUpShuffle {
                    shuffle_id: 7,
                    result: reply
                })
                .await
                .is_ok()
        );
        // This mailbox round trip ensures that cleanup was accepted before checking it.
        client.get_job_shuffle_ids(99).await.unwrap();
        assert!(
            matches!(cleaned.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "cleanup must wait for registration's delayed completion"
        );
        registration.send(Ok(reservation())).unwrap();
        assert!(registering.await.unwrap().is_err());
        cleaned.await.unwrap().unwrap();
        assert!(
            unregister_calls.try_recv().is_err(),
            "local cleanup must not unregister remotely"
        );
        let error = client
            .push_data(7, 0, 0, 0, Bytes::from_static(b"old"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not registered"), "{error}");

        // No permanent tombstone: the same explicit ID can be registered again.
        let registering = tokio::spawn({
            let client = client.clone();
            async move { client.register_shuffle(7, vec![0], false, 0).await }
        });
        registration_calls
            .recv()
            .await
            .unwrap()
            .send(Ok(reservation()))
            .unwrap();
        registering.await.unwrap().unwrap();
        client.stop().await.unwrap();
        system.join().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn client_cleanup_drains_read_refresh_and_coalesces_unregister() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let (registrations, mut registration_calls) = mpsc::unbounded_channel();
        let (unregisters, mut unregister_calls) = mpsc::unbounded_channel();
        let mut system = ActorSystem::new();
        let handle = system.spawn::<ShuffleClientActor>(ShuffleClientOptions::new(
            "cleanup-test".into(),
            Arc::new(ControlledLifecycle {
                registrations,
                unregisters,
                shuffle_ids: None,
                job_shuffle_ids: None,
                revives: None,
            }),
            None,
            CompressionCodec::None,
        ));
        let client = ShuffleClient::new(handle.clone());
        let registering = tokio::spawn({
            let client = client.clone();
            async move { client.register_shuffle(7, vec![0], false, 0).await }
        });
        registration_calls
            .recv()
            .await
            .unwrap()
            .send(Ok(reservation()))
            .unwrap();
        registering.await.unwrap().unwrap();
        let reading = tokio::spawn({
            let client = client.clone();
            async move { client.read_partition_stream(7, 0).await }
        });
        let refresh = registration_calls.recv().await.unwrap();
        let (reply, mut cleaned) = oneshot::channel();
        assert!(
            handle
                .send(ShuffleClientMessage::CleanUpShuffle {
                    shuffle_id: 7,
                    result: reply
                })
                .await
                .is_ok()
        );
        let (reply, mut unregistered) = oneshot::channel();
        assert!(
            handle
                .send(ShuffleClientMessage::UnregisterShuffle {
                    shuffle_id: 7,
                    result: reply
                })
                .await
                .is_ok()
        );
        let (reply, mut duplicate) = oneshot::channel();
        assert!(
            handle
                .send(ShuffleClientMessage::UnregisterShuffle {
                    shuffle_id: 7,
                    result: reply
                })
                .await
                .is_ok()
        );
        client.get_job_shuffle_ids(99).await.unwrap();
        assert!(matches!(
            cleaned.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(
            unregister_calls.try_recv().is_err(),
            "remote unregister must wait for local callbacks"
        );
        refresh.send(Ok(reservation())).unwrap();
        let mut stream = reading.await.unwrap();
        assert!(stream.next().await.unwrap().is_err());
        let remote = unregister_calls.recv().await.unwrap();
        client.get_job_shuffle_ids(99).await.unwrap();
        assert!(
            unregister_calls.try_recv().is_err(),
            "duplicate unregister must share its remote request"
        );
        for receiver in [&mut cleaned, &mut unregistered, &mut duplicate] {
            assert!(matches!(
                receiver.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
        }
        assert!(client.register_shuffle(7, vec![0], false, 0).await.is_err());
        assert!(registration_calls.try_recv().is_err());
        remote.send(Ok(())).unwrap();
        cleaned.await.unwrap().unwrap();
        unregistered.await.unwrap().unwrap();
        duplicate.await.unwrap().unwrap();
        let mut stream = client.read_partition_stream(7, 0).await;
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(error.to_string().contains("not registered"), "{error}");
        client.stop().await.unwrap();
        system.join().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn client_failed_unregister_stays_closed_until_explicit_retry_succeeds() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let (registrations, mut registration_calls) = mpsc::unbounded_channel();
        let (unregisters, mut unregister_calls) = mpsc::unbounded_channel();
        let mut system = ActorSystem::new();
        let handle = system.spawn::<ShuffleClientActor>(ShuffleClientOptions::new(
            "cleanup-test".into(),
            Arc::new(ControlledLifecycle {
                registrations,
                unregisters,
                shuffle_ids: None,
                job_shuffle_ids: None,
                revives: None,
            }),
            None,
            CompressionCodec::None,
        ));
        let client = ShuffleClient::new(handle);
        let registering = tokio::spawn({
            let client = client.clone();
            async move { client.register_shuffle(7, vec![0], false, 0).await }
        });
        registration_calls
            .recv()
            .await
            .unwrap()
            .send(Ok(reservation()))
            .unwrap();
        registering.await.unwrap().unwrap();
        let unregistering = tokio::spawn({
            let client = client.clone();
            async move { client.unregister_shuffle(7).await }
        });
        unregister_calls
            .recv()
            .await
            .unwrap()
            .send(Err(CelebornError::Application(
                "partial remote deletion".into(),
            )))
            .unwrap();
        assert!(unregistering.await.unwrap().is_err());
        // Cached locations would let PushData bypass the lifecycle manager's
        // failure guard, so the client must keep rejecting this ID as well.
        for after_local_cleanup in [false, true] {
            if after_local_cleanup {
                client.clean_up_shuffle(7).await.unwrap();
            }
            let error = client
                .push_data(7, 0, 0, 0, Bytes::from_static(b"old"))
                .await
                .unwrap_err();
            assert!(error.to_string().contains("closing"), "{error}");
            assert!(client.register_shuffle(7, vec![0], false, 0).await.is_err());
            assert!(registration_calls.try_recv().is_err());
            assert!(
                unregister_calls.try_recv().is_err(),
                "local cleanup must not retry remote deletion"
            );
        }
        let unregistering = tokio::spawn({
            let client = client.clone();
            async move { client.unregister_shuffle(7).await }
        });
        unregister_calls.recv().await.unwrap().send(Ok(())).unwrap();
        unregistering.await.unwrap().unwrap();
        let registering = tokio::spawn({
            let client = client.clone();
            async move { client.register_shuffle(7, vec![0], false, 0).await }
        });
        registration_calls
            .recv()
            .await
            .unwrap()
            .send(Ok(reservation()))
            .unwrap();
        registering.await.unwrap().unwrap();
        client.stop().await.unwrap();
        system.join().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn client_cleanup_does_not_cache_delayed_lookup_results() {
    tokio::time::timeout(Duration::from_secs(3), async {
        for lookup_all in [false, true] {
            let (registrations, _) = mpsc::unbounded_channel();
            let (unregisters, _) = mpsc::unbounded_channel();
            let (shuffle_ids, mut id_calls) = mpsc::unbounded_channel();
            let (job_shuffle_ids, mut job_calls) = mpsc::unbounded_channel();
            let mut system = ActorSystem::new();
            let handle = system.spawn::<ShuffleClientActor>(ShuffleClientOptions::new(
                "cleanup-test".into(),
                Arc::new(ControlledLifecycle {
                    registrations,
                    unregisters,
                    shuffle_ids: Some(shuffle_ids),
                    job_shuffle_ids: Some(job_shuffle_ids),
                    revives: None,
                }),
                None,
                CompressionCodec::None,
            ));
            let client = ShuffleClient::new(handle);
            let lookup = tokio::spawn({
                let client = client.clone();
                async move {
                    if lookup_all {
                        client.get_job_shuffle_ids(1).await
                    } else {
                        client.get_shuffle_id(1, 1).await.map(|id| vec![(1, id)])
                    }
                }
            });
            // Cleanup may complete before a lookup that already reached the
            // lifecycle manager. The result remains valid for that caller but
            // cannot be saved for a later lookup after cleanup.
            if lookup_all {
                let reply = job_calls.recv().await.unwrap();
                client.clean_up_shuffle(7).await.unwrap();
                reply.send(Ok(vec![(1, 7)])).unwrap();
            } else {
                let reply = id_calls.recv().await.unwrap();
                client.clean_up_shuffle(7).await.unwrap();
                reply.send(Ok(7)).unwrap();
            }
            assert_eq!(lookup.await.unwrap().unwrap(), vec![(1, 7)]);
            let lookup = tokio::spawn({
                let client = client.clone();
                async move { client.get_shuffle_id(1, 1).await }
            });
            id_calls.recv().await.unwrap().send(Ok(8)).unwrap();
            assert_eq!(lookup.await.unwrap().unwrap(), 8);
            client.stop().await.unwrap();
            system.join().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn client_cleanup_drains_delayed_push_revive() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let (registrations, mut registration_calls) = mpsc::unbounded_channel();
        let (unregisters, mut unregister_calls) = mpsc::unbounded_channel();
        let (revives, mut revive_calls) = mpsc::unbounded_channel();
        let mut system = ActorSystem::new();
        let handle = system.spawn::<ShuffleClientActor>(ShuffleClientOptions::new(
            "cleanup-test".into(),
            Arc::new(ControlledLifecycle {
                registrations,
                unregisters,
                shuffle_ids: None,
                job_shuffle_ids: None,
                revives: Some(revives),
            }),
            None,
            CompressionCodec::None,
        ));
        let client = ShuffleClient::new(handle.clone());
        let registering = tokio::spawn({
            let client = client.clone();
            async move { client.register_shuffle(7, vec![0], false, 0).await }
        });
        registration_calls
            .recv()
            .await
            .unwrap()
            .send(Ok(reservation()))
            .unwrap();
        registering.await.unwrap().unwrap();
        // The deliberately invalid worker port makes the push enter revive;
        // hold that operation until cleanup has started.
        let pushing = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .push_data(7, 0, 0, 0, Bytes::from_static(b"old"))
                    .await
            }
        });
        let revive = revive_calls.recv().await.unwrap();
        let (reply, mut cleaned) = oneshot::channel();
        assert!(
            handle
                .send(ShuffleClientMessage::CleanUpShuffle {
                    shuffle_id: 7,
                    result: reply
                })
                .await
                .is_ok()
        );
        client.get_job_shuffle_ids(99).await.unwrap();
        assert!(matches!(
            cleaned.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        let mut location = reservation().primary_locations.remove(&0).unwrap();
        location.epoch = 1;
        revive.send(Ok(location)).unwrap();
        let error = pushing.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("closing"), "{error}");
        cleaned.await.unwrap().unwrap();
        assert!(unregister_calls.try_recv().is_err());
        let error = client
            .push_data(7, 0, 0, 0, Bytes::from_static(b"old"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not registered"), "{error}");
        client.stop().await.unwrap();
        system.join().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn client_stop_drains_registration_and_coalesces_waiters() {
    check_client_stop_drains_operation(false).await;
}

#[tokio::test]
async fn client_stop_drains_read_refresh_before_acknowledgement() {
    check_client_stop_drains_operation(true).await;
}

async fn check_client_stop_drains_operation(reading: bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        let (registrations, mut registration_calls) = mpsc::unbounded_channel();
        let (unregisters, mut unregister_calls) = mpsc::unbounded_channel();
        let mut system = ActorSystem::new();
        let handle = system.spawn::<ShuffleClientActor>(ShuffleClientOptions::new(
            "stop-test".into(),
            Arc::new(ControlledLifecycle {
                registrations,
                unregisters,
                shuffle_ids: None,
                job_shuffle_ids: None,
                revives: None,
            }),
            None,
            CompressionCodec::None,
        ));
        let client = ShuffleClient::new(handle.clone());
        if reading {
            let registering = tokio::spawn({
                let client = client.clone();
                async move { client.register_shuffle(7, vec![0], false, 0).await }
            });
            registration_calls
                .recv()
                .await
                .unwrap()
                .send(Ok(reservation()))
                .unwrap();
            registering.await.unwrap().unwrap();
        }
        let operation = tokio::spawn({
            let client = client.clone();
            async move {
                if reading {
                    let mut stream = client.read_partition_stream(7, 0).await;
                    match stream.next().await {
                        Some(Err(error)) => Err(error),
                        _ => Ok(()),
                    }
                } else {
                    client
                        .register_shuffle(7, vec![0], false, 0)
                        .await
                        .map(|_| ())
                }
            }
        });
        let registration = registration_calls.recv().await.unwrap();
        let (reply, mut stopped) = oneshot::channel();
        assert!(
            handle
                .send(ShuffleClientMessage::Stop { result: reply })
                .await
                .is_ok()
        );
        let (reply, mut duplicate) = oneshot::channel();
        let duplicate_sent = handle
            .send(ShuffleClientMessage::Stop { result: reply })
            .await;
        // This later request is both an actor-mailbox barrier and a check that
        // stop rejects work on another ID instead of starting another RPC.
        assert!(client.register_shuffle(8, vec![0], false, 0).await.is_err());
        assert!(
            matches!(
                tokio::time::timeout(Duration::from_millis(100), client.unregister_shuffle(8))
                    .await,
                Ok(Err(_))
            ),
            "stop must reject new unregister transactions"
        );
        assert!(
            matches!(
                tokio::time::timeout(Duration::from_millis(100), client.clean_up_shuffle(8)).await,
                Ok(Err(_))
            ),
            "stop must reject new local cleanup requests"
        );
        assert!(unregister_calls.try_recv().is_err());
        assert!(
            matches!(stopped.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "stop acknowledged while a lifecycle callback was still pending"
        );
        assert!(duplicate_sent.is_ok());
        assert!(matches!(
            duplicate.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(registration_calls.try_recv().is_err());

        registration.send(Ok(reservation())).unwrap();
        assert!(
            operation.await.unwrap().is_err(),
            "a callback completed successfully after client shutdown began"
        );
        stopped.await.unwrap();
        duplicate.await.unwrap();
        system.join().await;
        assert!(client.register_shuffle(7, vec![0], false, 0).await.is_err());
        assert!(
            unregister_calls.try_recv().is_err(),
            "stopping one client must not unregister its shared lifecycle manager"
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn client_cleanup_releases_idle_worker_but_preserves_shared_connection() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (closed, connection_closed) = oneshot::channel();
        let worker = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            // Both shuffles, and the push after the first cleanup, must use
            // this one accepted connection rather than reconnecting.
            for _ in 0..3 {
                let header_length = socket.read_i32().await.unwrap() as usize;
                assert_eq!(socket.read_u8().await.unwrap(), 11); // PushData
                let body_length = socket.read_i32().await.unwrap() as usize;
                let mut header = vec![0; header_length];
                let mut body = vec![0; body_length];
                socket.read_exact(&mut header).await.unwrap();
                socket.read_exact(&mut body).await.unwrap();
                assert!(header.len() >= 8);
                // An empty successful RpcResponse carries the push request ID.
                socket.write_i32(12).await.unwrap();
                socket.write_u8(4).await.unwrap();
                socket.write_i32(0).await.unwrap();
                socket.write_all(&header[..8]).await.unwrap();
                socket.write_i32(0).await.unwrap();
            }
            assert_eq!(socket.read(&mut [0; 1]).await.unwrap(), 0);
            closed.send(()).unwrap();
        });
        let (registrations, mut registration_calls) = mpsc::unbounded_channel();
        let (unregisters, mut unregister_calls) = mpsc::unbounded_channel();
        let mut system = ActorSystem::new();
        let handle = system.spawn::<ShuffleClientActor>(ShuffleClientOptions::new(
            "worker-lifetime-test".into(),
            Arc::new(ControlledLifecycle {
                registrations,
                unregisters,
                shuffle_ids: None,
                job_shuffle_ids: None,
                revives: None,
            }),
            None,
            CompressionCodec::None,
        ));
        let client = ShuffleClient::new(handle);
        for shuffle_id in [7, 8] {
            let registering = tokio::spawn({
                let client = client.clone();
                async move { client.register_shuffle(shuffle_id, vec![0], false, 0).await }
            });
            let mut registered = reservation();
            let location = registered.primary_locations.get_mut(&0).unwrap();
            location.rpc_port = port;
            location.push_port = port;
            location.fetch_port = port;
            location.replicate_port = port;
            registration_calls
                .recv()
                .await
                .unwrap()
                .send(Ok(registered))
                .unwrap();
            registering.await.unwrap().unwrap();
        }
        for shuffle_id in [7, 8] {
            client
                .push_data(shuffle_id, 0, 0, 0, Bytes::from_static(b"first"))
                .await
                .unwrap();
        }
        client.clean_up_shuffle(7).await.unwrap();
        client
            .push_data(8, 0, 0, 0, Bytes::from_static(b"after other cleanup"))
            .await
            .unwrap();
        client.clean_up_shuffle(8).await.unwrap();
        let closed = tokio::time::timeout(Duration::from_millis(200), connection_closed).await;
        assert!(
            closed.is_ok(),
            "last-shuffle cleanup retained the idle worker's TCP connection"
        );
        closed.unwrap().unwrap();
        worker.await.unwrap();
        assert!(unregister_calls.try_recv().is_err());
        client.stop().await.unwrap();
        system.join().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn client_stop_drains_lookup_callbacks_without_publishing_late_results() {
    tokio::time::timeout(Duration::from_secs(3), async {
        for lookup_all in [false, true] {
            let (registrations, mut registration_calls) = mpsc::unbounded_channel();
            let (unregisters, mut unregister_calls) = mpsc::unbounded_channel();
            let (shuffle_ids, mut id_calls) = mpsc::unbounded_channel();
            let (job_shuffle_ids, mut job_calls) = mpsc::unbounded_channel();
            let mut system = ActorSystem::new();
            let handle = system.spawn::<ShuffleClientActor>(ShuffleClientOptions::new(
                "stop-lookup-test".into(),
                Arc::new(ControlledLifecycle {
                    registrations,
                    unregisters,
                    shuffle_ids: Some(shuffle_ids),
                    job_shuffle_ids: Some(job_shuffle_ids),
                    revives: None,
                }),
                None,
                CompressionCodec::None,
            ));
            let client = ShuffleClient::new(handle.clone());
            let lookup = tokio::spawn({
                let client = client.clone();
                async move {
                    if lookup_all {
                        client.get_job_shuffle_ids(1).await.map(|_| ())
                    } else {
                        client.get_shuffle_id(1, 1).await.map(|_| ())
                    }
                }
            });
            let id_reply = if lookup_all {
                None
            } else {
                Some(id_calls.recv().await.unwrap())
            };
            let job_reply = if lookup_all {
                Some(job_calls.recv().await.unwrap())
            } else {
                None
            };
            let (reply, mut stopped) = oneshot::channel();
            assert!(
                handle
                    .send(ShuffleClientMessage::Stop { result: reply })
                    .await
                    .is_ok()
            );
            assert!(client.register_shuffle(8, vec![0], false, 0).await.is_err());
            assert!(
                matches!(stopped.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
                "stop acknowledged while an ID lookup was still pending"
            );
            if let Some(reply) = id_reply {
                reply.send(Ok(7)).unwrap();
            }
            if let Some(reply) = job_reply {
                reply.send(Ok(vec![(1, 7)])).unwrap();
            }
            assert!(
                lookup.await.unwrap().is_err(),
                "a late lookup result was published after shutdown began"
            );
            stopped.await.unwrap();
            system.join().await;
            assert!(registration_calls.try_recv().is_err());
            assert!(unregister_calls.try_recv().is_err());
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn client_stop_drains_active_unregister_and_finishes_after_remote_failure() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let (registrations, mut registration_calls) = mpsc::unbounded_channel();
        let (unregisters, mut unregister_calls) = mpsc::unbounded_channel();
        let mut system = ActorSystem::new();
        let handle = system.spawn::<ShuffleClientActor>(ShuffleClientOptions::new(
            "stop-unregister-test".into(),
            Arc::new(ControlledLifecycle {
                registrations,
                unregisters,
                shuffle_ids: None,
                job_shuffle_ids: None,
                revives: None,
            }),
            None,
            CompressionCodec::None,
        ));
        let client = ShuffleClient::new(handle.clone());
        let registering = tokio::spawn({
            let client = client.clone();
            async move { client.register_shuffle(7, vec![0], false, 0).await }
        });
        registration_calls
            .recv()
            .await
            .unwrap()
            .send(Ok(reservation()))
            .unwrap();
        registering.await.unwrap().unwrap();
        let unregistering = tokio::spawn({
            let client = client.clone();
            async move { client.unregister_shuffle(7).await }
        });
        let remote_unregister = unregister_calls.recv().await.unwrap();
        let (reply, mut stopped) = oneshot::channel();
        assert!(
            handle
                .send(ShuffleClientMessage::Stop { result: reply })
                .await
                .is_ok()
        );
        assert!(client.register_shuffle(8, vec![0], false, 0).await.is_err());
        assert!(
            matches!(stopped.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "stop acknowledged while remote unregister was still pending"
        );
        remote_unregister
            .send(Err(CelebornError::Application(
                "partial remote deletion".into(),
            )))
            .unwrap();
        assert!(unregistering.await.unwrap().is_err());
        // The failed cleanup must retain Closing for ordinary callers, but it
        // has no active transaction left and must not keep client stop waiting.
        stopped.await.unwrap();
        system.join().await;
        assert!(
            unregister_calls.try_recv().is_err(),
            "client stop must not retry shared lifecycle cleanup"
        );
        assert!(registration_calls.try_recv().is_err());
    })
    .await
    .unwrap();
}
