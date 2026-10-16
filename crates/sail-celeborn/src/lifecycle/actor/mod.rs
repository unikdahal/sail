mod core;
mod handler;
mod message;
mod options;

use std::collections::HashMap;

pub use message::LifecycleManagerMessage;
pub use options::LifecycleManagerOptions;
use tokio::sync::oneshot;

use crate::common::{
    ApplicationMetrics, PartitionLocation, SlotReservation, WorkerIdentity, WorkerSlotLocations,
};
use crate::error::{CelebornError, CelebornResult};
use crate::master::MasterClient;
use crate::worker::WorkerClientPool;

pub(super) enum ApplicationRegistration {
    Pending,
    Succeeded,
    Failed { reason: String },
}

impl ApplicationRegistration {
    pub(super) fn error(&self) -> Option<CelebornError> {
        match self {
            Self::Pending => Some(CelebornError::Application(
                "registration is pending".to_string(),
            )),
            Self::Succeeded => None,
            Self::Failed { reason } => Some(CelebornError::Application(format!(
                "registration failed: {reason}"
            ))),
        }
    }
}

/// The global commit of a shuffle.
///
/// The commit request (the selected attempt of every mapper) is fixed once every
/// mapper has ended and is never revised. A worker that has committed a shuffle
/// stops storing pushes for it, so selecting another attempt afterwards would select
/// output that no worker stored. A failed worker commit is retried with the same
/// request, which workers answer idempotently, and if it still fails the shuffle is
/// lost.
enum ShuffleCommitState {
    /// Mappers are ending. Holds the first attempt of each mapper to end, or -1.
    Collecting {
        attempts: Vec<i32>,
    },
    /// Every mapper has ended and the workers are committing the selected attempts.
    /// `mapper_end` calls made meanwhile wait for the outcome.
    Committing {
        attempts: Vec<i32>,
        waiters: Vec<oneshot::Sender<CelebornResult<()>>>,
    },
    Committed,
    /// The commit failed. The shuffle's output cannot be read.
    Lost {
        reason: String,
    },
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
struct ShuffleKey {
    job_id: u64,
    stage: u64,
}

pub struct LifecycleManagerActor {
    options: LifecycleManagerOptions,
    client: MasterClient,
    worker_clients: WorkerClientPool,
    excluded_workers: HashMap<WorkerIdentity, PartitionLocation>,
    registered_shuffles: HashMap<i32, HashMap<WorkerIdentity, WorkerSlotLocations>>,
    reservations: HashMap<i32, SlotReservation>,
    pending_slot_requests: HashMap<i32, Vec<oneshot::Sender<CelebornResult<SlotReservation>>>>,
    pending_revives: HashMap<(i32, i32), Vec<oneshot::Sender<CelebornResult<PartitionLocation>>>>,
    shuffle_commits: HashMap<i32, ShuffleCommitState>,
    shuffle_ids: HashMap<ShuffleKey, i32>,
    shuffle_keys: HashMap<i32, ShuffleKey>,
    next_shuffle_id: i32,
    application_registration: ApplicationRegistration,
    application_metrics: ApplicationMetrics,
    heartbeat_metrics: Option<ApplicationMetrics>,
}
