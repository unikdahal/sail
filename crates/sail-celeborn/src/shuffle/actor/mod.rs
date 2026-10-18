mod core;
mod handler;
mod message;
mod options;

#[cfg(test)]
mod tests;

use std::collections::HashMap;

pub use message::ShuffleClientMessage;
pub use options::ShuffleClientOptions;
use tokio::sync::oneshot;

use crate::common::PartitionLocation;
use crate::error::CelebornResult;
use crate::worker::WorkerClientPool;

struct PendingClose {
    waiters: Vec<(bool, oneshot::Sender<CelebornResult<()>>)>,
    unregister: bool,
    request_sent: bool,
}

/// Serializes local shuffle-client operations using an external lifecycle manager.
pub struct ShuffleClientActor {
    options: ShuffleClientOptions,
    shuffle_ids: HashMap<(u64, u64), i32>,
    locations: HashMap<(i32, i32), PartitionLocation>,
    location_history: HashMap<(i32, i32), Vec<PartitionLocation>>,
    worker_clients: WorkerClientPool,
    batch_ids: HashMap<(i32, i32, i32), i32>,
    pending_operations: HashMap<i32, usize>,
    pending_closes: HashMap<i32, PendingClose>,
    // A bounded generation stamp prevents delayed lookups from restoring caches
    // across cleanup without retaining a tombstone for every completed shuffle.
    lookup_revision: u64,
}
