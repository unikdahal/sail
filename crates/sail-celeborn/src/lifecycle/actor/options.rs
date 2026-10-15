use std::sync::Arc;
use std::time::Duration;

use crate::common::PartitionSplitMode;
use crate::endpoint::EndpointResolver;
use crate::lifecycle::ShuffleCommitObserver;
use crate::master::MasterClientOptions;

/// Configuration owned by a lifecycle manager actor.
#[readonly::make]
pub struct LifecycleManagerOptions {
    pub application_id: String,
    pub master: MasterClientOptions,
    pub hostname: String,
    pub tenant_id: String,
    pub user_name: String,
    pub endpoint_resolver: Option<Arc<dyn EndpointResolver>>,
    pub partition_split_threshold: i64,
    pub partition_split_mode: PartitionSplitMode,
    pub heartbeat_interval: Duration,
    pub commit_observer: Option<Arc<dyn ShuffleCommitObserver>>,
}

impl std::fmt::Debug for LifecycleManagerOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LifecycleManagerOptions")
            .field("application_id", &self.application_id)
            .field("master", &self.master)
            .field("hostname", &self.hostname)
            .field("tenant_id", &self.tenant_id)
            .field("user_name", &self.user_name)
            .field("endpoint_resolver", &self.endpoint_resolver)
            .field("partition_split_threshold", &self.partition_split_threshold)
            .field("partition_split_mode", &self.partition_split_mode)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field(
                "commit_observer",
                &if self.commit_observer.is_some() {
                    "<present>"
                } else {
                    "<absent>"
                },
            )
            .finish()
    }
}

impl LifecycleManagerOptions {
    pub fn new(application_id: impl Into<String>, master: MasterClientOptions) -> Self {
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string());
        Self {
            application_id: application_id.into(),
            master,
            hostname,
            // Match Celeborn's DefaultIdentityProvider defaults.
            tenant_id: "default".to_string(),
            user_name: "default".to_string(),
            endpoint_resolver: None,
            partition_split_threshold: 1_i64 << 30,
            partition_split_mode: PartitionSplitMode::Soft,
            heartbeat_interval: Duration::from_secs(10),
            commit_observer: None,
        }
    }

    pub fn with_endpoint_resolver(mut self, endpoint_resolver: Arc<dyn EndpointResolver>) -> Self {
        self.endpoint_resolver = Some(endpoint_resolver);
        self
    }

    pub fn with_partition_split(mut self, threshold: i64, mode: PartitionSplitMode) -> Self {
        self.partition_split_threshold = threshold;
        self.partition_split_mode = mode;
        self
    }

    pub fn with_commit_observer(mut self, observer: Arc<dyn ShuffleCommitObserver>) -> Self {
        self.commit_observer = Some(observer);
        self
    }

    pub fn with_heartbeat_interval(mut self, heartbeat_interval: Duration) -> Self {
        self.heartbeat_interval = heartbeat_interval;
        self
    }
}
