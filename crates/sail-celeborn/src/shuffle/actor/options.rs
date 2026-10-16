use std::sync::Arc;

use crate::common::CompressionCodec;
use crate::dfs::DfsReadOptions;
use crate::endpoint::EndpointResolver;
use crate::lifecycle::LifecycleManager;

#[readonly::make]
pub struct ShuffleClientOptions {
    pub application_id: String,
    pub lifecycle_manager: Arc<dyn LifecycleManager>,
    pub endpoint_resolver: Option<Arc<dyn EndpointResolver>>,
    pub compression: CompressionCodec,
    /// Object stores, credentials, and read sizing for DFS-backed partition files.
    pub dfs_read_options: DfsReadOptions,
}

impl ShuffleClientOptions {
    pub fn new(
        application_id: String,
        lifecycle_manager: Arc<dyn LifecycleManager>,
        endpoint_resolver: Option<Arc<dyn EndpointResolver>>,
        compression: CompressionCodec,
    ) -> Self {
        Self {
            application_id,
            lifecycle_manager,
            endpoint_resolver,
            compression,
            dfs_read_options: DfsReadOptions::default(),
        }
    }

    /// Configure direct reads of partitions on HDFS or S3-compatible storage.
    pub fn with_dfs_read_options(mut self, options: DfsReadOptions) -> Self {
        self.dfs_read_options = options;
        self
    }
}
