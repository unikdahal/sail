use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use hdfs_native_object_store::HdfsObjectStoreBuilder;
use object_store::ObjectStore;
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::path::Path;
use url::{Position, Url};

use crate::error::{CelebornError, CelebornResult};

/// Object store configuration for reading Celeborn partitions on distributed storage.
#[derive(Clone)]
pub struct DfsReadOptions {
    /// S3-compatible store settings, using [`AmazonS3ConfigKey`] names such as
    /// `region`, `endpoint`, `access_key_id`, `secret_access_key`, `session_token`,
    /// `allow_http`, and `virtual_hosted_style_request`. These settings override
    /// the corresponding `AWS_*` settings read by [`AmazonS3Builder::from_env`].
    /// Without explicit credentials, the builder can use web identity, container,
    /// or instance metadata credentials. The partition URL selects the bucket.
    ///
    /// `s3`, `s3a`, and `s3n` URLs use this configuration. `oss` URLs use the S3
    /// compatibility API and require an explicit `endpoint`/`aws_endpoint_url_s3`
    /// setting or corresponding `AWS_ENDPOINT`/`AWS_ENDPOINT_URL_S3` environment
    /// variable. Map OSS access key ID, access key secret, and security token to
    /// `access_key_id`, `secret_access_key`, and `session_token`, respectively.
    /// OSS requires its S3 compatibility API with AWS signature V4 enabled.
    /// Use its signing region, a bucket-qualified endpoint, and set
    /// `virtual_hosted_style_request` to `true` (OSS requires virtual hosting).
    pub s3_options: HashMap<String, String>,
    /// Hadoop configuration properties passed to the native HDFS client. These
    /// override its default configuration files, for example
    /// `dfs.ha.namenodes.<nameservice>` and
    /// `dfs.namenode.rpc-address.<nameservice>.<namenode>` for HDFS HA.
    pub hdfs_options: HashMap<String, String>,
    /// Preconfigured stores keyed by canonical `scheme://authority`, for example
    /// `hdfs://namenode:8020`, `s3://bucket`, `oss://bucket`, or `memory://bucket`.
    /// These take precedence over built-in stores and may supply custom schemes
    /// or credentials. `s3a` and `s3n` URLs also look up `s3://<authority>`.
    pub object_stores: HashMap<String, Arc<dyn ObjectStore>>,
    /// Maximum bytes requested at once from a partition data file. Must be
    /// positive; defaults to 8 MiB. Batch headers and payloads may span requests.
    pub read_chunk_size: u64,
}

impl Default for DfsReadOptions {
    fn default() -> Self {
        Self {
            s3_options: HashMap::new(),
            hdfs_options: HashMap::new(),
            object_stores: HashMap::new(),
            read_chunk_size: 8 * 1024 * 1024,
        }
    }
}

impl Debug for DfsReadOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // Both configuration values and custom store Debug implementations may
        // contain credentials, so only report their counts.
        f.debug_struct("DfsReadOptions")
            .field("s3_options_count", &self.s3_options.len())
            .field("hdfs_options_count", &self.hdfs_options.len())
            .field("object_stores_count", &self.object_stores.len())
            .field("read_chunk_size", &self.read_chunk_size)
            .finish()
    }
}

impl DfsReadOptions {
    pub(super) fn resolve(&self, path: &str) -> CelebornResult<(Arc<dyn ObjectStore>, Path)> {
        if self.read_chunk_size == 0 {
            return Err(CelebornError::InvalidArgument(
                "DFS read chunk size must be positive".into(),
            ));
        }
        let url = Url::parse(path)
            .map_err(|_| CelebornError::InvalidArgument("invalid DFS partition URL".into()))?;
        if url.cannot_be_a_base()
            || url.host_str().is_none_or(str::is_empty)
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(CelebornError::InvalidArgument(
                "DFS partition URL must have an authority and no credentials, query, or fragment"
                    .into(),
            ));
        }
        let object_path = Path::from_url_path(url.path()).map_err(|_| {
            CelebornError::InvalidArgument("invalid DFS partition object path".into())
        })?;
        let key = &url[..Position::BeforePath];
        if let Some(store) = self.object_stores.get(key) {
            return Ok((store.clone(), object_path));
        }
        let authority = &url[Position::BeforeHost..Position::AfterPort];
        let store: Arc<dyn ObjectStore> = match url.scheme() {
            "s3" | "s3a" | "s3n" | "oss" => {
                if matches!(url.scheme(), "s3a" | "s3n")
                    && let Some(store) = self.object_stores.get(&format!("s3://{authority}"))
                {
                    return Ok((store.clone(), object_path));
                }
                if url.port().is_some() {
                    return Err(CelebornError::InvalidArgument(
                        "S3-compatible partition URLs must specify only a bucket; configure the endpoint separately"
                            .into(),
                    ));
                }
                let mut builder = AmazonS3Builder::from_env().with_url(format!("s3://{authority}"));
                for (key, value) in &self.s3_options {
                    let key = key.parse::<AmazonS3ConfigKey>().map_err(|_| {
                        CelebornError::InvalidArgument(
                            "unknown S3 DFS configuration key; use object_store AmazonS3ConfigKey names"
                                .into(),
                        )
                    })?;
                    builder = builder.with_config(key, value);
                }
                if url.scheme() == "oss"
                    && ![AmazonS3ConfigKey::Endpoint, AmazonS3ConfigKey::S3Endpoint]
                        .iter()
                        .any(|key| {
                            builder
                                .get_config_value(key)
                                .is_some_and(|value| !value.is_empty())
                        })
                {
                    return Err(CelebornError::InvalidArgument(
                        "OSS partition reads require an S3-compatible endpoint in DFS options or the AWS environment"
                            .into(),
                    ));
                }
                Arc::new(builder.build().map_err(|_| {
                    CelebornError::InvalidArgument(
                        "invalid S3-compatible DFS object store configuration".into(),
                    )
                })?)
            }
            "hdfs" => Arc::new(
                HdfsObjectStoreBuilder::new()
                    .with_url(key)
                    .with_config(self.hdfs_options.clone())
                    .build()
                    .map_err(|_| {
                        CelebornError::InvalidArgument(
                            "invalid HDFS object store configuration".into(),
                        )
                    })?,
            ),
            _ => {
                return Err(CelebornError::InvalidArgument(
                    "unsupported DFS partition URL scheme; register a matching object store".into(),
                ));
            }
        };
        Ok((store, object_path))
    }
}
