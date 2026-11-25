// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::sync::Arc;

use gcs_rust_fs::{
    BucketKind, CacheConfig, CachedFs, DataConfig, FileSystem, GcsFsBuilder, MetricsRecorder,
    Transport,
};

use super::{DEFAULT_PAGE_CACHE_CAPACITY, DEFAULT_STREAM_CHUNK_SIZE, GcsRustFs, STORE, map_error};
use crate::Result;

/// Environment variables consulted by [`GcsRustFsBuilder::from_env`] for the bucket name.
pub const BUCKET_ENV_VARS: [&str; 4] = [
    "GCS_RUST_FS_BUCKET",
    "GOOGLE_BUCKET",
    "GOOGLE_BUCKET_NAME",
    "BUCKET_NAME",
];

/// Environment variables consulted by [`GcsRustFsBuilder::from_env`] for a custom endpoint.
pub const ENDPOINT_ENV_VARS: [&str; 2] = ["GCS_RUST_FS_ENDPOINT", "GOOGLE_SERVICE_ENDPOINT"];

#[derive(Debug, thiserror::Error)]
pub(crate) enum BuilderError {
    #[error("Missing bucket name")]
    MissingBucketName,

    #[error("Invalid bucket name {bucket:?}: {reason}")]
    InvalidBucketName {
        bucket: String,
        reason: &'static str,
    },

    #[error("Invalid stream_chunk_size: must be at least 1 byte")]
    InvalidStreamChunkSize,
}

impl From<BuilderError> for crate::Error {
    fn from(source: BuilderError) -> Self {
        Self::Generic {
            store: STORE,
            source: Box::new(source),
        }
    }
}

pub(crate) fn validate_bucket_name(raw: &str) -> Result<String> {
    let trimmed = raw
        .strip_prefix("gs://")
        .or_else(|| raw.strip_prefix("gcs://"))
        .unwrap_or(raw)
        .trim_matches('/');

    if trimmed.is_empty() {
        return Err(BuilderError::MissingBucketName.into());
    }
    if trimmed.contains('/') {
        return Err(BuilderError::InvalidBucketName {
            bucket: raw.to_owned(),
            reason: "bucket name must not contain '/'",
        }
        .into());
    }
    if trimmed.contains('#') {
        return Err(BuilderError::InvalidBucketName {
            bucket: raw.to_owned(),
            reason: "bucket name must not contain '#'",
        }
        .into());
    }
    if trimmed.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err(BuilderError::InvalidBucketName {
            bucket: raw.to_owned(),
            reason: "bucket name must not contain whitespace or control characters",
        }
        .into());
    }
    Ok(trimmed.to_owned())
}

/// Default [`CacheConfig`] used by [`GcsRustFsBuilder`].
///
/// Enables Mode 2 shared `256 KiB` page caching (`512 MiB` capacity) together with
/// asynchronous sequential readahead while keeping `negative = false` and
/// `populate_from_find = false` so external writers are not masked by negative
/// directory-cache lookups.
pub fn default_cache_config() -> CacheConfig {
    CacheConfig {
        ttl: None,
        max_dirs: Some(1024),
        negative: false,
        populate_from_find: false,
        data: Some(DataConfig::page_cache(DEFAULT_PAGE_CACHE_CAPACITY)),
    }
}

/// Builder for configuring and constructing a [`GcsRustFs`] store.
#[derive(Clone)]
pub struct GcsRustFsBuilder {
    bucket_name: Option<String>,
    transport: Option<Transport>,
    endpoint: Option<String>,
    project: Option<String>,
    grpc_subchannel_count: Option<usize>,
    http_pool_size: Option<usize>,
    bucket_kinds: HashMap<String, BucketKind>,
    finalize_on_close: bool,
    cache_config: Option<CacheConfig>,
    metrics: Option<Arc<dyn MetricsRecorder>>,
    filesystem: Option<Arc<dyn FileSystem>>,
    stream_chunk_size: usize,
    use_env: bool,
}

impl Default for GcsRustFsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for GcsRustFsBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsRustFsBuilder")
            .field("bucket_name", &self.bucket_name)
            .field("transport", &self.transport)
            .field("endpoint", &self.endpoint)
            .field("project", &self.project)
            .field("grpc_subchannel_count", &self.grpc_subchannel_count)
            .field("http_pool_size", &self.http_pool_size)
            .field("bucket_kinds", &self.bucket_kinds)
            .field("finalize_on_close", &self.finalize_on_close)
            .field("cache_enabled", &self.cache_config.is_some())
            .field("custom_filesystem", &self.filesystem.is_some())
            .field("stream_chunk_size", &self.stream_chunk_size)
            .finish()
    }
}

impl GcsRustFsBuilder {
    /// Create a new builder with default `CachedFs` (`512 MiB` Mode 2 `PageCache` + readahead).
    pub fn new() -> Self {
        Self {
            bucket_name: None,
            transport: None,
            endpoint: None,
            project: None,
            grpc_subchannel_count: None,
            http_pool_size: None,
            bucket_kinds: HashMap::new(),
            finalize_on_close: true,
            cache_config: Some(default_cache_config()),
            metrics: None,
            filesystem: None,
            stream_chunk_size: DEFAULT_STREAM_CHUNK_SIZE,
            use_env: false,
        }
    }

    /// Create a builder pre-populated from environment variables:
    /// - Bucket: [`BUCKET_ENV_VARS`]
    /// - Endpoint: [`ENDPOINT_ENV_VARS`]
    /// - Transport, HTTP pool size, and project: [`gcs_rust_fs::GcsFsBuilder::from_env`]
    pub fn from_env() -> Self {
        let mut builder = Self::new();
        builder.use_env = true;
        builder.bucket_name = BUCKET_ENV_VARS
            .iter()
            .filter_map(|k| std::env::var(k).ok())
            .map(|v| v.trim().to_owned())
            .find(|v| !v.is_empty());
        builder.endpoint = ENDPOINT_ENV_VARS
            .iter()
            .filter_map(|k| std::env::var(k).ok())
            .map(|v| v.trim().to_owned())
            .find(|v| !v.is_empty());
        builder
    }

    /// Set the target Google Cloud Storage bucket name (e.g. `"my-bucket"` or `"gs://my-bucket"`).
    #[must_use]
    pub fn with_bucket_name(mut self, bucket: impl Into<String>) -> Self {
        self.bucket_name = Some(bucket.into());
        self
    }

    /// Select the data-plane [`Transport`] (`Transport::Grpc` by default).
    #[must_use]
    pub fn with_transport(mut self, transport: Transport) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Override the Cloud Storage service endpoint (e.g. for private endpoints or local testbenches).
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Set the GCP project ID associated with bucket-level operations.
    #[must_use]
    pub fn with_project(mut self, project: impl Into<String>) -> Self {
        self.project = Some(project.into());
        self
    }

    /// Configure the number of gRPC subchannels used for data reads.
    #[must_use]
    pub fn with_grpc_subchannel_count(mut self, count: usize) -> Self {
        self.grpc_subchannel_count = Some(count);
        self
    }

    /// Configure the number of striped HTTP clients used when [`Transport::Http`] is active.
    #[must_use]
    pub fn with_http_pool_size(mut self, count: usize) -> Self {
        self.http_pool_size = Some(count);
        self
    }

    /// Seed the [`BucketKind`] (`Flat`, `Hierarchical`, or `Zonal`) of a bucket upfront.
    #[must_use]
    pub fn with_bucket_kind(mut self, bucket: impl Into<String>, kind: BucketKind) -> Self {
        self.bucket_kinds.insert(bucket.into(), kind);
        self
    }

    /// Configure whether closing a write handle on a Zonal bucket finalizes the object
    /// (defaults to `true` in `GcsRustFsBuilder` so `ObjectStore` uploads publish finalized objects).
    #[must_use]
    pub fn with_finalize_on_close(mut self, finalize: bool) -> Self {
        self.finalize_on_close = finalize;
        self
    }

    /// Customize the [`CacheConfig`] applied when wrapping the underlying `GcsFs`.
    #[must_use]
    pub fn with_cache_config(mut self, config: CacheConfig) -> Self {
        self.cache_config = Some(config);
        self
    }

    /// Set the Mode 2 shared `PageCache` capacity in bytes while keeping default readahead settings.
    #[must_use]
    pub fn with_page_cache_capacity(mut self, capacity_bytes: usize) -> Self {
        let mut cfg = self.cache_config.unwrap_or_else(default_cache_config);
        cfg.data = Some(DataConfig::page_cache(capacity_bytes));
        self.cache_config = Some(cfg);
        self
    }

    /// Disable the [`CachedFs`] wrapper and issue all operations directly against `GcsFs`.
    #[must_use]
    pub fn without_cache(mut self) -> Self {
        self.cache_config = None;
        self
    }

    /// Register a custom [`MetricsRecorder`] on the underlying `GcsFs` and `CachedFs`.
    #[must_use]
    pub fn with_metrics_recorder(mut self, recorder: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = Some(recorder);
        self
    }

    /// Set the chunk size in bytes yielded by [`crate::ObjectStore::get_opts`] streams
    /// (default: `1 MiB`).
    #[must_use]
    pub fn with_stream_chunk_size(mut self, chunk_size: usize) -> Self {
        self.stream_chunk_size = chunk_size;
        self
    }

    /// Supply a pre-constructed [`FileSystem`] (such as `RemoteFs`, a custom `CachedFs`,
    /// or an in-memory test filesystem). When set, [`Self::build`] uses `filesystem`
    /// directly without opening Cloud Storage credentials.
    #[must_use]
    pub fn with_filesystem(mut self, filesystem: Arc<dyn FileSystem>) -> Self {
        self.filesystem = Some(filesystem);
        self
    }

    /// Build the [`GcsRustFs`] instance, connecting to Google Cloud Storage via
    /// Application Default Credentials unless [`Self::with_filesystem`] was provided.
    pub async fn build(self) -> Result<GcsRustFs> {
        if self.stream_chunk_size == 0 {
            return Err(BuilderError::InvalidStreamChunkSize.into());
        }
        let raw_bucket = self.bucket_name.ok_or(BuilderError::MissingBucketName)?;
        let bucket = validate_bucket_name(&raw_bucket)?;

        if let Some(fs) = self.filesystem {
            return Ok(GcsRustFs {
                bucket: Arc::from(bucket),
                fs,
                stream_chunk_size: self.stream_chunk_size,
            });
        }

        let mut gcs_builder = GcsFsBuilder::new();
        if self.use_env {
            gcs_builder = gcs_builder.from_env().map_err(|e| map_error(e, &bucket))?;
        }
        gcs_builder = gcs_builder.finalize_on_close(self.finalize_on_close);
        if let Some(transport) = self.transport {
            gcs_builder = gcs_builder.transport(transport);
        }
        if let Some(endpoint) = self.endpoint {
            gcs_builder = gcs_builder.endpoint(endpoint);
        }
        if let Some(project) = self.project {
            gcs_builder = gcs_builder.project(project);
        }
        if let Some(subchannels) = self.grpc_subchannel_count {
            gcs_builder = gcs_builder.grpc_subchannel_count(subchannels);
        }
        if let Some(pool_size) = self.http_pool_size {
            gcs_builder = gcs_builder.http_pool_size(pool_size);
        }
        for (b, kind) in self.bucket_kinds {
            let normalized = validate_bucket_name(&b)?;
            gcs_builder = gcs_builder.bucket_kind(normalized, kind);
        }
        if let Some(ref recorder) = self.metrics {
            gcs_builder = gcs_builder.with_metrics_recorder(Arc::clone(recorder));
        }

        let gcs_fs = gcs_builder
            .build()
            .await
            .map_err(|e| map_error(e, &bucket))?;

        let fs: Arc<dyn FileSystem> = match self.cache_config {
            Some(cache_cfg) => {
                let mut cached = CachedFs::new(gcs_fs, cache_cfg);
                if let Some(recorder) = self.metrics {
                    cached = cached.with_metrics_recorder(recorder);
                }
                Arc::new(cached)
            }
            None => Arc::new(gcs_fs),
        };

        Ok(GcsRustFs {
            bucket: Arc::from(bucket),
            fs,
            stream_chunk_size: self.stream_chunk_size,
        })
    }
}
