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

//! Google Cloud Storage [`ObjectStore`] adapter backed by [`gcs_rust_fs`].
//!
//! [`GcsRustFs`] bridges [`ObjectStore`] onto an existing
//! [`gcs_rust_fs::FileSystem`] (`0.1.3`), enabling:
//!
//! * gRPC `BidiReadObject` multi-range downloads (`Transport::Grpc`) or striped HTTP/2 pools (`Transport::Http`),
//! * Linux-style asynchronous sequential readahead (`8–64 MiB` windows) on streaming reads,
//! * Generation-keyed Mode 2 shared `256 KiB` page caching (`512 MiB` by default) across `get`, `get_range`, and `get_ranges`,
//! * Transparent Flat, Hierarchical Namespace (HNS), and Zonal bucket handling via Application Default Credentials (ADC).
//!
//! # Example
//!
//! ```no_run
//! use object_store::gcs_rust_fs::GcsRustFsBuilder;
//! use object_store::{ObjectStore, ObjectStoreExt, path::Path};
//!
//! # async fn demo() -> object_store::Result<()> {
//! let store = GcsRustFsBuilder::from_env()
//!     .with_bucket_name("my-bucket")
//!     .build()
//!     .await?;
//!
//! let path = Path::from("data/part-0.parquet");
//! store.put(&path, "hello gcs-rust-fs".into()).await?;
//! let bytes = store.get(&path).await?.bytes().await?;
//! assert_eq!(bytes.as_ref(), b"hello gcs-rust-fs");
//! # Ok(())
//! # }
//! ```

use std::collections::{BTreeSet, HashMap};
use std::io::SeekFrom;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::stream::{self, BoxStream};
use futures_util::{StreamExt, TryStreamExt};
use gcs_rust_fs::{
    ByteRange, Entry, EntryKind, ErrorKind, File, FileSystem, FindOptions, ListOptions, ObjectStat,
    OpenOptions, ReadPattern, WriteMode, WriteOptions,
};

use crate::path::Path;
use crate::util::InvalidGetRange;
use crate::{
    Attribute, Attributes, CopyMode, CopyOptions, Error, GetOptions, GetRange, GetResult,
    GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, RenameTargetMode,
    Result,
};

mod builder;
#[cfg(test)]
pub(crate) mod mock;
mod upload;

pub use builder::{BUCKET_ENV_VARS, ENDPOINT_ENV_VARS, GcsRustFsBuilder, default_cache_config};
pub use gcs_rust_fs::{
    BucketKind, CacheConfig, CacheStats, CachedFs, DataConfig, MetricsRecorder, PageCacheConfig,
    ReadaheadConfig, Transport,
};
use upload::GcsRustFsMultipartUpload;

pub(crate) const STORE: &str = "GcsRustFs";
pub(crate) const DEFAULT_PAGE_CACHE_CAPACITY: usize = 512 * 1024 * 1024;
pub(crate) const DEFAULT_STREAM_CHUNK_SIZE: usize = 1024 * 1024;
const DELETE_STREAM_CONCURRENCY: usize = 10;

#[derive(Debug, thiserror::Error)]
enum RangeError {
    #[error("Invalid range: {source}")]
    InvalidRange { source: InvalidGetRange },
}

fn range_error(source: InvalidGetRange) -> Error {
    Error::Generic {
        store: STORE,
        source: Box::new(RangeError::InvalidRange { source }),
    }
}

/// [`ObjectStore`] implementation backed by [`gcs_rust_fs::FileSystem`].
#[derive(Clone)]
pub struct GcsRustFs {
    pub(crate) bucket: Arc<str>,
    pub(crate) fs: Arc<dyn FileSystem>,
    pub(crate) stream_chunk_size: usize,
}

impl std::fmt::Debug for GcsRustFs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsRustFs")
            .field("bucket", &self.bucket)
            .field("stream_chunk_size", &self.stream_chunk_size)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for GcsRustFs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GcsRustFs({})", self.bucket)
    }
}

impl GcsRustFs {
    /// Start configuring a [`GcsRustFs`] instance.
    pub fn builder() -> GcsRustFsBuilder {
        GcsRustFsBuilder::new()
    }

    /// Return the configured Cloud Storage bucket name.
    pub fn bucket_name(&self) -> &str {
        &self.bucket
    }

    /// Return a reference to the underlying [`gcs_rust_fs::FileSystem`].
    pub fn filesystem(&self) -> &Arc<dyn FileSystem> {
        &self.fs
    }

    /// Format an [`ObjectStore`] [`Path`] (and optional generation `version`) into
    /// a `gcs-rust-fs` `"bucket/key"` or `"bucket/key#generation"` path.
    pub(crate) fn object_path(&self, location: &Path, version: Option<&str>) -> Result<String> {
        let key = location.as_ref();
        if key.is_empty() {
            return Err(Error::NotFound {
                path: location.to_string(),
                source: "root path does not name an object".into(),
            });
        }
        if has_generation_suffix(key) {
            return Err(Error::NotSupported {
                source: format!(
                    "object key {key:?} ends with '#<integer>', which conflicts with gcs-rust-fs generation syntax"
                )
                .into(),
            });
        }
        match version {
            Some(ver) => {
                let parsed_gen = ver.parse::<i64>().map_err(|e| Error::NotFound {
                    path: location.to_string(),
                    source: format!("invalid GCS generation version {ver:?}: {e}").into(),
                })?;
                Ok(format!("{}/{key}#{parsed_gen}", self.bucket))
            }
            None => Ok(format!("{}/{key}", self.bucket)),
        }
    }

    /// Strip `gs://`, `gcs://`, leading `/`, and `"{bucket}/"` from a `gcs-rust-fs` entry path.
    pub(crate) fn strip_bucket_prefix<'a>(&self, entry_path: &'a str) -> Option<&'a str> {
        let stripped = entry_path
            .strip_prefix("gs://")
            .or_else(|| entry_path.strip_prefix("gcs://"))
            .unwrap_or(entry_path)
            .trim_start_matches('/');
        let rest = stripped.strip_prefix(self.bucket.as_ref())?;
        rest.strip_prefix('/')
    }

    pub(crate) fn resolve_list_prefix(
        &self,
        prefix: Option<&Path>,
    ) -> Result<(String, Option<String>)> {
        let raw = prefix.map(|p| p.as_ref()).unwrap_or("");
        if raw.is_empty() {
            return Ok((self.bucket.to_string(), None));
        }
        if has_generation_suffix(raw) {
            return Err(Error::NotSupported {
                source: format!(
                    "list prefix {raw:?} ends with '#<integer>', which conflicts with gcs-rust-fs generation syntax"
                )
                .into(),
            });
        }
        Ok((format!("{}/{raw}", self.bucket), Some(format!("{raw}/"))))
    }

    pub(crate) async fn head_meta(
        &self,
        location: &Path,
        full_path: &str,
    ) -> Result<(ObjectMeta, Attributes)> {
        let entry = self
            .fs
            .info(full_path)
            .await
            .map_err(|e| map_error(e, location.as_ref()))?;
        entry_must_be_file(entry, location)
    }
}

/// Returns `true` when `key` ends with `#<i64>`, matching `gcs_rust_fs`'s `split_generation` rule.
pub(crate) fn has_generation_suffix(key: &str) -> bool {
    matches!(key.rsplit_once('#'), Some((_, suffix)) if suffix.parse::<i64>().is_ok())
}

pub(crate) fn parse_timestamp(stat: &ObjectStat) -> DateTime<Utc> {
    stat.updated
        .as_deref()
        .or(stat.time_created.as_deref())
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}

pub(crate) fn stat_to_meta(location: Path, stat: &ObjectStat) -> ObjectMeta {
    ObjectMeta {
        location,
        last_modified: parse_timestamp(stat),
        size: stat.size,
        e_tag: (!stat.etag.is_empty()).then(|| stat.etag.clone()),
        version: Some(stat.generation.to_string()),
    }
}

pub(crate) fn stat_to_attributes(stat: &ObjectStat) -> Attributes {
    let mut attrs = Attributes::new();
    if !stat.content_type.is_empty() {
        attrs.insert(Attribute::ContentType, stat.content_type.clone().into());
    }
    if let Some(ref encoding) = stat.content_encoding {
        if !encoding.is_empty() {
            attrs.insert(Attribute::ContentEncoding, encoding.clone().into());
        }
    }
    for (k, v) in &stat.metadata {
        attrs.insert(Attribute::Metadata(k.clone().into()), v.clone().into());
    }
    attrs
}

pub(crate) fn entry_to_meta(store: &GcsRustFs, entry: &Entry) -> Result<ObjectMeta> {
    let key = store
        .strip_bucket_prefix(&entry.path)
        .ok_or_else(|| Error::Generic {
            store: STORE,
            source: format!(
                "entry path {:?} did not start with bucket {:?}",
                entry.path, store.bucket
            )
            .into(),
        })?;
    let location = Path::parse(key)?;
    match &entry.stat {
        Some(stat) => Ok(stat_to_meta(location, stat)),
        None => Ok(ObjectMeta {
            location,
            last_modified: DateTime::<Utc>::UNIX_EPOCH,
            size: entry.size,
            e_tag: None,
            version: None,
        }),
    }
}

pub(crate) fn build_write_options(mode: &PutMode, attributes: &Attributes) -> Result<WriteOptions> {
    let write_mode = match mode {
        PutMode::Overwrite => WriteMode::Overwrite,
        PutMode::Create => WriteMode::Create,
        PutMode::Update(_) => {
            return Err(Error::NotImplemented {
                operation: "PutMode::Update".to_owned(),
                implementer: STORE.to_owned(),
            });
        }
    };

    let mut content_type = None;
    let mut metadata = HashMap::new();

    for (attr, val) in attributes {
        match attr {
            Attribute::ContentType => {
                content_type = Some(val.as_ref().to_owned());
            }
            Attribute::Metadata(k) => {
                metadata.insert(k.as_ref().to_owned(), val.as_ref().to_owned());
            }
            other => {
                return Err(Error::NotImplemented {
                    operation: format!("Attribute::{other:?}"),
                    implementer: STORE.to_owned(),
                });
            }
        }
    }

    let mut opts = WriteOptions::new().with_mode(write_mode);
    opts.content_type = content_type;
    opts.metadata = metadata;
    Ok(opts)
}

/// Map a [`gcs_rust_fs::Error`] into [`crate::Error`].
///
/// Note that `ErrorKind::IsADirectory` is mapped to [`Error::NotFound`] because
/// [`ObjectStore`] treats directory prefixes as non-existent objects on `get`, `head`, and `delete`.
pub(crate) fn map_error(err: gcs_rust_fs::Error, path: &str) -> Error {
    match err.kind() {
        ErrorKind::NotFound | ErrorKind::IsADirectory => Error::NotFound {
            path: path.to_owned(),
            source: Box::new(err),
        },
        ErrorKind::AlreadyExists => Error::AlreadyExists {
            path: path.to_owned(),
            source: Box::new(err),
        },
        ErrorKind::PreconditionFailed => Error::Precondition {
            path: path.to_owned(),
            source: Box::new(err),
        },
        ErrorKind::PermissionDenied => Error::PermissionDenied {
            path: path.to_owned(),
            source: Box::new(err),
        },
        ErrorKind::Unauthenticated => Error::Unauthenticated {
            path: path.to_owned(),
            source: Box::new(err),
        },
        ErrorKind::Unsupported => Error::NotSupported {
            source: Box::new(err),
        },
        _ => Error::Generic {
            store: STORE,
            source: Box::new(err),
        },
    }
}

pub(crate) fn entry_must_be_file(
    entry: Entry,
    location: &Path,
) -> Result<(ObjectMeta, Attributes)> {
    if entry.kind != EntryKind::File {
        return Err(Error::NotFound {
            path: location.to_string(),
            source: "location refers to a directory prefix, not an object".into(),
        });
    }
    let stat = entry.stat.unwrap_or_else(|| {
        let mut s = ObjectStat::default();
        s.size = entry.size;
        s
    });
    let meta = stat_to_meta(location.clone(), &stat);
    let attrs = stat_to_attributes(&stat);
    Ok((meta, attrs))
}

#[async_trait]
impl ObjectStore for GcsRustFs {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let PutOptions {
            mode,
            tags: _,
            attributes,
            extensions: _,
        } = opts;

        let full_path = self.object_path(location, None)?;
        let write_opts = build_write_options(&mode, &attributes)?;
        let bytes = Bytes::from(payload);

        self.fs
            .pipe_file(&full_path, bytes, write_opts)
            .await
            .map_err(|e| map_error(e, location.as_ref()))?;

        let (meta, _) = self.head_meta(location, &full_path).await?;
        Ok(PutResult {
            e_tag: meta.e_tag,
            version: meta.version,
            extensions: Default::default(),
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let PutMultipartOptions {
            tags: _,
            attributes,
            extensions: _,
        } = opts;

        let full_path = self.object_path(location, None)?;
        let write_opts = build_write_options(&PutMode::Overwrite, &attributes)?;

        Ok(Box::new(GcsRustFsMultipartUpload::new(
            Arc::clone(&self.fs),
            location.clone(),
            full_path,
            write_opts,
        )))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let GetOptions {
            if_match,
            if_none_match,
            if_modified_since,
            if_unmodified_since,
            range,
            version,
            head,
            extensions: _,
        } = options;

        if if_match.is_some()
            || if_none_match.is_some()
            || if_modified_since.is_some()
            || if_unmodified_since.is_some()
        {
            return Err(Error::NotSupported {
                source: "HTTP conditional GetOptions (if_match, if_none_match, if_modified_since, if_unmodified_since) are not supported by gcs-rust-fs 0.1.3".into(),
            });
        }

        if let Some(ref r) = range {
            r.is_valid().map_err(range_error)?;
        }

        let full_path = self.object_path(location, version.as_deref())?;

        if head {
            let (meta, attributes) = self.head_meta(location, &full_path).await?;
            let resolved_range = match range {
                Some(r) => r.as_range(meta.size).map_err(range_error)?,
                None => 0..meta.size,
            };
            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream::empty().boxed()),
                meta,
                range: resolved_range,
                attributes,
                extensions: Default::default(),
            });
        }

        let mut file = self
            .fs
            .open(&full_path, OpenOptions::read())
            .await
            .map_err(|e| map_error(e, location.as_ref()))?;

        let (meta, attributes) = match file.stat() {
            Some(stat) => (
                stat_to_meta(location.clone(), stat),
                stat_to_attributes(stat),
            ),
            None => self.head_meta(location, &full_path).await?,
        };

        let resolved_range = match range {
            Some(r) => r.as_range(meta.size).map_err(range_error)?,
            None => 0..meta.size,
        };

        if resolved_range.is_empty() {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream::empty().boxed()),
                meta,
                range: resolved_range,
                attributes,
                extensions: Default::default(),
            });
        }

        file.seek(SeekFrom::Start(resolved_range.start))
            .map_err(|e| map_error(e, location.as_ref()))?;

        let remaining = resolved_range.end - resolved_range.start;
        let chunk_size = self.stream_chunk_size;
        let loc_str = location.to_string();

        let byte_stream = stream::try_unfold(
            (file, remaining, loc_str),
            move |(mut file, rem, loc_str)| async move {
                if rem == 0 {
                    return Ok(None);
                }
                let step = (rem.min(chunk_size as u64)) as usize;
                let chunk = file
                    .read(Some(step))
                    .await
                    .map_err(|e| map_error(e, &loc_str))?;
                if chunk.is_empty() {
                    return Ok(None);
                }
                let next_rem = rem.saturating_sub(chunk.len() as u64);
                Ok(Some((chunk, (file, next_rem, loc_str))))
            },
        )
        .boxed();

        Ok(GetResult {
            payload: GetResultPayload::Stream(byte_stream),
            meta,
            range: resolved_range,
            attributes,
            extensions: Default::default(),
        })
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }
        for r in ranges {
            GetRange::Bounded(r.clone())
                .is_valid()
                .map_err(range_error)?;
        }

        let full_path = self.object_path(location, None)?;
        let mut open_opts = OpenOptions::read();
        open_opts.read_pattern = ReadPattern::Random;

        let file = self
            .fs
            .open(&full_path, open_opts)
            .await
            .map_err(|e| map_error(e, location.as_ref()))?;

        let size = match file.size() {
            Some(s) => s,
            None => self.head_meta(location, &full_path).await?.0.size,
        };

        let clamped: Vec<Range<u64>> = ranges
            .iter()
            .map(|r| {
                GetRange::Bounded(r.clone())
                    .as_range(size)
                    .map_err(range_error)
            })
            .collect::<Result<Vec<_>>>()?;

        let shared: Arc<dyn File> = Arc::from(file);
        let loc_str = location.to_string();

        futures_util::future::try_join_all(clamped.into_iter().map(|r| {
            let f = Arc::clone(&shared);
            let loc_str = loc_str.clone();
            async move {
                if r.is_empty() {
                    return Ok(Bytes::new());
                }
                f.read_range(ByteRange::from(r))
                    .await
                    .map_err(|e| map_error(e, &loc_str))
            }
        }))
        .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let store = self.clone();
        locations
            .map(move |location| {
                let store = store.clone();
                async move {
                    let location = location?;
                    let full_path = store.object_path(&location, None)?;
                    store
                        .fs
                        .rm_file(&full_path)
                        .await
                        .map_err(|e| map_error(e, location.as_ref()))?;
                    Ok(location)
                }
            })
            .buffered(DELETE_STREAM_CONCURRENCY)
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let (target, key_prefix) = match self.resolve_list_prefix(prefix) {
            Ok(v) => v,
            Err(e) => return stream::once(futures_util::future::ready(Err(e))).boxed(),
        };
        let store = self.clone();
        let prefix_label = prefix.map(|p| p.to_string()).unwrap_or_default();

        stream::once(async move {
            let find_opts = FindOptions {
                maxdepth: None,
                withdirs: false,
                versions: false,
            };
            let entries = match store.fs.find(&target, find_opts).await {
                Ok(entries) => entries,
                Err(e) if e.kind() == ErrorKind::NotFound && key_prefix.is_some() => {
                    if store.fs.info(&store.bucket).await.is_ok() {
                        Vec::new()
                    } else {
                        return Err(map_error(e, &prefix_label));
                    }
                }
                Err(e) => return Err(map_error(e, &prefix_label)),
            };

            let items: Vec<Result<ObjectMeta>> = entries
                .into_iter()
                .filter(|entry| entry.is_file())
                .filter(|entry| {
                    let Some(raw_key) = store.strip_bucket_prefix(&entry.path) else {
                        return false;
                    };
                    match &key_prefix {
                        Some(kp) => raw_key.starts_with(kp.as_str()),
                        None => !raw_key.is_empty(),
                    }
                })
                .map(|entry| entry_to_meta(&store, &entry))
                .collect();

            Ok::<_, Error>(stream::iter(items))
        })
        .try_flatten()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let offset = offset.clone();
        self.list(prefix)
            .try_filter(move |meta| futures_util::future::ready(meta.location > offset))
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let (target, key_prefix) = self.resolve_list_prefix(prefix)?;
        let prefix_label = prefix.map(|p| p.as_ref()).unwrap_or("");

        let ls_opts = ListOptions {
            versions: false,
            refresh: true,
        };
        let entries = match self.fs.ls(&target, ls_opts).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == ErrorKind::NotFound && key_prefix.is_some() => {
                if self.fs.info(&self.bucket).await.is_ok() {
                    return Ok(ListResult {
                        common_prefixes: Vec::new(),
                        objects: Vec::new(),
                        extensions: Default::default(),
                    });
                }
                return Err(map_error(e, prefix_label));
            }
            Err(e) => return Err(map_error(e, prefix_label)),
        };

        let mut objects = Vec::new();
        let mut common_prefixes = BTreeSet::new();

        for entry in entries {
            let Some(raw_key) = self.strip_bucket_prefix(&entry.path) else {
                continue;
            };
            match entry.kind {
                EntryKind::File => {
                    let keep = match &key_prefix {
                        Some(kp) => {
                            raw_key.starts_with(kp.as_str())
                                && !raw_key[kp.len()..].is_empty()
                                && !raw_key[kp.len()..].contains('/')
                        }
                        None => !raw_key.is_empty() && !raw_key.contains('/'),
                    };
                    if keep {
                        objects.push(entry_to_meta(self, &entry)?);
                    }
                }
                EntryKind::Directory => {
                    let dir_key = raw_key.trim_end_matches('/');
                    let keep = match &key_prefix {
                        Some(kp) => {
                            dir_key.starts_with(kp.as_str())
                                && !dir_key[kp.len()..].is_empty()
                                && !dir_key[kp.len()..].contains('/')
                        }
                        None => !dir_key.is_empty() && !dir_key.contains('/'),
                    };
                    if keep {
                        common_prefixes.insert(Path::parse(dir_key)?);
                    }
                }
            }
        }

        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
            extensions: Default::default(),
        })
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        match options.mode {
            CopyMode::Overwrite => {}
            CopyMode::Create => {
                return Err(Error::NotImplemented {
                    operation: "CopyMode::Create".to_owned(),
                    implementer: STORE.to_owned(),
                });
            }
        }
        let src = self.object_path(from, None)?;
        let dst = self.object_path(to, None)?;
        self.fs
            .copy_file(&src, &dst)
            .await
            .map_err(|e| map_error(e, from.as_ref()))
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        match options.target_mode {
            RenameTargetMode::Overwrite => {}
            RenameTargetMode::Create => {
                return Err(Error::NotImplemented {
                    operation: "RenameTargetMode::Create".to_owned(),
                    implementer: STORE.to_owned(),
                });
            }
        }
        let src = self.object_path(from, None)?;
        let dst = self.object_path(to, None)?;
        self.fs
            .move_file(&src, &dst)
            .await
            .map_err(|e| map_error(e, from.as_ref()))
    }
}

#[cfg(test)]
mod tests {
    use super::builder::validate_bucket_name;
    use super::mock::MockMemoryFs;
    use super::*;
    use crate::{ObjectStoreExt, UpdateVersion};

    async fn build_cached_mock_store() -> (GcsRustFs, MockMemoryFs) {
        let mock = MockMemoryFs::new_with_bucket("test-bkt");
        let cached = CachedFs::new(mock.clone(), default_cache_config());
        let store = GcsRustFs::builder()
            .with_bucket_name("test-bkt")
            .with_filesystem(Arc::new(cached))
            .with_stream_chunk_size(4)
            .build()
            .await
            .unwrap();
        (store, mock)
    }

    #[test]
    fn bucket_validation_enforces_boundaries() {
        assert_eq!(validate_bucket_name("my-bucket").unwrap(), "my-bucket");
        assert_eq!(
            validate_bucket_name("gs://my-bucket/").unwrap(),
            "my-bucket"
        );
        assert_eq!(
            validate_bucket_name("gcs://my-bucket").unwrap(),
            "my-bucket"
        );

        assert!(validate_bucket_name("").is_err());
        assert!(validate_bucket_name("gs:///").is_err());
        assert!(validate_bucket_name("my-bucket/sub").is_err());
        assert!(validate_bucket_name("my-bucket#1").is_err());
        assert!(validate_bucket_name("my bucket").is_err());
    }

    #[tokio::test]
    async fn generation_suffix_and_path_mapping_guards() {
        assert!(has_generation_suffix("dir/file#42"));
        assert!(has_generation_suffix("a#b#-1"));
        assert!(!has_generation_suffix("dir/file#notanumber"));
        assert!(!has_generation_suffix("dir/file"));

        let (store, _) = build_cached_mock_store().await;
        assert_eq!(store.to_string(), "GcsRustFs(test-bkt)");
        assert_eq!(
            store.object_path(&Path::from("a/b.txt"), None).unwrap(),
            "test-bkt/a/b.txt"
        );
        assert_eq!(
            store
                .object_path(&Path::from("a/b.txt"), Some("99"))
                .unwrap(),
            "test-bkt/a/b.txt#99"
        );
        assert!(matches!(
            store.object_path(&Path::parse("a/b#99").unwrap(), None),
            Err(Error::NotSupported { .. })
        ));
        assert!(matches!(
            store.object_path(&Path::from("a/b.txt"), Some("not-a-gen")),
            Err(Error::NotFound { .. })
        ));
        assert_eq!(
            store.strip_bucket_prefix("gs://test-bkt/dir/file.parquet"),
            Some("dir/file.parquet")
        );
        assert_eq!(store.strip_bucket_prefix("other-bkt/file"), None);
    }

    #[test]
    fn error_and_directory_mapping() {
        let dir_err = gcs_rust_fs::Error::new(ErrorKind::IsADirectory, "is dir");
        assert!(matches!(map_error(dir_err, "dir"), Error::NotFound { .. }));

        let exists_err = gcs_rust_fs::Error::new(ErrorKind::AlreadyExists, "exists");
        assert!(matches!(
            map_error(exists_err, "f"),
            Error::AlreadyExists { .. }
        ));

        let precond_err = gcs_rust_fs::Error::new(ErrorKind::PreconditionFailed, "412");
        assert!(matches!(
            map_error(precond_err, "f"),
            Error::Precondition { .. }
        ));

        let dir_entry = Entry::directory("b/dir");
        assert!(matches!(
            entry_must_be_file(dir_entry, &Path::from("dir")),
            Err(Error::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn read_ranges_bounds_versions_and_page_cache_hits() {
        let (store, mock) = build_cached_mock_store().await;

        let mut wopts = WriteOptions::new();
        wopts.content_type = Some("text/plain".to_owned());
        wopts
            .metadata
            .insert("author".to_owned(), "arrow".to_owned());

        mock.pipe_file(
            "test-bkt/dir/item.txt",
            Bytes::from_static(b"arbitrary data"),
            wopts.clone(),
        )
        .await
        .unwrap();

        let location = Path::from("dir/item.txt");

        assert!(matches!(
            store.head(&Path::from("dir")).await,
            Err(Error::NotFound { .. })
        ));
        assert!(matches!(
            store.get(&Path::from("dir")).await,
            Err(Error::NotFound { .. })
        ));

        let meta_v1 = store.head(&location).await.unwrap();
        assert_eq!(meta_v1.size, 14);
        assert!(meta_v1.e_tag.is_some());
        let v1_gen = meta_v1.version.clone().unwrap();

        mock.pipe_file(
            "test-bkt/dir/item.txt",
            Bytes::from_static(b"updated payload!"),
            wopts,
        )
        .await
        .unwrap();
        store.filesystem().invalidate_cache(None);

        let v1_res = store
            .get_opts(&location, GetOptions::new().with_version(Some(v1_gen)))
            .await
            .unwrap();
        assert_eq!(v1_res.meta.size, 14);
        assert_eq!(
            v1_res
                .attributes
                .get(&Attribute::ContentType)
                .unwrap()
                .as_ref(),
            "text/plain"
        );
        assert_eq!(
            v1_res
                .attributes
                .get(&Attribute::Metadata("author".into()))
                .unwrap()
                .as_ref(),
            "arrow"
        );
        assert_eq!(v1_res.bytes().await.unwrap().as_ref(), b"arbitrary data");

        let bounded = store
            .get_opts(&location, GetOptions::new().with_range(Some(2..7)))
            .await
            .unwrap();
        assert_eq!(bounded.meta.size, 16);
        assert_eq!(bounded.range, 2..7);
        assert_eq!(bounded.bytes().await.unwrap().as_ref(), b"dated");

        let clamped = store
            .get_opts(&location, GetOptions::new().with_range(Some(8..500)))
            .await
            .unwrap();
        assert_eq!(clamped.range, 8..16);
        assert_eq!(clamped.bytes().await.unwrap().as_ref(), b"payload!");

        let suffix = store
            .get_opts(
                &location,
                GetOptions::new().with_range(Some(GetRange::Suffix(8))),
            )
            .await
            .unwrap();
        assert_eq!(suffix.range, 8..16);
        assert_eq!(suffix.bytes().await.unwrap().as_ref(), b"payload!");

        let offset = store
            .get_opts(
                &location,
                GetOptions::new().with_range(Some(GetRange::Offset(8))),
            )
            .await
            .unwrap();
        assert_eq!(offset.range, 8..16);
        assert_eq!(offset.bytes().await.unwrap().as_ref(), b"payload!");

        assert!(store.get_range(&location, 200..300).await.is_err());
        assert!(store.get_range(&location, 5..5).await.is_err());
        assert!(
            store
                .get_opts(
                    &location,
                    GetOptions::new().with_range(Some(GetRange::Offset(100)))
                )
                .await
                .is_err()
        );

        let reads_before = mock.backend_reads();
        let slices1 = store
            .get_ranges(&location, &[0..7, 8..16, 0..16])
            .await
            .unwrap();
        assert_eq!(slices1[0].as_ref(), b"updated");
        assert_eq!(slices1[1].as_ref(), b"payload!");
        assert_eq!(slices1[2].as_ref(), b"updated payload!");

        let reads_after_first = mock.backend_reads();
        let slices2 = store.get_ranges(&location, &[0..7, 8..16]).await.unwrap();
        assert_eq!(slices2[0].as_ref(), b"updated");
        assert_eq!(slices2[1].as_ref(), b"payload!");
        assert_eq!(
            mock.backend_reads(),
            reads_after_first,
            "second get_ranges on same generation must be served from CachedFs PageCache without touching backend (before={reads_before}, after_first={reads_after_first})"
        );
    }

    #[tokio::test]
    async fn put_modes_multipart_order_isolation_copy_rename_delete() {
        let (store, _) = build_cached_mock_store().await;

        let p = Path::from("writes/obj.bin");
        let r1 = store
            .put_opts(&p, "first".into(), PutMode::Create.into())
            .await
            .unwrap();
        assert!(r1.e_tag.is_some());
        assert!(r1.version.is_some());

        // Create on existing object fails with AlreadyExists
        let conflict = store
            .put_opts(&p, "second".into(), PutMode::Create.into())
            .await
            .unwrap_err();
        assert!(matches!(conflict, Error::AlreadyExists { .. }));

        // PutMode::Update returns NotImplemented on 0.1.3
        let upd_err = store
            .put_opts(
                &p,
                "third".into(),
                PutMode::Update(UpdateVersion::from(r1)).into(),
            )
            .await
            .unwrap_err();
        assert!(matches!(upd_err, Error::NotImplemented { .. }));

        // Overwrite updates payload and invalidates CachedFs metadata
        store.put(&p, "overwritten".into()).await.unwrap();
        assert_eq!(
            store.get(&p).await.unwrap().bytes().await.unwrap().as_ref(),
            b"overwritten"
        );

        // Concurrent out-of-order multipart upload remains invisible until complete()
        let mp_path = Path::from("writes/multipart.bin");
        let mut upload = store.put_multipart(&mp_path).await.unwrap();
        let f0 = upload.put_part("part0-".into());
        let f1 = upload.put_part("part1-".into());
        let f2 = upload.put_part("part2".into());

        // Poll out of order: f2 -> f1 -> f0
        f2.await.unwrap();
        f1.await.unwrap();
        assert!(matches!(
            store.head(&mp_path).await,
            Err(Error::NotFound { .. })
        ));
        f0.await.unwrap();
        assert!(matches!(
            store.head(&mp_path).await,
            Err(Error::NotFound { .. })
        ));

        let mp_res = upload.complete().await.unwrap();
        assert!(mp_res.e_tag.is_some());
        assert_eq!(
            store
                .get(&mp_path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"part0-part1-part2"
        );

        // Aborted multipart upload publishes nothing
        let abort_path = Path::from("writes/aborted.bin");
        let mut aborted = store.put_multipart(&abort_path).await.unwrap();
        aborted.put_part("uncommitted".into()).await.unwrap();
        aborted.abort().await.unwrap();
        assert!(matches!(
            store.head(&abort_path).await,
            Err(Error::NotFound { .. })
        ));

        // Copy, rename, and delete_stream
        let copy_dst = Path::from("writes/copied.bin");
        let rename_dst = Path::from("writes/renamed.bin");
        store.copy(&mp_path, &copy_dst).await.unwrap();
        store.rename(&copy_dst, &rename_dst).await.unwrap();
        assert!(matches!(
            store.head(&copy_dst).await,
            Err(Error::NotFound { .. })
        ));
        assert_eq!(
            store
                .get(&rename_dst)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"part0-part1-part2"
        );

        let del_results: Vec<_> = store
            .delete_stream(
                stream::iter(vec![
                    Ok(p.clone()),
                    Ok(mp_path.clone()),
                    Ok(rename_dst.clone()),
                    Ok(Path::from("writes/missing.bin")),
                ])
                .boxed(),
            )
            .collect()
            .await;
        assert!(del_results[0].is_ok());
        assert!(del_results[1].is_ok());
        assert!(del_results[2].is_ok());
        assert!(matches!(del_results[3], Err(Error::NotFound { .. })));
    }

    #[tokio::test]
    async fn list_prefix_delimiter_offset_and_file_echo_filtering() {
        let (store, _) = build_cached_mock_store().await;

        let keys = [
            "root.txt",
            "foo/x.json",
            "foo.bar/y.json",
            "mydb/wb/foo.json",
            "mydb/wb/000/000/000.segment",
            "mydb/wb/001/001/000.segment",
        ];
        for k in keys {
            store.put(&Path::from(k), "payload".into()).await.unwrap();
        }

        // Root listing (None and Path::ROOT) yields all 6 objects
        let all_none: Vec<Path> = store
            .list(None)
            .map_ok(|m| m.location)
            .try_collect()
            .await
            .unwrap();
        let all_root: Vec<Path> = store
            .list(Some(&Path::ROOT))
            .map_ok(|m| m.location)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(all_none.len(), 6);
        assert_eq!(all_none, all_root);

        // Directory prefix excludes sibling prefix ("foo" vs "foo.bar")
        let foo_only: Vec<Path> = store
            .list(Some(&Path::from("foo")))
            .map_ok(|m| m.location)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(foo_only, vec![Path::from("foo/x.json")]);

        // File-path listing and missing-prefix listing return empty stream (never file-echo)
        let file_echo: Vec<Path> = store
            .list(Some(&Path::from("foo/x.json")))
            .map_ok(|m| m.location)
            .try_collect()
            .await
            .unwrap();
        assert!(file_echo.is_empty());

        let missing: Vec<Path> = store
            .list(Some(&Path::from("missing_prefix")))
            .map_ok(|m| m.location)
            .try_collect()
            .await
            .unwrap();
        assert!(missing.is_empty());

        // Exclusive list_with_offset
        let after_foo: Vec<Path> = store
            .list_with_offset(None, &Path::from("mydb/wb/001/001/000.segment"))
            .map_ok(|m| m.location)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            after_foo,
            vec![Path::from("mydb/wb/foo.json"), Path::from("root.txt")]
        );

        // Delimited listing separates direct files and immediate child directories
        let delimited = store
            .list_with_delimiter(Some(&Path::from("mydb/wb")))
            .await
            .unwrap();
        assert_eq!(
            delimited.common_prefixes,
            vec![Path::from("mydb/wb/000"), Path::from("mydb/wb/001")]
        );
        assert_eq!(delimited.objects.len(), 1);
        assert_eq!(
            delimited.objects[0].location,
            Path::from("mydb/wb/foo.json")
        );

        // Delimited listing on file path or missing prefix returns empty ListResult
        let delim_file = store
            .list_with_delimiter(Some(&Path::from("foo/x.json")))
            .await
            .unwrap();
        assert!(delim_file.objects.is_empty());
        assert!(delim_file.common_prefixes.is_empty());

        let delim_missing = store
            .list_with_delimiter(Some(&Path::from("not_there")))
            .await
            .unwrap();
        assert!(delim_missing.objects.is_empty());
        assert!(delim_missing.common_prefixes.is_empty());
    }

    async fn run_read_write_list_suite(store: &GcsRustFs) {
        use crate::integration::{
            list_uses_directories_correctly, list_with_delimiter, list_with_offset_exclusivity,
            multipart_out_of_order, multipart_race_condition, put_get_attributes,
            put_get_delete_list, put_opts, rename_and_copy, stream_get,
        };

        put_get_delete_list(store).await;
        list_with_offset_exclusivity(store).await;
        list_uses_directories_correctly(store).await;
        list_with_delimiter(store).await;
        rename_and_copy(store).await;
        stream_get(store).await;
        multipart_out_of_order(store).await;
        multipart_race_condition(store, true).await;
        put_opts(store, false).await;
        put_get_attributes(store).await;

        // Also verify round-trip of supported ContentType + custom Metadata attributes
        // across both single-shot put_opts and multipart put_multipart_opts.
        let supported_attrs = Attributes::from_iter([
            (Attribute::ContentType, "application/octet-stream"),
            (Attribute::Metadata("x-suite".into()), "gcs-rust-fs"),
        ]);
        let attr_path = Path::from("supported_attributes/item.bin");
        store
            .put_opts(
                &attr_path,
                "attr-payload".into(),
                supported_attrs.clone().into(),
            )
            .await
            .unwrap();
        let res = store.get(&attr_path).await.unwrap();
        assert_eq!(res.attributes, supported_attrs);

        let mut mp = store
            .put_multipart_opts(&attr_path, supported_attrs.clone().into())
            .await
            .unwrap();
        mp.put_part("mp-attr-payload".into()).await.unwrap();
        mp.complete().await.unwrap();
        let mp_res = store.get(&attr_path).await.unwrap();
        assert_eq!(mp_res.attributes, supported_attrs);
        store.delete(&attr_path).await.unwrap();
    }

    #[tokio::test]
    async fn gcs_rust_fs_offline_integration() {
        let mock = MockMemoryFs::new_with_bucket("integration-bkt");
        let cached = CachedFs::new(mock, default_cache_config());
        let store = GcsRustFs::builder()
            .with_bucket_name("integration-bkt")
            .with_filesystem(Arc::new(cached))
            .build()
            .await
            .unwrap();

        run_read_write_list_suite(&store).await;
    }

    #[tokio::test]
    async fn gcs_rust_fs_offline_nonexistent_bucket_and_object() {
        use crate::integration::get_nonexistent_object;

        let mock = MockMemoryFs::new_with_bucket("existing-bkt");
        let cached = Arc::new(CachedFs::new(mock, default_cache_config()));

        let existing = GcsRustFs::builder()
            .with_bucket_name("existing-bkt")
            .with_filesystem(Arc::clone(&cached) as Arc<dyn FileSystem>)
            .build()
            .await
            .unwrap();

        let err = get_nonexistent_object(&existing, None).await.unwrap_err();
        assert!(matches!(err, Error::NotFound { .. }), "{err}");

        let missing_bucket = GcsRustFs::builder()
            .with_bucket_name("nonexistent-bkt")
            .with_filesystem(cached)
            .build()
            .await
            .unwrap();

        let loc = Path::from("obj.bin");
        assert!(matches!(
            get_nonexistent_object(&missing_bucket, Some(loc.clone()))
                .await
                .unwrap_err(),
            Error::NotFound { .. }
        ));
        assert!(matches!(
            missing_bucket.put(&loc, "data".into()).await.unwrap_err(),
            Error::NotFound { .. }
        ));
        assert!(matches!(
            missing_bucket.delete(&loc).await.unwrap_err(),
            Error::NotFound { .. }
        ));
        assert!(matches!(
            missing_bucket
                .list(None)
                .try_collect::<Vec<_>>()
                .await
                .unwrap_err(),
            Error::NotFound { .. }
        ));
        assert!(matches!(
            missing_bucket.list_with_delimiter(None).await.unwrap_err(),
            Error::NotFound { .. }
        ));
    }

    #[tokio::test]
    async fn gcs_rust_fs_live_integration() {
        use crate::tests::maybe_skip_integration;

        let explicit_bucket = std::env::var("GCS_RUST_FS_TEST_BUCKET")
            .ok()
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty());

        let store = match explicit_bucket {
            Some(bucket) => GcsRustFsBuilder::from_env()
                .with_bucket_name(bucket)
                .build()
                .await
                .unwrap(),
            None => {
                maybe_skip_integration!();
                let has_env_bucket = BUCKET_ENV_VARS
                    .iter()
                    .any(|k| std::env::var(k).ok().is_some_and(|v| !v.trim().is_empty()));
                if !has_env_bucket {
                    eprintln!(
                        "Skipping live GcsRustFs integration test - set GCS_RUST_FS_TEST_BUCKET or GOOGLE_BUCKET"
                    );
                    return;
                }
                GcsRustFsBuilder::from_env().build().await.unwrap()
            }
        };

        run_read_write_list_suite(&store).await;
    }
}
