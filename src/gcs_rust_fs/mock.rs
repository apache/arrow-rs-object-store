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

//! In-memory [`gcs_rust_fs::FileSystem`] implementation faithfully reproducing
//! `GcsFs` (`0.1.3`) semantics (generations, unpublished write handles until
//! `close()`, `ls`/`find` file-echo and missing-path `NotFound`, and directory
//! `IsADirectory` errors) so `CachedFs<MockMemoryFs>` exercises real readahead,
//! Mode 2 `PageCache`, and `GcsRustFs` adapter logic offline.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::SeekFrom;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use bytes::{Bytes, BytesMut};
use chrono::Utc;
use gcs_rust_fs::{
    ByteRange, Entry, Error, ErrorKind, File, FileSystem, FindOptions, ListOptions, MkdirOptions,
    ObjectStat, OpenMode, OpenOptions, ReadOptions, WriteMode, WriteOptions,
};
use parking_lot::RwLock;

#[derive(Clone, Debug)]
struct VersionedObject {
    data: Bytes,
    stat: ObjectStat,
}

#[derive(Default)]
struct State {
    buckets: BTreeSet<String>,
    /// Map of `(bucket, key)` -> chronological versions (last is latest).
    objects: BTreeMap<(String, String), Vec<VersionedObject>>,
}

/// In-memory [`FileSystem`] test double with backend read counting.
#[derive(Clone)]
pub(crate) struct MockMemoryFs {
    state: Arc<RwLock<State>>,
    next_gen: Arc<AtomicI64>,
    backend_reads: Arc<AtomicUsize>,
}

impl MockMemoryFs {
    pub(crate) fn new_with_bucket(bucket: &str) -> Self {
        let mut state = State::default();
        state.buckets.insert(bucket.to_owned());
        Self {
            state: Arc::new(RwLock::new(state)),
            next_gen: Arc::new(AtomicI64::new(1)),
            backend_reads: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(crate) fn backend_reads(&self) -> usize {
        self.backend_reads.load(Ordering::SeqCst)
    }

    fn parse_loc(path: &str) -> gcs_rust_fs::Result<(String, String, Option<i64>)> {
        let stripped = path
            .strip_prefix("gs://")
            .or_else(|| path.strip_prefix("gcs://"))
            .unwrap_or(path)
            .trim_start_matches('/');
        let (without_gen, generation) = match stripped.rsplit_once('#') {
            Some((prefix, suffix)) => match suffix.parse::<i64>() {
                Ok(g) => (prefix, Some(g)),
                Err(_) => (stripped, None),
            },
            None => (stripped, None),
        };
        let (bucket, key) = without_gen.split_once('/').unwrap_or((without_gen, ""));
        let key = key.trim_end_matches('/');
        if bucket.is_empty() && (!key.is_empty() || generation.is_some()) {
            return Err(Error::new(ErrorKind::InvalidPath, "empty bucket"));
        }
        if key.is_empty() && generation.is_some() {
            return Err(Error::new(
                ErrorKind::InvalidPath,
                "generation requires object key",
            ));
        }
        Ok((bucket.to_owned(), key.to_owned(), generation))
    }

    fn ensure_bucket(&self, bucket: &str) -> gcs_rust_fs::Result<()> {
        let st = self.state.read();
        if st.buckets.contains(bucket) {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::NotFound,
                format!("bucket {bucket:?} not found"),
            ))
        }
    }

    fn has_children(st: &State, bucket: &str, dir_key: &str) -> bool {
        let prefix = if dir_key.is_empty() {
            String::new()
        } else {
            format!("{dir_key}/")
        };
        st.objects
            .range((bucket.to_owned(), prefix.clone())..)
            .take_while(|((b, k), _)| b == bucket && k.starts_with(&prefix))
            .any(|((_, k), vers)| !vers.is_empty() && k.len() > prefix.len())
    }

    fn commit_object(
        &self,
        bucket: &str,
        key: &str,
        data: Bytes,
        mode: WriteMode,
        content_type: Option<String>,
        metadata: HashMap<String, String>,
    ) -> gcs_rust_fs::Result<ObjectStat> {
        if key.is_empty() {
            return Err(Error::new(ErrorKind::IsADirectory, bucket));
        }
        let mut st = self.state.write();
        if !st.buckets.contains(bucket) {
            return Err(Error::new(
                ErrorKind::NotFound,
                format!("Server returned non-2xx status code: 404 Not Found (bucket {bucket})"),
            ));
        }
        let map_key = (bucket.to_owned(), key.to_owned());
        if mode == WriteMode::Create && st.objects.get(&map_key).is_some_and(|v| !v.is_empty()) {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                format!("{bucket}/{key} already exists"),
            ));
        }

        let generation = self.next_gen.fetch_add(1, Ordering::SeqCst);
        let mut stat = ObjectStat::default();
        stat.bucket = bucket.to_owned();
        stat.name = key.to_owned();
        stat.size = data.len() as u64;
        stat.generation = generation;
        stat.metageneration = 1;
        stat.etag = format!("\"etag-{generation}\"");
        stat.content_type = content_type.unwrap_or_default();
        stat.updated = Some(Utc::now().to_rfc3339());
        stat.metadata = metadata;

        st.objects
            .entry(map_key)
            .or_default()
            .push(VersionedObject {
                data,
                stat: stat.clone(),
            });
        Ok(stat)
    }

    fn get_versioned(
        &self,
        bucket: &str,
        key: &str,
        generation: Option<i64>,
    ) -> gcs_rust_fs::Result<VersionedObject> {
        self.ensure_bucket(bucket)?;
        if key.is_empty() {
            return Err(Error::new(ErrorKind::IsADirectory, bucket));
        }
        let st = self.state.read();
        if let Some(versions) = st.objects.get(&(bucket.to_owned(), key.to_owned())) {
            let found = match generation {
                Some(g) => versions.iter().find(|v| v.stat.generation == g).cloned(),
                None => versions.last().cloned(),
            };
            if let Some(v) = found {
                return Ok(v);
            }
        }
        if generation.is_none() && Self::has_children(&st, bucket, key) {
            return Err(Error::new(
                ErrorKind::IsADirectory,
                format!("{bucket}/{key}"),
            ));
        }
        Err(Error::new(ErrorKind::NotFound, format!("{bucket}/{key}")))
    }
}

struct MockReadFile {
    path: String,
    data: Bytes,
    stat: ObjectStat,
    pos: u64,
    closed: bool,
    backend_reads: Arc<AtomicUsize>,
}

#[gcs_rust_fs::async_trait]
impl File for MockReadFile {
    fn path(&self) -> &str {
        &self.path
    }

    fn mode(&self) -> OpenMode {
        OpenMode::Read
    }

    fn closed(&self) -> bool {
        self.closed
    }

    fn tell(&self) -> u64 {
        self.pos
    }

    fn size(&self) -> Option<u64> {
        Some(self.stat.size)
    }

    fn stat(&self) -> Option<&ObjectStat> {
        Some(&self.stat)
    }

    fn seek(&mut self, pos: SeekFrom) -> gcs_rust_fs::Result<u64> {
        if self.closed {
            return Err(Error::new(ErrorKind::Closed, &self.path));
        }
        let target = match pos {
            SeekFrom::Start(n) => n as i64,
            SeekFrom::End(d) => self.stat.size as i64 + d,
            SeekFrom::Current(d) => self.pos as i64 + d,
        };
        if target < 0 {
            return Err(Error::invalid_range("negative seek"));
        }
        self.pos = (target as u64).min(self.stat.size);
        Ok(self.pos)
    }

    async fn read(&mut self, len: Option<usize>) -> gcs_rust_fs::Result<Bytes> {
        if self.closed {
            return Err(Error::new(ErrorKind::Closed, &self.path));
        }
        let avail = (self.stat.size.saturating_sub(self.pos)) as usize;
        let take = len.unwrap_or(avail).min(avail);
        if take == 0 {
            return Ok(Bytes::new());
        }
        self.backend_reads.fetch_add(1, Ordering::SeqCst);
        let start = self.pos as usize;
        let out = self.data.slice(start..start + take);
        self.pos += take as u64;
        Ok(out)
    }

    async fn read_range(&self, range: ByteRange) -> gcs_rust_fs::Result<Bytes> {
        if self.closed {
            return Err(Error::new(ErrorKind::Closed, &self.path));
        }
        let clamped = range.clamp(self.stat.size);
        if clamped.is_empty() {
            return Ok(Bytes::new());
        }
        self.backend_reads.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .data
            .slice(clamped.start as usize..clamped.end as usize))
    }

    async fn write(&mut self, _data: Bytes) -> gcs_rust_fs::Result<()> {
        Err(Error::new(ErrorKind::Unsupported, "read-only handle"))
    }

    async fn flush(&mut self) -> gcs_rust_fs::Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> gcs_rust_fs::Result<()> {
        self.closed = true;
        Ok(())
    }

    async fn discard(&mut self) -> gcs_rust_fs::Result<()> {
        self.closed = true;
        Ok(())
    }
}

struct MockWriteFile {
    fs: MockMemoryFs,
    bucket: String,
    key: String,
    path: String,
    mode: OpenMode,
    content_type: Option<String>,
    metadata: HashMap<String, String>,
    buf: BytesMut,
    closed: bool,
    published_stat: Option<ObjectStat>,
}

#[gcs_rust_fs::async_trait]
impl File for MockWriteFile {
    fn path(&self) -> &str {
        &self.path
    }

    fn mode(&self) -> OpenMode {
        self.mode
    }

    fn closed(&self) -> bool {
        self.closed
    }

    fn tell(&self) -> u64 {
        self.buf.len() as u64
    }

    fn size(&self) -> Option<u64> {
        self.published_stat.as_ref().map(|s| s.size)
    }

    fn stat(&self) -> Option<&ObjectStat> {
        self.published_stat.as_ref()
    }

    fn seek(&mut self, _pos: SeekFrom) -> gcs_rust_fs::Result<u64> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "write handle not seekable",
        ))
    }

    async fn read(&mut self, _len: Option<usize>) -> gcs_rust_fs::Result<Bytes> {
        Err(Error::new(ErrorKind::Unsupported, "write-only handle"))
    }

    async fn read_range(&self, _range: ByteRange) -> gcs_rust_fs::Result<Bytes> {
        Err(Error::new(ErrorKind::Unsupported, "write-only handle"))
    }

    async fn write(&mut self, data: Bytes) -> gcs_rust_fs::Result<()> {
        if self.closed {
            return Err(Error::new(ErrorKind::Closed, &self.path));
        }
        self.buf.extend_from_slice(&data);
        Ok(())
    }

    async fn flush(&mut self) -> gcs_rust_fs::Result<()> {
        if self.closed {
            return Err(Error::new(ErrorKind::Closed, &self.path));
        }
        Ok(())
    }

    async fn close(&mut self) -> gcs_rust_fs::Result<()> {
        if self.closed {
            return Ok(());
        }
        let write_mode = match self.mode {
            OpenMode::CreateNew => WriteMode::Create,
            _ => WriteMode::Overwrite,
        };
        let payload = self.buf.split().freeze();
        let stat = self.fs.commit_object(
            &self.bucket,
            &self.key,
            payload,
            write_mode,
            self.content_type.clone(),
            self.metadata.clone(),
        )?;
        self.published_stat = Some(stat);
        self.closed = true;
        Ok(())
    }

    async fn discard(&mut self) -> gcs_rust_fs::Result<()> {
        self.buf.clear();
        self.closed = true;
        Ok(())
    }
}

#[gcs_rust_fs::async_trait]
impl FileSystem for MockMemoryFs {
    async fn info(&self, path: &str) -> gcs_rust_fs::Result<Entry> {
        let (bucket, key, generation) = Self::parse_loc(path)?;
        if bucket.is_empty() {
            return Ok(Entry::directory(""));
        }
        self.ensure_bucket(&bucket)?;
        if key.is_empty() {
            return Ok(Entry::directory(bucket));
        }
        match self.get_versioned(&bucket, &key, generation) {
            Ok(v) => Ok(Entry::file(format!("{bucket}/{key}"), v.stat)),
            Err(e) if e.kind() == ErrorKind::IsADirectory => {
                Ok(Entry::directory(format!("{bucket}/{key}")))
            }
            Err(e) => Err(e),
        }
    }

    async fn ls(&self, path: &str, opts: ListOptions) -> gcs_rust_fs::Result<Vec<Entry>> {
        let (bucket, key, _) = Self::parse_loc(path)?;
        if bucket.is_empty() {
            let st = self.state.read();
            return Ok(st.buckets.iter().map(Entry::directory).collect());
        }
        self.ensure_bucket(&bucket)?;
        let prefix = if key.is_empty() {
            String::new()
        } else {
            format!("{key}/")
        };

        let mut files = Vec::new();
        let mut dirs = BTreeSet::new();
        {
            let st = self.state.read();
            for ((b, k), vers) in st.objects.range((bucket.clone(), prefix.clone())..) {
                if b != &bucket || !k.starts_with(&prefix) {
                    break;
                }
                if vers.is_empty() {
                    continue;
                }
                let rest = &k[prefix.len()..];
                if rest.is_empty() {
                    continue;
                }
                if let Some((child_dir, _)) = rest.split_once('/') {
                    dirs.insert(format!("{bucket}/{prefix}{child_dir}"));
                } else if opts.versions {
                    for v in vers {
                        files.push(Entry::file(
                            format!("{bucket}/{k}#{}", v.stat.generation),
                            v.stat.clone(),
                        ));
                    }
                } else if let Some(latest) = vers.last() {
                    files.push(Entry::file(format!("{bucket}/{k}"), latest.stat.clone()));
                }
            }
        }

        let mut out = files;
        out.extend(dirs.into_iter().map(Entry::directory));

        // Match GcsFs::ls exact behavior: if nothing exists under `key/`, fall back to `info()`
        // (returning `[file]` on a file path or `ErrorKind::NotFound` on a missing path).
        if out.is_empty() && !key.is_empty() {
            let entry = self.info(&format!("{bucket}/{key}")).await?;
            return Ok(if entry.is_file() {
                vec![entry]
            } else {
                Vec::new()
            });
        }

        out.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(out)
    }

    async fn find(&self, path: &str, opts: FindOptions) -> gcs_rust_fs::Result<Vec<Entry>> {
        let (bucket, key, _) = Self::parse_loc(path)?;
        self.ensure_bucket(&bucket)?;
        let prefix = if key.is_empty() {
            String::new()
        } else {
            format!("{key}/")
        };

        let mut out = Vec::new();
        {
            let st = self.state.read();
            for ((b, k), vers) in st.objects.range((bucket.clone(), prefix.clone())..) {
                if b != &bucket || !k.starts_with(&prefix) {
                    break;
                }
                if let Some(latest) = vers.last() {
                    out.push(Entry::file(format!("{bucket}/{k}"), latest.stat.clone()));
                }
            }
        }

        // Match GcsFs::find exact behavior: if nothing exists below `key/`, check `info()`
        // (returning `[file]` on a file path, or `[]` on a missing path).
        if out.is_empty() && !key.is_empty() {
            return match self.info(&format!("{bucket}/{key}")).await {
                Ok(e) if e.is_file() || opts.withdirs => Ok(vec![e]),
                Ok(_) => Ok(Vec::new()),
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(Vec::new()),
                Err(e) => Err(e),
            };
        }

        out.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(out)
    }

    async fn open(&self, path: &str, opts: OpenOptions) -> gcs_rust_fs::Result<Box<dyn File>> {
        let (bucket, key, generation) = Self::parse_loc(path)?;
        self.ensure_bucket(&bucket)?;
        if key.is_empty() {
            return Err(Error::new(ErrorKind::IsADirectory, bucket));
        }
        match opts.mode {
            OpenMode::Read => {
                let v = self.get_versioned(&bucket, &key, generation)?;
                Ok(Box::new(MockReadFile {
                    path: format!("{bucket}/{key}"),
                    data: v.data,
                    stat: v.stat,
                    pos: 0,
                    closed: false,
                    backend_reads: Arc::clone(&self.backend_reads),
                }))
            }
            OpenMode::Write | OpenMode::CreateNew => Ok(Box::new(MockWriteFile {
                fs: self.clone(),
                path: format!("{bucket}/{key}"),
                bucket,
                key,
                mode: opts.mode,
                content_type: opts.content_type,
                metadata: opts.metadata,
                buf: BytesMut::new(),
                closed: false,
                published_stat: None,
            })),
            OpenMode::Append => Err(Error::new(ErrorKind::Unsupported, "append unsupported")),
        }
    }

    async fn pipe_file(
        &self,
        path: &str,
        data: Bytes,
        opts: WriteOptions,
    ) -> gcs_rust_fs::Result<()> {
        let (bucket, key, _) = Self::parse_loc(path)?;
        self.commit_object(
            &bucket,
            &key,
            data,
            opts.mode,
            opts.content_type,
            opts.metadata,
        )?;
        Ok(())
    }

    async fn cat_file(
        &self,
        path: &str,
        range: ByteRange,
        _opts: ReadOptions,
    ) -> gcs_rust_fs::Result<Bytes> {
        let (bucket, key, generation) = Self::parse_loc(path)?;
        let v = self.get_versioned(&bucket, &key, generation)?;
        self.backend_reads.fetch_add(1, Ordering::SeqCst);
        let clamped = range.clamp(v.stat.size);
        if clamped.is_empty() {
            return Ok(Bytes::new());
        }
        Ok(v.data.slice(clamped.start as usize..clamped.end as usize))
    }

    async fn rm_file(&self, path: &str) -> gcs_rust_fs::Result<()> {
        let (bucket, key, _) = Self::parse_loc(path)?;
        self.ensure_bucket(&bucket)?;
        if key.is_empty() {
            return Err(Error::new(ErrorKind::IsADirectory, bucket));
        }
        let mut st = self.state.write();
        if st.objects.remove(&(bucket.clone(), key.clone())).is_some() {
            return Ok(());
        }
        if Self::has_children(&st, &bucket, &key) {
            return Err(Error::new(
                ErrorKind::IsADirectory,
                format!("{bucket}/{key}"),
            ));
        }
        Err(Error::new(ErrorKind::NotFound, format!("{bucket}/{key}")))
    }

    async fn copy_file(&self, src: &str, dst: &str) -> gcs_rust_fs::Result<()> {
        let (sb, sk, sg) = Self::parse_loc(src)?;
        let (db, dk, dg) = Self::parse_loc(dst)?;
        if dg.is_some() {
            return Err(Error::new(
                ErrorKind::InvalidPath,
                "cannot copy onto generation",
            ));
        }
        let v = self.get_versioned(&sb, &sk, sg)?;
        let ct = (!v.stat.content_type.is_empty()).then_some(v.stat.content_type);
        self.commit_object(&db, &dk, v.data, WriteMode::Overwrite, ct, v.stat.metadata)?;
        Ok(())
    }

    async fn move_file(&self, src: &str, dst: &str) -> gcs_rust_fs::Result<()> {
        if src == dst {
            return Ok(());
        }
        self.copy_file(src, dst).await?;
        self.rm_file(src).await
    }

    async fn mkdir(&self, path: &str, _opts: MkdirOptions) -> gcs_rust_fs::Result<()> {
        let (bucket, key, _) = Self::parse_loc(path)?;
        if !bucket.is_empty() && key.is_empty() {
            self.state.write().buckets.insert(bucket);
        }
        Ok(())
    }

    async fn rmdir(&self, _path: &str) -> gcs_rust_fs::Result<()> {
        Ok(())
    }
}
