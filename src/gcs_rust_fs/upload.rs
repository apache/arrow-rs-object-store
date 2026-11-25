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

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use gcs_rust_fs::{File, FileSystem, OpenOptions, WriteOptions};
use tokio::sync::Mutex;

use super::{STORE, entry_must_be_file, map_error};
use crate::path::Path;
use crate::{Error, MultipartUpload, PutPayload, PutResult, Result, UploadPart};

struct MultipartInner {
    fs: Arc<dyn FileSystem>,
    location: Path,
    full_path: String,
    write_opts: WriteOptions,
    file: Option<Box<dyn File>>,
    next_idx: usize,
    pending: BTreeMap<usize, PutPayload>,
    aborted: bool,
    completed: bool,
}

impl MultipartInner {
    async fn drain_contiguous(&mut self) -> Result<()> {
        while let Some(payload) = self.pending.remove(&self.next_idx) {
            if self.file.is_none() {
                let open_opts = OpenOptions::from_write(self.write_opts.clone());
                let opened = self
                    .fs
                    .open(&self.full_path, open_opts)
                    .await
                    .map_err(|e| map_error(e, self.location.as_ref()))?;
                self.file = Some(opened);
            }
            let file = self.file.as_mut().expect("file just initialized");
            for chunk in payload {
                if !chunk.is_empty() {
                    file.write(chunk)
                        .await
                        .map_err(|e| map_error(e, self.location.as_ref()))?;
                }
            }
            self.next_idx += 1;
        }
        Ok(())
    }
}

pub(crate) struct GcsRustFsMultipartUpload {
    part_idx: usize,
    state: Arc<Mutex<MultipartInner>>,
}

impl std::fmt::Debug for GcsRustFsMultipartUpload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsRustFsMultipartUpload")
            .field("part_idx", &self.part_idx)
            .finish_non_exhaustive()
    }
}

impl GcsRustFsMultipartUpload {
    pub(crate) fn new(
        fs: Arc<dyn FileSystem>,
        location: Path,
        full_path: String,
        write_opts: WriteOptions,
    ) -> Self {
        Self {
            part_idx: 0,
            state: Arc::new(Mutex::new(MultipartInner {
                fs,
                location,
                full_path,
                write_opts,
                file: None,
                next_idx: 0,
                pending: BTreeMap::new(),
                aborted: false,
                completed: false,
            })),
        }
    }
}

#[async_trait]
impl MultipartUpload for GcsRustFsMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let idx = self.part_idx;
        self.part_idx += 1;
        let state = Arc::clone(&self.state);

        Box::pin(async move {
            let mut guard = state.lock().await;
            if guard.aborted {
                return Err(Error::Generic {
                    store: STORE,
                    source: "multipart upload has already been aborted".into(),
                });
            }
            if guard.completed {
                return Err(Error::Generic {
                    store: STORE,
                    source: "multipart upload has already been completed".into(),
                });
            }
            guard.pending.insert(idx, data);
            guard.drain_contiguous().await
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let mut guard = self.state.lock().await;
        if guard.aborted {
            return Err(Error::Generic {
                store: STORE,
                source: "cannot complete an aborted multipart upload".into(),
            });
        }
        if guard.completed {
            return Err(Error::Generic {
                store: STORE,
                source: "multipart upload has already been completed".into(),
            });
        }

        guard.drain_contiguous().await?;

        if guard.next_idx != self.part_idx || !guard.pending.is_empty() {
            return Err(Error::Generic {
                store: STORE,
                source: format!(
                    "multipart complete called before all {} parts finished (completed contiguous prefix: {})",
                    self.part_idx, guard.next_idx
                )
                .into(),
            });
        }

        let (e_tag, version) = if let Some(mut file) = guard.file.take() {
            file.close()
                .await
                .map_err(|e| map_error(e, guard.location.as_ref()))?;
            match file.stat() {
                Some(stat) => (
                    (!stat.etag.is_empty()).then(|| stat.etag.clone()),
                    Some(stat.generation.to_string()),
                ),
                None => {
                    let entry = guard
                        .fs
                        .info(&guard.full_path)
                        .await
                        .map_err(|e| map_error(e, guard.location.as_ref()))?;
                    let (meta, _) = entry_must_be_file(entry, &guard.location)?;
                    (meta.e_tag, meta.version)
                }
            }
        } else {
            guard
                .fs
                .pipe_file(&guard.full_path, Bytes::new(), guard.write_opts.clone())
                .await
                .map_err(|e| map_error(e, guard.location.as_ref()))?;
            let entry = guard
                .fs
                .info(&guard.full_path)
                .await
                .map_err(|e| map_error(e, guard.location.as_ref()))?;
            let (meta, _) = entry_must_be_file(entry, &guard.location)?;
            (meta.e_tag, meta.version)
        };

        guard.completed = true;
        Ok(PutResult {
            e_tag,
            version,
            extensions: Default::default(),
        })
    }

    async fn abort(&mut self) -> Result<()> {
        let mut guard = self.state.lock().await;
        if guard.completed {
            return Err(Error::Generic {
                store: STORE,
                source: "cannot abort an already completed multipart upload".into(),
            });
        }
        guard.aborted = true;
        guard.pending.clear();
        if let Some(mut file) = guard.file.take() {
            file.discard()
                .await
                .map_err(|e| map_error(e, guard.location.as_ref()))?;
        }
        Ok(())
    }
}
