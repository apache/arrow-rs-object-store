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

//! An object store that traces calls to the wrapped implementation.
use crate::{
    path::Path, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart,
};
use async_trait::async_trait;
use futures::stream::BoxStream;
use tracing::instrument;

/// An [`ObjectStore`] wrapper that traces operations made to the wrapped store.
#[derive(Debug)]
pub struct TracingStore<T: ObjectStore> {
    store: T,
    prefix: String,
    path_prefix: String,
}

impl<T: ObjectStore> TracingStore<T> {
    /// Create a new tracing store by wrapping an inner store.
    #[must_use]
    pub fn new(inner: T, prefix: impl Into<String>, path_prefix: impl Into<String>) -> Self {
        Self {
            store: inner,
            prefix: prefix.into(),
            path_prefix: path_prefix.into(),
        }
    }
}

impl<T: ObjectStore> std::fmt::Display for TracingStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TracingStore \"{}\" path prefix: \"{}\" ({})",
            self.prefix, self.path_prefix, self.store
        )
    }
}

#[async_trait]
impl<T: ObjectStore> ObjectStore for TracingStore<T> {
    #[instrument(level = "debug", skip_all, fields(store = self.prefix, location, range))]
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        tracing::Span::current().record("location", format!("{}/{}", self.path_prefix, location));
        let range = if options.head {
            "N/A: HEAD only request".to_owned()
        } else {
            match &options.range {
                Some(GetRange::Bounded(get_range)) => {
                    let len = get_range
                        .end
                        .checked_sub(get_range.start)
                        .expect("Get range length is negative");
                    format!(
                        "bytes {} to {}, len {}",
                        get_range.start, get_range.end, len
                    )
                }
                Some(GetRange::Offset(start_pos)) => {
                    format!("byte {start_pos} to EOF")
                }
                Some(GetRange::Suffix(pos)) => {
                    format!("last {pos} bytes of object")
                }
                None => "complete file range".to_owned(),
            }
        };
        tracing::Span::current().record("range", &range);
        self.store.get_opts(location, options).await
    }

    #[instrument(level = "debug", skip_all, fields(store = self.prefix, location))]
    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        tracing::Span::current().record("location", format!("{}/{}", self.path_prefix, location));
        self.store.head(location).await
    }

    #[instrument(level = "debug", skip_all, fields(store = self.prefix, location))]
    async fn delete(&self, location: &Path) -> Result<()> {
        tracing::Span::current().record("location", format!("{}/{}", self.path_prefix, location));
        self.store.delete(location).await
    }

    #[instrument(level = "debug", skip_all, fields(store = self.prefix, prefix))]
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        tracing::Span::current().record(
            "prefix",
            format!(
                "{}/{}",
                self.path_prefix,
                prefix.unwrap_or(&Path::default())
            ),
        );
        self.store.list(prefix)
    }

    #[instrument(level = "debug", skip_all, fields(store = self.prefix, prefix))]
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        tracing::Span::current().record(
            "prefix",
            format!(
                "{}/{}",
                self.path_prefix,
                prefix.unwrap_or(&Path::default())
            ),
        );
        self.store.list_with_delimiter(prefix).await
    }

    #[instrument(level = "debug", skip_all, fields(store = self.prefix, from, to))]
    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        tracing::Span::current().record("from", format!("{}/{}", self.path_prefix, from));
        tracing::Span::current().record("to", format!("{}/{}", self.path_prefix, to));
        self.store.copy(from, to).await
    }

    #[instrument(level = "debug", skip_all, fields(store = self.prefix, from, to))]
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        tracing::Span::current().record("from", format!("{}/{}", self.path_prefix, from));
        tracing::Span::current().record("to", format!("{}/{}", self.path_prefix, to));
        self.store.copy_if_not_exists(from, to).await
    }

    #[instrument(level = "debug", skip_all, fields(store = self.prefix, location, length))]
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        tracing::Span::current().record("location", format!("{}/{}", self.path_prefix, location));
        tracing::Span::current().record("length", payload.content_length());
        self.store.put_opts(location, payload, opts).await
    }

    #[instrument(level = "debug", skip_all, fields(store = self.prefix, location))]
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        tracing::Span::current().record("location", format!("{}/{}", self.path_prefix, location));
        let part_upload = self.store.put_multipart_opts(location, opts).await?;
        Ok(Box::new(TracingMultipartUpload::new(
            part_upload,
            &self.prefix,
            format!("{}/{}", self.path_prefix, location),
        )) as Box<dyn MultipartUpload>)
    }
}

#[derive(Debug)]
struct TracingMultipartUpload {
    inner: Box<dyn MultipartUpload>,
    prefix: String,
    path: String,
}

impl TracingMultipartUpload {
    fn new(
        inner: Box<dyn MultipartUpload>,
        prefix: impl Into<String>,
        path: impl Into<String>,
    ) -> Self {
        Self {
            inner,
            prefix: prefix.into(),
            path: path.into(),
        }
    }
}

#[async_trait]
impl MultipartUpload for TracingMultipartUpload {
    #[instrument(level = "debug", skip_all, fields(store = self.prefix, location = self.path, length))]
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        tracing::Span::current().record("length", data.content_length());
        self.inner.put_part(data)
    }

    #[instrument(level = "debug", skip_all, fields(store = self.prefix, location = self.path))]
    async fn complete(&mut self) -> Result<PutResult> {
        self.inner.complete().await
    }

    #[instrument(level = "debug", skip_all, fields(store = self.prefix, location = self.path))]
    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

#[cfg(test)]
mod tests {
    use crate::{integration::*, memory::InMemory, trace::TracingStore};

    #[tokio::test]
    async fn log_test() {
        let integration = make_store();

        put_get_delete_list(&integration).await;
        get_opts(&integration).await;
        list_uses_directories_correctly(&integration).await;
        list_with_delimiter(&integration).await;
        rename_and_copy(&integration).await;
        copy_if_not_exists(&integration).await;
        stream_get(&integration).await;
        put_opts(&integration, true).await;
        put_get_attributes(&integration).await;
    }

    fn make_store() -> TracingStore<InMemory> {
        let inner = InMemory::new();
        TracingStore::new(inner, "TEST", "memory:/")
    }
}
