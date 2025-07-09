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

//! An object store that logs calls to the wrapped implementation.
use crate::{
    path::Path, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart,
};
use async_trait::async_trait;
use futures::stream::BoxStream;
use log::debug;

/// An [`ObjectStore`] wrapper that logs operations made to the wrapped store. The logs are written using the ['log'] crate.
/// 
/// Logs are written at the "debug" logging level.
#[derive(Debug)]
pub struct LoggingStore<T: ObjectStore> {
    store: T,
    prefix: String,
    path_prefix: String,
}

impl<T: ObjectStore> LoggingStore<T> {
    /// Create a new logging store by wrapping an inner store.
    #[must_use]
    pub fn new(inner: T, prefix: impl Into<String>, path_prefix: impl Into<String>) -> Self {
        Self {
            store: inner,
            prefix: prefix.into(),
            path_prefix: path_prefix.into(),
        }
    }
}

impl<T: ObjectStore> std::fmt::Display for LoggingStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LoggingStore \"{}\" path prefix: \"{}\" ({})",
            self.prefix, self.path_prefix, self.store
        )
    }
}

#[async_trait]
impl<T: ObjectStore> ObjectStore for LoggingStore<T> {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if !options.head {
            match &options.range {
                Some(GetRange::Bounded(get_range)) => {
                    let len = get_range
                        .end
                        .checked_sub(get_range.start)
                        .expect("Get range length is negative");
                    debug!(
                        "{} get request for {}/{} byte range {} to {} = {} bytes",
                        self.prefix,
                        self.path_prefix,
                        location,
                        get_range.start,
                        get_range.end,
                        len,
                    );
                }
                Some(GetRange::Offset(start_pos)) => {
                    debug!(
                        "{} get request for {}/{} for byte {} to EOF",
                        self.prefix, self.path_prefix, location, start_pos,
                    );
                }
                Some(GetRange::Suffix(pos)) => {
                    debug!(
                        "{} get request for {}/{} for last {} bytes of object",
                        self.prefix, self.path_prefix, location, pos,
                    );
                }
                None => {
                    debug!(
                        "{} get request for {}/{} for complete file range",
                        self.prefix, self.path_prefix, location
                    );
                }
            }
        }
        self.store.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        debug!(
            "{} head request for {}/{}",
            self.prefix, self.path_prefix, location
        );
        self.store.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        debug!(
            "{} delete request for {}/{}",
            self.prefix, self.path_prefix, location
        );
        self.store.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        debug!(
            "{} list request for {}/{}",
            self.prefix,
            self.path_prefix,
            prefix.unwrap_or(&Path::default())
        );
        self.store.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        debug!(
            "{} list_with_delimeter request for {}/{}",
            self.prefix,
            self.path_prefix,
            prefix.unwrap_or(&Path::default())
        );
        self.store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        debug!(
            "{} copy request from {}/{} to {}/{}",
            self.prefix, self.path_prefix, from, self.path_prefix, to
        );
        self.store.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        debug!(
            "{} copy_if_not_exists request from {}/{} to {}/{}",
            self.prefix, self.path_prefix, from, self.path_prefix, to
        );
        self.store.copy_if_not_exists(from, to).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        debug!(
            "{} put request for {}/{} of {} bytes",
            self.prefix,
            self.path_prefix,
            location,
            payload.content_length()
        );
        self.store.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        debug!(
            "{} put multipart request for {}/{}",
            self.prefix, self.path_prefix, location
        );
        let part_upload = self.store.put_multipart_opts(location, opts).await?;
        Ok(Box::new(LoggingMultipartUpload::new(
            part_upload,
            &self.prefix,
            format!("{}/{}", self.path_prefix, location),
        )) as Box<dyn MultipartUpload>)
    }
}

#[derive(Debug)]
struct LoggingMultipartUpload {
    inner: Box<dyn MultipartUpload>,
    prefix: String,
    path: String,
}

impl LoggingMultipartUpload {
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
impl MultipartUpload for LoggingMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        debug!(
            "{} put_part request for {} of {} bytes",
            self.prefix,
            self.path,
            data.content_length()
        );
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> Result<PutResult> {
        debug!("multipart complete for {}", self.path);
        self.inner.complete().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        integration::*, logging::LoggingStore, memory::InMemory, GetOptions, GetRange, ObjectStore,
        PutOptions, Result,
    };
    use log::Level;

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

    fn make_store() -> LoggingStore<InMemory> {
        let inner = InMemory::new();
        LoggingStore::new(inner, "TEST", "memory:/")
    }

    #[tokio::test]
    async fn zero_log() {
        // Given
        testing_logger::setup();
        let _store = make_store();

        // When
        // no-op

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 0);
        });
    }

    #[tokio::test]
    async fn ranged_get_log() -> Result<()> {
        // Given
        testing_logger::setup();
        let store = make_store();
        store.put(&"test_file".into(), "some_data".into()).await?;

        // When
        store.get_range(&"test_file".into(), 1..5).await?;

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 2);
            assert_eq!(
                captured_logs[1].body,
                "TEST get request for memory://test_file byte range 1 to 5 = 4 bytes"
            );
            assert_eq!(captured_logs[1].level, Level::Debug);
        });

        Ok(())
    }

    #[tokio::test]
    async fn offset_get_log() -> Result<()> {
        // Given
        testing_logger::setup();
        let store = make_store();
        store.put(&"test_file".into(), "some_data".into()).await?;

        // When
        let opts = GetOptions {
            range: Some(GetRange::Offset(3)),
            ..Default::default()
        };
        store.get_opts(&"test_file".into(), opts).await?;

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 2);
            assert_eq!(
                captured_logs[1].body,
                "TEST get request for memory://test_file for byte 3 to EOF"
            );
            assert_eq!(captured_logs[1].level, Level::Debug);
        });

        Ok(())
    }

    #[tokio::test]
    async fn suffix_get_log() -> Result<()> {
        // Given
        testing_logger::setup();
        let store = make_store();
        store.put(&"test_file".into(), "some_data".into()).await?;

        // When
        let opts = GetOptions {
            range: Some(GetRange::Suffix(3)),
            ..Default::default()
        };
        store.get_opts(&"test_file".into(), opts).await?;

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 2);
            assert_eq!(
                captured_logs[1].body,
                "TEST get request for memory://test_file for last 3 bytes of object"
            );
            assert_eq!(captured_logs[1].level, Level::Debug);
        });

        Ok(())
    }

    #[tokio::test]
    async fn no_range_get_log() -> Result<()> {
        // Given
        testing_logger::setup();
        let store = make_store();
        store.put(&"test_file".into(), "some_data".into()).await?;

        // When
        let opts = GetOptions::default();
        store.get_opts(&"test_file".into(), opts).await?;

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 2);
            assert_eq!(
                captured_logs[1].body,
                "TEST get request for memory://test_file for complete file range"
            );
            assert_eq!(captured_logs[1].level, Level::Debug);
        });

        Ok(())
    }

    #[tokio::test]
    async fn head_log() -> Result<()> {
        // Given
        testing_logger::setup();
        let store = make_store();
        store.put(&"test_file".into(), "some_data".into()).await?;

        // When
        store.head(&"test_file".into()).await?;

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 2);
            assert_eq!(
                captured_logs[1].body,
                "TEST head request for memory://test_file"
            );
            assert_eq!(captured_logs[1].level, Level::Debug);
        });

        Ok(())
    }

    #[tokio::test]
    async fn delete_log() -> Result<()> {
        // Given
        testing_logger::setup();
        let store = make_store();
        store.put(&"test_file".into(), "some_data".into()).await?;

        // When
        store.delete(&"test_file".into()).await?;

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 2);
            assert_eq!(
                captured_logs[1].body,
                "TEST delete request for memory://test_file"
            );
            assert_eq!(captured_logs[1].level, Level::Debug);
        });

        Ok(())
    }

    #[tokio::test]
    async fn list_log() -> Result<()> {
        // Given
        testing_logger::setup();
        let store = make_store();

        // When
        #[allow(unused_must_use)]
        store.list(Some(&"foo".into()));

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 1);
            assert_eq!(captured_logs[0].body, "TEST list request for memory://foo");
            assert_eq!(captured_logs[0].level, Level::Debug);
        });

        Ok(())
    }

    #[tokio::test]
    async fn list_with_delimeter_log() -> Result<()> {
        // Given
        testing_logger::setup();
        let store = make_store();

        // When
        #[allow(unused_must_use)]
        store.list_with_delimiter(Some(&"foo".into())).await?;

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 1);
            assert_eq!(
                captured_logs[0].body,
                "TEST list_with_delimeter request for memory://foo"
            );
            assert_eq!(captured_logs[0].level, Level::Debug);
        });

        Ok(())
    }

    #[tokio::test]
    async fn list_path_none_log() -> Result<()> {
        // Given
        testing_logger::setup();
        let store = make_store();

        // When
        #[allow(unused_must_use)]
        store.list(None);

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 1);
            assert_eq!(captured_logs[0].body, "TEST list request for memory://");
            assert_eq!(captured_logs[0].level, Level::Debug);
        });

        Ok(())
    }

    #[tokio::test]
    async fn list_with_delimeter_path_none_log() -> Result<()> {
        // Given
        testing_logger::setup();
        let store = make_store();

        // When
        #[allow(unused_must_use)]
        store.list_with_delimiter(None).await?;

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 1);
            assert_eq!(
                captured_logs[0].body,
                "TEST list_with_delimeter request for memory://"
            );
            assert_eq!(captured_logs[0].level, Level::Debug);
        });

        Ok(())
    }

    #[tokio::test]
    async fn copy_log() -> Result<()> {
        // Given
        testing_logger::setup();
        let store = make_store();
        store.put(&"test_file".into(), "some_data".into()).await?;

        // When
        store
            .copy(&"test_file".into(), &"test_file2".into())
            .await?;

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 2);
            assert_eq!(
                captured_logs[1].body,
                "TEST copy request from memory://test_file to memory://test_file2"
            );
            assert_eq!(captured_logs[1].level, Level::Debug);
        });

        Ok(())
    }

    #[tokio::test]
    async fn copy_if_not_exists_log() -> Result<()> {
        // Given
        testing_logger::setup();
        let store = make_store();
        store.put(&"test_file".into(), "some_data".into()).await?;

        // When
        store
            .copy_if_not_exists(&"test_file".into(), &"test_file2".into())
            .await?;

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 2);
            assert_eq!(
                captured_logs[1].body,
                "TEST copy_if_not_exists request from memory://test_file to memory://test_file2"
            );
            assert_eq!(captured_logs[1].level, Level::Debug);
        });

        Ok(())
    }

    #[tokio::test]
    async fn put_log() -> Result<()> {
        // Given
        testing_logger::setup();
        let store = make_store();

        // When
        store
            .put_opts(&"test_file".into(), "foo".into(), PutOptions::default())
            .await?;

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 1);
            assert_eq!(
                captured_logs[0].body,
                "TEST put request for memory://test_file of 3 bytes"
            );
            assert_eq!(captured_logs[0].level, Level::Debug);
        });

        Ok(())
    }

    #[tokio::test]
    async fn put_multipart_log() -> Result<()> {
        // Given
        testing_logger::setup();
        let store = make_store();

        // When
        let mut part = store.put_multipart(&"test_file".into()).await?;
        part.put_part("foo".into()).await?;
        part.put_part("foo1".into()).await?;
        part.put_part("foo12".into()).await?;
        part.complete().await?;

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 5);
            assert_eq!(
                captured_logs[0].body,
                "TEST put multipart request for memory://test_file"
            );
            assert_eq!(captured_logs[0].level, Level::Debug);
            assert_eq!(
                captured_logs[1].body,
                "TEST put_part request for memory://test_file of 3 bytes"
            );
            assert_eq!(captured_logs[1].level, Level::Debug);
            assert_eq!(
                captured_logs[2].body,
                "TEST put_part request for memory://test_file of 4 bytes"
            );
            assert_eq!(captured_logs[2].level, Level::Debug);
            assert_eq!(
                captured_logs[3].body,
                "TEST put_part request for memory://test_file of 5 bytes"
            );
            assert_eq!(captured_logs[3].level, Level::Debug);
            assert_eq!(
                captured_logs[4].body,
                "multipart complete for memory://test_file"
            );
            assert_eq!(captured_logs[4].level, Level::Debug);
        });

        let retrieved_data = String::from_utf8(
            store
                .get(&"test_file".into())
                .await?
                .bytes()
                .await?
                .to_vec(),
        )
        .expect("String should be valid UTF-8");
        assert_eq!(retrieved_data, "foofoo1foo12");
        Ok(())
    }
}
