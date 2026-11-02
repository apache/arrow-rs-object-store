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

//! Cloud Multipart Upload
//!
//! This crate provides an asynchronous interface for multipart file uploads to
//! cloud storage services. It's designed to offer efficient, non-blocking operations,
//! especially useful when dealing with large files or high-throughput systems.

use async_trait::async_trait;

use crate::path::Path;
use crate::{
    Error, Extensions, MultipartId, PutMode, PutMultipartOptions, PutPayload, PutResult, Result,
};

/// Represents a part of a file that has been successfully uploaded in a multipart upload process.
#[derive(Debug, Clone)]
pub struct PartId {
    /// Id of this part
    pub content_id: String,
}

/// Options for [`MultipartStore::complete_multipart_opts`]
#[derive(Debug, Clone, Default)]
pub struct CompleteMultipartOptions {
    /// Configure the [`PutMode`] for this completion
    ///
    /// [`PutMode::Create`] fails with [`Error::AlreadyExists`] if an object already
    /// exists at the path.
    pub mode: PutMode,
    /// Implementation-specific extensions. Intended for use by [`MultipartStore`] implementations
    /// that need to pass context-specific information (like tracing spans) via trait methods.
    ///
    /// These extensions are ignored entirely by backends offered through this crate.
    ///
    /// They are also excluded from [`PartialEq`] and [`Eq`].
    pub extensions: Extensions,
}

impl PartialEq<Self> for CompleteMultipartOptions {
    fn eq(&self, other: &Self) -> bool {
        let Self {
            mode,
            extensions: _,
        } = self;
        let Self {
            mode: other_mode,
            extensions: _,
        } = other;
        mode == other_mode
    }
}

impl Eq for CompleteMultipartOptions {}

impl From<PutMode> for CompleteMultipartOptions {
    fn from(mode: PutMode) -> Self {
        Self {
            mode,
            ..Default::default()
        }
    }
}

/// A low-level interface for interacting with multipart upload APIs
///
/// Most use-cases should prefer [`ObjectStore::put_multipart_opts`] as this is supported by more
/// backends, including [`LocalFileSystem`], and automatically handles uploading fixed
/// size parts of sufficient size in parallel
///
/// See [`AmazonS3::create_multipart`] for a full example of this API.
///
/// [`ObjectStore::put_multipart_opts`]: crate::ObjectStore::put_multipart_opts
/// [`LocalFileSystem`]: crate::local::LocalFileSystem
/// [`AmazonS3::create_multipart`]: crate::aws::AmazonS3::create_multipart
#[async_trait]
pub trait MultipartStore: Send + Sync + 'static {
    /// Creates a new multipart upload, returning the [`MultipartId`]
    ///
    /// See [`AmazonS3::create_multipart`] for a full example of this API.
    ///
    /// [`AmazonS3::create_multipart`]: crate::aws::AmazonS3::create_multipart
    async fn create_multipart(&self, path: &Path) -> Result<MultipartId>;

    /// Creates a new multipart upload with the given options, returning the [`MultipartId`]
    ///
    /// This allows callers using the low-level multipart API to provide object attributes,
    /// tags, or implementation-specific extensions when initiating the upload.
    ///
    /// See [`AmazonS3::create_multipart`] for a full example of this API.
    ///
    /// [`AmazonS3::create_multipart`]: crate::aws::AmazonS3::create_multipart
    async fn create_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> Result<MultipartId> {
        let PutMultipartOptions {
            tags,
            attributes,
            extensions: _,
        } = opts;

        if !tags.is_empty() || !attributes.is_empty() {
            return Err(Error::NotSupported {
                source: "create_multipart_opts with non-default options".into(),
            });
        }

        self.create_multipart(path).await
    }

    /// Uploads a new part with index `part_idx`
    ///
    /// `part_idx` should be an integer in the range `0..N` where `N` is the number of
    /// parts in the upload. Parts may be uploaded concurrently and in any order.
    ///
    /// Most stores require that all parts excluding the last are at least 5 MiB, and some
    /// further require that all parts excluding the last be the same size, e.g. [R2].
    /// [`WriteMultipart`] performs writes in fixed size blocks of 5 MiB, and clients wanting
    /// to maximise compatibility should look to do likewise.
    ///
    /// [R2]: https://developers.cloudflare.com/r2/objects/multipart-objects/#limitations
    /// [`WriteMultipart`]: crate::upload::WriteMultipart
    async fn put_part(
        &self,
        path: &Path,
        id: &MultipartId,
        part_idx: usize,
        data: PutPayload,
    ) -> Result<PartId>;

    /// Completes a multipart upload
    ///
    /// The `i`'th value of `parts` must be a [`PartId`] returned by a call to [`Self::put_part`]
    /// with a `part_idx` of `i`, and the same `path` and `id` as provided to this method. Calling
    /// this method with out of sequence or repeated [`PartId`], or [`PartId`] returned for other
    /// values of `path` or `id`, will result in implementation-defined behaviour
    ///
    /// This overwrites any object at `path`. See [`Self::complete_multipart_opts`] to complete
    /// the upload only if no object exists.
    async fn complete_multipart(
        &self,
        path: &Path,
        id: &MultipartId,
        parts: Vec<PartId>,
    ) -> Result<PutResult>;

    /// Completes a multipart upload with the given options
    ///
    /// The same requirements on `parts` apply as for [`Self::complete_multipart`].
    ///
    /// With [`PutMode::Create`], the completion fails with [`Error::AlreadyExists`] if an
    /// object already exists at `path`. The check and the write are one atomic operation. If
    /// the request fails without a response, the object may or may not have been created.
    ///
    /// Implementations that cannot enforce `opts.mode` return an error. The default
    /// implementation supports only [`PutMode::Overwrite`], and returns
    /// [`Error::NotSupported`] for any other mode.
    async fn complete_multipart_opts(
        &self,
        path: &Path,
        id: &MultipartId,
        parts: Vec<PartId>,
        opts: CompleteMultipartOptions,
    ) -> Result<PutResult> {
        let CompleteMultipartOptions {
            mode,
            extensions: _,
        } = opts;

        // An implementation that does not override this method cannot check for an existing
        // object, so it must not fall back to an unconditional completion.
        match mode {
            PutMode::Overwrite => self.complete_multipart(path, id, parts).await,
            PutMode::Create | PutMode::Update(_) => Err(Error::NotSupported {
                source: "complete_multipart_opts with a mode other than PutMode::Overwrite".into(),
            }),
        }
    }

    /// Aborts a multipart upload
    async fn abort_multipart(&self, path: &Path, id: &MultipartId) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestMultipartStore;

    #[async_trait]
    impl MultipartStore for TestMultipartStore {
        async fn create_multipart(&self, _path: &Path) -> Result<MultipartId> {
            Ok("test".into())
        }

        async fn put_part(
            &self,
            _path: &Path,
            _id: &MultipartId,
            _part_idx: usize,
            _data: PutPayload,
        ) -> Result<PartId> {
            unreachable!()
        }

        async fn complete_multipart(
            &self,
            _path: &Path,
            _id: &MultipartId,
            _parts: Vec<PartId>,
        ) -> Result<PutResult> {
            Ok(PutResult {
                e_tag: Some("test".into()),
                version: None,
                extensions: Default::default(),
            })
        }

        async fn abort_multipart(&self, _path: &Path, _id: &MultipartId) -> Result<()> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn default_complete_multipart_opts_overwrites() {
        let result = TestMultipartStore
            .complete_multipart_opts(
                &Path::from("test"),
                &"test".into(),
                vec![],
                PutMode::Overwrite.into(),
            )
            .await
            .unwrap();

        assert_eq!(result.e_tag.as_deref(), Some("test"));
    }

    #[tokio::test]
    async fn default_complete_multipart_opts_rejects_create() {
        // The default must fail closed: an implementation that cannot check for an
        // existing object must not fall back to an overwrite.
        let err = TestMultipartStore
            .complete_multipart_opts(
                &Path::from("test"),
                &"test".into(),
                vec![],
                PutMode::Create.into(),
            )
            .await
            .unwrap_err();

        assert!(matches!(err, Error::NotSupported { .. }), "{err}");
    }

    #[tokio::test]
    async fn default_create_multipart_opts_ignores_extensions() {
        let mut extensions = Extensions::new();
        extensions.insert("extension");
        let opts = PutMultipartOptions {
            extensions,
            ..Default::default()
        };

        let id = TestMultipartStore
            .create_multipart_opts(&Path::from("test"), opts)
            .await
            .unwrap();

        assert_eq!(id, "test");
    }
}
