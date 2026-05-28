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

//! An object store implementation for Cloudflare R2 using the REST API
//!
//! Cloudflare R2 is an object storage service that provides three interfaces:
//!
//! 1. **S3-compatible API** – Works with the existing [`AmazonS3`](crate::aws::AmazonS3) backend
//! 2. **Cloudflare REST API** – This module (uses `https://api.cloudflare.com/client/v4/`)
//! 3. **Workers Bindings** – For use within Cloudflare Workers runtime
//!
//! This module implements interface #2 (the native Cloudflare REST API), which uses
//! Bearer token authentication with a Cloudflare API token.
//!
//! ## Example
//!
//! ```no_run
//! # use object_store::cloudflare::CloudflareR2Builder;
//! # use object_store::ObjectStore;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let r2 = CloudflareR2Builder::new()
//!     .with_account_id("my-account-id")
//!     .with_bucket_name("my-bucket")
//!     .with_api_token("my-api-token")
//!     .build()?;
//!
//! // Use like any other ObjectStore
//! let path = object_store::path::Path::from("hello.txt");
//! r2.put_opts(&path, "hello world".into(), Default::default()).await?;
//! let result = r2.get_opts(&path, Default::default()).await?;
//! let bytes = result.bytes().await?;
//! assert_eq!(&bytes[..], b"hello world");
//! # Ok(())
//! # }
//! ```
//!
//! ## Multipart uploads
//!
//! Multipart uploads can be initiated with the [`ObjectStore::put_multipart_opts`] method.
//! R2 supports up to 10,000 parts per multipart upload, with each part being at most 5 GB.
//! The minimum part size is 5 MB (except for the last part).
//!
//! ## Configuration via environment variables
//!
//! The builder supports reading configuration from environment variables:
//!
//! | Variable | Description |
//! |----------|-------------|
//! | `CLOUDFLARE_ACCOUNT_ID` or `CF_ACCOUNT_ID` | Cloudflare account ID |
//! | `CLOUDFLARE_R2_BUCKET` or `CF_R2_BUCKET` | R2 bucket name |
//! | `CLOUDFLARE_API_TOKEN` or `CF_API_TOKEN` | Cloudflare API token |
//! | `CLOUDFLARE_API_BASE_URL` | Custom API base URL (for testing) |

use crate::client::CredentialProvider;
use crate::client::get::GetClientExt;
use crate::client::list::{ListClient, ListClientExt};
use crate::client::parts::Parts;
use crate::list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore};
use crate::multipart::{MultipartStore, PartId};
use crate::path::Path;
use crate::{
    CopyMode, CopyOptions, GetOptions, GetResult, ListResult, MultipartId, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    UploadPart,
};
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use std::sync::Arc;

use client::CloudflareClient;
pub use builder::{CloudflareConfigKey, CloudflareR2Builder};
pub use credential::CloudflareCredential;

mod builder;
pub(crate) mod client;
pub(crate) mod credential;

const STORE: &str = "CloudflareR2";

/// [`CredentialProvider`] for [`CloudflareR2`]
pub type CloudflareCredentialProvider = Arc<dyn CredentialProvider<Credential = CloudflareCredential>>;

/// Interface for [Cloudflare R2](https://developers.cloudflare.com/r2/) using the REST API.
#[derive(Debug)]
pub struct CloudflareR2 {
    client: Arc<CloudflareClient>,
}

impl CloudflareR2 {
    /// Returns the [`CloudflareCredentialProvider`] used by [`CloudflareR2`]
    pub fn credentials(&self) -> &CloudflareCredentialProvider {
        &self.client.config().credentials
    }
}

impl std::fmt::Display for CloudflareR2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CloudflareR2(account: {}, bucket: {})",
            self.client.config().account_id,
            self.client.config().bucket_name
        )
    }
}

#[derive(Debug)]
struct CloudflareMultipartUpload {
    state: Arc<UploadState>,
    part_idx: usize,
}

#[derive(Debug)]
struct UploadState {
    client: Arc<CloudflareClient>,
    path: Path,
    multipart_id: MultipartId,
    parts: Parts,
}

#[async_trait]
impl MultipartUpload for CloudflareMultipartUpload {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        let idx = self.part_idx;
        self.part_idx += 1;
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let part = state
                .client
                .put_part(&state.path, &state.multipart_id, idx + 1, payload)
                .await?;
            state.parts.put(idx, part);
            Ok(())
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let parts = self.state.parts.finish(self.part_idx)?;
        self.state
            .client
            .complete_multipart(&self.state.path, &self.state.multipart_id, parts)
            .await
    }

    async fn abort(&mut self) -> Result<()> {
        self.state
            .client
            .abort_multipart(&self.state.path, &self.state.multipart_id)
            .await
    }
}

#[async_trait]
impl ObjectStore for CloudflareR2 {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.client.put_object(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let upload_id = self.client.create_multipart(location, opts).await?;
        Ok(Box::new(CloudflareMultipartUpload {
            state: Arc::new(UploadState {
                client: Arc::clone(&self.client),
                path: location.clone(),
                multipart_id: upload_id,
                parts: Default::default(),
            }),
            part_idx: 0,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.client.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let client = Arc::clone(&self.client);
        locations
            .map(move |location| {
                let client = Arc::clone(&client);
                async move {
                    let location = location?;
                    client.delete_object(&location).await?;
                    Ok(location)
                }
            })
            .buffered(10)
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.client.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.client.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.client.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        match options.mode {
            CopyMode::Overwrite => self.client.copy_object(from, to).await,
            CopyMode::Create => {
                // R2 does not support copy_if_not_exists natively via the REST API
                Err(crate::Error::NotImplemented {
                    operation: "copy_if_not_exists".into(),
                    implementer: self.to_string(),
                })
            }
        }
    }
}

#[async_trait]
impl MultipartStore for CloudflareR2 {
    async fn create_multipart(&self, path: &Path) -> Result<MultipartId> {
        self.client
            .create_multipart(path, PutMultipartOptions::default())
            .await
    }

    async fn put_part(
        &self,
        path: &Path,
        id: &MultipartId,
        part_idx: usize,
        payload: PutPayload,
    ) -> Result<PartId> {
        self.client.put_part(path, id, part_idx + 1, payload).await
    }

    async fn complete_multipart(
        &self,
        path: &Path,
        id: &MultipartId,
        parts: Vec<PartId>,
    ) -> Result<PutResult> {
        self.client.complete_multipart(path, id, parts).await
    }

    async fn abort_multipart(&self, path: &Path, id: &MultipartId) -> Result<()> {
        self.client.abort_multipart(path, id).await
    }
}

#[async_trait]
impl PaginatedListStore for CloudflareR2 {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        opts: PaginatedListOptions,
    ) -> Result<PaginatedListResult> {
        self.client.list_request(prefix, opts).await
    }
}
