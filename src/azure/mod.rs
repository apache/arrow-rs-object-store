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

//! An object store implementation for Azure blob storage
//!
//! See the [Feature Flags](crate#feature-flags) section for the difference
//! between the `azure` and `azure-base` features.
//!
//! ## Streaming uploads
//!
//! [`ObjectStore::put_multipart_opts`] will upload data in blocks and write a blob from those blocks.
//!
//! Unused blocks will automatically be dropped after 7 days.
//!
use crate::{
    CopyMode, CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartId, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    UploadPart,
    multipart::{MultipartStore, PartId},
    path::Path,
    signer::Signer,
};
use async_trait::async_trait;
use futures_util::stream::{BoxStream, StreamExt, TryStreamExt};
use http::Method;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

use crate::client::CredentialProvider;
use crate::client::get::GetClientExt;
use crate::client::list::{ListClient, ListClientExt};
pub use credential::{AzureAccessKey, AzureAuthorizer, authority_hosts};

mod builder;
mod client;
mod credential;

/// [`CredentialProvider`] for [`MicrosoftAzure`]
pub type AzureCredentialProvider = Arc<dyn CredentialProvider<Credential = AzureCredential>>;
use crate::azure::client::AzureClient;
use crate::client::parts::Parts;
use crate::list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore};
pub use builder::{AzureConfigKey, MicrosoftAzureBuilder, split_sas};
pub use credential::AzureCredential;

const STORE: &str = "MicrosoftAzure";

/// Interface for [Microsoft Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/).
#[derive(Debug)]
pub struct MicrosoftAzure {
    client: Arc<AzureClient>,
}

impl MicrosoftAzure {
    /// Returns the [`AzureCredentialProvider`] used by [`MicrosoftAzure`]
    pub fn credentials(&self) -> &AzureCredentialProvider {
        &self.client.config().credentials
    }

    /// Create a full URL to the resource specified by `path` with this instance's configuration.
    fn path_url(&self, path: &Path) -> Url {
        self.client.config().path_url(path)
    }
}

impl std::fmt::Display for MicrosoftAzure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MicrosoftAzure {{ account: {}, container: {} }}",
            self.client.config().account,
            self.client.config().container
        )
    }
}

#[async_trait]
impl ObjectStore for MicrosoftAzure {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.client.put_blob(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Ok(Box::new(AzureMultiPartUpload {
            part_idx: 0,
            opts,
            state: Arc::new(UploadState {
                client: Arc::clone(&self.client),
                location: location.clone(),
                parts: Default::default(),
            }),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.client.get_opts(location, options).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.client.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        if self.client.config().is_emulator {
            // Azurite doesn't support the startFrom query parameter,
            // fall back to client-side filtering
            //
            // See https://github.com/Azure/Azurite/issues/2619#issuecomment-3660701055
            let offset = offset.clone();
            self.list(prefix)
                .try_filter(move |f| futures_util::future::ready(f.location > offset))
                .boxed()
        } else {
            self.client.list_with_offset(prefix, offset)
        }
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let client = Arc::clone(&self.client);
        locations
            .try_chunks(256)
            .map(move |locations| {
                let client = Arc::clone(&client);
                async move {
                    // Early return the error. We ignore the paths that have already been
                    // collected into the chunk.
                    let locations = locations.map_err(|e| e.1)?;
                    client
                        .bulk_delete_request(locations)
                        .await
                        .map(futures_util::stream::iter)
                }
            })
            .buffered(20)
            .try_flatten()
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.client.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let CopyOptions {
            mode,
            extensions: _,
        } = options;

        match mode {
            CopyMode::Overwrite => self.client.copy_request(from, to, true).await,
            CopyMode::Create => self.client.copy_request(from, to, false).await,
        }
    }
}

#[async_trait]
impl Signer for MicrosoftAzure {
    /// Create a URL containing the relevant [Service SAS] query parameters that authorize a request
    /// via `method` to the resource at `path` valid for the duration specified in `expires_in`.
    ///
    /// [Service SAS]: https://learn.microsoft.com/en-us/rest/api/storageservices/create-service-sas
    ///
    /// # Example
    ///
    /// This example returns a URL that will enable a user to upload a file to
    /// "some-folder/some-file.txt" in the next hour.
    ///
    /// ```
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # use object_store::{azure::MicrosoftAzureBuilder, path::Path, signer::Signer};
    /// # use http::Method;
    /// # use std::time::Duration;
    /// #
    /// let azure = MicrosoftAzureBuilder::new()
    ///     .with_account("my-account")
    ///     .with_access_key("my-access-key")
    ///     .with_container_name("my-container")
    ///     .build()?;
    ///
    /// let url = azure.signed_url(
    ///     Method::PUT,
    ///     &Path::from("some-folder/some-file.txt"),
    ///     Duration::from_secs(60 * 60)
    /// ).await?;
    /// #     Ok(())
    /// # }
    /// ```
    async fn signed_url(&self, method: Method, path: &Path, expires_in: Duration) -> Result<Url> {
        if self.client.config().encryption_headers.is_enabled() {
            return Err(crate::Error::NotSupported {
                source: "Azure signed URLs cannot be used with customer-provided keys because CPK values must be supplied as request headers".into(),
            });
        }

        let mut url = self.path_url(path);
        let signer = self.client.signer(expires_in).await?;
        signer.sign(&method, &mut url)?;
        Ok(url)
    }

    async fn signed_urls(
        &self,
        method: Method,
        paths: &[Path],
        expires_in: Duration,
    ) -> Result<Vec<Url>> {
        if self.client.config().encryption_headers.is_enabled() {
            return Err(crate::Error::NotSupported {
                source: "Azure signed URLs cannot be used with customer-provided keys because CPK values must be supplied as request headers".into(),
            });
        }

        let mut urls = Vec::with_capacity(paths.len());
        let signer = self.client.signer(expires_in).await?;
        for path in paths {
            let mut url = self.path_url(path);
            signer.sign(&method, &mut url)?;
            urls.push(url);
        }
        Ok(urls)
    }
}

/// Relevant docs: <https://azure.github.io/Storage/docs/application-and-user-data/basics/azure-blob-storage-upload-apis/>
/// In Azure Blob Store, parts are "blocks"
/// put_multipart_part -> PUT block
/// complete -> PUT block list
/// abort -> No equivalent; blocks are simply dropped after 7 days
#[derive(Debug)]
struct AzureMultiPartUpload {
    part_idx: usize,
    state: Arc<UploadState>,
    opts: PutMultipartOptions,
}

#[derive(Debug)]
struct UploadState {
    location: Path,
    parts: Parts,
    client: Arc<AzureClient>,
}

#[async_trait]
impl MultipartUpload for AzureMultiPartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let idx = self.part_idx;
        self.part_idx += 1;
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let part = state.client.put_block(&state.location, idx, data).await?;
            state.parts.put(idx, part);
            Ok(())
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let parts = self.state.parts.finish(self.part_idx)?;

        self.state
            .client
            .put_block_list(&self.state.location, parts, std::mem::take(&mut self.opts))
            .await
    }

    async fn abort(&mut self) -> Result<()> {
        // Nothing to do
        Ok(())
    }
}

#[async_trait]
impl MultipartStore for MicrosoftAzure {
    async fn create_multipart(&self, _: &Path) -> Result<MultipartId> {
        Ok(String::new())
    }

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
                source: "`create_multipart_opts` with non-default options is not supported by MicrosoftAzure".into(),
            });
        }

        self.create_multipart(path).await
    }

    async fn put_part(
        &self,
        path: &Path,
        _: &MultipartId,
        part_idx: usize,
        data: PutPayload,
    ) -> Result<PartId> {
        self.client.put_block(path, part_idx, data).await
    }

    async fn complete_multipart(
        &self,
        path: &Path,
        _: &MultipartId,
        parts: Vec<PartId>,
    ) -> Result<PutResult> {
        self.client
            .put_block_list(path, parts, Default::default())
            .await
    }

    async fn abort_multipart(&self, _: &Path, _: &MultipartId) -> Result<()> {
        // There is no way to drop blocks that have been uploaded. Instead, they simply
        // expire in 7 days.
        Ok(())
    }
}

#[async_trait]
impl PaginatedListStore for MicrosoftAzure {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        opts: PaginatedListOptions,
    ) -> Result<PaginatedListResult> {
        self.client.list_request(prefix, opts).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ObjectStoreExt;
    use crate::integration::*;
    use crate::tests::*;
    use crate::{Attribute, Attributes};
    use base64::Engine;
    use base64::prelude::BASE64_STANDARD;
    use bytes::Bytes;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[cfg(feature = "reqwest")]
    #[tokio::test]
    async fn azure_create_multipart_opts_rejects_attributes() {
        let integration = MicrosoftAzureBuilder::new()
            .with_container_name("test")
            .with_use_emulator(true)
            .build()
            .unwrap();
        let opts = PutMultipartOptions {
            attributes: Attributes::from_iter([(Attribute::Metadata("key".into()), "value")]),
            ..Default::default()
        };

        let err = integration
            .create_multipart_opts(&Path::from("test"), opts)
            .await
            .unwrap_err();

        assert!(matches!(err, Error::NotSupported { .. }));
    }

    #[tokio::test]
    async fn azure_blob_test() {
        maybe_skip_integration!();
        // tag the extensions of every HTTP response with a marker,
        // allowing response_extensions to verify their propagation
        let integration = MicrosoftAzureBuilder::from_env()
            .with_http_connector(MarkerHttpConnector::default())
            .build()
            .unwrap();

        put_get_delete_list(&integration).await;
        list_with_offset_exclusivity(&integration).await;
        get_opts(&integration).await;
        list_uses_directories_correctly(&integration).await;
        list_with_delimiter(&integration).await;
        rename_and_copy(&integration).await;
        copy_if_not_exists(&integration).await;
        stream_get(&integration).await;
        put_opts(&integration, true).await;
        multipart(&integration, &integration).await;
        multipart_put_part_out_of_order(&integration, &integration).await;
        multipart_race_condition(&integration, false).await;
        multipart_out_of_order(&integration).await;
        signing(&integration).await;
        list_paginated(&integration, &integration).await;
        response_extensions(&integration, true).await;

        let validate = !integration.client.config().disable_tagging;
        tagging(
            Arc::new(MicrosoftAzure {
                client: Arc::clone(&integration.client),
            }),
            validate,
            |p| {
                let client = Arc::clone(&integration.client);
                async move { client.get_blob_tagging(&p).await }
            },
        )
        .await;

        // Azurite doesn't support attributes properly
        if !integration.client.config().is_emulator {
            put_get_attributes(&integration).await;
        }
    }

    #[ignore = "Used for manual testing against a real Workspace Private Link Endpoint."]
    #[tokio::test]
    async fn azure_onelake_wspl_test() {
        maybe_skip_integration!();

        let url =
            std::env::var("AZURE_ONELAKE_URL").expect("Set AZURE_ONELAKE_URL to a WS-PL FQDN");
        let parsed = url::Url::parse(&url).unwrap();

        let path = match parsed.scheme() {
            "abfss" | "abfs" => {
                // abfss://<container>@<host>/<path...>
                // container is in username, entire path is the object path
                let segments: Vec<&str> = parsed.path_segments().unwrap().collect();
                Path::from(segments.join("/"))
            }
            _ => {
                // https://<host>/<container>/<path...>
                // first segment is container, rest is the object path
                let segments: Vec<&str> = parsed.path_segments().unwrap().collect();
                Path::from(segments[1..].join("/"))
            }
        };

        let store = MicrosoftAzureBuilder::new()
            .with_url(&url)
            .with_bearer_token_authorization(
                std::env::var("AZURE_STORAGE_TOKEN").expect("Set AZURE_STORAGE_TOKEN"),
            )
            .build()
            .unwrap();

        let data = Bytes::from("Hello OneLake WSPL");

        store.put(&path, data.clone().into()).await.unwrap();
        let result = store.get(&path).await.unwrap();
        let loaded = result.bytes().await.unwrap();
        assert_eq!(data, loaded);
        store.delete(&path).await.unwrap();
    }

    // Azurite doesn't support CPK (just ignores it)
    #[ignore = "Used for manual testing against a real storage account."]
    #[tokio::test]
    async fn azure_blob_cpk_test() {
        let base = MicrosoftAzureBuilder::from_env();
        let key = BASE64_STANDARD.encode([7_u8; 32]);
        let wrong_key = BASE64_STANDARD.encode([9_u8; 32]);

        let encrypted = base.clone().with_encryption_key(&key).build().unwrap();
        let unencrypted = base.clone().build().unwrap();
        let wrong = base.with_encryption_key(&wrong_key).build().unwrap();

        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = Path::from(format!("cpk-test-{suffix}.txt"));
        let copy_path = Path::from(format!("cpk-test-copy-{suffix}.txt"));
        let payload = Bytes::from("customer-provided-key");

        encrypted.put(&path, payload.clone().into()).await.unwrap();

        let loaded = encrypted.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(loaded, payload);

        let range = encrypted.get_range(&path, 9..17).await.unwrap();
        assert_eq!(range, Bytes::from("provided"));

        let meta = encrypted.head(&path).await.unwrap();
        assert_eq!(meta.size, payload.len() as u64);

        encrypted.copy(&path, &copy_path).await.unwrap();
        let copied = encrypted
            .get(&copy_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(copied, payload);

        assert!(unencrypted.get(&path).await.is_err());
        assert!(wrong.get(&path).await.is_err());
        assert!(unencrypted.head(&path).await.is_err());
        assert!(wrong.get_range(&path, 0..8).await.is_err());
        assert!(unencrypted.get(&copy_path).await.is_err());
        assert!(wrong.get(&copy_path).await.is_err());
        let meta = encrypted.head(&copy_path).await.unwrap();
        assert_eq!(meta.size, payload.len() as u64);

        encrypted.delete(&copy_path).await.unwrap();
        encrypted.delete(&path).await.unwrap();
    }

    #[tokio::test]
    async fn azure_signed_url_rejects_cpk_configuration() {
        let store = MicrosoftAzureBuilder::new()
            .with_account("testaccount")
            .with_container_name("testcontainer")
            .with_access_key("Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
            .with_encryption_key(BASE64_STANDARD.encode([7_u8; 32]))
            .build()
            .unwrap();

        let err = store
            .signed_url(
                Method::GET,
                &Path::from("file.txt"),
                Duration::from_secs(60),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, crate::Error::NotSupported { .. }));
        assert!(err.to_string().contains("customer-provided keys"));

        let err = store
            .signed_urls(
                Method::GET,
                &[Path::from("file.txt")],
                Duration::from_secs(60),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, crate::Error::NotSupported { .. }));
        assert!(err.to_string().contains("customer-provided keys"));
    }

    #[ignore = "Used for manual testing against a real storage account."]
    #[tokio::test]
    async fn test_user_delegation_key() {
        let account = std::env::var("AZURE_ACCOUNT_NAME").unwrap();
        let container = std::env::var("AZURE_CONTAINER_NAME").unwrap();
        let client_id = std::env::var("AZURE_CLIENT_ID").unwrap();
        let client_secret = std::env::var("AZURE_CLIENT_SECRET").unwrap();
        let tenant_id = std::env::var("AZURE_TENANT_ID").unwrap();
        let integration = MicrosoftAzureBuilder::new()
            .with_account(account)
            .with_container_name(container)
            .with_client_id(client_id)
            .with_client_secret(client_secret)
            .with_tenant_id(&tenant_id)
            .build()
            .unwrap();

        let data = Bytes::from("hello world");
        let path = Path::from("file.txt");
        integration.put(&path, data.clone().into()).await.unwrap();

        let signed = integration
            .signed_url(Method::GET, &path, Duration::from_secs(60))
            .await
            .unwrap();

        let resp = reqwest::get(signed).await.unwrap();
        let loaded = resp.bytes().await.unwrap();

        assert_eq!(data, loaded);
    }

    #[test]
    fn azure_test_config_get_value() {
        let azure_client_id = "object_store:fake_access_key_id".to_string();
        let azure_storage_account_name = "object_store:fake_secret_key".to_string();
        let azure_storage_token = "object_store:fake_default_region".to_string();
        let builder = MicrosoftAzureBuilder::new()
            .with_config(AzureConfigKey::ClientId, &azure_client_id)
            .with_config(AzureConfigKey::AccountName, &azure_storage_account_name)
            .with_config(AzureConfigKey::Token, &azure_storage_token);

        assert_eq!(
            builder.get_config_value(&AzureConfigKey::ClientId).unwrap(),
            azure_client_id
        );
        assert_eq!(
            builder
                .get_config_value(&AzureConfigKey::AccountName)
                .unwrap(),
            azure_storage_account_name
        );
        assert_eq!(
            builder.get_config_value(&AzureConfigKey::Token).unwrap(),
            azure_storage_token
        );
    }
}
