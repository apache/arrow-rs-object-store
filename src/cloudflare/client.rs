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

//! HTTP client for the Cloudflare R2 REST API
//!
//! This uses the Cloudflare API v4 endpoints:
//! `https://api.cloudflare.com/client/v4/accounts/{account_id}/r2/buckets/{bucket_name}/objects`

use crate::client::builder::HttpRequestBuilder;
use crate::client::get::GetClient;
use crate::client::header::{HeaderConfig, get_put_result};
use crate::client::list::ListClient;
use crate::client::retry::{RetryContext, RetryExt};
use crate::client::{GetOptionsExt, HttpClient, HttpError, HttpResponse};
use crate::cloudflare::{CloudflareCredentialProvider, STORE};
use crate::list::{PaginatedListOptions, PaginatedListResult};
use crate::multipart::PartId;
use crate::path::Path;
use crate::{
    Attribute, Attributes, ClientOptions, GetOptions, ListResult, MultipartId, ObjectMeta,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, RetryConfig,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::header::{
    CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_LENGTH,
    CONTENT_TYPE, IF_MATCH, IF_NONE_MATCH,
};
use http::{HeaderName, Method};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const VERSION_HEADER: &str = "etag";
const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";
const USER_DEFINED_METADATA_HEADER_PREFIX: &str = "cf-r2-meta-";

/// Default Cloudflare API base URL
pub(crate) const DEFAULT_API_BASE_URL: &str = "https://api.cloudflare.com/client/v4";

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("Error performing list request: {}", source)]
    ListRequest {
        source: crate::client::retry::RetryError,
    },

    #[error("Error getting list response body: {}", source)]
    ListResponseBody { source: HttpError },

    #[allow(dead_code)]
    #[error("Got invalid list response: {}", source)]
    InvalidListResponse { source: serde_json::Error },

    #[error("Error performing get request {}: {}", path, source)]
    GetRequest {
        source: crate::client::retry::RetryError,
        path: String,
    },

    #[error("Error performing request {}: {}", path, source)]
    Request {
        source: crate::client::retry::RetryError,
        path: String,
    },

    #[allow(dead_code)]
    #[error("Error getting put response body: {}", source)]
    PutResponseBody { source: HttpError },

    #[allow(dead_code)]
    #[error("Got invalid put response: {}", source)]
    InvalidPutResponse { source: serde_json::Error },

    #[error("Unable to extract metadata from headers: {}", source)]
    Metadata {
        source: crate::client::header::Error,
    },

    #[error("Error performing multipart request: {}", source)]
    MultipartRequest {
        source: crate::client::retry::RetryError,
    },

    #[error("Error getting multipart response body: {}", source)]
    MultipartResponseBody { source: HttpError },

    #[allow(dead_code)]
    #[error("Got invalid multipart response: {}", source)]
    InvalidMultipartResponse { source: serde_json::Error },

    #[allow(dead_code)]
    #[error("R2 API error: {} (code: {})", message, code)]
    ApiError { code: u32, message: String },
}

impl From<Error> for crate::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::GetRequest { source, path } | Error::Request { source, path } => {
                source.error(STORE, path)
            }
            _ => Self::Generic {
                store: STORE,
                source: Box::new(err),
            },
        }
    }
}

/// Configuration for the Cloudflare R2 client
#[derive(Debug)]
pub(crate) struct CloudflareConfig {
    /// The Cloudflare account ID
    pub account_id: String,

    /// The R2 bucket name
    pub bucket_name: String,

    /// The API base URL (default: https://api.cloudflare.com/client/v4)
    pub base_url: String,

    /// Credential provider for R2 API token
    pub credentials: CloudflareCredentialProvider,

    /// Retry configuration
    pub retry_config: RetryConfig,

    /// Client options
    pub client_options: ClientOptions,
}

impl CloudflareConfig {
    /// Returns the URL for the objects endpoint
    pub(crate) fn objects_url(&self) -> String {
        format!(
            "{}/accounts/{}/r2/buckets/{}/objects",
            self.base_url, self.account_id, self.bucket_name
        )
    }

    /// Returns the URL for a specific object
    pub(crate) fn object_url(&self, path: &Path) -> String {
        format!("{}/{}", self.objects_url(), path)
    }

    /// Returns the URL for bucket operations (e.g., listing)
    #[allow(dead_code)]
    pub(crate) fn bucket_url(&self) -> String {
        format!(
            "{}/accounts/{}/r2/buckets/{}",
            self.base_url, self.account_id, self.bucket_name
        )
    }
}

/// A builder for a request allowing customisation of the headers and query string
pub(crate) struct Request<'a> {
    path: &'a Path,
    config: &'a CloudflareConfig,
    payload: Option<PutPayload>,
    builder: HttpRequestBuilder,
    idempotent: bool,
}

impl Request<'_> {
    fn header(self, k: &HeaderName, v: &str) -> Self {
        let builder = self.builder.header(k, v);
        Self { builder, ..self }
    }

    #[allow(dead_code)]
    fn query<T: Serialize + ?Sized + Sync>(self, query: &T) -> Self {
        let builder = self.builder.query(query);
        Self { builder, ..self }
    }

    fn idempotent(mut self, idempotent: bool) -> Self {
        self.idempotent = idempotent;
        self
    }

    fn with_attributes(self, attributes: Attributes) -> Self {
        let mut builder = self.builder;
        let mut has_content_type = false;
        for (k, v) in &attributes {
            builder = match k {
                Attribute::CacheControl => builder.header(CACHE_CONTROL, v.as_ref()),
                Attribute::ContentDisposition => builder.header(CONTENT_DISPOSITION, v.as_ref()),
                Attribute::ContentEncoding => builder.header(CONTENT_ENCODING, v.as_ref()),
                Attribute::ContentLanguage => builder.header(CONTENT_LANGUAGE, v.as_ref()),
                Attribute::ContentType => {
                    has_content_type = true;
                    builder.header(CONTENT_TYPE, v.as_ref())
                }
                Attribute::Metadata(k_suffix) => builder.header(
                    &format!("{USER_DEFINED_METADATA_HEADER_PREFIX}{k_suffix}"),
                    v.as_ref(),
                ),
                // R2 doesn't support storage class via custom header in the REST API
                Attribute::StorageClass => builder,
            };
        }

        if !has_content_type {
            let value = self.config.client_options.get_content_type(self.path);
            builder = builder.header(CONTENT_TYPE, value.unwrap_or(DEFAULT_CONTENT_TYPE))
        }
        Self { builder, ..self }
    }

    fn with_payload(self, payload: PutPayload) -> Self {
        let content_length = payload.content_length();
        Self {
            builder: self.builder.header(CONTENT_LENGTH, content_length),
            payload: Some(payload),
            ..self
        }
    }

    fn with_extensions(self, extensions: ::http::Extensions) -> Self {
        let builder = self.builder.extensions(extensions);
        Self { builder, ..self }
    }

    async fn send(self) -> Result<HttpResponse> {
        let credential = self.config.credentials.get_credential().await?;
        let resp = self
            .builder
            .bearer_auth(&credential.api_token)
            .retryable(&self.config.retry_config)
            .idempotent(self.idempotent)
            .payload(self.payload)
            .send()
            .await
            .map_err(|source| {
                let path = self.path.as_ref().into();
                Error::Request { source, path }
            })?;
        Ok(resp)
    }

    async fn do_put(self) -> Result<PutResult> {
        let response = self.send().await?;
        Ok(get_put_result(response.headers(), VERSION_HEADER)
            .map_err(|source| Error::Metadata { source })?)
    }
}

/// Cloudflare R2 REST API client
#[derive(Debug)]
pub(crate) struct CloudflareClient {
    config: CloudflareConfig,
    client: HttpClient,
}

impl CloudflareClient {
    pub(crate) fn new(config: CloudflareConfig, client: HttpClient) -> Result<Self> {
        Ok(Self { config, client })
    }

    pub(crate) fn config(&self) -> &CloudflareConfig {
        &self.config
    }

    fn request<'a>(&'a self, method: Method, path: &'a Path) -> Request<'a> {
        let url = self.config.object_url(path);
        Request {
            path,
            config: &self.config,
            payload: None,
            builder: self.client.request(method, url),
            idempotent: false,
        }
    }

    /// Perform a put object request
    pub(crate) async fn put_object(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let PutOptions {
            mode,
            tags: _tags,
            attributes,
            extensions,
        } = opts;

        let request = self
            .request(Method::PUT, path)
            .with_payload(payload)
            .with_attributes(attributes)
            .with_extensions(extensions);

        match mode {
            PutMode::Overwrite => request.idempotent(true).do_put().await,
            PutMode::Create => {
                match request.header(&IF_NONE_MATCH, "*").do_put().await {
                    Err(e @ crate::Error::Precondition { .. }) => Err(crate::Error::AlreadyExists {
                        path: path.to_string(),
                        source: Box::new(e),
                    }),
                    r => r,
                }
            }
            PutMode::Update(v) => {
                let etag = v.e_tag.ok_or_else(|| crate::Error::Generic {
                    store: STORE,
                    source: "ETag required for conditional update".to_string().into(),
                })?;
                request.header(&IF_MATCH, &etag).do_put().await
            }
        }
    }

    /// Perform a delete object request
    pub(crate) async fn delete_object(&self, path: &Path) -> Result<()> {
        self.request(Method::DELETE, path)
            .idempotent(true)
            .send()
            .await?;
        Ok(())
    }

    /// Perform a copy object request
    pub(crate) async fn copy_object(&self, from: &Path, to: &Path) -> Result<()> {
        let url = self.config.object_url(to);
        let credential = self.config.credentials.get_credential().await?;

        let source_path = format!(
            "{}/{}",
            self.config.bucket_name,
            from
        );

        self.client
            .request(Method::PUT, url)
            .header(&HeaderName::from_static("cf-r2-copy-source"), &source_path)
            .bearer_auth(&credential.api_token)
            .retryable(&self.config.retry_config)
            .idempotent(true)
            .send()
            .await
            .map_err(|source| Error::Request {
                source,
                path: to.to_string(),
            })?;

        Ok(())
    }

    /// Initiate a multipart upload
    pub(crate) async fn create_multipart(
        &self,
        path: &Path,
        _opts: PutMultipartOptions,
    ) -> Result<MultipartId> {
        let url = format!("{}?uploads", self.config.object_url(path));
        let credential = self.config.credentials.get_credential().await?;

        let response = self
            .client
            .request(Method::POST, url)
            .bearer_auth(&credential.api_token)
            .retryable(&self.config.retry_config)
            .idempotent(true)
            .send()
            .await
            .map_err(|source| Error::MultipartRequest { source })?;

        let body = response
            .into_body()
            .json::<CreateMultipartUploadResponse>()
            .await
            .map_err(|source| Error::MultipartResponseBody { source })?;

        Ok(body.upload_id)
    }

    /// Upload a part of a multipart upload
    pub(crate) async fn put_part(
        &self,
        path: &Path,
        upload_id: &MultipartId,
        part_number: usize,
        payload: PutPayload,
    ) -> Result<PartId> {
        let url = format!(
            "{}?partNumber={}&uploadId={}",
            self.config.object_url(path),
            part_number,
            upload_id
        );
        let credential = self.config.credentials.get_credential().await?;

        let content_length = payload.content_length();
        let response = self
            .client
            .request(Method::PUT, url)
            .header(CONTENT_LENGTH, content_length)
            .bearer_auth(&credential.api_token)
            .retryable(&self.config.retry_config)
            .idempotent(true)
            .payload(Some(payload))
            .send()
            .await
            .map_err(|source| Error::MultipartRequest { source })?;

        let etag = response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_default();

        Ok(PartId {
            content_id: etag,
        })
    }

    /// Complete a multipart upload
    pub(crate) async fn complete_multipart(
        &self,
        path: &Path,
        upload_id: &MultipartId,
        parts: Vec<PartId>,
    ) -> Result<PutResult> {
        let url = format!(
            "{}?uploadId={}",
            self.config.object_url(path),
            upload_id
        );
        let credential = self.config.credentials.get_credential().await?;

        let body = CompleteMultipartUpload {
            parts: parts
                .into_iter()
                .enumerate()
                .map(|(i, p)| CompletedPart {
                    part_number: i + 1,
                    etag: p.content_id,
                })
                .collect(),
        };

        let body_bytes = serde_json::to_vec(&body).map_err(|source| crate::Error::Generic {
            store: STORE,
            source: Box::new(source),
        })?;

        let response = self
            .client
            .request(Method::POST, url)
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, body_bytes.len())
            .bearer_auth(&credential.api_token)
            .retryable(&self.config.retry_config)
            .idempotent(true)
            .payload(Some(PutPayload::from(bytes::Bytes::from(body_bytes))))
            .send()
            .await
            .map_err(|source| Error::MultipartRequest { source })?;

        Ok(get_put_result(response.headers(), VERSION_HEADER)
            .map_err(|source| Error::Metadata { source })?)
    }

    /// Abort a multipart upload
    pub(crate) async fn abort_multipart(
        &self,
        path: &Path,
        upload_id: &MultipartId,
    ) -> Result<()> {
        let url = format!(
            "{}?uploadId={}",
            self.config.object_url(path),
            upload_id
        );
        let credential = self.config.credentials.get_credential().await?;

        self.client
            .request(Method::DELETE, url)
            .bearer_auth(&credential.api_token)
            .retryable(&self.config.retry_config)
            .idempotent(true)
            .send()
            .await
            .map_err(|source| Error::MultipartRequest { source })?;

        Ok(())
    }
}

#[async_trait]
impl GetClient for CloudflareClient {
    const STORE: &'static str = STORE;

    const HEADER_CONFIG: HeaderConfig = HeaderConfig {
        etag_required: false,
        last_modified_required: false,
        version_header: None,
        user_defined_metadata_prefix: Some(USER_DEFINED_METADATA_HEADER_PREFIX),
    };

    fn retry_config(&self) -> &RetryConfig {
        &self.config.retry_config
    }

    async fn get_request(
        &self,
        _ctx: &mut RetryContext,
        path: &Path,
        options: GetOptions,
    ) -> Result<HttpResponse> {
        let credential = self.config.credentials.get_credential().await?;
        let url = self.config.object_url(path);

        let mut builder = self.client.request(Method::GET, url);
        builder = builder.bearer_auth(&credential.api_token);
        builder = builder.with_get_options(options);

        let response = builder
            .retryable(&self.config.retry_config)
            .idempotent(true)
            .send()
            .await
            .map_err(|source| Error::GetRequest {
                source,
                path: path.to_string(),
            })?;

        Ok(response)
    }
}

#[async_trait]
impl ListClient for Arc<CloudflareClient> {
    async fn list_request(
        &self,
        prefix: Option<&str>,
        options: PaginatedListOptions,
    ) -> Result<PaginatedListResult> {
        let credential = self.config.credentials.get_credential().await?;
        let url = self.config.objects_url();

        let mut query_pairs: Vec<(&str, String)> = Vec::new();
        if let Some(prefix) = prefix {
            query_pairs.push(("prefix", prefix.to_string()));
        }
        if let Some(delimiter) = &options.delimiter {
            query_pairs.push(("delimiter", delimiter.to_string()));
        }
        if let Some(token) = &options.page_token {
            query_pairs.push(("cursor", token.clone()));
        }
        if let Some(max_keys) = options.max_keys {
            query_pairs.push(("limit", max_keys.to_string()));
        }

        let response = self
            .client
            .request(Method::GET, url)
            .query(&query_pairs)
            .bearer_auth(&credential.api_token)
            .retryable(&self.config.retry_config)
            .idempotent(true)
            .send()
            .await
            .map_err(|source| Error::ListRequest { source })?;

        let body = response
            .into_body()
            .json::<R2ListResponse>()
            .await
            .map_err(|source| Error::ListResponseBody { source })?;

        let objects = body
            .result
            .objects
            .into_iter()
            .map(|o| {
                Ok(ObjectMeta {
                    location: crate::path::Path::parse(&o.key)?,
                    last_modified: o.uploaded,
                    size: o.size,
                    e_tag: o.etag,
                    version: None,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let common_prefixes = body
            .result
            .delimited_prefixes
            .into_iter()
            .map(|p| crate::path::Path::parse(p))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let page_token = if body.result.truncated {
            body.result.cursor
        } else {
            None
        };

        Ok(PaginatedListResult {
            result: ListResult {
                common_prefixes,
                objects,
            },
            page_token,
        })
    }
}

// --- Response types for the Cloudflare R2 REST API ---

#[derive(Debug, Deserialize)]
struct R2ListResponse {
    result: R2ListResult,
    #[allow(dead_code)]
    success: bool,
}

#[derive(Debug, Deserialize)]
struct R2ListResult {
    objects: Vec<R2Object>,
    #[serde(default)]
    truncated: bool,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    delimited_prefixes: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct R2Object {
    key: String,
    size: u64,
    uploaded: DateTime<Utc>,
    #[serde(default)]
    etag: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CreateMultipartUploadResponse {
    #[serde(rename = "uploadId")]
    upload_id: String,
}

#[derive(Debug, Serialize)]
struct CompleteMultipartUpload {
    parts: Vec<CompletedPart>,
}

#[derive(Debug, Serialize)]
struct CompletedPart {
    #[serde(rename = "partNumber")]
    part_number: usize,
    etag: String,
}
