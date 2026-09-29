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

use crate::aws::builder::S3EncryptionHeaders;
use crate::aws::checksum::Checksum;
use crate::aws::credential::{AwsCredential, CredentialExt};
use crate::aws::{
    AwsAuthorizer, AwsCredentialProvider, COPY_SOURCE_HEADER, S3ConditionalPut, S3CopyIfNotExists,
    STORE, STRICT_PATH_ENCODE_SET, TAGS_HEADER,
};
use crate::client::builder::{HttpRequestBuilder, RequestBuilderError};
use crate::client::get::GetClient;
use crate::client::header::{HeaderConfig, get_etag};
use crate::client::header::{get_put_result, get_version};
use crate::client::list::ListClient;
use crate::client::retry::{RetryContext, RetryError, RetryExt, RetryableRequestBuilder};
use crate::client::s3::{
    CompleteMultipartUpload, CompleteMultipartUploadResult, CopyPartResult,
    InitiateMultipartUploadResult, ListResponse, PartMetadata,
};
use crate::client::{
    CryptoProvider, DigestAlgorithm, GetOptionsExt, HttpClient, HttpError, HttpResponse,
    crypto_provider,
};
use crate::list::{PaginatedListOptions, PaginatedListResult};
use crate::multipart::PartId;
use crate::{
    Attribute, Attributes, ClientOptions, GetOptions, ListResult, MultipartId, Path,
    PutMultipartOptions, PutPayload, PutResult, Result, RetryConfig, TagSet,
};
use async_trait::async_trait;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bytes::{Buf, Bytes};
use http::header::{
    CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_LENGTH,
    CONTENT_TYPE,
};
use http::{HeaderMap, HeaderName, Method, StatusCode};
use itertools::Itertools;
use md5::{Digest, Md5};
use percent_encoding::{PercentEncode, utf8_percent_encode};
use quick_xml::events::{self as xml_events};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const VERSION_HEADER: &str = "x-amz-version-id";
const SHA256_CHECKSUM: &str = "x-amz-checksum-sha256";
const CRC64NVME_CHECKSUM: &str = "x-amz-checksum-crc64nvme";
const USER_DEFINED_METADATA_HEADER_PREFIX: &str = "x-amz-meta-";
const ALGORITHM: &str = "x-amz-checksum-algorithm";
const STORAGE_CLASS: &str = "x-amz-storage-class";

/// A specialized `Error` for object store-related errors
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("Error performing DeleteObjects request: {}", source)]
    DeleteObjectsRequest {
        source: crate::client::retry::RetryError,
        paths: Vec<String>,
    },

    #[error(
        "DeleteObjects request failed for key {}: {} (code: {})",
        path,
        message,
        code
    )]
    DeleteFailed {
        path: String,
        code: String,
        message: String,
    },

    #[error("Error getting DeleteObjects response body: {}", source)]
    DeleteObjectsResponse { source: HttpError },

    #[error("Got invalid DeleteObjects response: {}", source)]
    InvalidDeleteObjectsResponse {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[error("Error performing list request: {}", source)]
    ListRequest {
        source: crate::client::retry::RetryError,
    },

    #[error("Error getting list response body: {}", source)]
    ListResponseBody { source: HttpError },

    #[error("Error getting create multipart response body: {}", source)]
    CreateMultipartResponseBody { source: HttpError },

    #[error("Error performing complete multipart request: {}: {}", path, source)]
    CompleteMultipartRequest {
        source: crate::client::retry::RetryError,
        path: String,
    },

    #[error("Error getting complete multipart response body: {}", source)]
    CompleteMultipartResponseBody { source: HttpError },

    #[error("Got invalid list response: {}", source)]
    InvalidListResponse { source: quick_xml::de::DeError },

    #[error("Got invalid multipart response: {}", source)]
    InvalidMultipartResponse { source: quick_xml::de::DeError },

    #[error("Unable to extract metadata from headers: {}", source)]
    Metadata {
        source: crate::client::header::Error,
    },

    #[error("Error performing bucket request {}: {}", bucket, source)]
    BucketRequest {
        source: crate::client::retry::RetryError,
        bucket: String,
    },
}

impl From<Error> for crate::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::CompleteMultipartRequest { source, path } => source.error(STORE, path),
            Error::DeleteObjectsRequest { source, paths } => source.error(STORE, paths.join(",")),
            Error::BucketRequest { source, bucket } => source.error(STORE, bucket),
            _ => Self::Generic {
                store: STORE,
                source: Box::new(err),
            },
        }
    }
}

pub(crate) enum PutPartPayload<'a> {
    Part(PutPayload),
    Copy(&'a Path),
}

impl Default for PutPartPayload<'_> {
    fn default() -> Self {
        Self::Part(PutPayload::default())
    }
}

pub(crate) enum CompleteMultipartMode {
    Overwrite,
    Create,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase", rename = "DeleteResult")]
struct BatchDeleteResponse {
    #[serde(rename = "$value")]
    content: Vec<DeleteObjectResult>,
}

#[derive(Deserialize)]
enum DeleteObjectResult {
    #[allow(unused)]
    Deleted(DeletedObject),
    Error(DeleteError),
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase", rename = "Deleted")]
struct DeletedObject {
    #[allow(dead_code)]
    key: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase", rename = "Error")]
struct DeleteError {
    key: String,
    code: String,
    message: String,
}

impl From<DeleteError> for Error {
    fn from(err: DeleteError) -> Self {
        Self::DeleteFailed {
            path: err.key,
            code: err.code,
            message: err.message,
        }
    }
}

#[derive(Debug)]
pub(crate) struct S3Config {
    pub region: String,
    pub bucket: String,
    pub bucket_endpoint: String,
    pub credentials: AwsCredentialProvider,
    pub crypto: Option<Arc<dyn CryptoProvider>>,
    pub session_provider: Option<AwsCredentialProvider>,
    pub retry_config: RetryConfig,
    pub client_options: ClientOptions,
    pub sign_payload: bool,
    pub skip_signature: bool,
    pub disable_tagging: bool,
    pub disable_bulk_delete: bool,
    pub checksum: Option<Checksum>,
    pub copy_if_not_exists: Option<S3CopyIfNotExists>,
    pub conditional_put: S3ConditionalPut,
    pub request_payer: bool,
    pub(super) encryption_headers: S3EncryptionHeaders,
}

impl S3Config {
    pub(crate) fn path_url(&self, path: &Path) -> String {
        format!("{}/{}", self.bucket_endpoint, encode_path(path))
    }

    async fn get_session_credential(&self) -> Result<Option<SessionCredential<'_>>> {
        Ok(match self.skip_signature {
            false => {
                let provider = self.session_provider.as_ref().unwrap_or(&self.credentials);
                let credential = provider.get_credential().await?;
                Some(SessionCredential {
                    credential,
                    session_token: self.session_provider.is_some(),
                    config: self,
                })
            }
            true => None,
        })
    }

    pub(crate) async fn get_credential(&self) -> Result<Option<Arc<AwsCredential>>> {
        Ok(match self.skip_signature {
            false => Some(self.credentials.get_credential().await?),
            true => None,
        })
    }

    #[inline]
    pub(crate) fn is_s3_express(&self) -> bool {
        self.session_provider.is_some()
    }

    pub(crate) fn crypto(&self) -> Result<&dyn CryptoProvider> {
        crypto_provider(self.crypto.as_deref())
    }
}

struct SessionCredential<'a> {
    credential: Arc<AwsCredential>,
    session_token: bool,
    config: &'a S3Config,
}

impl SessionCredential<'_> {
    fn authorizer(&self) -> Result<AwsAuthorizer<'_>> {
        let mut authorizer =
            AwsAuthorizer::new(self.credential.as_ref(), "s3", &self.config.region)
                .with_sign_payload(self.config.sign_payload)
                .with_request_payer(self.config.request_payer)
                .with_crypto(self.config.crypto()?);

        if self.session_token {
            let token = HeaderName::from_static("x-amz-s3session-token");
            authorizer = authorizer.with_token_header(token)
        }

        Ok(authorizer)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RequestError {
    #[error(transparent)]
    Generic {
        #[from]
        source: crate::Error,
    },

    #[error("Retry")]
    Retry {
        source: crate::client::retry::RetryError,
        path: String,
    },
}

impl From<RequestError> for crate::Error {
    fn from(value: RequestError) -> Self {
        match value {
            RequestError::Generic { source } => source,
            RequestError::Retry { source, path } => source.error(STORE, path),
        }
    }
}

/// A builder for a request allowing customization of the headers and query string
pub(crate) struct Request<'a> {
    path: &'a Path,
    config: &'a S3Config,
    builder: HttpRequestBuilder,
    payload_sha256: Option<[u8; 32]>,
    payload: Option<PutPayload>,
    use_session_creds: bool,
    idempotent: bool,
    retry_on_conflict: bool,
    retry_error_body: bool,
}

impl Request<'_> {
    pub(crate) fn query<T: Serialize + ?Sized + Sync>(self, query: &T) -> Self {
        let builder = self.builder.query(query);
        Self { builder, ..self }
    }

    pub(crate) fn header<K>(self, k: K, v: &str) -> Self
    where
        K: TryInto<HeaderName>,
        K::Error: Into<RequestBuilderError>,
    {
        let builder = self.builder.header(k, v);
        Self { builder, ..self }
    }

    pub(crate) fn headers(self, headers: HeaderMap) -> Self {
        let builder = self.builder.headers(headers);
        Self { builder, ..self }
    }

    pub(crate) fn idempotent(self, idempotent: bool) -> Self {
        Self { idempotent, ..self }
    }

    pub(crate) fn retry_on_conflict(self, retry_on_conflict: bool) -> Self {
        Self {
            retry_on_conflict,
            ..self
        }
    }

    pub(crate) fn retry_error_body(self, retry_error_body: bool) -> Self {
        Self {
            retry_error_body,
            ..self
        }
    }

    pub(crate) fn with_encryption_headers(self) -> Self {
        let headers = self.config.encryption_headers.clone().into();
        let builder = self.builder.headers(headers);
        Self { builder, ..self }
    }

    pub(crate) fn with_session_creds(self, use_session_creds: bool) -> Self {
        Self {
            use_session_creds,
            ..self
        }
    }

    pub(crate) fn with_tags(mut self, tags: TagSet) -> Self {
        let tags = tags.encoded();
        if !tags.is_empty() && !self.config.disable_tagging {
            self.builder = self.builder.header(&TAGS_HEADER, tags);
        }
        self
    }

    pub(crate) fn with_attributes(self, attributes: Attributes) -> Self {
        let mut has_content_type = false;
        let mut builder = self.builder;
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
                Attribute::StorageClass => builder.header(STORAGE_CLASS, v.as_ref()),
                Attribute::Metadata(k_suffix) => builder.header(
                    &format!("{USER_DEFINED_METADATA_HEADER_PREFIX}{k_suffix}"),
                    v.as_ref(),
                ),
            };
        }

        if !has_content_type {
            if let Some(value) = self.config.client_options.get_content_type(self.path) {
                builder = builder.header(CONTENT_TYPE, value);
            }
        }
        Self { builder, ..self }
    }

    pub(crate) fn with_extensions(self, extensions: ::http::Extensions) -> Self {
        let builder = self.builder.extensions(extensions);
        Self { builder, ..self }
    }

    pub(crate) fn with_payload(mut self, payload: PutPayload) -> Result<Self> {
        let mut cached_digest: Option<[u8; 32]> = None;
        let mut sha256_digest = || -> Result<[u8; 32]> {
            if let Some(digest) = cached_digest {
                return Ok(digest);
            }
            let mut ctx = self.config.crypto()?.digest(DigestAlgorithm::Sha256)?;
            for part in &payload {
                ctx.update(part);
            }
            let digest = ctx.finish()?.try_into().unwrap();
            cached_digest = Some(digest);
            Ok(digest)
        };

        if !self.config.skip_signature && self.config.sign_payload {
            self.payload_sha256 = Some(sha256_digest()?);
        }

        match self.config.checksum {
            Some(Checksum::SHA256) => {
                self.builder = self
                    .builder
                    .header(SHA256_CHECKSUM, BASE64_STANDARD.encode(sha256_digest()?));
            }
            Some(Checksum::CRC64NVME) => {
                let crc_algo = crc_fast::CrcAlgorithm::Crc64Nvme;
                let mut digest = crc_fast::Digest::new(crc_algo);
                payload.iter().for_each(|x| digest.update(x));
                let checksum = digest.finalize();

                self.builder = self.builder.header(
                    CRC64NVME_CHECKSUM,
                    BASE64_STANDARD.encode(checksum.to_be_bytes()),
                )
            }
            None => {}
        }

        let content_length = payload.content_length();
        self.builder = self.builder.header(CONTENT_LENGTH, content_length);
        self.payload = Some(payload);
        Ok(self)
    }

    pub(crate) async fn send(self) -> Result<HttpResponse, RequestError> {
        let credential = match self.use_session_creds {
            true => self.config.get_session_credential().await?,
            false => {
                let credential = self.config.get_credential().await?;
                credential.map(|credential| SessionCredential {
                    credential,
                    session_token: false,
                    config: self.config,
                })
            }
        };
        let authorizer = credential.as_ref().map(|x| x.authorizer()).transpose()?;

        let sha = self.payload_sha256.as_ref().map(|x| x.as_ref());

        let path = self.path.as_ref();
        self.builder
            .with_aws_sigv4(authorizer, sha)?
            .retryable(&self.config.retry_config)
            .retry_on_conflict(self.retry_on_conflict)
            .idempotent(self.idempotent)
            .retry_error_body(self.retry_error_body)
            .payload(self.payload)
            .send()
            .await
            .map_err(|source| {
                let path = path.into();
                RequestError::Retry { source, path }
            })
    }

    pub(crate) async fn do_put(self) -> Result<PutResult> {
        let response = self.send().await?;
        Ok(
            get_put_result(response, VERSION_HEADER)
                .map_err(|source| Error::Metadata { source })?,
        )
    }
}

#[derive(Debug)]
pub(crate) struct S3Client {
    pub config: S3Config,
    pub client: HttpClient,
}

impl S3Client {
    pub(crate) fn new(config: S3Config, client: HttpClient) -> Self {
        Self { config, client }
    }

    pub(crate) fn request<'a>(&'a self, method: Method, path: &'a Path) -> Request<'a> {
        let url = self.config.path_url(path);
        let mut builder = self.client.request(method, url);
        if let Some(headers) = self.config.client_options.get_default_headers() {
            builder = builder.headers(headers.clone());
        }
        Request {
            path,
            builder,
            payload: None,
            payload_sha256: None,
            config: &self.config,
            use_session_creds: true,
            idempotent: false,
            retry_on_conflict: false,
            retry_error_body: false,
        }
    }

    /// Make an S3 Delete Objects request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html>
    ///
    /// Produces a vector of results, one for each path in the input vector. If
    /// the delete was successful, the path is returned in the `Ok` variant. If
    /// there was an error for a certain path, the error will be returned in the
    /// vector. If there was an issue with making the overall request, an error
    /// will be returned at the top level.
    pub(crate) async fn bulk_delete_request(&self, paths: Vec<Path>) -> Result<Vec<Result<Path>>> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }

        let credential = self.config.get_session_credential().await?;
        let authorizer = credential.as_ref().map(|x| x.authorizer()).transpose()?;
        let url = format!("{}?delete", self.config.bucket_endpoint);

        let mut buffer = Vec::new();
        let mut writer = quick_xml::Writer::new(&mut buffer);
        writer
            .write_event(xml_events::Event::Start(
                xml_events::BytesStart::new("Delete")
                    .with_attributes([("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/")]),
            ))
            .unwrap();
        for path in &paths {
            // <Object><Key>{path}</Key></Object>
            writer
                .write_event(xml_events::Event::Start(xml_events::BytesStart::new(
                    "Object",
                )))
                .unwrap();
            writer
                .write_event(xml_events::Event::Start(xml_events::BytesStart::new("Key")))
                .unwrap();
            writer
                .write_event(xml_events::Event::Text(xml_events::BytesText::new(
                    path.as_ref(),
                )))
                .map_err(|err| crate::Error::Generic {
                    store: STORE,
                    source: Box::new(err),
                })?;
            writer
                .write_event(xml_events::Event::End(xml_events::BytesEnd::new("Key")))
                .unwrap();
            writer
                .write_event(xml_events::Event::End(xml_events::BytesEnd::new("Object")))
                .unwrap();
        }
        writer
            .write_event(xml_events::Event::End(xml_events::BytesEnd::new("Delete")))
            .unwrap();

        let body = Bytes::from(buffer);

        let mut builder = self.client.request(Method::POST, url);
        if let Some(headers) = self.config.client_options.get_default_headers() {
            builder = builder.headers(headers.clone());
        }

        let crypto = self.config.crypto()?;
        let mut ctx = crypto.digest(DigestAlgorithm::Sha256)?;
        ctx.update(body.as_ref());
        let digest = ctx.finish()?;

        builder = builder.header(SHA256_CHECKSUM, BASE64_STANDARD.encode(digest));

        // S3 *requires* DeleteObjects to include a Content-MD5 header:
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
        // >   "The Content-MD5 request header is required for all Multi-Object Delete requests"
        // Some platforms, like MinIO, enforce this requirement and fail requests without the header.
        let mut hasher = Md5::new();
        hasher.update(&body);
        builder = builder.header("Content-MD5", BASE64_STANDARD.encode(hasher.finalize()));

        let response = builder
            .header(CONTENT_TYPE, "application/xml")
            .body(body)
            .with_aws_sigv4(authorizer, Some(digest))?
            .retryable(&self.config.retry_config)
            .retry_error_body(true)
            .send()
            .await
            .map_err(|source| Error::DeleteObjectsRequest {
                source,
                paths: paths.iter().map(|p| p.to_string()).collect(),
            })?
            .into_body()
            .bytes()
            .await
            .map_err(|source| Error::DeleteObjectsResponse { source })?;

        let response: BatchDeleteResponse =
            quick_xml::de::from_reader(response.reader()).map_err(|err| {
                Error::InvalidDeleteObjectsResponse {
                    source: Box::new(err),
                }
            })?;

        // Assume all were ok, then fill in errors. This guarantees output order
        // matches input order.
        let mut results: Vec<Result<Path>> = paths.iter().cloned().map(Ok).collect();
        for content in response.content.into_iter() {
            if let DeleteObjectResult::Error(error) = content {
                let path =
                    Path::parse(&error.key).map_err(|err| Error::InvalidDeleteObjectsResponse {
                        source: Box::new(err),
                    })?;
                let i = paths.iter().find_position(|&p| p == &path).unwrap().0;
                results[i] = Err(Error::from(error).into());
            }
        }

        Ok(results)
    }

    /// Make a single-object S3 Delete request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html>
    ///
    /// Unlike [`bulk_delete_request`](Self::bulk_delete_request), this issues a
    /// plain `DELETE /key` request, which is part of the core S3 API and is
    /// supported by every S3-compatible provider.
    pub(crate) async fn delete_request(&self, path: &Path) -> Result<()> {
        self.request(Method::DELETE, path).send().await?;
        Ok(())
    }

    /// Make an S3 Copy request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html>
    pub(crate) fn copy_request<'a>(&'a self, from: &Path, to: &'a Path) -> Request<'a> {
        let source = format!("{}/{}", self.config.bucket, encode_path(from));

        let mut copy_source_encryption_headers = HeaderMap::new();
        if let Some(customer_algorithm) = self
            .config
            .encryption_headers
            .0
            .get("x-amz-server-side-encryption-customer-algorithm")
        {
            copy_source_encryption_headers.insert(
                "x-amz-copy-source-server-side-encryption-customer-algorithm",
                customer_algorithm.clone(),
            );
        }
        if let Some(customer_key) = self
            .config
            .encryption_headers
            .0
            .get("x-amz-server-side-encryption-customer-key")
        {
            copy_source_encryption_headers.insert(
                "x-amz-copy-source-server-side-encryption-customer-key",
                customer_key.clone(),
            );
        }
        if let Some(customer_key_md5) = self
            .config
            .encryption_headers
            .0
            .get("x-amz-server-side-encryption-customer-key-MD5")
        {
            copy_source_encryption_headers.insert(
                "x-amz-copy-source-server-side-encryption-customer-key-MD5",
                customer_key_md5.clone(),
            );
        }

        self.request(Method::PUT, to)
            .idempotent(true)
            .retry_error_body(true)
            .header(&COPY_SOURCE_HEADER, &source)
            .headers(self.config.encryption_headers.clone().into())
            .headers(copy_source_encryption_headers)
            .with_session_creds(false)
    }

    pub(crate) async fn create_multipart(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<MultipartId> {
        let PutMultipartOptions {
            tags,
            attributes,
            extensions,
        } = opts;

        let mut request = self.request(Method::POST, location);
        if let Some(algorithm) = self.config.checksum {
            match algorithm {
                Checksum::SHA256 => {
                    request = request.header(ALGORITHM, "SHA256");
                }
                Checksum::CRC64NVME => {
                    request = request.header(ALGORITHM, "CRC64NVME");
                }
            }
        }
        let response = request
            .query(&[("uploads", "")])
            .with_encryption_headers()
            .with_attributes(attributes)
            .with_tags(tags)
            .with_extensions(extensions)
            .header(CONTENT_LENGTH, "0")
            .idempotent(true)
            .send()
            .await?
            .into_body()
            .bytes()
            .await
            .map_err(|source| Error::CreateMultipartResponseBody { source })?;

        let response: InitiateMultipartUploadResult = quick_xml::de::from_reader(response.reader())
            .map_err(|source| Error::InvalidMultipartResponse { source })?;

        Ok(response.upload_id)
    }

    pub(crate) async fn put_part(
        &self,
        path: &Path,
        upload_id: &MultipartId,
        part_idx: usize,
        data: PutPartPayload<'_>,
    ) -> Result<PartId> {
        let is_copy = matches!(data, PutPartPayload::Copy(_));
        let part = (part_idx + 1).to_string();

        let mut request = self
            .request(Method::PUT, path)
            .query(&[("partNumber", &part), ("uploadId", upload_id)])
            .idempotent(true);

        request = match data {
            PutPartPayload::Part(payload) => request.with_payload(payload)?,
            PutPartPayload::Copy(path) => request.header(
                "x-amz-copy-source",
                &format!("{}/{}", self.config.bucket, encode_path(path)),
            ),
        };

        if self
            .config
            .encryption_headers
            .0
            .contains_key("x-amz-server-side-encryption-customer-algorithm")
        {
            // If SSE-C is used, we must include the encryption headers in every upload request.
            request = request.with_encryption_headers();
        }

        let (parts, body) = request.send().await?.into_parts();
        let (e_tag, checksum_sha256, checksum_crc64nvme) = if is_copy {
            let response = body
                .bytes()
                .await
                .map_err(|source| Error::CreateMultipartResponseBody { source })?;
            let response: CopyPartResult = quick_xml::de::from_reader(response.reader())
                .map_err(|source| Error::InvalidMultipartResponse { source })?;
            (
                response.e_tag,
                response.checksum_sha256,
                response.checksum_crc64nvme,
            )
        } else {
            let e_tag = get_etag(&parts.headers).map_err(|source| Error::Metadata { source })?;
            let checksum_sha256 = parts
                .headers
                .get(SHA256_CHECKSUM)
                .and_then(|v| v.to_str().ok())
                .map(|v| v.to_string());
            let checksum_crc64nvme = parts
                .headers
                .get(CRC64NVME_CHECKSUM)
                .and_then(|v| v.to_str().ok())
                .map(|v| v.to_string());
            (e_tag, checksum_sha256, checksum_crc64nvme)
        };

        let content_id = match self.config.checksum {
            Some(_) => {
                let meta = PartMetadata {
                    e_tag,
                    checksum_sha256,
                    checksum_crc64nvme,
                };
                quick_xml::se::to_string(&meta).unwrap()
            }
            None => e_tag,
        };

        Ok(PartId { content_id })
    }

    pub(crate) async fn abort_multipart(&self, location: &Path, upload_id: &str) -> Result<()> {
        self.request(Method::DELETE, location)
            .query(&[("uploadId", upload_id)])
            .with_encryption_headers()
            .send()
            .await?;

        Ok(())
    }

    pub(crate) async fn complete_multipart(
        &self,
        location: &Path,
        upload_id: &str,
        parts: Vec<PartId>,
        mode: CompleteMultipartMode,
    ) -> Result<PutResult> {
        let parts = if parts.is_empty() {
            // If no parts were uploaded, upload an empty part
            // otherwise the completion request will fail
            let part = self
                .put_part(
                    location,
                    &upload_id.to_string(),
                    0,
                    PutPartPayload::default(),
                )
                .await?;
            vec![part]
        } else {
            parts
        };
        let request = CompleteMultipartUpload::from(parts);
        let body = quick_xml::se::to_string(&request).unwrap();

        let credential = self.config.get_session_credential().await?;
        let authorizer = credential.as_ref().map(|x| x.authorizer()).transpose()?;
        let url = self.config.path_url(location);

        let mut builder = self.client.post(url);
        if let Some(headers) = self.config.client_options.get_default_headers() {
            builder = builder.headers(headers.clone());
        }

        let request = builder
            .query(&[("uploadId", upload_id)])
            .body(body)
            .with_aws_sigv4(authorizer, None)?;

        let request = match mode {
            CompleteMultipartMode::Overwrite => request,
            CompleteMultipartMode::Create => request.header("If-None-Match", "*"),
        };

        let response = request
            .retryable(&self.config.retry_config)
            .idempotent(true)
            .retry_error_body(true)
            .send()
            .await
            .map_err(|source| Error::CompleteMultipartRequest {
                source,
                path: location.as_ref().to_string(),
            })?;

        let (parts, body) = response.into_parts();

        let version = get_version(&parts.headers, VERSION_HEADER)
            .map_err(|source| Error::Metadata { source })?;

        let data = body
            .bytes()
            .await
            .map_err(|source| Error::CompleteMultipartResponseBody { source })?;

        let response: CompleteMultipartUploadResult = quick_xml::de::from_reader(data.reader())
            .map_err(|source| Error::InvalidMultipartResponse { source })?;

        Ok(PutResult {
            e_tag: Some(response.e_tag),
            version,
            extensions: parts.extensions,
        })
    }

    /// Make an S3 CreateBucket request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html>
    pub(crate) async fn create_bucket(&self) -> Result<()> {
        let builder = self.bucket_request(Method::PUT);
        let builder = match self.config.region.as_str() {
            // S3 rejects us-east-1 as a LocationConstraint, and R2's "auto" is not a location
            "us-east-1" | "auto" => builder.header(CONTENT_LENGTH, "0"),
            region => {
                let region = quick_xml::escape::escape(region);
                let body = Bytes::from(format!(
                    "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><LocationConstraint>{region}</LocationConstraint></CreateBucketConfiguration>"
                ));
                builder
                    .header(CONTENT_TYPE, "application/xml")
                    .header(CONTENT_LENGTH, body.len())
                    .body(body)
            }
        };

        self.sign_bucket_request(builder)
            .await?
            .send()
            .await
            .map_err(|source| self.bucket_error(source))?;
        Ok(())
    }

    /// Make an S3 DeleteBucket request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html>
    pub(crate) async fn delete_bucket(&self) -> Result<()> {
        let request = self
            .sign_bucket_request(self.bucket_request(Method::DELETE))
            .await?;

        match request.send().await {
            Ok(_) => Ok(()),
            // A conflict means the bucket is not empty, not that it already exists
            Err(source) if source.status() == Some(StatusCode::CONFLICT) => {
                Err(crate::Error::Generic {
                    store: STORE,
                    source: Box::new(self.bucket_error(source)),
                })
            }
            Err(source) => Err(self.bucket_error(source).into()),
        }
    }

    /// Make an S3 HeadBucket request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html>
    pub(crate) async fn bucket_exists(&self) -> Result<bool> {
        let request = self
            .sign_bucket_request(self.bucket_request(Method::HEAD))
            .await?;

        match request.idempotent(true).send().await {
            Ok(_) => Ok(true),
            Err(source) if source.status() == Some(StatusCode::NOT_FOUND) => Ok(false),
            Err(source) => Err(self.bucket_error(source).into()),
        }
    }

    /// Start a request addressed to the bucket itself rather than to an object in it
    fn bucket_request(&self, method: Method) -> HttpRequestBuilder {
        let mut builder = self.client.request(method, &self.config.bucket_endpoint);
        if let Some(headers) = self.config.client_options.get_default_headers() {
            builder = builder.headers(headers.clone());
        }
        builder
    }

    /// Sign a request from [`Self::bucket_request`], ready to send
    async fn sign_bucket_request(
        &self,
        builder: HttpRequestBuilder,
    ) -> Result<RetryableRequestBuilder> {
        crate::bucket::validate_bucket_name(STORE, &self.config.bucket)?;
        if self.config.is_s3_express() {
            return Err(crate::Error::NotSupported {
                source:
                    "bucket operations are not supported for S3 Express One Zone directory buckets"
                        .into(),
            });
        }
        let credential = self.config.get_session_credential().await?;
        let authorizer = credential.as_ref().map(|x| x.authorizer()).transpose()?;
        Ok(builder
            .with_aws_sigv4(authorizer, None)?
            .retryable(&self.config.retry_config))
    }

    fn bucket_error(&self, source: RetryError) -> Error {
        Error::BucketRequest {
            source,
            bucket: self.config.bucket.clone(),
        }
    }

    #[cfg(test)]
    pub(crate) async fn get_object_tagging(&self, path: &Path) -> Result<HttpResponse> {
        let credential = self.config.get_session_credential().await?;
        let authorizer = credential.as_ref().map(|x| x.authorizer()).transpose()?;
        let url = format!("{}?tagging", self.config.path_url(path));
        let response = self
            .client
            .request(Method::GET, url)
            .with_aws_sigv4(authorizer, None)?
            .send_retry(&self.config.retry_config)
            .await
            .map_err(|e| e.error(STORE, path.to_string()))?;
        Ok(response)
    }
}

#[async_trait]
impl GetClient for S3Client {
    const STORE: &'static str = STORE;

    const HEADER_CONFIG: HeaderConfig = HeaderConfig {
        etag_required: false,
        last_modified_required: false,
        version_header: Some(VERSION_HEADER),
        user_defined_metadata_prefix: Some(USER_DEFINED_METADATA_HEADER_PREFIX),
    };

    fn retry_config(&self) -> &RetryConfig {
        &self.config.retry_config
    }

    /// Make an S3 GET request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html>
    async fn get_request(
        &self,
        ctx: &mut RetryContext,
        path: &Path,
        options: GetOptions,
    ) -> Result<HttpResponse> {
        let credential = self.config.get_session_credential().await?;
        let authorizer = credential.as_ref().map(|x| x.authorizer()).transpose()?;
        let url = self.config.path_url(path);
        let method = match options.head {
            true => Method::HEAD,
            false => Method::GET,
        };

        let mut builder = self.client.request(method, url);
        if let Some(headers) = self.config.client_options.get_default_headers() {
            builder = builder.headers(headers.clone());
        }
        if self
            .config
            .encryption_headers
            .0
            .contains_key("x-amz-server-side-encryption-customer-algorithm")
        {
            builder = builder.headers(self.config.encryption_headers.clone().into());
        }

        if let Some(v) = &options.version {
            builder = builder.query(&[("versionId", v)])
        }

        let response = builder
            .with_get_options(options)
            .with_aws_sigv4(authorizer, None)?
            .retryable_request()
            .send(ctx)
            .await
            .map_err(|e| e.error(STORE, path.to_string()))?;

        Ok(response)
    }
}

#[async_trait]
impl ListClient for Arc<S3Client> {
    /// Make an S3 List request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html>
    async fn list_request(
        &self,
        prefix: Option<&str>,
        opts: PaginatedListOptions,
    ) -> Result<PaginatedListResult> {
        let credential = self.config.get_session_credential().await?;
        let authorizer = credential.as_ref().map(|x| x.authorizer()).transpose()?;
        let url = self.config.bucket_endpoint.clone();

        let mut query = Vec::with_capacity(4);

        if let Some(token) = &opts.page_token {
            query.push(("continuation-token", token.as_ref()))
        }

        if let Some(d) = &opts.delimiter {
            query.push(("delimiter", d.as_ref()))
        }

        query.push(("list-type", "2"));

        if let Some(prefix) = prefix {
            query.push(("prefix", prefix))
        }

        if let Some(offset) = &opts.offset {
            query.push(("start-after", offset.as_ref()))
        }

        let max_keys_str;
        if let Some(max_keys) = &opts.max_keys {
            max_keys_str = max_keys.to_string();
            query.push(("max-keys", max_keys_str.as_ref()))
        }

        let response = self
            .client
            .request(Method::GET, &url)
            .extensions(opts.extensions)
            .query(&query)
            .with_aws_sigv4(authorizer, None)?
            .send_retry(&self.config.retry_config)
            .await
            .map_err(|source| Error::ListRequest { source })?;

        let (parts, body) = response.into_parts();

        let response = body
            .bytes()
            .await
            .map_err(|source| Error::ListResponseBody { source })?;

        let mut response: ListResponse = quick_xml::de::from_reader(response.reader())
            .map_err(|source| Error::InvalidListResponse { source })?;

        let token = response.next_continuation_token.take();

        let mut result: ListResult = response.try_into()?;
        result.extensions = parts.extensions;

        Ok(PaginatedListResult {
            result,
            page_token: token,
        })
    }
}

fn encode_path(path: &Path) -> PercentEncode<'_> {
    utf8_percent_encode(path.as_ref(), &STRICT_PATH_ENCODE_SET)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GetOptions;
    use crate::ObjectStore;
    use crate::aws::{AmazonS3, AmazonS3Builder};
    use crate::bucket::BucketStore;
    use crate::client::HttpClient;
    use crate::client::get::GetClient;
    use crate::client::mock_server::MockServer;
    use crate::client::retry::RetryContext;
    use futures_util::{StreamExt, TryStreamExt};
    use http::Response;
    use http::header::{AUTHORIZATION, CONTENT_LENGTH};
    use http_body_util::BodyExt;
    use hyper::Request;
    use hyper::body::Incoming;

    #[cfg(feature = "reqwest")]
    #[tokio::test]
    async fn test_create_multipart_has_content_length() {
        let mock = MockServer::new().await;

        mock.push_fn(|req| {
            // Verify Content-Length header is present and set to 0
            assert_eq!(req.headers().get(CONTENT_LENGTH).unwrap(), "0");
            assert!(req.uri().query().unwrap_or("").contains("uploads"));

            Response::builder()
                .status(200)
                .body("<InitiateMultipartUploadResult><UploadId>test-upload-id</UploadId></InitiateMultipartUploadResult>".to_string())
                .unwrap()
        });

        let credential = AwsCredential {
            key_id: "key".to_string(),
            secret_key: "secret".to_string(),
            token: None,
        };

        let config = S3Config {
            bucket_endpoint: mock.url().to_string(),
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            credentials: Arc::new(crate::StaticCredentialProvider::new(credential)),
            client_options: ClientOptions::new().with_allow_http(true),
            skip_signature: true,
            session_provider: None,
            retry_config: Default::default(),
            sign_payload: false,
            disable_tagging: false,
            disable_bulk_delete: false,
            checksum: None,
            copy_if_not_exists: None,
            conditional_put: Default::default(),
            encryption_headers: Default::default(),
            request_payer: false,
            crypto: None,
        };

        let client = S3Client::new(config, HttpClient::new(reqwest::Client::new()));
        let result = client
            .create_multipart(&Path::from("test"), PutMultipartOptions::default())
            .await;

        assert_eq!(result.unwrap(), "test-upload-id");
        mock.shutdown().await;
    }

    fn assert_default_headers_signed(req: &Request<Incoming>) {
        assert_eq!(req.headers().get("x-amz-meta-test").unwrap(), "test-value");
        assert_eq!(req.headers().get("x-amz-tagging").unwrap(), "key=value");

        let auth = req.headers().get(AUTHORIZATION).unwrap().to_str().unwrap();
        assert!(
            auth.contains("x-amz-meta-test"),
            "x-amz-meta-test not in SignedHeaders: {auth}"
        );
        assert!(
            auth.contains("x-amz-tagging"),
            "x-amz-tagging not in SignedHeaders: {auth}"
        );
    }

    fn default_headers_config(mock: &MockServer) -> S3Config {
        let credential = AwsCredential {
            key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            token: None,
        };

        let mut default_headers = HeaderMap::new();
        default_headers.insert("x-amz-meta-test", "test-value".parse().unwrap());
        default_headers.insert("x-amz-tagging", "key=value".parse().unwrap());

        S3Config {
            bucket_endpoint: mock.url().to_string(),
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            credentials: Arc::new(crate::StaticCredentialProvider::new(credential)),
            client_options: ClientOptions::new()
                .with_allow_http(true)
                .with_default_headers(default_headers),
            crypto: None,
            skip_signature: false,
            session_provider: None,
            retry_config: Default::default(),
            sign_payload: false,
            disable_tagging: false,
            disable_bulk_delete: false,
            checksum: None,
            copy_if_not_exists: None,
            conditional_put: Default::default(),
            encryption_headers: Default::default(),
            request_payer: false,
        }
    }

    #[cfg(feature = "reqwest")]
    #[tokio::test]
    async fn test_default_headers_signed_request() {
        let mock = MockServer::new().await;
        mock.push_fn(|req| {
            assert_default_headers_signed(&req);
            Response::builder()
                .status(200)
                .header("etag", "\"test-etag\"")
                .body(String::new())
                .unwrap()
        });

        let config = default_headers_config(&mock);
        let client = S3Client::new(config, HttpClient::new(reqwest::Client::new()));
        let result = client
            .request(Method::PUT, &Path::from("test"))
            .with_payload(PutPayload::default())
            .unwrap()
            .do_put()
            .await;

        assert!(result.is_ok());
        mock.shutdown().await;
    }

    #[cfg(feature = "reqwest")]
    #[tokio::test]
    async fn test_default_headers_signed_bulk_delete() {
        let mock = MockServer::new().await;
        mock.push_fn(|req| {
            assert_default_headers_signed(&req);
            Response::builder()
                .status(200)
                .body("<DeleteResult><Deleted><Key>test</Key></Deleted></DeleteResult>".to_string())
                .unwrap()
        });

        let config = default_headers_config(&mock);
        let client = S3Client::new(config, HttpClient::new(reqwest::Client::new()));
        let result = client.bulk_delete_request(vec![Path::from("test")]).await;

        assert!(result.is_ok());
        mock.shutdown().await;
    }

    /// `(method, path, query)` captured for assertion outside the mock closure.
    ///
    /// `MockServer` swallows panics raised inside its response handler (the
    /// connection just resets and the S3 retry logic can still surface an `Ok`
    /// result), so assertions placed inside the closure are silently ignored.
    /// We capture into shared state and assert in the test body instead.
    type CapturedRequest = (Method, String, Option<String>);

    fn capture(captured: &Arc<std::sync::Mutex<Vec<CapturedRequest>>>, req: &Request<Incoming>) {
        captured.lock().unwrap().push((
            req.method().clone(),
            req.uri().path().to_string(),
            req.uri().query().map(|s| s.to_string()),
        ));
    }

    /// Build an `AmazonS3` via the public builder so that `bucket_endpoint`
    /// is computed by the library from the addressing-style option — i.e.
    /// the option under test actually drives the URL the client emits.
    fn make_store(mock: &MockServer, virtual_hosted: bool, disable_bulk_delete: bool) -> AmazonS3 {
        AmazonS3Builder::new()
            .with_endpoint(mock.url())
            .with_bucket_name("test-bucket")
            .with_region("us-east-1")
            .with_allow_http(true)
            .with_skip_signature(true)
            .with_virtual_hosted_style_request(virtual_hosted)
            .with_disable_bulk_delete(disable_bulk_delete)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_delete_default() {
        // Default: path-style + bulk delete enabled.
        // `delete_stream` must issue a single `POST /{bucket}?delete`.
        let mock = MockServer::new().await;
        let captured: Arc<std::sync::Mutex<Vec<CapturedRequest>>> = Default::default();
        let c = Arc::clone(&captured);
        mock.push_fn(move |req| {
            capture(&c, &req);
            Response::builder()
                .status(200)
                .body("<DeleteResult><Deleted><Key>foo</Key></Deleted></DeleteResult>".to_string())
                .unwrap()
        });

        let store = make_store(&mock, false, false);
        let locations = futures_util::stream::iter(vec![Ok(Path::from("foo"))]).boxed();
        let deleted: Vec<_> = store.delete_stream(locations).try_collect().await.unwrap();
        assert_eq!(deleted.len(), 1);

        let captured = captured.lock().unwrap().clone();
        assert_eq!(captured.len(), 1, "expected one bulk delete request");
        assert_eq!(captured[0].0, Method::POST);
        assert_eq!(captured[0].1, "/test-bucket");
        assert_eq!(captured[0].2.as_deref(), Some("delete"));

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn test_delete_default_with_disable_bulk() {
        // Path-style + bulk delete disabled.
        // `delete_stream` must fan out into `DELETE /{bucket}/{key}` (one per
        // object, no `?delete` query).
        let mock = MockServer::new().await;
        let captured: Arc<std::sync::Mutex<Vec<CapturedRequest>>> = Default::default();
        for _ in 0..2 {
            let c = Arc::clone(&captured);
            mock.push_fn(move |req| {
                capture(&c, &req);
                Response::builder().status(204).body(String::new()).unwrap()
            });
        }

        let store = make_store(&mock, false, true);
        let locations =
            futures_util::stream::iter(vec![Ok(Path::from("foo")), Ok(Path::from("bar"))]).boxed();
        let deleted: Vec<_> = store.delete_stream(locations).try_collect().await.unwrap();
        assert_eq!(deleted.len(), 2);

        let mut captured = captured.lock().unwrap().clone();
        captured.sort_by(|a, b| a.1.cmp(&b.1));
        assert_eq!(captured.len(), 2, "expected one DELETE per object");
        assert_eq!(
            captured[0],
            (Method::DELETE, "/test-bucket/bar".to_string(), None)
        );
        assert_eq!(
            captured[1],
            (Method::DELETE, "/test-bucket/foo".to_string(), None)
        );

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn test_delete_virtual_hosted() {
        // Virtual-hosted style + bulk delete enabled.
        // `delete_stream` must issue a single `POST /?delete` (bucket is in
        // the host, not the path).
        let mock = MockServer::new().await;
        let captured: Arc<std::sync::Mutex<Vec<CapturedRequest>>> = Default::default();
        let c = Arc::clone(&captured);
        mock.push_fn(move |req| {
            capture(&c, &req);
            Response::builder()
                .status(200)
                .body("<DeleteResult><Deleted><Key>foo</Key></Deleted></DeleteResult>".to_string())
                .unwrap()
        });

        let store = make_store(&mock, true, false);
        let locations = futures_util::stream::iter(vec![Ok(Path::from("foo"))]).boxed();
        let deleted: Vec<_> = store.delete_stream(locations).try_collect().await.unwrap();
        assert_eq!(deleted.len(), 1);

        let captured = captured.lock().unwrap().clone();
        assert_eq!(captured.len(), 1, "expected one bulk delete request");
        assert_eq!(captured[0].0, Method::POST);
        assert_eq!(captured[0].1, "/");
        assert_eq!(captured[0].2.as_deref(), Some("delete"));

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn test_delete_virtual_hosted_with_disable_bulk() {
        // Virtual-hosted style + bulk delete disabled.
        // `delete_stream` must fan out into `DELETE /{key}` (no bucket in
        // path, no `?delete` query).
        let mock = MockServer::new().await;
        let captured: Arc<std::sync::Mutex<Vec<CapturedRequest>>> = Default::default();
        for _ in 0..2 {
            let c = Arc::clone(&captured);
            mock.push_fn(move |req| {
                capture(&c, &req);
                Response::builder().status(204).body(String::new()).unwrap()
            });
        }

        let store = make_store(&mock, true, true);
        let locations =
            futures_util::stream::iter(vec![Ok(Path::from("foo")), Ok(Path::from("bar"))]).boxed();
        let deleted: Vec<_> = store.delete_stream(locations).try_collect().await.unwrap();
        assert_eq!(deleted.len(), 2);

        let mut captured = captured.lock().unwrap().clone();
        captured.sort_by(|a, b| a.1.cmp(&b.1));
        assert_eq!(captured.len(), 2, "expected one DELETE per object");
        assert_eq!(captured[0], (Method::DELETE, "/bar".to_string(), None));
        assert_eq!(captured[1], (Method::DELETE, "/foo".to_string(), None));

        mock.shutdown().await;
    }

    #[cfg(feature = "reqwest")]
    #[tokio::test]
    async fn test_default_headers_signed_get_request() {
        let mock = MockServer::new().await;
        mock.push_fn(|req| {
            assert_default_headers_signed(&req);
            Response::builder()
                .status(200)
                .body("test-body".to_string())
                .unwrap()
        });

        let config = default_headers_config(&mock);
        let client = S3Client::new(config, HttpClient::new(reqwest::Client::new()));
        let mut ctx = RetryContext::new(&client.config.retry_config);
        let result = client
            .get_request(&mut ctx, &Path::from("test"), GetOptions::default())
            .await;

        assert!(result.is_ok());
        mock.shutdown().await;
    }

    #[cfg(feature = "reqwest")]
    #[tokio::test]
    async fn test_default_headers_signed_complete_multipart() {
        let mock = MockServer::new().await;
        mock.push_fn(|req| {
            assert_default_headers_signed(&req);
            assert!(req.uri().query().unwrap_or("").contains("uploadId"));

            Response::builder()
                .status(200)
                .body("<CompleteMultipartUploadResult><ETag>\"test-etag\"</ETag></CompleteMultipartUploadResult>".to_string())
                .unwrap()
        });

        let config = default_headers_config(&mock);
        let client = S3Client::new(config, HttpClient::new(reqwest::Client::new()));

        let parts = vec![PartId {
            content_id: "\"part-etag\"".to_string(),
        }];
        let _ = client
            .complete_multipart(
                &Path::from("test"),
                "test-upload-id",
                parts,
                CompleteMultipartMode::Overwrite,
            )
            .await
            .unwrap();
        mock.shutdown().await;
    }

    const EU_WEST_1_CREATE_BUCKET_BODY: &str = "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><LocationConstraint>eu-west-1</LocationConstraint></CreateBucketConfiguration>";

    /// A bucket request recorded by [`push_bucket_response`].
    #[derive(Debug)]
    struct CapturedBucketRequest {
        method: Method,
        path: String,
        query: Option<String>,
        headers: HeaderMap,
        body: Bytes,
    }

    type CapturedBucketRequests = Arc<std::sync::Mutex<Vec<CapturedBucketRequest>>>;

    /// Queue a response with `status`, recording the request it answers into `captured`.
    fn push_bucket_response(mock: &MockServer, captured: &CapturedBucketRequests, status: u16) {
        let captured = Arc::clone(captured);
        mock.push_async_fn(move |req| async move {
            let (parts, body) = req.into_parts();
            let body = body.collect().await.unwrap().to_bytes();
            captured.lock().unwrap().push(CapturedBucketRequest {
                method: parts.method,
                path: parts.uri.path().to_string(),
                query: parts.uri.query().map(str::to_string),
                headers: parts.headers,
                body,
            });
            Response::builder()
                .status(status)
                .body(String::new())
                .unwrap()
        });
    }

    fn single_bucket_request(captured: &CapturedBucketRequests) -> CapturedBucketRequest {
        let mut captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 1, "expected one request: {captured:?}");
        captured.pop().unwrap()
    }

    /// Like [`make_store`], but in `region`, and returning the builder for further options.
    fn bucket_store_builder(mock: &MockServer, region: &str) -> AmazonS3Builder {
        AmazonS3Builder::new()
            .with_endpoint(mock.url())
            .with_bucket_name("test-bucket")
            .with_region(region)
            .with_allow_http(true)
            .with_skip_signature(true)
    }

    #[tokio::test]
    async fn test_create_bucket_us_east_1_path_style() {
        let mock = MockServer::new().await;
        let captured = CapturedBucketRequests::default();
        push_bucket_response(&mock, &captured, 200);

        let store = make_store(&mock, false, false);
        store.create_bucket().await.unwrap();

        let req = single_bucket_request(&captured);
        assert_eq!(req.method, Method::PUT);
        assert_eq!(req.path, "/test-bucket");
        assert_eq!(req.query, None);
        assert_eq!(req.headers.get(CONTENT_LENGTH).unwrap(), "0");
        assert!(req.body.is_empty(), "{:?}", req.body);

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn test_create_bucket_virtual_hosted() {
        let mock = MockServer::new().await;
        let captured = CapturedBucketRequests::default();
        push_bucket_response(&mock, &captured, 200);

        let store = make_store(&mock, true, false);
        store.create_bucket().await.unwrap();

        let req = single_bucket_request(&captured);
        assert_eq!(req.method, Method::PUT);
        assert_eq!(req.path, "/");
        assert_eq!(req.query, None);

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn test_create_bucket_location_constraint() {
        let mock = MockServer::new().await;
        let captured = CapturedBucketRequests::default();
        push_bucket_response(&mock, &captured, 200);

        let store = bucket_store_builder(&mock, "eu-west-1").build().unwrap();
        store.create_bucket().await.unwrap();

        let req = single_bucket_request(&captured);
        assert_eq!(req.method, Method::PUT);
        assert_eq!(req.path, "/test-bucket");
        assert_eq!(req.headers.get(CONTENT_TYPE).unwrap(), "application/xml");
        assert_eq!(
            req.headers.get(CONTENT_LENGTH).unwrap(),
            EU_WEST_1_CREATE_BUCKET_BODY.len().to_string().as_str()
        );
        assert_eq!(req.body, EU_WEST_1_CREATE_BUCKET_BODY);

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn test_create_bucket_escapes_region() {
        let mock = MockServer::new().await;
        let captured = CapturedBucketRequests::default();
        push_bucket_response(&mock, &captured, 200);

        let store = bucket_store_builder(&mock, "a&b<c").build().unwrap();
        store.create_bucket().await.unwrap();

        let req = single_bucket_request(&captured);
        assert_eq!(
            req.body,
            "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><LocationConstraint>a&amp;b&lt;c</LocationConstraint></CreateBucketConfiguration>"
        );

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn test_create_bucket_auto_region() {
        let mock = MockServer::new().await;
        let captured = CapturedBucketRequests::default();
        push_bucket_response(&mock, &captured, 200);

        let store = bucket_store_builder(&mock, "auto").build().unwrap();
        store.create_bucket().await.unwrap();

        let req = single_bucket_request(&captured);
        assert_eq!(req.method, Method::PUT);
        assert_eq!(req.path, "/test-bucket");
        assert_eq!(req.headers.get(CONTENT_LENGTH).unwrap(), "0");
        assert!(req.body.is_empty(), "{:?}", req.body);
        assert!(req.headers.get(CONTENT_TYPE).is_none(), "{req:?}");

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn test_create_bucket_signs_body() {
        let mock = MockServer::new().await;
        let captured = CapturedBucketRequests::default();
        push_bucket_response(&mock, &captured, 200);

        let store = bucket_store_builder(&mock, "eu-west-1")
            .with_skip_signature(false)
            .with_access_key_id("AKIAIOSFODNN7EXAMPLE")
            .with_secret_access_key("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
            .build()
            .unwrap();
        store.create_bucket().await.unwrap();

        let req = single_bucket_request(&captured);
        assert!(req.headers.contains_key(AUTHORIZATION));
        // SHA256 of EU_WEST_1_CREATE_BUCKET_BODY
        assert_eq!(
            req.headers.get("x-amz-content-sha256").unwrap(),
            "a2531158b25edb57200e54140cc1b12d0af943f596ea467c2fbbedcc16223cc5"
        );

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn test_create_bucket_ignores_checksum_algorithm() {
        let mock = MockServer::new().await;
        let captured = CapturedBucketRequests::default();
        push_bucket_response(&mock, &captured, 200);

        let store = bucket_store_builder(&mock, "eu-west-1")
            .with_checksum_algorithm(Checksum::SHA256)
            .build()
            .unwrap();
        store.create_bucket().await.unwrap();

        let req = single_bucket_request(&captured);
        assert!(req.headers.get(SHA256_CHECKSUM).is_none(), "{req:?}");
        assert_eq!(req.body, EU_WEST_1_CREATE_BUCKET_BODY);

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn test_create_bucket_conflict_is_already_exists() {
        let mock = MockServer::new().await;
        let captured = CapturedBucketRequests::default();
        push_bucket_response(&mock, &captured, 409);

        let store = make_store(&mock, false, false);
        let err = store.create_bucket().await.unwrap_err();
        assert!(
            matches!(&err, crate::Error::AlreadyExists { path, .. } if path == "test-bucket"),
            "{err}"
        );

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn test_bucket_ops_s3_express_not_supported() {
        let mock = MockServer::new().await;
        let captured = CapturedBucketRequests::default();
        push_bucket_response(&mock, &captured, 200);

        let store = AmazonS3Builder::new()
            .with_endpoint(mock.url())
            .with_bucket_name("test-bucket--usw2-az1--x-s3")
            .with_region("us-west-2")
            .with_allow_http(true)
            .with_access_key_id("key")
            .with_secret_access_key("secret")
            .with_s3_express(true)
            .build()
            .unwrap();
        let err = store.create_bucket().await.unwrap_err();
        assert!(matches!(err, crate::Error::NotSupported { .. }), "{err}");
        let err = store.delete_bucket().await.unwrap_err();
        assert!(matches!(err, crate::Error::NotSupported { .. }), "{err}");
        let err = store.bucket_exists().await.unwrap_err();
        assert!(matches!(err, crate::Error::NotSupported { .. }), "{err}");
        assert!(captured.lock().unwrap().is_empty(), "no request expected");

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn test_bucket_ops_reject_unsafe_bucket_name() {
        let mock = MockServer::new().await;
        let captured = CapturedBucketRequests::default();
        push_bucket_response(&mock, &captured, 200);

        let store = bucket_store_builder(&mock, "us-east-1")
            .with_bucket_name("test-bucket?tagging")
            .build()
            .unwrap();
        let err = store.create_bucket().await.unwrap_err();
        assert!(matches!(err, crate::Error::Generic { .. }), "{err}");
        let err = store.delete_bucket().await.unwrap_err();
        assert!(matches!(err, crate::Error::Generic { .. }), "{err}");
        let err = store.bucket_exists().await.unwrap_err();
        assert!(matches!(err, crate::Error::Generic { .. }), "{err}");
        assert!(captured.lock().unwrap().is_empty(), "no request expected");

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn test_bucket_exists() {
        let mock = MockServer::new().await;
        let captured = CapturedBucketRequests::default();
        for status in [200, 404, 403] {
            push_bucket_response(&mock, &captured, status);
        }

        let store = make_store(&mock, false, false);
        assert!(store.bucket_exists().await.unwrap());
        assert!(!store.bucket_exists().await.unwrap());
        let err = store.bucket_exists().await.unwrap_err();
        assert!(
            matches!(&err, crate::Error::PermissionDenied { path, .. } if path == "test-bucket"),
            "{err}"
        );

        let captured = std::mem::take(&mut *captured.lock().unwrap());
        assert_eq!(captured.len(), 3);
        for req in captured {
            assert_eq!(req.method, Method::HEAD);
            assert_eq!(req.path, "/test-bucket");
            assert_eq!(req.query, None);
        }

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn test_delete_bucket() {
        let mock = MockServer::new().await;
        let captured = CapturedBucketRequests::default();
        for status in [204, 409, 404] {
            push_bucket_response(&mock, &captured, status);
        }

        let store = make_store(&mock, false, false);
        store.delete_bucket().await.unwrap();
        // A non-empty bucket must not be reported as AlreadyExists
        let err = store.delete_bucket().await.unwrap_err();
        assert!(matches!(err, crate::Error::Generic { .. }), "{err}");
        assert!(err.to_string().contains("test-bucket"), "{err}");
        let err = store.delete_bucket().await.unwrap_err();
        assert!(
            matches!(&err, crate::Error::NotFound { path, .. } if path == "test-bucket"),
            "{err}"
        );

        let captured = std::mem::take(&mut *captured.lock().unwrap());
        assert_eq!(captured.len(), 3);
        for req in captured {
            assert_eq!(req.method, Method::DELETE);
            assert_eq!(req.path, "/test-bucket");
            assert_eq!(req.query, None);
        }

        mock.shutdown().await;
    }
}
