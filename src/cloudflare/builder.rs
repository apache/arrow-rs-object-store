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

use crate::client::HttpClient;
use crate::cloudflare::client::{CloudflareClient, CloudflareConfig, DEFAULT_API_BASE_URL};
use crate::cloudflare::credential::StaticCloudflareCredentialProvider;
use crate::cloudflare::{CloudflareCredentialProvider, CloudflareR2, STORE};
use crate::{ClientConfigKey, ClientOptions, Result, RetryConfig};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use url::Url;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Missing account ID")]
    MissingAccountId,

    #[error("Missing bucket name")]
    MissingBucketName,

    #[error("Missing API token")]
    MissingApiToken,

    #[error("Unable parse source url. Url: {}, Error: {}", url, source)]
    UnableToParseUrl {
        source: url::ParseError,
        url: String,
    },

    #[error(
        "Unknown url scheme cannot be parsed into storage location: {}",
        scheme
    )]
    UnknownUrlScheme { scheme: String },

    #[error("URL did not match any known pattern for scheme: {}", url)]
    UrlNotRecognised { url: String },

    #[error("Configuration key: '{}' is not known.", key)]
    UnknownConfigurationKey { key: String },
}

impl From<Error> for crate::Error {
    fn from(source: Error) -> Self {
        match source {
            Error::UnknownConfigurationKey { key } => {
                Self::UnknownConfigurationKey { store: STORE, key }
            }
            _ => Self::Generic {
                store: STORE,
                source: Box::new(source),
            },
        }
    }
}

/// Configuration keys for [`CloudflareR2Builder`]
///
/// # Example
/// ```
/// # use object_store::cloudflare::CloudflareConfigKey;
/// let key: CloudflareConfigKey = "account_id".parse().unwrap();
/// assert_eq!(key, CloudflareConfigKey::AccountId);
/// ```
#[derive(PartialEq, Eq, Hash, Clone, Debug, Copy, Serialize, Deserialize)]
#[non_exhaustive]
pub enum CloudflareConfigKey {
    /// The Cloudflare account ID
    ///
    /// Supported keys:
    /// - `account_id`
    /// - `cloudflare_account_id`
    AccountId,

    /// The R2 bucket name
    ///
    /// Supported keys:
    /// - `bucket_name`
    /// - `bucket`
    /// - `cloudflare_bucket_name`
    BucketName,

    /// The Cloudflare API token
    ///
    /// Supported keys:
    /// - `api_token`
    /// - `cloudflare_api_token`
    /// - `token`
    ApiToken,

    /// Custom API base URL (for testing)
    ///
    /// Supported keys:
    /// - `api_base_url`
    /// - `endpoint`
    ApiBaseUrl,

    /// Client configuration key
    Client(ClientConfigKey),
}

impl AsRef<str> for CloudflareConfigKey {
    fn as_ref(&self) -> &str {
        match self {
            Self::AccountId => "account_id",
            Self::BucketName => "bucket_name",
            Self::ApiToken => "api_token",
            Self::ApiBaseUrl => "api_base_url",
            Self::Client(key) => key.as_ref(),
        }
    }
}

impl FromStr for CloudflareConfigKey {
    type Err = crate::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "account_id" | "cloudflare_account_id" => Ok(Self::AccountId),
            "bucket_name" | "bucket" | "cloudflare_bucket_name" => Ok(Self::BucketName),
            "api_token" | "cloudflare_api_token" | "token" => Ok(Self::ApiToken),
            "api_base_url" | "endpoint" => Ok(Self::ApiBaseUrl),
            _ => match s.parse::<ClientConfigKey>() {
                Ok(key) => Ok(Self::Client(key)),
                Err(_) => Err(Error::UnknownConfigurationKey { key: s.into() }.into()),
            },
        }
    }
}


/// Builder for [`CloudflareR2`] using the Cloudflare REST API
///
/// # Example
///
/// ```no_run
/// # use object_store::cloudflare::CloudflareR2Builder;
/// let r2 = CloudflareR2Builder::new()
///     .with_account_id("my-account-id")
///     .with_bucket_name("my-bucket")
///     .with_api_token("my-api-token")
///     .build()
///     .unwrap();
/// ```
#[derive(Debug, Clone, Default)]
pub struct CloudflareR2Builder {
    /// The Cloudflare account ID
    account_id: Option<String>,
    /// The R2 bucket name
    bucket_name: Option<String>,
    /// Cloudflare API token
    api_token: Option<String>,
    /// Custom API base URL
    api_base_url: Option<String>,
    /// Retry config
    retry_config: RetryConfig,
    /// Client options
    client_options: ClientOptions,
    /// Credentials provider
    credentials: Option<CloudflareCredentialProvider>,
    /// URL
    url: Option<String>,
}

impl CloudflareR2Builder {
    /// Create a new [`CloudflareR2Builder`] with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new [`CloudflareR2Builder`] from environment variables
    ///
    /// Reads the following environment variables:
    /// - `CLOUDFLARE_ACCOUNT_ID` or `CF_ACCOUNT_ID`
    /// - `CLOUDFLARE_R2_BUCKET` or `CF_R2_BUCKET`
    /// - `CLOUDFLARE_API_TOKEN` or `CF_API_TOKEN`
    /// - `CLOUDFLARE_API_BASE_URL`
    pub fn from_env() -> Self {
        let mut builder = Self::new();

        if let Ok(account_id) = std::env::var("CLOUDFLARE_ACCOUNT_ID")
            .or_else(|_| std::env::var("CF_ACCOUNT_ID"))
        {
            builder.account_id = Some(account_id);
        }

        if let Ok(bucket) =
            std::env::var("CLOUDFLARE_R2_BUCKET").or_else(|_| std::env::var("CF_R2_BUCKET"))
        {
            builder.bucket_name = Some(bucket);
        }

        if let Ok(token) =
            std::env::var("CLOUDFLARE_API_TOKEN").or_else(|_| std::env::var("CF_API_TOKEN"))
        {
            builder.api_token = Some(token);
        }

        if let Ok(base_url) = std::env::var("CLOUDFLARE_API_BASE_URL") {
            builder.api_base_url = Some(base_url);
        }

        builder
    }

    /// Set the Cloudflare account ID
    pub fn with_account_id(mut self, account_id: impl Into<String>) -> Self {
        self.account_id = Some(account_id.into());
        self
    }

    /// Set the R2 bucket name
    pub fn with_bucket_name(mut self, bucket_name: impl Into<String>) -> Self {
        self.bucket_name = Some(bucket_name.into());
        self
    }

    /// Set the Cloudflare API token
    pub fn with_api_token(mut self, api_token: impl Into<String>) -> Self {
        self.api_token = Some(api_token.into());
        self
    }

    /// Set a custom API base URL (for testing or self-hosted)
    pub fn with_api_base_url(mut self, api_base_url: impl Into<String>) -> Self {
        self.api_base_url = Some(api_base_url.into());
        self
    }

    /// Set the retry configuration
    pub fn with_retry(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }

    /// Set the [`ClientOptions`]
    pub fn with_client_options(mut self, options: ClientOptions) -> Self {
        self.client_options = options;
        self
    }

    /// Set the [`CloudflareCredentialProvider`]
    pub fn with_credentials(mut self, credentials: CloudflareCredentialProvider) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Set the URL
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Set a configuration option by key
    pub fn with_config(mut self, key: CloudflareConfigKey, value: impl Into<String>) -> Self {
        match key {
            CloudflareConfigKey::AccountId => self.account_id = Some(value.into()),
            CloudflareConfigKey::BucketName => self.bucket_name = Some(value.into()),
            CloudflareConfigKey::ApiToken => self.api_token = Some(value.into()),
            CloudflareConfigKey::ApiBaseUrl => self.api_base_url = Some(value.into()),
            CloudflareConfigKey::Client(key) => {
                self.client_options = self.client_options.with_config(key, value)
            }
        }
        self
    }

    /// Parse a URL into builder configuration
    fn parse_url(&mut self, url: &str) -> Result<()> {
        let parsed = Url::parse(url).map_err(|source| Error::UnableToParseUrl {
            source,
            url: url.to_string(),
        })?;

        match parsed.scheme() {
            "r2" => {
                // r2://bucket_name/path
                let host = parsed.host_str().ok_or_else(|| Error::UrlNotRecognised {
                    url: url.to_string(),
                })?;
                self.bucket_name = Some(host.to_string());
            }
            "https" => {
                // https://api.cloudflare.com/client/v4/accounts/{account_id}/r2/buckets/{bucket}
                // or custom endpoint
                let host = parsed.host_str().unwrap_or_default();
                if host == "api.cloudflare.com" {
                    // Try to extract account_id and bucket from path
                    let path = parsed.path();
                    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
                    // Expected: ["client", "v4", "accounts", "{account_id}", "r2", "buckets", "{bucket}"]
                    if segments.len() >= 7
                        && segments[0] == "client"
                        && segments[1] == "v4"
                        && segments[2] == "accounts"
                        && segments[4] == "r2"
                        && segments[5] == "buckets"
                    {
                        self.account_id = Some(segments[3].to_string());
                        self.bucket_name = Some(segments[6].to_string());
                    }
                }
            }
            scheme => return Err(Error::UnknownUrlScheme { scheme: scheme.into() }.into()),
        }

        Ok(())
    }

    /// Build the [`CloudflareR2`] instance
    pub fn build(mut self) -> Result<CloudflareR2> {
        if let Some(url) = self.url.take() {
            self.parse_url(&url)?;
        }

        let account_id = self.account_id.ok_or(Error::MissingAccountId)?;
        let bucket_name = self.bucket_name.ok_or(Error::MissingBucketName)?;
        let base_url = self
            .api_base_url
            .unwrap_or_else(|| DEFAULT_API_BASE_URL.to_string());

        let credentials = if let Some(credentials) = self.credentials {
            credentials
        } else {
            let api_token = self.api_token.ok_or(Error::MissingApiToken)?;
            Arc::new(StaticCloudflareCredentialProvider::new(api_token))
        };

        let config = CloudflareConfig {
            account_id,
            bucket_name,
            base_url,
            credentials,
            retry_config: self.retry_config,
            client_options: self.client_options.clone(),
        };

        let http_client = HttpClient::new(self.client_options.client()?);
        let client = CloudflareClient::new(config, http_client)?;

        Ok(CloudflareR2 {
            client: Arc::new(client),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_key_parsing() {
        let key: CloudflareConfigKey = "account_id".parse().unwrap();
        assert_eq!(key, CloudflareConfigKey::AccountId);

        let key: CloudflareConfigKey = "cloudflare_account_id".parse().unwrap();
        assert_eq!(key, CloudflareConfigKey::AccountId);

        let key: CloudflareConfigKey = "bucket_name".parse().unwrap();
        assert_eq!(key, CloudflareConfigKey::BucketName);

        let key: CloudflareConfigKey = "bucket".parse().unwrap();
        assert_eq!(key, CloudflareConfigKey::BucketName);

        let key: CloudflareConfigKey = "api_token".parse().unwrap();
        assert_eq!(key, CloudflareConfigKey::ApiToken);

        let key: CloudflareConfigKey = "cloudflare_api_token".parse().unwrap();
        assert_eq!(key, CloudflareConfigKey::ApiToken);

        let key: CloudflareConfigKey = "api_base_url".parse().unwrap();
        assert_eq!(key, CloudflareConfigKey::ApiBaseUrl);
    }

    #[test]
    fn test_parse_r2_url() {
        let mut builder = CloudflareR2Builder::new();
        builder.parse_url("r2://my-bucket/path").unwrap();
        assert_eq!(builder.bucket_name.as_deref(), Some("my-bucket"));
    }

    #[test]
    fn test_parse_https_url() {
        let mut builder = CloudflareR2Builder::new();
        builder
            .parse_url("https://api.cloudflare.com/client/v4/accounts/abc123/r2/buckets/my-bucket")
            .unwrap();
        assert_eq!(builder.account_id.as_deref(), Some("abc123"));
        assert_eq!(builder.bucket_name.as_deref(), Some("my-bucket"));
    }

    #[test]
    fn test_invalid_scheme() {
        let mut builder = CloudflareR2Builder::new();
        assert!(builder.parse_url("ftp://example.com").is_err());
    }
}
