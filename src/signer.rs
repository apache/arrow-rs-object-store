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

//! Abstraction of signed URL generation for those object store implementations that support it

use crate::{Result, path::Path};
use async_trait::async_trait;
use http::Method;
use std::{fmt, time::Duration};
use url::Url;

/// Universal API to generate presigned URLs from multiple object store services.
#[async_trait]
pub trait Signer: Send + Sync + fmt::Debug + 'static {
    /// Given the intended [`Method`] and [`Path`] to use and the desired length of time for which
    /// the URL should be valid, return a signed [`Url`] created with the object store
    /// implementation's credentials such that the URL can be handed to something that doesn't have
    /// access to the object store's credentials, to allow limited access to the object store.
    async fn signed_url(&self, method: Method, path: &Path, expires_in: Duration) -> Result<Url>;

    /// Like [`Signer::signed_url`], but additionally folds `extra_query` parameters and
    /// `signed_headers` into the signature.
    ///
    /// This is required for presigned requests that carry signed query parameters — for example
    /// signing a multipart `UploadPart` URL with `partNumber` and `uploadId`, or pinning a
    /// `versionId` — and for binding request headers to the signature, such as a checksum
    /// (`x-amz-checksum-sha256`), `content-type`, or server-side-encryption headers.
    ///
    /// Both `extra_query` and `signed_headers` are `(name, value)` pairs. For a presigned URL the
    /// values are fixed at signing time, so the recipient must send exactly these query parameters
    /// and headers, with these values, for the request to be accepted.
    ///
    /// The default implementation delegates to [`Signer::signed_url`] when `extra_query` and
    /// `signed_headers` are both empty, and otherwise returns an error: implementations that do
    /// not support signing additional query parameters or headers must not silently drop them, as
    /// that would produce a URL that does not enforce the requested constraints.
    async fn signed_url_with(
        &self,
        method: Method,
        path: &Path,
        extra_query: &[(String, String)],
        signed_headers: &[(String, String)],
        expires_in: Duration,
    ) -> Result<Url> {
        if extra_query.is_empty() && signed_headers.is_empty() {
            return self.signed_url(method, path, expires_in).await;
        }
        Err(crate::Error::NotSupported {
            source: "this object store does not support signing URLs with additional \
                     query parameters or headers"
                .into(),
        })
    }

    /// Generate signed urls for multiple paths.
    ///
    /// See [`Signer::signed_url`] for more details.
    async fn signed_urls(
        &self,
        method: Method,
        paths: &[Path],
        expires_in: Duration,
    ) -> Result<Vec<Url>> {
        let mut urls = Vec::with_capacity(paths.len());
        for path in paths {
            urls.push(self.signed_url(method.clone(), path, expires_in).await?);
        }
        Ok(urls)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;

    /// A [`Signer`] that only implements the required `signed_url`, relying on the default
    /// `signed_url_with` — mirroring providers such as Azure and GCS.
    #[derive(Debug)]
    struct MinimalSigner;

    #[async_trait]
    impl Signer for MinimalSigner {
        async fn signed_url(
            &self,
            _method: Method,
            path: &Path,
            _expires_in: Duration,
        ) -> Result<Url> {
            Ok(Url::parse(&format!("https://example.com/{path}")).unwrap())
        }
    }

    #[tokio::test]
    async fn default_signed_url_with_delegates_when_empty() {
        let signer = MinimalSigner;
        let url = signer
            .signed_url_with(
                Method::GET,
                &Path::from("file.txt"),
                &[],
                &[],
                Duration::from_secs(60),
            )
            .await
            .unwrap();
        assert_eq!(url.as_str(), "https://example.com/file.txt");
    }

    #[tokio::test]
    async fn default_signed_url_with_rejects_extras() {
        let signer = MinimalSigner;

        let query_err = signer
            .signed_url_with(
                Method::PUT,
                &Path::from("file.txt"),
                &[("partNumber".to_string(), "1".to_string())],
                &[],
                Duration::from_secs(60),
            )
            .await
            .unwrap_err();
        assert!(matches!(query_err, Error::NotSupported { .. }));

        let header_err = signer
            .signed_url_with(
                Method::PUT,
                &Path::from("file.txt"),
                &[],
                &[("content-type".to_string(), "text/plain".to_string())],
                Duration::from_secs(60),
            )
            .await
            .unwrap_err();
        assert!(matches!(header_err, Error::NotSupported { .. }));
    }
}
