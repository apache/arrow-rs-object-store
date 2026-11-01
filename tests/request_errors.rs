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

#![cfg(all(feature = "aws", feature = "azure", feature = "gcp", feature = "http"))]

use async_trait::async_trait;
use object_store::ClientOptions;
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse, HttpService,
};
use std::error::Error;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn find_error<'a, T: Error + 'static>(mut error: &'a (dyn Error + 'static)) -> Option<&'a T> {
    loop {
        if let Some(error) = error.downcast_ref::<T>() {
            return Some(error);
        }
        error = error.source()?;
    }
}

fn assert_invalid_request(error: &object_store::Error) {
    let http_error = find_error::<HttpError>(error).expect("HttpError missing from error chain");
    assert_eq!(http_error.kind(), HttpErrorKind::InvalidRequest, "{error}");
}

#[derive(Debug, Clone)]
struct NoNetwork(Arc<AtomicUsize>);

#[async_trait]
impl HttpService for NoNetwork {
    async fn call(&self, _: HttpRequest) -> Result<HttpResponse, HttpError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(http::Response::builder()
            .status(400)
            .body(String::from("test transport").into())
            .unwrap())
    }
}

impl HttpConnector for NoNetwork {
    fn connect(&self, _: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(self.clone()))
    }
}

mod requests {
    use super::*;
    use futures_util::TryStreamExt;
    use object_store::{Attribute, Attributes, ObjectStore, ObjectStoreExt, PutOptions};

    fn build_store(
        provider: &str,
        endpoint: &str,
        signed: bool,
        bad_token: bool,
        transport: NoNetwork,
    ) -> object_store::Result<Box<dyn ObjectStore>> {
        let token = if bad_token { "test\ninvalid" } else { "test" };
        match provider {
            "s3" => object_store::aws::AmazonS3Builder::new()
                .with_bucket_name("bucket")
                .with_region("us-east-1")
                .with_access_key_id("test")
                .with_secret_access_key("test")
                .with_token(token)
                .with_endpoint(endpoint)
                .with_allow_http(true)
                .with_virtual_hosted_style_request(true)
                .with_skip_signature(!signed)
                .with_http_connector(transport)
                .build()
                .map(|s| Box::new(s) as _),
            "gcs" => object_store::gcp::GoogleCloudStorageBuilder::new()
                .with_bucket_name("bucket")
                .with_base_url(endpoint)
                .with_bearer_token(token)
                .with_skip_signature(!signed)
                .with_client_options(ClientOptions::new().with_allow_http(true))
                .with_http_connector(transport)
                .build()
                .map(|s| Box::new(s) as _),
            "azure" => object_store::azure::MicrosoftAzureBuilder::new()
                .with_account("account")
                .with_container_name("container")
                .with_endpoint(endpoint.into())
                .with_bearer_token_authorization(token)
                .with_allow_http(true)
                .with_skip_signature(!signed)
                .with_http_connector(transport)
                .build()
                .map(|s| Box::new(s) as _),
            "http" => object_store::http::HttpBuilder::new()
                .with_url(endpoint)
                .with_client_options(ClientOptions::new().with_allow_http(true))
                .with_http_connector(transport)
                .build()
                .map(|s| Box::new(s) as _),
            _ => unreachable!(),
        }
    }

    async fn check_request(provider: &str, endpoint: &str, signed: bool, op: &str) {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut stage = "build";
        let ret = (async {
            let store = build_store(
                provider,
                endpoint,
                signed,
                op == "bad_token",
                NoNetwork(calls.clone()),
            )?;
            stage = "operation";
            match op {
                "list" => store.list(None).try_collect::<Vec<_>>().await.map(|_| ()),
                "bad_header" => {
                    let mut attributes = Attributes::new();
                    attributes.insert(Attribute::ContentType, "test\ninvalid".into());
                    store
                        .put_opts(
                            &"key".into(),
                            "data".into(),
                            PutOptions {
                                attributes,
                                ..Default::default()
                            },
                        )
                        .await
                        .map(|_| ())
                }
                "get" | "bad_token" => store.get(&"key".into()).await.map(|_| ()),
                _ => unreachable!("unknown operation: {op}"),
            }
        })
        .await;
        let error = ret.expect_err("expected request failure");
        // The custom transport accepts relative URIs. Signed S3 requests reject
        // them earlier because the authorizer requires an absolute URL.
        let expected_calls = usize::from(
            endpoint == "http://bucket.example.invalid" && !op.starts_with("bad_")
                || provider == "gcs" && endpoint == "/relative"
                || provider == "s3" && endpoint == "/relative" && !signed,
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            expected_calls,
            "{provider} {endpoint} {op} {stage}"
        );
        if expected_calls == 1 {
            assert!(
                error
                    .to_string()
                    .contains("400 Bad Request: test transport"),
                "{error}"
            );
            assert!(find_error::<HttpError>(&error).is_none());
        } else if stage == "operation" {
            assert_invalid_request(&error);
        } else {
            // Invalid endpoints rejected at build time have provider-specific errors.
            assert!(find_error::<HttpError>(&error).is_none());
        }
    }

    #[tokio::test]
    async fn test_invalid_requests() {
        for provider in ["s3", "gcs", "azure", "http"] {
            for endpoint in [
                "bucket.example.invalid",
                "127.0.0.1:18764",
                "http://[",
                "/relative",
                "data:text/plain,test",
                "mailto:user@example.invalid",
                "http://bucket.example.invalid",
            ] {
                for signed in [true, false] {
                    if provider == "http" && signed {
                        continue;
                    }
                    for op in ["list", "get"] {
                        check_request(provider, endpoint, signed, op).await;
                    }
                }
            }
        }
        for provider in ["s3", "gcs", "azure", "http"] {
            for signed in [true, false] {
                if provider == "http" && signed {
                    continue;
                }
                check_request(
                    provider,
                    "http://bucket.example.invalid",
                    signed,
                    "bad_header",
                )
                .await;
            }
            if provider != "http" {
                check_request(provider, "http://bucket.example.invalid", true, "bad_token").await;
            }
        }
    }
}

mod signing {
    use super::*;
    use object_store::{Attribute, Attributes, ObjectStore, ObjectStoreExt, PutOptions};

    async fn error_without_transport<T>(
        name: &str,
        calls: &AtomicUsize,
        future: impl std::future::Future<Output = object_store::Result<T>>,
    ) -> object_store::Error {
        let error = future.await.err().expect("expected signing failure");
        assert_eq!(calls.load(Ordering::SeqCst), 0, "{name}");
        error
    }

    #[tokio::test]
    async fn test_invalid_signing() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut headers = http::HeaderMap::new();
        headers.insert("x-test", http::HeaderValue::from_bytes(&[0xff]).unwrap());
        let aws = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name("bucket")
            .with_region("us-east-1")
            .with_access_key_id("test")
            .with_secret_access_key("test")
            .with_endpoint("http://bucket.example.invalid")
            .with_virtual_hosted_style_request(true)
            .with_client_options(
                ClientOptions::new()
                    .with_allow_http(true)
                    .with_default_headers(headers),
            )
            .with_http_connector(NoNetwork(calls.clone()))
            .build()
            .unwrap();
        let error =
            error_without_transport("s3_default_header_non_utf8", &calls, aws.get(&"key".into()))
                .await;
        assert_invalid_request(&error);
        let azure = object_store::azure::MicrosoftAzureBuilder::new()
            .with_account("account")
            .with_container_name("container")
            .with_endpoint("http://bucket.example.invalid".into())
            .with_access_key("dGVzdA==")
            .with_allow_http(true)
            .with_http_connector(NoNetwork(calls.clone()))
            .build()
            .unwrap();
        let mut attributes = Attributes::new();
        attributes.insert(Attribute::Metadata("label".into()), "中文".into());
        let error = error_without_transport(
            "azure_shared_key_unicode_metadata",
            &calls,
            azure.put_opts(
                &"key".into(),
                "data".into(),
                PutOptions {
                    attributes,
                    ..Default::default()
                },
            ),
        )
        .await;
        assert_invalid_request(&error);
    }
}

mod transport {
    use super::*;
    use futures_util::TryStreamExt;
    use object_store::{ObjectStore, ObjectStoreExt};

    #[tokio::test]
    async fn test_reqwest_invalid_uri() {
        let gcs_relative = object_store::gcp::GoogleCloudStorageBuilder::new()
            .with_bucket_name("bucket")
            .with_base_url("/relative")
            .with_bearer_token("test")
            .with_skip_signature(true)
            .with_client_options(ClientOptions::new().with_allow_http(true))
            .build()
            .unwrap();
        let error = gcs_relative.get(&"key".into()).await.unwrap_err();
        assert_invalid_request(&error);
        let error = gcs_relative
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert_invalid_request(&error);
    }
}
