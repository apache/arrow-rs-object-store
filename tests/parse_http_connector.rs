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

#![cfg(feature = "cloud-base")]

use object_store::client::HttpConnector;
use object_store::client::{HttpClient, HttpError, HttpRequest, HttpResponse, HttpService};
use object_store::{ClientConfigKey, ClientOptions, ObjectStoreExt};
use object_store::{parse_url_opts, parse_url_opts_with_http_connector};
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Debug, Default)]
struct Connector {
    options: Mutex<Vec<Option<String>>>,
    requests: Arc<Mutex<Vec<http::Uri>>>,
}

impl HttpConnector for Connector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        self.options
            .lock()
            .unwrap()
            .push(options.get_config_value(&ClientConfigKey::UserAgent));
        Ok(HttpClient::new(Service(self.requests.clone())))
    }
}

#[derive(Debug)]
struct Service(Arc<Mutex<Vec<http::Uri>>>);

#[async_trait::async_trait]
impl HttpService for Service {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        self.0.lock().unwrap().push(request.uri().clone());
        Ok(http::Response::builder()
            .status(403)
            .body(bytes::Bytes::new().into())
            .unwrap())
    }
}

#[tokio::test]
async fn test_resolver_connector_provider_requests() {
    let urls: &[&str] = &[
        #[cfg(feature = "aws-base")]
        "s3://bucket/a%20b",
        #[cfg(feature = "aws-base")]
        "s3a://bucket/a%20b",
        #[cfg(feature = "aws-base")]
        "https://s3.us-east-1.amazonaws.com/bucket/a%20b",
        #[cfg(feature = "aws-base")]
        "https://bucket.s3.us-east-1.amazonaws.com/a%20b",
        #[cfg(feature = "aws-base")]
        "https://account.r2.cloudflarestorage.com/bucket/a%20b",
        #[cfg(feature = "gcp-base")]
        "gs://bucket/a%20b",
        #[cfg(feature = "azure-base")]
        "az://container/a%20b",
        #[cfg(feature = "azure-base")]
        "abfss://container@account.dfs.core.windows.net/a%20b",
        #[cfg(feature = "azure-base")]
        "https://account.blob.core.windows.net/container/a%20b",
        #[cfg(feature = "http-base")]
        "https://example.com/a%20b",
    ];
    for url in urls {
        let connector = Arc::new(Connector::default());
        let options = [
            ("USER_AGENT", "test-parser"),
            ("SKIP_SIGNATURE", "true"),
            ("AWS_REGION", "us-east-1"),
            ("AWS_ACCESS_KEY_ID", "key"),
            ("AWS_SECRET_ACCESS_KEY", "secret"),
            ("AZURE_STORAGE_ACCOUNT_NAME", "account"),
            ("AZURE_STORAGE_TOKEN", "token"),
            (
                "GOOGLE_SERVICE_ACCOUNT_KEY",
                r#"{"private_key":"","private_key_id":"","client_email":"","disable_oauth":true}"#,
            ),
            ("unknown_option", "ignored"),
        ];
        let parsed = url.parse().unwrap();
        let (store, path) =
            parse_url_opts_with_http_connector(&parsed, options, Some(connector.clone())).unwrap();
        assert_eq!(path.as_ref(), "a b", "{url}");
        match (
            parse_url_opts(&parsed, options),
            parse_url_opts_with_http_connector(&parsed, options, None),
        ) {
            (Ok((a, ap)), Ok((b, bp))) => {
                assert_eq!(a.to_string(), b.to_string());
                assert_eq!(a.to_string(), store.to_string());
                assert_eq!(ap, bp);
                assert_eq!(ap, path);
            }
            (Err(a), Err(b)) => assert_eq!(a.to_string(), b.to_string()),
            result => panic!("default resolver results differ for {url}: {result:?}"),
        }
        assert!(store.get(&path).await.is_err(), "{url}");
        let requests = connector.requests.lock().unwrap();
        assert_eq!(requests.len(), 1, "{url}");
        assert!(requests[0].path().ends_with("/a%20b"), "{url}");
        assert!(
            connector
                .options
                .lock()
                .unwrap()
                .iter()
                .any(|value| value.as_deref() == Some("test-parser")),
            "{url}"
        );
    }
}

#[tokio::test]
async fn test_resolver_connector_local_memory_and_errors() {
    let connector = Arc::new(Connector::default());
    let urls = [
        "memory:///a%20b",
        #[cfg(all(feature = "fs", not(target_arch = "wasm32")))]
        "file:///a%20b",
        "file://remote/path",
        "memory://remote/",
        "unix:/run/foo.socket",
        #[cfg(not(feature = "aws-base"))]
        "s3://bucket/path",
        #[cfg(not(feature = "gcp-base"))]
        "gs://bucket/path",
        #[cfg(not(feature = "azure-base"))]
        "az://container/path",
        #[cfg(not(feature = "http-base"))]
        "https://example.com/path",
    ];
    for url in urls {
        let url = url.parse().unwrap();
        let options = std::iter::empty::<(&str, &str)>;
        let legacy = parse_url_opts(&url, options());
        let default = parse_url_opts_with_http_connector(&url, options(), None);
        let custom = parse_url_opts_with_http_connector(&url, options(), Some(connector.clone()));
        match (legacy, default, custom) {
            (Ok((a, ap)), Ok((b, bp)), Ok((c, cp))) => {
                assert_eq!(a.to_string(), b.to_string());
                assert_eq!(a.to_string(), c.to_string());
                assert_eq!(ap, bp);
                assert_eq!(ap, cp);
                if url.scheme() == "memory" {
                    a.put(&ap, bytes::Bytes::from_static(b"value").into())
                        .await
                        .unwrap();
                    assert!(c.get(&cp).await.is_err());
                }
            }
            (Err(a), Err(b), Err(c)) => {
                assert_eq!(a.to_string(), b.to_string());
                assert_eq!(a.to_string(), c.to_string());
            }
            result => panic!("resolver results differ for {url}: {result:?}"),
        }
    }
    assert!(connector.options.lock().unwrap().is_empty());
    assert!(connector.requests.lock().unwrap().is_empty());
}
