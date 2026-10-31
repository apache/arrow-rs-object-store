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

#![cfg(feature = "gcp-base")]

use async_trait::async_trait;
use bytes::Bytes;
use object_store::ClientOptions;
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpResponseBody, HttpService,
};
use object_store::gcp::GoogleCloudStorageBuilder;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::Arc;
use tempfile::NamedTempFile;
use url::form_urlencoded;

#[derive(Debug, Clone)]
struct RecordingService {
    requests: Arc<Mutex<Vec<HttpRequest>>>,
}

#[async_trait]
impl HttpService for RecordingService {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        self.requests.lock().push(request);
        Ok(http::Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(HttpResponseBody::from(Bytes::from_static(
                br#"{"access_token":"custom-token","expires_in":3600}"#,
            )))
            .unwrap())
    }
}

#[derive(Debug, Clone)]
struct RecordingConnector {
    service: RecordingService,
}

impl HttpConnector for RecordingConnector {
    fn connect(&self, _options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(self.service.clone()))
    }
}

#[tokio::test]
async fn gcp_base_authorized_user_uses_identity_encoding() {
    let credentials = NamedTempFile::new().unwrap();
    std::fs::write(
        credentials.path(),
        br#"{"type":"authorized_user","client_id":"client","client_secret":"secret","refresh_token":"refresh"}"#,
    )
    .unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let service = RecordingService {
        requests: Arc::clone(&requests),
    };
    let mut default_headers = http::HeaderMap::new();
    default_headers.insert("accept-encoding", "gzip".parse().unwrap());
    let store = GoogleCloudStorageBuilder::new()
        .with_bucket_name("bucket")
        .with_application_credentials(credentials.path().to_str().unwrap())
        .with_client_options(ClientOptions::default().with_default_headers(default_headers))
        .with_http_connector(RecordingConnector { service })
        .build()
        .unwrap();

    let credential = store.credentials().get_credential().await.unwrap();

    assert_eq!(credential.bearer, "custom-token");
    let requests = requests.lock();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method(), http::Method::POST);
    assert_eq!(requests[0].uri().path(), "/o/oauth2/token");
    assert_eq!(
        requests[0].headers().get("accept-encoding").unwrap(),
        "identity"
    );
    assert_eq!(
        requests[0].headers().get("content-type").unwrap(),
        "application/x-www-form-urlencoded"
    );
    let fields = form_urlencoded::parse(requests[0].body().as_bytes().unwrap())
        .into_owned()
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        fields,
        BTreeMap::from([
            ("client_id".to_string(), "client".to_string()),
            ("client_secret".to_string(), "secret".to_string()),
            ("grant_type".to_string(), "refresh_token".to_string()),
            ("refresh_token".to_string(), "refresh".to_string()),
        ])
    );
}
