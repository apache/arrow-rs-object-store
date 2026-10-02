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

use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};

const PAYLOAD: &[u8] = b"downstream feature matrix";

#[cfg(feature = "custom-http")]
mod transport {
    use async_trait::async_trait;
    use http_body_util::BodyExt;
    use object_store::client::{
        ClientOptions, HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse,
        HttpResponseBody, HttpService,
    };
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Clone, Default)]
    pub struct Connector(pub Arc<Mutex<Vec<Captured>>>);

    #[derive(Debug)]
    pub struct Captured {
        pub authorization: String,
        pub payload: Vec<u8>,
    }

    impl HttpConnector for Connector {
        fn connect(&self, _: &ClientOptions) -> object_store::Result<HttpClient> {
            Ok(HttpClient::new(self.clone()))
        }
    }

    #[async_trait]
    impl HttpService for Connector {
        async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
            let (parts, mut body) = request.into_parts();
            assert_eq!(parts.method, http::Method::PUT);
            assert_eq!(parts.uri.path(), "/fixture-bucket/object");
            let authorization = parts.headers["authorization"].to_str().unwrap().to_owned();
            let mut payload = Vec::new();
            while let Some(frame) = body.frame().await {
                payload.extend_from_slice(&frame?.into_data().unwrap());
            }
            self.0.lock().unwrap().push(Captured {
                authorization,
                payload,
            });
            Ok(http::Response::builder()
                .header("etag", "\"fixture-etag\"")
                .body(HttpResponseBody::from(Vec::new()))
                .unwrap())
        }
    }
}

#[cfg(not(feature = "custom-http"))]
mod transport {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread::{self, JoinHandle};

    pub fn start() -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let handle = thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(std::time::Instant::now() < deadline, "no HTTP connection");
                        thread::sleep(std::time::Duration::from_millis(10));
                    }
                    Err(error) => panic!("HTTP accept failed: {error}"),
                }
            };
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 4096];
            while !request
                .windows(super::PAYLOAD.len())
                .any(|part| part == super::PAYLOAD)
            {
                let count = stream.read(&mut buffer).unwrap();
                assert!(count > 0, "request ended before the PUT payload");
                request.extend_from_slice(&buffer[..count]);
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("PUT /fixture-bucket/object HTTP/1.1"));
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("authorization: aws4-hmac-sha256")
            );
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nETag: \"fixture-etag\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .unwrap();
        });
        (endpoint, handle)
    }
}

#[cfg(feature = "custom-crypto")]
mod crypto {
    use hmac::{Hmac, Mac};
    use object_store::client::{
        CryptoProvider, DigestAlgorithm, DigestContext, HmacContext, Signer, SigningAlgorithm,
    };
    use sha2::{Digest, Sha256};

    #[derive(Debug)]
    pub struct Provider;

    fn unsupported() -> object_store::Error {
        object_store::Error::Generic {
            store: "feature-matrix",
            source: Box::new(std::io::Error::other("unsupported signing algorithm")),
        }
    }

    impl CryptoProvider for Provider {
        fn digest(
            &self,
            algorithm: DigestAlgorithm,
        ) -> object_store::Result<Box<dyn DigestContext>> {
            match algorithm {
                DigestAlgorithm::Sha256 => {
                    Ok(Box::new(DigestState(Some(Sha256::new()), Vec::new())))
                }
                _ => Err(unsupported()),
            }
        }

        fn hmac(
            &self,
            algorithm: DigestAlgorithm,
            secret: &[u8],
        ) -> object_store::Result<Box<dyn HmacContext>> {
            match algorithm {
                DigestAlgorithm::Sha256 => Ok(Box::new(HmacState(
                    Some(<Hmac<Sha256> as Mac>::new_from_slice(secret).unwrap()),
                    Vec::new(),
                ))),
                _ => Err(unsupported()),
            }
        }

        fn sign(&self, _: SigningAlgorithm, _: &[u8]) -> object_store::Result<Box<dyn Signer>> {
            Err(unsupported())
        }
    }

    struct DigestState(Option<Sha256>, Vec<u8>);

    impl DigestContext for DigestState {
        fn update(&mut self, data: &[u8]) {
            self.0.as_mut().unwrap().update(data);
        }

        fn finish(&mut self) -> object_store::Result<&[u8]> {
            if let Some(context) = self.0.take() {
                self.1 = context.finalize().to_vec();
            }
            Ok(&self.1)
        }
    }

    struct HmacState(Option<Hmac<Sha256>>, Vec<u8>);

    impl HmacContext for HmacState {
        fn update(&mut self, data: &[u8]) {
            self.0.as_mut().unwrap().update(data);
        }

        fn finish(&mut self) -> object_store::Result<&[u8]> {
            if let Some(context) = self.0.take() {
                self.1 = context.finalize().into_bytes().to_vec();
            }
            Ok(&self.1)
        }
    }
}

pub async fn run() {
    #[cfg(feature = "custom-http")]
    let (endpoint, connector) = (
        "http://fixture.invalid".to_owned(),
        transport::Connector::default(),
    );
    #[cfg(not(feature = "custom-http"))]
    let (endpoint, server) = transport::start();

    let builder = AmazonS3Builder::new()
        .with_bucket_name("fixture-bucket")
        .with_region("us-east-1")
        .with_access_key_id("fixture-key")
        .with_secret_access_key("fixture-secret")
        .with_endpoint(endpoint)
        .with_allow_http(true)
        .with_unsigned_payload(false);
    #[cfg(feature = "custom-http")]
    let builder = builder.with_http_connector(connector.clone());
    #[cfg(feature = "custom-crypto")]
    let builder = builder.with_crypto_provider(std::sync::Arc::new(crypto::Provider));
    let store = builder.build().unwrap();
    store
        .put(&Path::from("object"), PutPayload::from_static(PAYLOAD))
        .await
        .unwrap();

    #[cfg(feature = "custom-http")]
    {
        let requests = connector.0.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].authorization.starts_with("AWS4-HMAC-SHA256"));
        assert_eq!(requests[0].payload, PAYLOAD);
    }
    #[cfg(not(feature = "custom-http"))]
    server.join().unwrap();
}
