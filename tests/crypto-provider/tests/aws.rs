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

use std::error::Error as StdError;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use http::{Method, request::Parts};
use http_body_util::{BodyExt, Full};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::client::{
    ClientOptions, CryptoProvider, DigestAlgorithm, DigestContext, HmacContext, HttpClient,
    HttpConnector, HttpError, HttpRequest, HttpResponse, HttpResponseBody, HttpService, Signer,
    SigningAlgorithm,
};
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};
use ring::{digest, hmac};

const KEY_ID: &str = "fixture-key";
const SECRET: &str = "fixture-secret";
const REGION: &str = "us-east-1";
const SHA256_ABC: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
const SENTINEL: &str = "external crypto finish sentinel";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Digest,
    Hmac,
}

#[derive(Debug, Clone)]
struct Trace {
    kind: Kind,
    key: Vec<u8>,
    chunks: Vec<Vec<u8>>,
    output: Option<Vec<u8>>,
}

#[derive(Debug, Default)]
struct State {
    traces: Mutex<Vec<Trace>>,
    rsa_calls: AtomicUsize,
}

#[derive(Debug, Default)]
struct ExternalRing {
    state: Arc<State>,
    fail_finish: Option<Kind>,
}

fn crypto_error(message: &'static str) -> object_store::Error {
    object_store::Error::Generic {
        store: "external-ring-fixture",
        source: Box::new(std::io::Error::other(message)),
    }
}

impl ExternalRing {
    fn trace(&self, kind: Kind, key: &[u8]) -> usize {
        let mut traces = self.state.traces.lock().unwrap();
        let index = traces.len();
        traces.push(Trace {
            kind,
            key: key.to_vec(),
            chunks: Vec::new(),
            output: None,
        });
        index
    }
}

impl CryptoProvider for ExternalRing {
    fn digest(&self, algorithm: DigestAlgorithm) -> object_store::Result<Box<dyn DigestContext>> {
        match algorithm {
            DigestAlgorithm::Sha256 => Ok(Box::new(ExternalDigest {
                context: Some(digest::Context::new(&digest::SHA256)),
                output: None,
                state: self.state.clone(),
                index: self.trace(Kind::Digest, &[]),
                fail: self.fail_finish == Some(Kind::Digest),
            })),
            _ => Err(crypto_error("unsupported digest algorithm")),
        }
    }

    fn hmac(
        &self,
        algorithm: DigestAlgorithm,
        secret: &[u8],
    ) -> object_store::Result<Box<dyn HmacContext>> {
        match algorithm {
            DigestAlgorithm::Sha256 => Ok(Box::new(ExternalHmac {
                context: Some(hmac::Context::with_key(&hmac::Key::new(
                    hmac::HMAC_SHA256,
                    secret,
                ))),
                output: None,
                state: self.state.clone(),
                index: self.trace(Kind::Hmac, secret),
                fail: self.fail_finish == Some(Kind::Hmac),
            })),
            _ => Err(crypto_error("unsupported HMAC algorithm")),
        }
    }

    fn sign(
        &self,
        _algorithm: SigningAlgorithm,
        _pem: &[u8],
    ) -> object_store::Result<Box<dyn Signer>> {
        self.state.rsa_calls.fetch_add(1, Ordering::SeqCst);
        Err(crypto_error("RSA is outside this fixture"))
    }
}

struct ExternalDigest {
    context: Option<digest::Context>,
    output: Option<digest::Digest>,
    state: Arc<State>,
    index: usize,
    fail: bool,
}

impl DigestContext for ExternalDigest {
    fn update(&mut self, data: &[u8]) {
        self.context.as_mut().unwrap().update(data);
        self.state.traces.lock().unwrap()[self.index]
            .chunks
            .push(data.to_vec());
    }

    fn finish(&mut self) -> object_store::Result<&[u8]> {
        if self.fail {
            return Err(crypto_error(SENTINEL));
        }
        let output = self.output.insert(self.context.take().unwrap().finish());
        self.state.traces.lock().unwrap()[self.index].output = Some(output.as_ref().to_vec());
        Ok(digest::Digest::as_ref(output))
    }
}

struct ExternalHmac {
    context: Option<hmac::Context>,
    output: Option<hmac::Tag>,
    state: Arc<State>,
    index: usize,
    fail: bool,
}

impl HmacContext for ExternalHmac {
    fn update(&mut self, data: &[u8]) {
        self.context.as_mut().unwrap().update(data);
        self.state.traces.lock().unwrap()[self.index]
            .chunks
            .push(data.to_vec());
    }

    fn finish(&mut self) -> object_store::Result<&[u8]> {
        if self.fail {
            return Err(crypto_error(SENTINEL));
        }
        let output = self.output.insert(self.context.take().unwrap().sign());
        self.state.traces.lock().unwrap()[self.index].output = Some(output.as_ref().to_vec());
        Ok(hmac::Tag::as_ref(output))
    }
}

#[derive(Debug)]
struct Captured {
    parts: Parts,
    chunks: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, Default)]
struct MemoryHttp(Arc<Mutex<Vec<Captured>>>);

impl HttpConnector for MemoryHttp {
    fn connect(&self, _options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(self.clone()))
    }
}

#[async_trait]
impl HttpService for MemoryHttp {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        let (parts, mut body) = request.into_parts();
        let mut chunks = Vec::new();
        while let Some(frame) = body.frame().await {
            chunks.push(frame?.into_data().unwrap().to_vec());
        }
        self.0.lock().unwrap().push(Captured { parts, chunks });
        Ok(http::Response::builder()
            .header("etag", "\"fixture-etag\"")
            .body(HttpResponseBody::new(
                Full::new(Bytes::new()).map_err(|never| match never {}),
            ))
            .unwrap())
    }
}

fn store(http: MemoryHttp, crypto: Option<Arc<dyn CryptoProvider>>) -> AmazonS3 {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name("fixture-bucket")
        .with_region(REGION)
        .with_access_key_id(KEY_ID)
        .with_secret_access_key(SECRET)
        .with_endpoint("https://s3.fixture.invalid:9000")
        .with_unsigned_payload(false)
        .with_http_connector(http);
    if let Some(crypto) = crypto {
        builder = builder.with_crypto_provider(crypto);
    }
    builder.build().unwrap()
}

fn payload() -> PutPayload {
    [Bytes::from_static(b"a"), Bytes::from_static(b"bc")]
        .into_iter()
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn assert_signed_put(request: &Captured) -> (Vec<u8>, Vec<u8>) {
    let parts = &request.parts;
    assert_eq!(parts.method, Method::PUT);
    assert_eq!(
        parts.uri.authority().unwrap().as_str(),
        "s3.fixture.invalid:9000"
    );
    assert_eq!(parts.uri.path(), "/fixture-bucket/object");
    assert!(parts.uri.query().is_none());
    assert_eq!(request.chunks, [b"a".to_vec(), b"bc".to_vec()]);
    assert!(parts.headers.get("host").is_none());
    assert_eq!(parts.headers["content-length"], "3");
    assert_eq!(parts.headers["x-amz-content-sha256"], SHA256_ABC);
    let date = parts.headers["x-amz-date"].to_str().unwrap();
    assert_eq!(date.len(), 16);
    let scope = format!("{}/{REGION}/s3/aws4_request", &date[..8]);
    let auth = parts.headers["authorization"].to_str().unwrap();
    let fields = auth.split(", ").collect::<Vec<_>>();
    assert_eq!(fields.len(), 3);
    assert_eq!(
        fields[0],
        format!("AWS4-HMAC-SHA256 Credential={KEY_ID}/{scope}")
    );
    let signed = fields[1].strip_prefix("SignedHeaders=").unwrap();
    let names = signed.split(';').collect::<Vec<_>>();
    let mut sorted = names.clone();
    sorted.sort_unstable();
    assert_eq!(names, sorted);
    assert!(names.contains(&"host"));
    assert!(names.contains(&"x-amz-date"));
    let canonical_headers = names
        .iter()
        .map(|name| {
            let value = if *name == "host" {
                parts.uri.authority().unwrap().as_str().to_owned()
            } else {
                let values = parts
                    .headers
                    .get_all(*name)
                    .iter()
                    .map(|value| {
                        value
                            .to_str()
                            .unwrap()
                            .split_whitespace()
                            .collect::<Vec<_>>()
                            .join(" ")
                    })
                    .collect::<Vec<_>>();
                assert!(!values.is_empty());
                values.join(",")
            };
            format!("{name}:{value}\n")
        })
        .collect::<String>();
    let canonical = format!(
        "{}\n{}\n\n{canonical_headers}\n{signed}\n{SHA256_ABC}",
        parts.method,
        parts.uri.path()
    );
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{date}\n{scope}\n{}",
        hex(digest::digest(&digest::SHA256, canonical.as_bytes()).as_ref())
    );
    let mut key = format!("AWS4{SECRET}").into_bytes();
    for value in [&date[..8], REGION, "s3", "aws4_request"] {
        key = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, &key), value.as_bytes())
            .as_ref()
            .to_vec();
    }
    let signature = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, &key), to_sign.as_bytes());
    assert_eq!(fields[2], format!("Signature={}", hex(signature.as_ref())));
    (canonical.into_bytes(), to_sign.into_bytes())
}

#[test]
fn external_provider_known_vectors() {
    let crypto = ExternalRing::default();
    let mut digest = crypto.digest(DigestAlgorithm::Sha256).unwrap();
    digest.update(b"a");
    digest.update(b"bc");
    assert_eq!(hex(digest.finish().unwrap()), SHA256_ABC);
    let mut hmac = crypto.hmac(DigestAlgorithm::Sha256, b"Jefe").unwrap();
    hmac.update(b"what do ya ");
    hmac.update(b"want for nothing?");
    assert_eq!(
        hex(hmac.finish().unwrap()),
        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
    );
    assert_eq!(crypto.state.rsa_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn custom_provider_signs_public_put() {
    let http = MemoryHttp::default();
    let crypto = Arc::new(ExternalRing::default());
    let result = store(http.clone(), Some(crypto.clone()))
        .put(&Path::from("object"), payload())
        .await
        .unwrap();
    assert_eq!(result.e_tag.as_deref(), Some("\"fixture-etag\""));
    let captured = http.0.lock().unwrap();
    assert_eq!(captured.len(), 1);
    let (canonical, to_sign) = assert_signed_put(&captured[0]);
    let traces = crypto.state.traces.lock().unwrap();
    let digests = traces
        .iter()
        .filter(|trace| trace.kind == Kind::Digest)
        .collect::<Vec<_>>();
    assert_eq!(digests.len(), 2);
    assert_eq!(digests[0].chunks, [b"a".to_vec(), b"bc".to_vec()]);
    assert_eq!(hex(digests[0].output.as_ref().unwrap()), SHA256_ABC);
    assert_eq!(digests[1].chunks, [canonical]);
    let hmacs = traces
        .iter()
        .filter(|trace| trace.kind == Kind::Hmac)
        .collect::<Vec<_>>();
    assert_eq!(hmacs.len(), 5);
    assert_eq!(hmacs[4].chunks, [to_sign]);
    for trace in hmacs {
        let expected = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, &trace.key),
            &trace.chunks.concat(),
        );
        assert_eq!(trace.output.as_ref().unwrap().as_slice(), expected.as_ref());
    }
    assert_eq!(crypto.state.rsa_calls.load(Ordering::SeqCst), 0);
}

async fn assert_finish_error(kind: Kind) {
    let http = MemoryHttp::default();
    let crypto = Arc::new(ExternalRing {
        fail_finish: Some(kind),
        ..Default::default()
    });
    let error = store(http.clone(), Some(crypto.clone()))
        .put(&Path::from("object"), payload())
        .await
        .unwrap_err();
    let mut source: Option<&dyn StdError> = Some(&error);
    let mut found = false;
    while let Some(error) = source {
        found |= error.to_string() == SENTINEL;
        source = error.source();
    }
    assert!(found);
    assert!(http.0.lock().unwrap().is_empty());
    let traces = crypto.state.traces.lock().unwrap();
    assert!(
        traces
            .iter()
            .any(|trace| trace.kind == kind && trace.output.is_none())
    );
    assert_eq!(crypto.state.rsa_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn digest_finish_error_prevents_http() {
    assert_finish_error(Kind::Digest).await;
}

#[tokio::test]
async fn hmac_finish_error_prevents_http() {
    assert_finish_error(Kind::Hmac).await;
}

#[cfg(not(any(feature = "bundled-ring", feature = "default-aws")))]
#[tokio::test]
async fn aws_base_without_crypto_prevents_http() {
    let http = MemoryHttp::default();
    let error = store(http.clone(), None)
        .put(&Path::from("object"), payload())
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Must enable aws-lc-rs, ring, or specify custom CryptoProvider")
    );
    assert!(http.0.lock().unwrap().is_empty());
}

#[cfg(any(feature = "bundled-ring", feature = "default-aws"))]
#[tokio::test]
async fn bundled_crypto_signs_public_put() {
    let http = MemoryHttp::default();
    let result = store(http.clone(), None)
        .put(&Path::from("object"), payload())
        .await
        .unwrap();
    assert_eq!(result.e_tag.as_deref(), Some("\"fixture-etag\""));
    let captured = http.0.lock().unwrap();
    assert_eq!(captured.len(), 1);
    assert_signed_put(&captured[0]);
}
