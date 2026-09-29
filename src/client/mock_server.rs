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

use crate::client::{HttpResponse, HttpResponseBody};
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};

pub(crate) type ResponseFn =
    Box<dyn FnOnce(Request<Incoming>) -> BoxFuture<'static, HttpResponse> + Send>;

/// A mock server
pub(crate) struct MockServer {
    responses: Arc<Mutex<VecDeque<ResponseFn>>>,
    shutdown: oneshot::Sender<()>,
    handle: JoinHandle<()>,
    url: String,
}

impl MockServer {
    pub(crate) async fn new() -> Self {
        let responses: Arc<Mutex<VecDeque<ResponseFn>>> =
            Arc::new(Mutex::new(VecDeque::with_capacity(10)));

        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = TcpListener::bind(addr).await.unwrap();

        let (shutdown, mut rx) = oneshot::channel::<()>();

        let url = format!("http://{}", listener.local_addr().unwrap());

        let r = Arc::clone(&responses);
        let handle = tokio::spawn(async move {
            let mut set = JoinSet::new();

            loop {
                let (stream, _) = tokio::select! {
                    conn = listener.accept() => conn.unwrap(),
                    _ = &mut rx => break,
                };

                let r = Arc::clone(&r);
                set.spawn(async move {
                    let _ = http1::Builder::new()
                        .serve_connection(
                            TokioIo::new(stream),
                            service_fn(move |req| {
                                let r = Arc::clone(&r);
                                let next = r.lock().pop_front();
                                async move {
                                    Ok::<_, Infallible>(match next {
                                        Some(r) => r(req).await,
                                        None => HttpResponse::new("Hello World".to_string().into()),
                                    })
                                }
                            }),
                        )
                        .await;
                });
            }

            set.abort_all();
        });

        Self {
            responses,
            shutdown,
            handle,
            url,
        }
    }

    /// The url of the mock server
    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    /// Add a response
    pub(crate) fn push<B: Into<HttpResponseBody>>(&self, response: Response<B>) {
        let resp = response.map(Into::into);
        self.push_fn(|_| resp)
    }

    /// Add a response function
    pub(crate) fn push_fn<F, B>(&self, f: F)
    where
        F: FnOnce(Request<Incoming>) -> Response<B> + Send + 'static,
        B: Into<HttpResponseBody>,
    {
        let f = Box::new(|req| async move { f(req).map(Into::into) }.boxed());
        self.responses.lock().push_back(f)
    }

    pub(crate) fn push_async_fn<F, Fut>(&self, f: F)
    where
        F: FnOnce(Request<Incoming>) -> Fut + Send + 'static,
        Fut: Future<Output = Response<String>> + Send + 'static,
    {
        let f = Box::new(|r| f(r).map(|b| b.map(Into::into)).boxed());
        self.responses.lock().push_back(f)
    }

    /// Shutdown the mock server
    pub(crate) async fn shutdown(self) {
        let _ = self.shutdown.send(());
        self.handle.await.unwrap()
    }
}

/// A request answered by [`MockServer::push_recorded`].
#[cfg(any(feature = "aws-base", feature = "azure-base", feature = "gcp-base"))]
#[derive(Debug)]
pub(crate) struct RecordedRequest {
    pub(crate) method: http::Method,
    pub(crate) path: String,
    pub(crate) query: Option<String>,
    pub(crate) headers: http::HeaderMap,
    pub(crate) body: bytes::Bytes,
}

/// The requests answered by [`MockServer::push_recorded`], in arrival order.
///
/// `MockServer` swallows panics raised in response handlers, so assert on these in the test body.
#[cfg(any(feature = "aws-base", feature = "azure-base", feature = "gcp-base"))]
#[derive(Debug, Default, Clone)]
pub(crate) struct RequestLog(Arc<Mutex<Vec<RecordedRequest>>>);

#[cfg(any(feature = "aws-base", feature = "azure-base", feature = "gcp-base"))]
impl RequestLog {
    /// Remove and return every recorded request.
    pub(crate) fn take(&self) -> Vec<RecordedRequest> {
        std::mem::take(&mut *self.0.lock())
    }

    /// Remove and return the only recorded request.
    pub(crate) fn single(&self) -> RecordedRequest {
        let mut requests = self.take();
        assert_eq!(requests.len(), 1, "expected one request: {requests:?}");
        requests.pop().unwrap()
    }
}

#[cfg(any(feature = "aws-base", feature = "azure-base", feature = "gcp-base"))]
impl MockServer {
    /// Answer the next request with an empty `status` response, recording it into `log`.
    pub(crate) fn push_recorded(&self, log: &RequestLog, status: u16) {
        use http_body_util::BodyExt;

        let log = log.clone();
        self.push_async_fn(move |req| async move {
            let (parts, body) = req.into_parts();
            let body = body.collect().await.unwrap().to_bytes();
            log.0.lock().push(RecordedRequest {
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
}
