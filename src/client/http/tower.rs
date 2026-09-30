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

//! Adapt a [`tower::Service`] into an [`HttpConnector`].
//!
//! This lets users compose anything from `tower-http` (`TraceLayer`,
//! `TimeoutLayer`, `RetryLayer`, …), `tower-otel-*`, or any other
//! `tower::Service<http::Request<_>>` against `object_store`.
//!
//! ```ignore
//! use object_store::client::TowerHttpConnector;
//! use object_store::ClientOptions;
//! use tower::ServiceBuilder;
//! use tower_http::trace::TraceLayer;
//!
//! let make = move |_opts: &ClientOptions| {
//!     // build a `tower::Service<http::Request<HttpRequestBody>,
//!     //                       Response = http::Response<HttpResponseBody>>`
//!     // — typically by wrapping a `reqwest::Client` or `hyper::Client`.
//!     let inner = my_reqwest_tower_adapter()?;
//!     Ok(ServiceBuilder::new()
//!         .layer(TraceLayer::new_for_http())
//!         .service(inner))
//! };
//! HttpBuilder::new()
//!     .with_http_connector(TowerHttpConnector(make))
//!     .build();
//! ```

use crate::ClientOptions;
use crate::client::{
    HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse, HttpService,
};
use async_trait::async_trait;
use std::error::Error as StdError;
use std::sync::Mutex;
use tower::{Service, ServiceExt};

/// Adapt a `tower::Service`-builder closure into an [`HttpConnector`].
///
/// `connect` is invoked once per [`HttpClient::new`] with the resolved
/// [`ClientOptions`] and must return a service that takes
/// `http::Request<HttpRequestBody>` and yields
/// `http::Response<HttpResponseBody>` — the body types `object_store`
/// already uses internally, so no conversion is required.
pub struct TowerHttpConnector<F>(pub F);

impl<F> std::fmt::Debug for TowerHttpConnector<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TowerHttpConnector").finish_non_exhaustive()
    }
}

impl<F, S> HttpConnector for TowerHttpConnector<F>
where
    F: Fn(&ClientOptions) -> crate::Result<S> + Send + Sync + 'static,
    S: Service<HttpRequest, Response = HttpResponse> + Clone + Send + Sync + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn StdError + Send + Sync>> + Send + Sync + 'static,
{
    fn connect(&self, options: &ClientOptions) -> crate::Result<HttpClient> {
        let svc = (self.0)(options)?;
        Ok(HttpClient::new(TowerHttpService::new(svc)))
    }
}

/// `HttpService` that drives an inner `tower::Service` per request.
///
/// `HttpService::call` takes `&self`, but `tower::Service::call` takes
/// `&mut self` and `poll_ready`/`call` are stateful, so each request
/// clones the inner service before driving it. This is the standard
/// tower idiom for shared services.
struct TowerHttpService<S> {
    /// `Mutex` only to satisfy `HttpService`'s `Send + Sync`; the
    /// critical section is one `clone()`.
    svc: Mutex<S>,
}

impl<S> std::fmt::Debug for TowerHttpService<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TowerHttpService").finish_non_exhaustive()
    }
}

impl<S: Clone> TowerHttpService<S> {
    fn new(svc: S) -> Self {
        Self {
            svc: Mutex::new(svc),
        }
    }

    fn clone_inner(&self) -> S {
        self.svc.lock().unwrap().clone()
    }
}

#[async_trait]
impl<S> HttpService for TowerHttpService<S>
where
    S: Service<HttpRequest, Response = HttpResponse> + Clone + Send + Sync + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn StdError + Send + Sync>> + Send + Sync + 'static,
{
    async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        let mut svc = self.clone_inner();
        ServiceExt::ready(&mut svc)
            .await
            .map_err(|e| HttpError::new(HttpErrorKind::Connect, BoxStdError(e.into())))?;
        Service::call(&mut svc, req)
            .await
            .map_err(|e| HttpError::new(HttpErrorKind::Request, BoxStdError(e.into())))
    }
}

/// Newtype that adapts `Box<dyn StdError + Send + Sync>` (which itself
/// implements `StdError`) to satisfy the `E: Error + Send + Sync + 'static`
/// bound on [`HttpError::new`].
#[derive(Debug)]
struct BoxStdError(Box<dyn StdError + Send + Sync>);

impl std::fmt::Display for BoxStdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl StdError for BoxStdError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.0.source()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{HttpRequestBody, HttpResponseBody};
    use bytes::Bytes;
    use http::StatusCode;
    use http_body_util::BodyExt;
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn ok_response(body: &'static str) -> HttpResponse {
        let body = HttpResponseBody::new(
            http_body_util::Full::new(Bytes::from_static(body.as_bytes()))
                .map_err(|never| match never {}),
        );
        let mut resp = HttpResponse::new(body);
        *resp.status_mut() = StatusCode::OK;
        resp
    }

    #[tokio::test]
    async fn happy_path_routes_request_through_tower_service() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_svc = Arc::clone(&calls);
        let svc = tower::service_fn(move |_req: HttpRequest| {
            let calls_for_svc = Arc::clone(&calls_for_svc);
            async move {
                calls_for_svc.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Infallible>(ok_response("hi"))
            }
        });

        let svc = TowerHttpService::new(svc);
        let req = HttpRequest::new(HttpRequestBody::empty());
        let resp = svc.call(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().bytes().await.unwrap();
        assert_eq!(body.as_ref(), b"hi");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn call_error_maps_to_request_kind() {
        #[derive(Debug, thiserror::Error)]
        #[error("boom")]
        struct Boom;

        let svc = tower::service_fn(|_req: HttpRequest| async { Err::<HttpResponse, Boom>(Boom) });
        let svc = TowerHttpService::new(svc);
        let req = HttpRequest::new(HttpRequestBody::empty());
        let err = svc.call(req).await.unwrap_err();
        assert_eq!(err.kind(), HttpErrorKind::Request);
    }
}
