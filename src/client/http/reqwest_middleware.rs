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

//! `HttpService` impl for [`reqwest_middleware::ClientWithMiddleware`].
//!
//! Mirrors the [`reqwest::Client`] impl in `connection.rs` so consumers
//! that have already composed a `reqwest_middleware` middleware stack
//! (e.g. `reqwest_tracing::TracingMiddleware`) can hand it directly to
//! [`crate::client::HttpClient::new`].

use crate::client::{HttpError, HttpRequest, HttpResponse, HttpResponseBody, HttpService};
use async_trait::async_trait;
use http_body_util::BodyExt;

#[async_trait]
impl HttpService for reqwest_middleware::ClientWithMiddleware {
    async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        let (parts, body) = req.into_parts();

        let url = parts.uri.to_string().parse().unwrap();
        let mut req = reqwest::Request::new(parts.method, url);
        *req.headers_mut() = parts.headers;
        *req.body_mut() = Some(body.into_reqwest());

        let r = self
            .execute(req)
            .await
            .map_err(HttpError::reqwest_middleware)?;
        let res: http::Response<reqwest::Body> = r.into();
        let (parts, body) = res.into_parts();

        let body = HttpResponseBody::new(body.map_err(HttpError::reqwest));
        Ok(HttpResponse::from_parts(parts, body))
    }
}

#[cfg(test)]
mod tests {
    use crate::RetryConfig;
    use crate::client::HttpClient;
    use crate::client::mock_server::MockServer;
    use crate::client::retry::RetryExt;
    use http::HeaderValue;
    use hyper::Response;
    use reqwest::Request;
    use reqwest_middleware::{ClientBuilder, Middleware, Next};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Middleware that injects a known header so the test can assert
    /// the middleware stack actually ran for each request.
    struct InjectHeader;

    #[async_trait::async_trait]
    impl Middleware for InjectHeader {
        async fn handle(
            &self,
            mut req: Request,
            extensions: &mut http::Extensions,
            next: Next<'_>,
        ) -> reqwest_middleware::Result<reqwest::Response> {
            req.headers_mut()
                .insert("x-test-middleware", HeaderValue::from_static("hit"));
            next.run(req, extensions).await
        }
    }

    #[tokio::test]
    async fn client_with_middleware_runs_middleware() {
        let mock = MockServer::new().await;

        let saw_header = Arc::new(AtomicBool::new(false));
        let saw_header_in_handler = Arc::clone(&saw_header);
        mock.push_fn(move |req| {
            if req.headers().get("x-test-middleware") == Some(&HeaderValue::from_static("hit")) {
                saw_header_in_handler.store(true, Ordering::SeqCst);
            }
            Response::new("BANANAS".to_string())
        });

        let inner = reqwest::Client::new();
        let client_with_mw = ClientBuilder::new(inner).with(InjectHeader).build();
        let http_client = HttpClient::new(client_with_mw);

        let url = mock.url().to_string();
        let retry = RetryConfig::default();
        let resp = http_client.get(url).send_retry(&retry).await.unwrap();
        let payload = resp.into_body().bytes().await.unwrap();
        assert_eq!(payload.as_ref(), b"BANANAS");

        assert!(
            saw_header.load(Ordering::SeqCst),
            "middleware did not run before the server saw the request",
        );
    }
}
