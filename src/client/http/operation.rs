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

//! Per-call operation context surfaced via `http::Request::extensions`.
//!
//! Each backend inserts an [`ObjectStoreOperation`] into the outbound
//! [`HttpRequest`]'s extensions before it reaches an [`HttpService`]. A
//! wrapping `HttpService` (or `tower::Service` / `reqwest_middleware::Middleware`)
//! can then read it via `req.extensions().get::<ObjectStoreOperation>()` to
//! produce meaningful trace spans without sniffing URLs and headers.
//!
//! [`http::Extensions`] are in-process only and never reach the wire, so this
//! is invisible to remote servers.
//!
//! [`HttpRequest`]: crate::client::HttpRequest
//! [`HttpService`]: crate::client::HttpService

use crate::path::Path;

/// Identifies the high-level `ObjectStore` operation that produced an
/// outbound HTTP request.
///
/// New variants may be added without a major version bump (`#[non_exhaustive]`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OperationKind {
    /// `ObjectStore::get` / `get_opts` / `get_range` / `get_ranges`
    Get,
    /// `ObjectStore::head`
    Head,
    /// `ObjectStore::put` / `put_opts`
    Put,
    /// Any phase of a `put_multipart` upload (create, upload-part, complete, abort)
    PutMultipart,
    /// `ObjectStore::delete`
    Delete,
    /// `ObjectStore::copy` / `copy_if_not_exists` / `rename` / `rename_if_not_exists`
    Copy,
    /// `ObjectStore::list` / `list_with_offset`
    List,
    /// `ObjectStore::list_with_delimiter`
    ListWithDelimiter,
}

impl OperationKind {
    /// A short, snake_case identifier suitable for span attributes.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Head => "head",
            Self::Put => "put",
            Self::PutMultipart => "put_multipart",
            Self::Delete => "delete",
            Self::Copy => "copy",
            Self::List => "list",
            Self::ListWithDelimiter => "list_with_delimiter",
        }
    }
}

/// Per-call operation context attached to outbound HTTP requests via
/// [`http::Extensions`].
///
/// New fields may be added without a major version bump (`#[non_exhaustive]`).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ObjectStoreOperation {
    /// The high-level operation that triggered this request.
    pub kind: OperationKind,
    /// The user-supplied [`Path`], when applicable. `None` for operations
    /// that don't take a path argument (e.g. bulk delete, list root, copy
    /// per-side, multipart sub-requests where the path is implicit).
    pub location: Option<Path>,
    /// The backend that issued the request: `"s3"`, `"gcs"`, `"azure"`,
    /// or `"http"`.
    pub backend: &'static str,
}

impl ObjectStoreOperation {
    /// Construct a new [`ObjectStoreOperation`].
    pub fn new(kind: OperationKind, backend: &'static str) -> Self {
        Self {
            kind,
            location: None,
            backend,
        }
    }

    /// Attach a [`Path`] to this operation.
    pub fn with_location(mut self, location: Path) -> Self {
        self.location = Some(location);
        self
    }
}
