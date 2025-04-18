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

use crate::client::pagination::stream_paginated;
use crate::path::Path;
use crate::ListResult;
use crate::{ListOpts, Result};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;

/// A client that can perform paginated list requests
#[async_trait]
pub(crate) trait ListClient: Send + Sync + 'static {
    async fn list_request(
        &self,
        prefix: Option<&str>,
        delimiter: bool,
        token: Option<&str>,
        offset: Option<&str>,
        extensions: http::Extensions,
    ) -> Result<(ListResult, Option<String>)>;
}

/// Extension trait for [`ListClient`] that adds common listing functionality
#[async_trait]
pub(crate) trait ListClientExt {
    fn list_paginated(
        &self,
        prefix: Option<&Path>,
        opts: ListOpts,
    ) -> BoxStream<'static, Result<ListResult>>;
}

#[async_trait]
impl<T: ListClient + Clone> ListClientExt for T {
    fn list_paginated(
        &self,
        prefix: Option<&Path>,
        opts: ListOpts,
    ) -> BoxStream<'static, Result<ListResult>> {
        let ListOpts {
            delimiter,
            offset,
            extensions,
        } = opts;

        let offset = offset.map(|x| x.to_string());
        let prefix = prefix
            .filter(|x| !x.as_ref().is_empty())
            .map(|p| format!("{}{}", p.as_ref(), crate::path::DELIMITER));

        stream_paginated(
            self.clone(),
            (prefix, offset, extensions),
            move |client, (prefix, offset, extensions), token| async move {
                let (r, next_token) = client
                    .list_request(
                        prefix.as_deref(),
                        delimiter,
                        token.as_deref(),
                        offset.as_deref(),
                        extensions.clone(),
                    )
                    .await?;
                Ok((r, (prefix, offset, extensions), next_token))
            },
        )
        .boxed()
    }
}
