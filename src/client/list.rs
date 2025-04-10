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
        max_keys: Option<usize>,
        extensions: ::http::Extensions,
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
            max_keys,
            extensions,
        } = opts;

        let offset = offset.map(|x| x.to_string());
        let prefix = prefix
            .filter(|x| !x.as_ref().is_empty())
            .map(|p| format!("{}{}", p.as_ref(), crate::path::DELIMITER));

        stream_paginated(
            self.clone(),
            (prefix, offset, max_keys, extensions),
            move |client, (prefix, offset, max_keys, extensions), token| async move {
                if let Some(remaining) = max_keys {
                    if remaining == 0 {
                        return Ok((
                            ListResult::empty(),
                            (prefix, offset, max_keys, extensions),
                            None,
                        ));
                    }
                }

                let (r, next_token) = client
                    .list_request(
                        prefix.as_deref(),
                        delimiter,
                        token.as_deref(),
                        offset.as_deref(),
                        max_keys,
                        extensions.clone(),
                    )
                    .await?;
                let key_count = r.common_prefixes.len() + r.objects.len();
                let remaining = max_keys.map(|x| (x - key_count).max(0));
                let next_token = match remaining {
                    None => next_token,
                    Some(remaining) if remaining > 0 => next_token,
                    _ => None,
                };
                Ok((r, (prefix, offset, remaining, extensions), next_token))
            },
        )
        .boxed()
    }
}
