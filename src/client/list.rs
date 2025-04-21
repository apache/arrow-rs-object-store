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
use crate::{ListOptions, Result};
use crate::{ListResult, ObjectMeta};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use std::collections::BTreeSet;

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
    fn list_opts(
        &self,
        prefix: Option<&Path>,
        opts: ListOptions,
    ) -> BoxStream<'static, Result<ListResult>>;

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>>;

    #[allow(unused)]
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>>;

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult>;
}

#[async_trait]
impl<T: ListClient + Clone> ListClientExt for T {
    fn list_opts(
        &self,
        prefix: Option<&Path>,
        opts: ListOptions,
    ) -> BoxStream<'static, Result<ListResult>> {
        let ListOptions {
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

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.list_opts(
            prefix,
            ListOptions {
                offset: None,
                delimiter: false,
                extensions: Default::default(),
            },
        )
        .map_ok(|r| futures::stream::iter(r.objects.into_iter().map(Ok)))
        .try_flatten()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.list_opts(
            prefix,
            ListOptions {
                offset: Some(offset.clone()),
                delimiter: false,
                extensions: Default::default(),
            },
        )
        .map_ok(|r| futures::stream::iter(r.objects.into_iter().map(Ok)))
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let mut stream = self.list_opts(
            prefix,
            ListOptions {
                offset: None,
                delimiter: true,
                extensions: Default::default(),
            },
        );

        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();

        while let Some(result) = stream.next().await {
            let response = result?;
            common_prefixes.extend(response.common_prefixes.into_iter());
            objects.extend(response.objects.into_iter());
        }

        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }
}

pub(crate) fn filter_list_result(
    stream: BoxStream<'static, Result<ListResult>>,
    offset: Option<Path>,
) -> BoxStream<'static, Result<ListResult>> {
    match offset {
        Some(offset) if !offset.to_string().is_empty() => stream
            .map_ok(move |r| {
                let objects: Vec<ObjectMeta> = r
                    .objects
                    .into_iter()
                    .filter(|f| f.location > offset)
                    .collect();
                let common_prefixes: Vec<Path> = r
                    .common_prefixes
                    .into_iter()
                    .filter(|p| p > &offset)
                    .collect();
                ListResult {
                    common_prefixes,
                    objects,
                }
            })
            .try_filter(move |f| {
                futures::future::ready(f.objects.len() + f.common_prefixes.len() > 0)
            })
            .boxed(),
        _ => stream,
    }
}
