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

//! Tests the default implementation of get_range handles GetResult::File correctly (#4350)
#![cfg(all(feature = "fs", not(target_arch = "wasm32")))]

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::*;
use std::fmt::Formatter;
use tempfile::tempdir;

#[derive(Debug)]
struct MyStore(LocalFileSystem);

impl std::fmt::Display for MyStore {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[async_trait]
impl ObjectStore for MyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.0.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        todo!()
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.0.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        _: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        todo!()
    }

    fn list(&self, _: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        todo!()
    }

    async fn list_with_delimiter(&self, _: Option<&Path>) -> Result<ListResult> {
        todo!()
    }

    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> Result<()> {
        todo!()
    }
}

#[tokio::test]
async fn test_get_range() {
    let tmp = tempdir().unwrap();
    let store = MyStore(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
    let path = Path::from("foo");

    let expected = Bytes::from_static(b"hello world");
    store.put(&path, expected.clone().into()).await.unwrap();
    let fetched = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(expected, fetched);

    for range in [0..10, 3..5, 0..expected.len() as u64] {
        let data = store.get_range(&path, range.clone()).await.unwrap();
        assert_eq!(
            &data[..],
            &expected[range.start as usize..range.end as usize]
        )
    }

    let over_range = 0..(expected.len() as u64 * 2);
    let data = store.get_range(&path, over_range.clone()).await.unwrap();
    assert_eq!(&data[..], expected)
}

/// Test that, when a requesting a range which overhangs the end of the resource,
/// the resulting [GetResult::range] reports the returned range,
/// not the requested.
#[tokio::test]
async fn test_get_opts_over_range() {
    let tmp = tempdir().unwrap();
    let store = MyStore(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
    let path = Path::from("foo");

    let expected = Bytes::from_static(b"hello world");
    store.put(&path, expected.clone().into()).await.unwrap();

    let opts =
        GetOptions::new().with_range(Some(GetRange::Bounded(0..(expected.len() as u64 * 2))));
    let res = store.get_opts(&path, opts).await.unwrap();
    assert_eq!(res.range, 0..expected.len() as u64);
    assert_eq!(res.bytes().await.unwrap(), expected);
}

#[tokio::test]
async fn test_get_range_opts_with_etag() {
    let tmp = tempdir().unwrap();
    let store = MyStore(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
    let path = Path::from("foo");

    let expected = Bytes::from_static(b"hello world");
    store.put(&path, expected.clone().into()).await.unwrap();

    // pull the file to get the etag
    let file = store.get(&path).await.unwrap();
    let etag = file.meta.e_tag.clone().unwrap();

    let opts = GetOptions::new()
        .with_if_match(Some(etag))
        .with_range(Some(0..(expected.len() as u64 * 2)));
    let res = store.get_opts(&path, opts).await.unwrap();
    assert_eq!(res.bytes().await.unwrap(), expected);

    // pulling a file with an invalid etag should fail
    let opts = GetOptions::new()
        .with_if_match(Some("invalid-etag"))
        .with_range(Some(0..(expected.len() as u64 * 2)));
    let err = store.get_opts(&path, opts).await;
    assert!(err.is_err());
}
