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

//! Bucket create, delete and existence checks for the object stores that support them

use crate::Result;
use async_trait::async_trait;
use std::fmt;

/// Create, delete, and check the existence of the bucket a store is configured for.
///
/// Every [`ObjectStore`](crate::ObjectStore) operates on paths inside one bucket, fixed when the
/// store is built. This trait manages that bucket itself. It is implemented by
/// [`AmazonS3`](crate::aws::AmazonS3), [`GoogleCloudStorage`](crate::gcp::GoogleCloudStorage) and
/// [`MicrosoftAzure`](crate::azure::MicrosoftAzure), where the Azure "container" plays the role of
/// the bucket.
///
/// To manage several buckets, build one store per bucket name. A store held as
/// `Arc<dyn ObjectStore>` cannot be converted to a `BucketStore`, so keep the concrete type, or a
/// separate `Arc<dyn BucketStore>`, where bucket operations are needed.
#[async_trait]
pub trait BucketStore: Send + Sync + fmt::Debug + 'static {
    /// Create the bucket this store is configured for.
    ///
    /// # Errors
    ///
    /// - [`Error::AlreadyExists`](crate::Error::AlreadyExists) if the bucket exists, including a
    ///   name owned by another account on S3 and GCS, or if it was deleted moments ago (at least
    ///   30 seconds on Azure).
    /// - [`Error::PermissionDenied`](crate::Error::PermissionDenied) or
    ///   [`Error::Unauthenticated`](crate::Error::Unauthenticated) if the credentials cannot
    ///   create buckets.
    ///
    /// S3 in `us-east-1` returns `Ok(())`, and resets the bucket's ACLs, when re-creating a bucket
    /// the caller owns. A create that times out may still have created the bucket. A create the
    /// provider completes but answers with a server error is retried, and the retry reports
    /// [`Error::AlreadyExists`](crate::Error::AlreadyExists).
    async fn create_bucket(&self) -> Result<()>;

    /// Delete the bucket this store is configured for.
    ///
    /// S3 and GCS require the bucket to be empty, including noncurrent object versions. Azure
    /// accepts a non-empty container and deletes its contents asynchronously. A delete the
    /// provider completes but answers with a server error is retried, and the retry reports
    /// [`Error::NotFound`](crate::Error::NotFound).
    ///
    /// # Errors
    ///
    /// - [`Error::NotFound`](crate::Error::NotFound) if the bucket does not exist.
    /// - [`Error::Generic`](crate::Error::Generic) if the bucket is not empty or is already being
    ///   deleted.
    async fn delete_bucket(&self) -> Result<()>;

    /// Return whether the bucket this store is configured for exists.
    ///
    /// Returns `Ok(false)` only when the provider reports that the bucket does not exist.
    ///
    /// # Errors
    ///
    /// - [`Error::PermissionDenied`](crate::Error::PermissionDenied) if the credentials cannot
    ///   read the bucket. On S3 and GCS this is also the result for a bucket owned by another
    ///   account or project, which does exist.
    /// - [`Error::Unauthenticated`](crate::Error::Unauthenticated) if the credentials are
    ///   rejected.
    async fn bucket_exists(&self) -> Result<bool>;
}

/// Returns an error unless `name` can be put in a bucket-level URL without changing which
/// resource the URL names (no `/`, `?`, `#`, `%`, whitespace, or dot segments).
#[cfg(any(feature = "aws-base", feature = "gcp-base"))]
pub(crate) fn validate_bucket_name(store: &'static str, name: &str) -> Result<()> {
    let valid = !name.is_empty()
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_'));
    match valid {
        true => Ok(()),
        false => Err(crate::Error::Generic {
            store,
            source: format!(
                "invalid bucket name {name:?}: bucket operations accept only ASCII letters, digits, '-', '.' and '_'"
            )
            .into(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Debug, Default)]
    struct MinimalBucketStore {
        exists: AtomicBool,
    }

    #[async_trait]
    impl BucketStore for MinimalBucketStore {
        async fn create_bucket(&self) -> Result<()> {
            self.exists.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn delete_bucket(&self) -> Result<()> {
            self.exists.store(false, Ordering::SeqCst);
            Ok(())
        }

        async fn bucket_exists(&self) -> Result<bool> {
            Ok(self.exists.load(Ordering::SeqCst))
        }
    }

    #[tokio::test]
    async fn usable_as_trait_object() {
        let store: Arc<dyn BucketStore> = Arc::new(MinimalBucketStore::default());

        assert!(!store.bucket_exists().await.unwrap());
        store.create_bucket().await.unwrap();
        assert!(store.bucket_exists().await.unwrap());
        store.delete_bucket().await.unwrap();
        assert!(!store.bucket_exists().await.unwrap());
    }
}
