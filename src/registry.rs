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

//! ObjectStoreRegistry holds all the object stores at Runtime with a scheme for each store.
//! This allows the user to resolve a URL to an ObjectStore at runtime. Unlike
//! [`crate::parse_url`], this allows for custom logic to be executed
//! when a URL is resolved to an ObjectStore. It also serves as a cache for object stores
//! to avoid repeated creation.

use crate::{parse_url_opts, ObjectStore};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use url::Url;

/// [`ObjectStoreRegistry`] maps a URL to an [`ObjectStore`] instance,
/// and allows users to read from different [`ObjectStore`]
/// instances. For example the registry might be configured so that
///
/// 1. `s3://my_bucket/lineitem/` mapped to the `/lineitem` path on an
///    AWS S3 object store bound to `my_bucket`
///
/// 2. `s3://my_other_bucket/lineitem/` mapped to the (same)
///    `/lineitem` path on a *different* AWS S3 object store bound to
///    `my_other_bucket`
///
/// In this particular case, the url `s3://my_bucket/lineitem/` will be provided to
/// [`ObjectStoreRegistry::get_store`] and one of three things will happen:
///
/// - If an [`ObjectStore`] has been registered with [`ObjectStoreRegistry::register_store`] with
///   `s3://my_bucket`, that [`ObjectStore`] will be returned
///
/// - If an AWS S3 object store can be ad-hoc discovered by the url `s3://my_bucket/lineitem/`, this
///   object store will be registered with key `s3://my_bucket` and returned.
///
/// - Otherwise `None` will be returned, indicating that no suitable [`ObjectStore`] could
///   be found
///
/// This allows for two different use-cases:
///
/// 1. Systems where object store buckets are explicitly created using DDL, can register these
///    buckets using [`ObjectStoreRegistry::register_store`]
///
/// 2. Systems relying on ad-hoc discovery, without corresponding DDL, can create [`ObjectStore`]
///    lazily by providing a custom implementation of [`ObjectStoreRegistry`]
pub trait ObjectStoreRegistry: Send + Sync + std::fmt::Debug + 'static {
    /// If a store with the same key existed before, it is replaced and returned
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>>;

    /// Get a suitable store for the provided URL. For example:
    ///
    /// - URL with scheme `file:///` or no scheme will return the default LocalFS store
    /// - URL with scheme `s3://bucket/` will return the S3 store
    /// - URL with scheme `hdfs://hostname:port/` will return the hdfs store
    ///
    /// If no [`ObjectStore`] found for the `url`, ad-hoc discovery may be executed depending on
    /// the `url` and [`ObjectStoreRegistry`] implementation. An [`ObjectStore`] may be lazily
    /// created and registered.
    fn get_store(&self, url: &Url) -> Option<Arc<dyn ObjectStore>>;

    /// Get a suitable store for the provided URL. For example:
    ///
    /// - URL with scheme `file:///` or no scheme will return the default LocalFS store
    /// - URL with scheme `s3://bucket/` will return the S3 store
    /// - URL with scheme `hdfs://hostname:port/` will return the hdfs store
    ///
    /// If no [`ObjectStore`] found for the `url`, ad-hoc discovery may be executed depending on
    /// the `url` and [`ObjectStoreRegistry`] implementation. An [`ObjectStore`] may be lazily
    /// created and registered. If `opts` are supplied, they will be passed to the ad-hoc discovery
    /// function.
    fn get_store_with_opts<I, K, V>(&self, url: &Url, opts: I) -> Option<Arc<dyn ObjectStore>>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>;

    /// List all registered URLs
    fn list_urls(&self) -> Vec<String>;
}

/// The default [`ObjectStoreRegistry`]
pub struct DefaultObjectStoreRegistry {
    /// A map from scheme to object store that serve list / read operations for the store
    object_stores: RwLock<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl std::fmt::Debug for DefaultObjectStoreRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stores = self.object_stores.read().unwrap();
        f.debug_struct("DefaultObjectStoreRegistry")
            .field("schemes", &stores.keys().cloned().collect::<Vec<_>>())
            .finish()
    }
}

impl Default for DefaultObjectStoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultObjectStoreRegistry {
    /// Create a new [`DefaultObjectStoreRegistry`] with no registered stores
    pub fn new() -> Self {
        let object_stores = RwLock::new(HashMap::new());
        Self { object_stores }
    }
}

///
/// Stores are registered based on the scheme, host and port of the provided URL.
///
/// For example:
///
/// - `file:///my_path` will return the default LocalFS store
/// - `s3://bucket/path` will return a store registered with `s3://bucket` if any
/// - `hdfs://host:port/path` will return a store registered with `hdfs://host:port` if any
impl ObjectStoreRegistry for DefaultObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let s = get_url_key(url);
        let mut stores = self.object_stores.write().unwrap();
        stores.insert(s, store)
    }

    fn get_store(&self, url: &Url) -> Option<Arc<dyn ObjectStore>> {
        self.get_store_with_opts(url, std::iter::empty::<(&str, &str)>())
    }

    fn get_store_with_opts<I, K, V>(&self, url: &Url, opts: I) -> Option<Arc<dyn ObjectStore>>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let s = get_url_key(url);
        let mut stores = self.object_stores.write().unwrap();

        if let Some(store) = stores.get(&s) {
            return Some(Arc::clone(store));
        }

        match parse_url_opts(url, opts) {
            Ok((store, _)) => {
                let store = Arc::from(store);
                stores.insert(s, Arc::clone(&store));
                Some(store)
            }
            Err(_) => None,
        }
    }

    fn list_urls(&self) -> Vec<String> {
        let stores = self.object_stores.read().unwrap();
        stores.keys().cloned().collect()
    }
}

/// Get the key of a url for object store registration.
/// The credential info will be removed
fn get_url_key(url: &Url) -> String {
    format!(
        "{}://{}",
        url.scheme(),
        &url[url::Position::BeforeHost..url::Position::AfterPort],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_url_key() {
        let file = Url::parse("file://").unwrap();
        let key = get_url_key(&file);
        assert_eq!(key.as_str(), "file://");

        let url = Url::parse("s3://bucket").unwrap();
        let key = get_url_key(&url);
        assert_eq!(key.as_str(), "s3://bucket");

        let url = Url::parse("s3://username:password@host:123").unwrap();
        let key = get_url_key(&url);
        assert_eq!(key.as_str(), "s3://host:123");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_register_store() {
        use crate::local::LocalFileSystem;
        let registry = DefaultObjectStoreRegistry::new();
        let url = Url::parse("file:///foo/bar").unwrap();
        let store = Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>;
        let old_store = registry.register_store(&url, Arc::clone(&store));
        assert!(
            old_store.is_none(),
            "Should not return a previous store when registering a new one"
        );
        let retrieved_store = registry
            .get_store(&url)
            .expect("Should retrieve a store when a valid URL is used");
        // Validate that it’s exactly the same Arc we registered above
        assert!(
            Arc::ptr_eq(&retrieved_store, &store),
            "Retrieved store is not the same LocalFileSystem instance"
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_register_store_on_get() {
        let registry = DefaultObjectStoreRegistry::new();
        let url = Url::parse("file:///foo/bar").unwrap();
        // on first get, should lazily create & register a LocalFileSystem
        let retrieved_store = registry
            .get_store(&url)
            .expect("Should retrieve a store when a valid URL is used");
        // Validate that this really is a LocalFileSystem under the hood by
        // looking at its Debug representation
        let dbg = format!("{:?}", retrieved_store);
        assert!(
            dbg.starts_with("LocalFileSystem"),
            "Expected a LocalFileSystem, but Debug printed: {}",
            dbg
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_list_urls() {
        use crate::local::LocalFileSystem;
        let registry = DefaultObjectStoreRegistry::new();
        let url = Url::parse("file:///foo/bar").unwrap();
        let store = Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>;
        registry.register_store(&url, store);
        let urls = registry.list_urls();
        assert_eq!(urls.len(), 1);
        assert_eq!(urls[0], "file://");
    }

    #[test]
    fn test_registry_with_bad_scheme() {
        let registry = DefaultObjectStoreRegistry::new();
        let url = Url::parse("unknown://foo/bar").unwrap();
        assert!(registry.get_store(&url).is_none());
    }
}
