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

//! ObjectStoreRegistry holds object stores at runtime with a URL for each store.
//! The registry serves as a cache for object stores to avoid repeated creation. It
//! also simplifies converting an [`ObjectStore`] back to a URL:
//!
//! ```rust
//! use std::sync::Arc;
//! use url::Url;
//! use object_store::ObjectStore;
//! use object_store::memory::InMemory;
//! use object_store::registry::{DefaultObjectStoreRegistry, ObjectStoreRegistry};
//!
//! let expected_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
//! let registry = DefaultObjectStoreRegistry::new();
//! let url = Url::parse("inmemory://").unwrap();
//! registry.register_store(&url, expected_store.clone());
//! let prefixes = registry.get_store_prefixes();
//! for prefix in prefixes {
//!     let store = registry.get_store(&prefix).unwrap();
//!     if Arc::ptr_eq(&store, &expected_store) {
//!         assert_eq!(prefix, url);
//!     }
//! }
//! ```
use crate::ObjectStore;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use url::Url;

/// [`ObjectStoreRegistry`] maps a URL prefix to an [`ObjectStore`] instance. The definition of
/// a URL prefix depends on the [`ObjectStoreRegistry`] implementation. See implementation docs
/// for more details.
pub trait ObjectStoreRegistry: Send + Sync + std::fmt::Debug + 'static {
    /// Register a new store for any URL that begins with `prefix`
    ///
    /// If a store with the same prefix existed before, it is replaced and returned
    fn register_store(
        &self,
        prefix: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>>;

    /// Get a suitable store for the provided URL. The definition of a "suitable store" depends on
    /// the [`ObjectStoreRegistry`] implementation. See implementation docs for more details.
    ///
    /// If no [`ObjectStore`] found for the `url`, ad-hoc discovery may be executed depending on
    /// the `url` and [`ObjectStoreRegistry`] implementation. An [`ObjectStore`] may be lazily
    /// created and registered. The logic for doing so is left to each [`ObjectStoreRegistry`]
    /// implementation.
    fn get_store(&self, url: &Url) -> Option<Arc<dyn ObjectStore>>;

    /// List all registered store prefixes
    fn get_store_prefixes(&self) -> Vec<Url>;
}

/// A simple [`ObjectStoreRegistry`] implementation that registers stores based on the provided
/// URL prefix.
pub struct DefaultObjectStoreRegistry {
    /// A map from URL prefix to object store that serve list / read operations for the store
    object_stores: RwLock<HashMap<Url, Arc<dyn ObjectStore>>>,
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
/// Stores are registered based on the URL prefix of the provided URL.
///
/// For example:
///
/// - `file:///foo/bar` will return a store registered with `file:///foo/bar` if any
/// - `s3://bucket/path` will return a store registered with `s3://bucket/path` if any
/// - `hdfs://host:port/path` will return a store registered with `hdfs://host:port/path` if any
impl ObjectStoreRegistry for DefaultObjectStoreRegistry {
    fn register_store(
        &self,
        prefix: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let mut stores = self.object_stores.write().unwrap();
        stores.insert(prefix.clone(), store)
    }

    /// Get a store that was registered with the provided URL prefix.
    ///
    /// If no store was registered with the provided URL prefix, `None` is returned.
    fn get_store(&self, prefix: &Url) -> Option<Arc<dyn ObjectStore>> {
        let stores = self.object_stores.read().unwrap();
        stores.get(prefix).map(|s| Arc::clone(s))
    }

    /// Returns a vector of all registered store prefixes.
    fn get_store_prefixes(&self) -> Vec<Url> {
        let stores = self.object_stores.read().unwrap();
        stores.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(not(target_arch = "wasm32"))]
    use crate::local::LocalFileSystem;

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_register_store() {
        let registry = DefaultObjectStoreRegistry::new();
        let url = Url::parse("file:///foo/bar").unwrap();
        let store = Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>;
        let old_store = registry.register_store(&url, Arc::clone(&store));
        assert!(old_store.is_none());
        let retrieved_store = registry.get_store(&url).unwrap();
        assert!(Arc::ptr_eq(&retrieved_store, &store));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_get_store_miss() {
        let registry = DefaultObjectStoreRegistry::new();
        let url = Url::parse("file:///foo/bar").unwrap();
        assert!(registry.get_store(&url).is_none());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_list_urls() {
        let registry = DefaultObjectStoreRegistry::new();
        let url = Url::parse("file:///foo/bar").unwrap();
        let store = Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>;
        registry.register_store(&url, store);
        let urls = registry.get_store_prefixes();
        assert_eq!(urls.len(), 1);
        assert_eq!(urls[0], Url::parse("file:///foo/bar").unwrap());
    }

    #[test]
    fn test_registry_with_bad_scheme() {
        let registry = DefaultObjectStoreRegistry::new();
        let url = Url::parse("unknown://foo/bar").unwrap();
        assert!(registry.get_store(&url).is_none());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_subprefix_miss() {
        let registry = DefaultObjectStoreRegistry::new();
        let base_url = Url::parse("file:///foo/bar").unwrap();
        let store = Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>;
        registry.register_store(&base_url, Arc::clone(&store));
        let subprefix_url = Url::parse("file:///foo/bar/baz").unwrap();
        let retrieved_store = registry.get_store(&subprefix_url);
        assert!(retrieved_store.is_none());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_invalid_file_url_format() {
        let registry = DefaultObjectStoreRegistry::new();
        let base_url = Url::parse("file:///foo/bar").unwrap();
        let store = Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>;
        registry.register_store(&base_url, Arc::clone(&store));
        let invalid_url = Url::parse("file://foo/bar").unwrap();
        let result = registry.get_store(&invalid_url);
        assert!(result.is_none());
    }
}
