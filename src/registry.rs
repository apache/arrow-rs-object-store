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
//! The registry serves as a cache for object stores to avoid repeated creation.
//! It also lets you convert an [`ObjectStore`] back to its registered URL via
//! `get_prefix`:
//!
//! ```rust
//! use std::sync::Arc;
//! use url::Url;
//! use object_store::ObjectStore;
//! use object_store::memory::InMemory;
//! use object_store::registry::{DefaultObjectStoreRegistry, ObjectStoreRegistry};
//!
//! let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
//! let registry = DefaultObjectStoreRegistry::new();
//! let url = Url::parse("inmemory://").unwrap();
//! registry.register_store(&url, Arc::clone(&store));
//! let found_store = registry.get_store(&url).unwrap();
//! assert!(Arc::ptr_eq(&found_store, &store));
//! let found_url = registry.get_prefix(Arc::clone(&store)).unwrap();
//! assert_eq!(found_url, url);
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

    /// Get a store for the provided URL. The definition of a "suitable store" depends on
    /// the [`ObjectStoreRegistry`] implementation. See implementation docs for more details.
    ///
    /// If no [`ObjectStore`] is found for the `url`, an [`ObjectStore`] may be lazily be
    /// created and registered. The logic for doing so is left to each [`ObjectStoreRegistry`]
    /// implementation.
    fn get_store(&self, url: &Url) -> Option<Arc<dyn ObjectStore>>;

    /// Given one of the `Arc<dyn ObjectStore>`s you registered, return its URL.
    fn get_prefix(&self, store: Arc<dyn ObjectStore>) -> Option<Url>;

    /// List all registered store prefixes
    fn get_store_prefixes(&self) -> Vec<Url>;
}

/// A simple [`ObjectStoreRegistry`] implementation that has no prefix logic. It simply returns
/// a store registered with the provided URL if one exists. For example, if a store is registered
/// with `file:///foo`, then:
///
/// - `file:///foo` will match
/// - `file://foo` will not match
/// - `file:///foo/bar` will not match
/// - `s3://foo` will not match
pub struct DefaultObjectStoreRegistry {
    /// A map from URL to object store that serve list / read operations for the store
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

impl ObjectStoreRegistry for DefaultObjectStoreRegistry {
    /// Register a new store for the provided URL
    ///
    /// If a store with the same URL existed before, it is replaced and returned
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let mut stores = self.object_stores.write().unwrap();
        stores.insert(url.clone(), store)
    }

    /// Get a store that was registered with the provided URL.
    ///
    /// If no store was registered with the provided URL, `None` is returned.
    fn get_store(&self, url: &Url) -> Option<Arc<dyn ObjectStore>> {
        let stores = self.object_stores.read().unwrap();
        stores.get(url).map(Arc::clone)
    }

    /// Given one of the `Arc<dyn ObjectStore>`s you registered, return its URL.
    ///
    /// If no store was registered with the provided `Arc<dyn ObjectStore>`, `None` is returned.
    fn get_prefix(&self, store: Arc<dyn ObjectStore>) -> Option<Url> {
        let map = self.object_stores.read().unwrap();
        // scan for pointer-equal entry
        for (url, registered) in map.iter() {
            if Arc::ptr_eq(&store, registered) {
                return Some(url.clone());
            }
        }
        None
    }

    /// Returns a vector of all registered store prefixes.
    fn get_store_prefixes(&self) -> Vec<Url> {
        let stores = self.object_stores.read().unwrap();
        stores.keys().cloned().collect()
    }
}

struct PrefixObjectStoreRegistry {
    inner: DefaultObjectStoreRegistry,
    prefix_fn: Box<dyn Fn(&Url) -> Result<Url, url::ParseError> + Send + Sync>,
}

impl std::fmt::Debug for PrefixObjectStoreRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrefixObjectStoreRegistry")
            .field("inner", &self.inner)
            .finish()
    }
}

impl Default for PrefixObjectStoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PrefixObjectStoreRegistry {
    fn new() -> Self {
        Self::with_prefix_fn(Box::new(Self::default_prefix_fn))
    }

    fn with_prefix_fn(
        prefix_fn: Box<dyn Fn(&Url) -> Result<Url, url::ParseError> + Send + Sync>,
    ) -> Self {
        Self {
            inner: DefaultObjectStoreRegistry::new(),
            prefix_fn,
        }
    }

    fn default_prefix_fn(url: &Url) -> Result<Url, url::ParseError> {
        let prefix = format!(
            "{}://{}",
            url.scheme(),
            &url[url::Position::BeforeHost..url::Position::AfterPort],
        );
        Url::parse(&prefix)
    }
}

impl ObjectStoreRegistry for PrefixObjectStoreRegistry {
    fn register_store(
        &self,
        prefix: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.inner.register_store(prefix, store)
    }

    fn get_store(&self, url: &Url) -> Option<Arc<dyn ObjectStore>> {
        (self.prefix_fn)(url)
            .ok()
            .map(|prefix| self.inner.get_store(&prefix))
            .flatten()
    }

    fn get_prefix(&self, store: Arc<dyn ObjectStore>) -> Option<Url> {
        self.inner.get_prefix(store)
    }

    fn get_store_prefixes(&self) -> Vec<Url> {
        self.inner.get_store_prefixes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::InMemory;

    #[test]
    fn test_register_store() {
        let registry = DefaultObjectStoreRegistry::new();
        let url = Url::parse("inmemory://foo").unwrap();
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let old_store = registry.register_store(&url, Arc::clone(&store));
        assert!(old_store.is_none());
        let retrieved_store = registry.get_store(&url).unwrap();
        assert!(Arc::ptr_eq(&retrieved_store, &store));
    }

    #[test]
    fn test_reregister_store() {
        let registry = DefaultObjectStoreRegistry::new();
        let url = Url::parse("inmemory://foo").unwrap();
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let old_store = registry.register_store(&url, Arc::clone(&store));
        assert!(old_store.is_none());
        let old_store = registry.register_store(&url, Arc::new(InMemory::new()));
        assert!(Arc::ptr_eq(&old_store.unwrap(), &store));
    }

    #[test]
    fn test_get_store_miss() {
        let registry = DefaultObjectStoreRegistry::new();
        let url = Url::parse("inmemory://foo").unwrap();
        assert!(registry.get_store(&url).is_none());
    }

    #[test]
    fn test_get_prefix_round_trip() {
        let registry = DefaultObjectStoreRegistry::new();
        let url = Url::parse("inmemory://").unwrap();
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        registry.register_store(&url, Arc::clone(&store));
        assert_eq!(registry.get_prefix(Arc::clone(&store)).unwrap(), url);
    }

    #[test]
    fn test_get_prefix_miss() {
        let registry = DefaultObjectStoreRegistry::new();
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        assert!(registry.get_prefix(store).is_none());
    }

    #[test]
    fn test_list_urls() {
        let registry = DefaultObjectStoreRegistry::new();
        let url = Url::parse("inmemory://foo").unwrap();
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        registry.register_store(&url, store);
        let urls = registry.get_store_prefixes();
        assert_eq!(urls.len(), 1);
        assert_eq!(urls[0], url);
    }

    #[test]
    fn test_subprefix_miss() {
        let registry = DefaultObjectStoreRegistry::new();
        let base_url = Url::parse("inmemory://foo").unwrap();
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        registry.register_store(&base_url, Arc::clone(&store));
        let subprefix_url = Url::parse("inmemory://foo/bar").unwrap();
        let retrieved_store = registry.get_store(&subprefix_url);
        assert!(retrieved_store.is_none());
    }

    #[test]
    fn test_exact_url_match_behavior() {
        let registry = DefaultObjectStoreRegistry::new();
        let base_url = Url::parse("file:///foo").unwrap();
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        registry.register_store(&base_url, Arc::clone(&store));

        // Case 1: Exact match should work
        let exact_match = Url::parse("file:///foo").unwrap();
        let retrieved_store = registry.get_store(&exact_match);
        assert!(retrieved_store.is_some());
        assert!(Arc::ptr_eq(&retrieved_store.unwrap(), &store));

        // Case 2: Different URL format should not match
        let different_format = Url::parse("file://foo").unwrap();
        let retrieved_store = registry.get_store(&different_format);
        assert!(retrieved_store.is_none());

        // Case 3: Subpath should not match
        let subpath = Url::parse("file:///foo/bar").unwrap();
        let retrieved_store = registry.get_store(&subpath);
        assert!(retrieved_store.is_none());

        // Case 4: Different scheme should not match
        let different_scheme = Url::parse("s3://foo").unwrap();
        let retrieved_store = registry.get_store(&different_scheme);
        assert!(retrieved_store.is_none());
    }
}
