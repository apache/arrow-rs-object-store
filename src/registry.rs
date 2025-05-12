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

/// A function that takes a URL and returns a prefix for that URL
pub type PrefixFn = Box<dyn Fn(&Url) -> Result<Url, url::ParseError> + Send + Sync>;

/// A [`ObjectStoreRegistry`] that uses a prefix function to determine the prefix for a URL when
/// retrieving a store. Stores are registered with a prefix. When a user calls `get_store`, the
/// prefix function is applied to the supplied URL to determine the prefix for the URL. The
/// registered store with the matching prefix is then returned.
///
/// ```rust
/// use std::sync::Arc;
/// use url::Url;
/// use object_store::ObjectStore;
/// use object_store::memory::InMemory;
/// use object_store::registry::{PrefixObjectStoreRegistry, ObjectStoreRegistry};
///
/// let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
/// let registry = PrefixObjectStoreRegistry::new();
/// let parent_url = Url::parse("memory://parent").unwrap();
/// let child_url = Url::parse("memory://parent/child").unwrap();
/// registry.register_store(&parent_url, Arc::clone(&store));
/// let found_store = registry.get_store(&child_url).unwrap();
/// assert!(Arc::ptr_eq(&found_store, &store));
/// let found_url = registry.get_prefix(Arc::clone(&store)).unwrap();
/// assert_eq!(found_url, parent_url);
/// ```
pub struct PrefixObjectStoreRegistry {
    inner: DefaultObjectStoreRegistry,
    prefix_fn: PrefixFn,
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
    /// Create a new [`PrefixObjectStoreRegistry`] with the default prefix function
    pub fn new() -> Self {
        Self {
            inner: DefaultObjectStoreRegistry::new(),
            prefix_fn: Box::new(Self::default_prefix_fn),
        }
    }

    /// Create a new [`PrefixObjectStoreRegistry`] with the provided prefix function
    pub fn with_prefix_fn(mut self, prefix_fn: PrefixFn) -> Self {
        self.prefix_fn = prefix_fn;
        self
    }

    /// The default prefix function. Returns a URL with in the format
    /// `scheme://host:port`. For example:
    ///
    /// - `memory://` -> `memory://`
    /// - `memory://child` -> `memory://child`
    /// - `memory://child/grandchild` -> `memory://child`
    pub fn default_prefix_fn(url: &Url) -> Result<Url, url::ParseError> {
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
            .and_then(|prefix| self.inner.get_store(&prefix))
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

    #[test]
    fn test_default_prefix_fn() {
        // Test simple URLs without hosts
        let url = Url::parse("memory://").unwrap();
        let prefix = PrefixObjectStoreRegistry::default_prefix_fn(&url).unwrap();
        assert_eq!(prefix.as_str(), "memory://");

        // Test s3 URLs
        let url = Url::parse("s3://bucket").unwrap();
        let prefix = PrefixObjectStoreRegistry::default_prefix_fn(&url).unwrap();
        assert_eq!(prefix.as_str(), "s3://bucket");

        // Test s3 URLs with path
        let url = Url::parse("s3://bucket/path/to/file").unwrap();
        let prefix = PrefixObjectStoreRegistry::default_prefix_fn(&url).unwrap();
        assert_eq!(prefix.as_str(), "s3://bucket");

        // Test http URLs with port
        let url = Url::parse("http://example.com:8080/path").unwrap();
        let prefix = PrefixObjectStoreRegistry::default_prefix_fn(&url).unwrap();
        assert_eq!(prefix.as_str(), "http://example.com:8080/");

        // Test http URLs user, pass, and port
        let url = Url::parse("http://user:pass@example.com:8080/path").unwrap();
        let prefix = PrefixObjectStoreRegistry::default_prefix_fn(&url).unwrap();
        assert_eq!(prefix.as_str(), "http://example.com:8080/");

        // Test file URLs
        let url = Url::parse("file:///path/to/file").unwrap();
        let prefix = PrefixObjectStoreRegistry::default_prefix_fn(&url).unwrap();
        assert_eq!(prefix.as_str(), "file:///");
    }

    #[test]
    fn test_prefix_registry_register_store() {
        let registry = PrefixObjectStoreRegistry::new();
        let url = Url::parse("memory://foo").unwrap();
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        // Test initial registration
        let old_store = registry.register_store(&url, Arc::clone(&store));
        assert!(old_store.is_none());

        // Test retrieval
        let child_url = Url::parse("memory://foo/bar").unwrap();
        let retrieved_store = registry.get_store(&child_url).unwrap();
        assert!(Arc::ptr_eq(&retrieved_store, &store));

        // Test parent miss
        let parent_url = Url::parse("memory://").unwrap();
        assert!(registry.get_store(&parent_url).is_none());
    }

    #[test]
    fn test_prefix_registry_get_store() {
        let registry = PrefixObjectStoreRegistry::new();

        // Register a store with a prefix
        let prefix_url = Url::parse("s3://mybucket").unwrap();
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        registry.register_store(&prefix_url, Arc::clone(&store));

        // Test with exact match
        let exact_url = Url::parse("s3://mybucket").unwrap();
        let retrieved_store = registry.get_store(&exact_url).unwrap();
        assert!(Arc::ptr_eq(&retrieved_store, &store));

        // Test with path - should still match because of prefix
        let path_url = Url::parse("s3://mybucket/path/to/object").unwrap();
        let retrieved_store = registry.get_store(&path_url).unwrap();
        assert!(Arc::ptr_eq(&retrieved_store, &store));

        // Test with different bucket - should not match
        let different_url = Url::parse("s3://otherbucket/path").unwrap();
        assert!(registry.get_store(&different_url).is_none());

        // Test with different scheme - should not match
        let different_scheme = Url::parse("file:///path").unwrap();
        assert!(registry.get_store(&different_scheme).is_none());
    }

    #[test]
    fn test_prefix_registry_get_prefix() {
        let registry = PrefixObjectStoreRegistry::new();
        let url = Url::parse("memory://foo").unwrap();
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        // Register store
        registry.register_store(&url, Arc::clone(&store));

        // Test get_prefix retrieves the correct URL
        let retrieved_url = registry.get_prefix(Arc::clone(&store)).unwrap();
        assert_eq!(retrieved_url, url);

        // Test with unregistered store
        let unregistered_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        assert!(registry.get_prefix(unregistered_store).is_none());
    }

    #[test]
    fn test_prefix_registry_get_store_prefixes() {
        let registry = PrefixObjectStoreRegistry::new();

        // Register multiple stores
        let url1 = Url::parse("s3://bucket1").unwrap();
        let url2 = Url::parse("s3://bucket2").unwrap();
        let url3 = Url::parse("inmemory://test").unwrap();

        registry.register_store(&url1, Arc::new(InMemory::new()));
        registry.register_store(&url2, Arc::new(InMemory::new()));
        registry.register_store(&url3, Arc::new(InMemory::new()));

        // Get all prefixes
        let prefixes = registry.get_store_prefixes();

        // Verify number of prefixes
        assert_eq!(prefixes.len(), 3);

        // Verify all registered URLs are in the result
        assert!(prefixes.contains(&url1));
        assert!(prefixes.contains(&url2));
        assert!(prefixes.contains(&url3));
    }

    #[test]
    fn test_prefix_registry_with_custom_prefix_fn() {
        // Create a custom prefix function that adds a path segment
        let custom_prefix_fn: PrefixFn = Box::new(|url| {
            let mut prefix = PrefixObjectStoreRegistry::default_prefix_fn(url)?;
            prefix.set_path("/custom");
            Ok(prefix)
        });

        let registry = PrefixObjectStoreRegistry::new().with_prefix_fn(custom_prefix_fn);

        // Register a store with this prefix
        let url = Url::parse("s3://bucket/custom").unwrap();
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        registry.register_store(&url, Arc::clone(&store));

        // Test lookup with a URL that should map to the custom prefix
        let lookup_url = Url::parse("s3://bucket/path/to/object").unwrap();

        // When using our custom_prefix_fn, the URL for lookup_url maps to s3://bucket/custom
        // which is our registered URL, so it should return the store
        let retrieved_store = registry.get_store(&lookup_url);
        assert!(retrieved_store.is_some());
        assert!(Arc::ptr_eq(&retrieved_store.unwrap(), &store));

        // Another URL that should map to the same store
        let custom_path_url = Url::parse("s3://bucket/custom/file.txt").unwrap();
        let retrieved_store = registry.get_store(&custom_path_url);

        // This URL will map to s3://bucket/custom via our custom prefix function,
        // which matches our registered URL, so it should return the store
        assert!(retrieved_store.is_some());
        assert!(Arc::ptr_eq(&retrieved_store.unwrap(), &store));
    }
}
