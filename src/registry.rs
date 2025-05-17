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
use crate::{parse_url, Error, ObjectStore};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use url::Url;

type GetStoreResult = Result<Option<(Arc<dyn ObjectStore>, Url)>, Error>;

/// [`ObjectStoreRegistry`] maps a URL to an [`ObjectStore`] instance. The meaning of
/// a URL mapping depends on the [`ObjectStoreRegistry`] implementation. See implementation
/// docs for more details.
pub trait ObjectStoreRegistry: Send + Sync + std::fmt::Debug + 'static {
    /// Register a new store for the provided URL
    ///
    /// ## Returns
    ///
    /// If a store with the same URL mapping exists before, it is replaced and returned along
    /// with the mapped URL.
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<(Arc<dyn ObjectStore>, Url)>;

    /// Get a store for the provided URL. The input URL is mapped to an [`ObjectStore`]
    /// instance based on the [`ObjectStoreRegistry`] implementation. See implementation docs
    /// for more details.
    ///
    /// If no [`ObjectStore`] is found for the `url`, an [`ObjectStore`] may be lazily be
    /// created and registered. The logic for doing so is left to each [`ObjectStoreRegistry`]
    /// implementation.
    ///
    /// ## Returns
    ///
    /// If a store is found for the `url`, it is returned along with the mapped URL.
    ///
    /// If no store is found for the `url`, `None` is returned.
    ///
    /// ## Errors
    ///
    /// Returns an error if an implementation can't parse a URL or create a store.
    fn get_store(&self, url: &Url) -> GetStoreResult;

    /// List all registered store URLs. These are the URL mappings for all registered stores.
    ///
    /// ## Returns
    ///
    /// A vector of all registered store URLs.
    fn get_store_urls(&self) -> Vec<Url>;
}

/// An [`ObjectStoreRegistry`] implementation that maps URLs to object stores using
/// `scheme://host:port`.
///
/// ## Examples
///
/// Registering a store:
///
/// ```
/// # use std::sync::Arc;
/// # use url::Url;
/// # use object_store::ObjectStore;
/// # use object_store::memory::InMemory;
/// # use object_store::registry::{ObjectStoreRegistry, DefaultObjectStoreRegistry};
/// let registry = DefaultObjectStoreRegistry::new();
/// let url = Url::parse("memory://path/to/store").unwrap();
/// let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
/// registry.register_store(&url, Arc::clone(&store));
/// let (retrieved_store, mapped_url) = registry.get_store(&url).unwrap().unwrap();
/// assert_eq!(mapped_url.as_str(), "memory://");
/// assert!(Arc::ptr_eq(&retrieved_store, &store));
/// ```
///
/// Dynamically creating a store:
///
/// ```
/// # use std::sync::Arc;
/// # use url::Url;
/// # use object_store::ObjectStore;
/// # use object_store::registry::{ObjectStoreRegistry, DefaultObjectStoreRegistry};
/// let registry = DefaultObjectStoreRegistry::new();
/// let url = Url::parse("memory://path/to/store").unwrap();
/// let (store, mapped_url) = registry.get_store(&url).unwrap().unwrap();
/// assert_eq!(mapped_url.as_str(), "memory://");
/// ```
pub struct DefaultObjectStoreRegistry {
    /// A map from URL to object store that serve list / read operations for the store
    object_stores: RwLock<HashMap<Url, Arc<dyn ObjectStore>>>,
}

impl std::fmt::Debug for DefaultObjectStoreRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stores = self.object_stores.read().unwrap();
        f.debug_struct("DefaultObjectStoreRegistry")
            .field("urls", &stores.keys().cloned().collect::<Vec<_>>())
            .finish()
    }
}

impl Default for DefaultObjectStoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultObjectStoreRegistry {
    /// Create a new [`DefaultObjectStoreRegistry`] with no registered stores.
    pub fn new() -> Self {
        let object_stores = RwLock::new(HashMap::new());
        Self { object_stores }
    }

    /// Get the key of a url for object store registration. Mapping rules are as follows:
    ///
    /// - Any URL with a `file` scheme is mapped to `file:///`
    /// - Any URL with a `memory` scheme is mapped to `memory://`
    /// - All other URLs are mapped to `scheme://host:port`
    ///
    /// ## Returns
    ///
    /// A [`Url`] with the same scheme and host as the input, but with an empty path.
    ///
    /// ## Errors
    ///
    /// Returns an error if the input is not a valid URL.
    fn map_url_to_key(url: &Url) -> Url {
        match url.scheme() {
            // Don't include the host for memory or path. Just hard code it
            // since [`crate::parse::parse_url`] expects these to never have
            // a "host" component.
            "memory" => Url::parse(&format!("{}://", url.scheme())),
            // Note this will handle file://path/to/file as well
            // as file:///path/to/file even though file://path/to/file
            // is not technically a valid URL.
            "file" => Url::parse(&format!("{}:///", url.scheme())),
            _ => Url::parse(&format!(
                "{}://{}",
                url.scheme(),
                &url[url::Position::BeforeHost..url::Position::AfterPort],
            )),
        }
        .unwrap()
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
    ) -> Option<(Arc<dyn ObjectStore>, Url)> {
        let key = Self::map_url_to_key(url);
        let mut stores = self.object_stores.write().unwrap();
        stores
            .insert(key.clone(), store)
            .map(|old_store| (old_store, key))
    }

    /// Get a store that was registered with the provided URL.
    ///
    /// If no store was registered with the provided URL, `None` is returned.
    fn get_store(&self, url: &Url) -> Result<Option<(Arc<dyn ObjectStore>, Url)>, crate::Error> {
        let key = Self::map_url_to_key(url);
        eprintln!("key: {key}");
        let mut stores = self.object_stores.write().unwrap();
        if let Some(store) = stores.get(&key) {
            Ok(Some((Arc::clone(store), key)))
        } else {
            let (store, _) = parse_url(&key)?;
            let store: Arc<dyn ObjectStore> = store.into();
            stores.insert(key.clone(), Arc::clone(&store));
            Ok(Some((store, key)))
        }
    }

    /// Returns a vector of all registered store URLs.
    fn get_store_urls(&self) -> Vec<Url> {
        let stores = self.object_stores.read().unwrap();
        stores.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::InMemory;

    #[test]
    fn test_register_store() {
        let registry = DefaultObjectStoreRegistry::new();
        let url = Url::parse("memory://").unwrap();
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let old_store = registry.register_store(&url, Arc::clone(&store));
        assert!(old_store.is_none());
        let new_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let (old_store, mapped_url) = registry
            .register_store(&url, Arc::clone(&new_store))
            .unwrap();
        assert_eq!(mapped_url.as_str(), "memory://");
        assert!(Arc::ptr_eq(&old_store, &store));
        let (retrieved_store, mapped_url) = registry.get_store(&url).unwrap().unwrap();
        assert_eq!(mapped_url.as_str(), "memory://");
        assert!(Arc::ptr_eq(&retrieved_store, &new_store));
    }

    #[tokio::test]
    async fn test_dynamic_register_store() {
        let registry = DefaultObjectStoreRegistry::new();
        let url = Url::parse("memory://").unwrap();
        let (first_store, mapped_url) = registry.get_store(&url).unwrap().unwrap();
        assert_eq!(mapped_url.as_str(), "memory://");
        first_store.put(&"/foo".into(), "bar".into()).await.unwrap();
        let (second_store, mapped_url) = registry.get_store(&url).unwrap().unwrap();
        assert_eq!(mapped_url.as_str(), "memory://");
        eprintln!("first_store: {:?}", first_store);
        eprintln!("second_store: {:?}", second_store);
        assert!(Arc::ptr_eq(&second_store, &first_store));
        let val = second_store
            .get(&"/foo".into())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(val.as_ref(), b"bar");
    }

    #[test]
    fn test_list_urls() {
        let registry = DefaultObjectStoreRegistry::new();
        let url = Url::parse("memory://").unwrap();
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        registry.register_store(&url, store);
        let urls = registry.get_store_urls();
        assert_eq!(urls.len(), 1);
        assert_eq!(urls[0].as_str(), "memory://");
    }

    #[test]
    fn test_get_child_url() {
        let registry = DefaultObjectStoreRegistry::new();
        let base_url = Url::parse("memory://").unwrap();
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        registry.register_store(&base_url, Arc::clone(&store));
        let subprefix_url = Url::parse("memory://foo/bar").unwrap();
        let (retrieved_store, mapped_url) = registry.get_store(&subprefix_url).unwrap().unwrap();
        assert_eq!(mapped_url.as_str(), "memory://");
        assert!(Arc::ptr_eq(&retrieved_store, &store));
    }

    #[test]
    fn test_map_url_to_key() {
        let test_cases = [
            ("s3://bucket", "s3://bucket"),
            ("s3://bucket/path", "s3://bucket"),
            ("s3://bucket/path?param=value", "s3://bucket"),
            ("memory://", "memory://"),
            ("memory://path", "memory://"),
            ("file:///", "file:///"),
            ("file:///path", "file:///"),
            ("http://host:1234", "http://host:1234"),
            ("http://host:1234/path", "http://host:1234"),
            (
                "http://user:pass@host:1234/path/to/file",
                "http://host:1234",
            ),
        ];

        for (input, expected) in test_cases {
            let input_url = Url::parse(input).unwrap();
            let expected_url = Url::parse(expected).unwrap();
            let result = DefaultObjectStoreRegistry::map_url_to_key(&input_url);

            assert_eq!(
                result.as_str(),
                expected_url.as_str(),
                "Expected '{}' to map to '{}', but got '{}'",
                input,
                expected,
                result
            );
        }
    }
}
