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

//! Map object URLs to [`ObjectStore`]

use crate::path::{InvalidPart, Path, PathPart};
use crate::{parse_url_opts, ObjectStore};
use parking_lot::RwLock;
use std::borrow::Borrow;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, Bound};
use std::sync::Arc;
use url::Url;

/// Error type for [`ObjectStoreRegistry`]
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
enum Error {
    #[error("ObjectStore not found")]
    NotFound,

    #[error("Error parsing URL path segment")]
    InvalidPart(#[from] InvalidPart),
}

impl From<Error> for crate::Error {
    fn from(value: Error) -> Self {
        Self::Generic {
            store: "ObjectStoreRegistry",
            source: Box::new(value),
        }
    }
}

/// [`ObjectStoreRegistry`] maps a URL to an [`ObjectStore`] instance
pub trait ObjectStoreRegistry: Send + Sync + std::fmt::Debug + 'static {
    /// Register a new store for the provided store URL
    ///
    /// If a store with the same URL existed before, it is replaced and returned
    fn register(&self, url: Url, store: Arc<dyn ObjectStore>) -> Option<Arc<dyn ObjectStore>>;

    /// Unregister a store for the provided store URL
    fn unregister(&self, url: &Url) -> Option<Arc<dyn ObjectStore>>;

    /// Resolve an object URL
    ///
    /// If [`ObjectStoreRegistry::register`] has been called with a URL with the same
    /// scheme and host as the object URL, and a path that is a prefix of the object URL's
    /// it should be returned with along with the trailing path. In the event of multiple
    /// possibilities the longest path match should be returned.
    ///
    /// If a store hasn't been registered, [`ObjectStoreRegistry`] may lazily create
    /// one if the URL is understood, or return an [`Error::NotFound`]
    ///
    /// For example
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use url::Url;
    /// # use object_store::memory::InMemory;
    /// # use object_store::ObjectStore;
    /// # use object_store::prefix::PrefixStore;
    /// # use object_store::registry::{DefaultObjectStoreRegistry, ObjectStoreRegistry};
    /// #
    /// let registry = DefaultObjectStoreRegistry::new();
    ///
    /// let bucket1 = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    /// let base = Url::parse("s3://bucket1/").unwrap();
    /// registry.register(base, bucket1.clone());
    ///
    /// let url = Url::parse("s3://bucket1/path/to/object").unwrap();
    /// let (ret, path) = registry.resolve(&url).unwrap();
    /// assert_eq!(path.as_ref(), "path/to/object");
    /// assert!(Arc::ptr_eq(&ret, &bucket1));
    ///
    /// let bucket2 = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    /// let base = Url::parse("https://s3.region.amazonaws.com/bucket").unwrap();
    /// registry.register(base, bucket2.clone());
    ///
    /// let url = Url::parse("https://s3.region.amazonaws.com/bucket/path/to/object").unwrap();
    /// let (ret, path) = registry.resolve(&url).unwrap();
    /// assert_eq!(path.as_ref(), "path/to/object");
    /// assert!(Arc::ptr_eq(&ret, &bucket2));
    ///
    /// let bucket3 = Arc::new(PrefixStore::new(InMemory::new(), "path")) as Arc<dyn ObjectStore>;
    /// let base = Url::parse("https://s3.region.amazonaws.com/bucket/path").unwrap();
    /// registry.register(base, bucket3.clone());
    ///
    /// let url = Url::parse("https://s3.region.amazonaws.com/bucket/path/to/object").unwrap();
    /// let (ret, path) = registry.resolve(&url).unwrap();
    /// assert_eq!(path.as_ref(), "to/object");
    /// assert!(Arc::ptr_eq(&ret, &bucket3));
    /// ```
    fn resolve(&self, url: &Url) -> crate::Result<(Arc<dyn ObjectStore>, Path)>;
}

/// An [`ObjectStoreRegistry`] that uses [`parse_url`] to create stores based on the environment
#[derive(Debug, Default)]
pub struct DefaultObjectStoreRegistry {
    map: RwLock<BTreeMap<StrUrl, RegistryEntry>>,
}

/// Wrapper around [`Url`] implementing `Borrow<str>`
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
struct StrUrl(Url);

impl Borrow<str> for StrUrl {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug)]
struct RegistryEntry {
    store: Arc<dyn ObjectStore>,
    /// The number of non-empty path segments in the store's URL
    path_segments: usize,
}

impl RegistryEntry {
    fn new(url: &Url, store: Arc<dyn ObjectStore>) -> Self {
        let path_segments = match url.path() {
            "/" => 0,
            path => path.as_bytes().iter().filter(|&&c| c == b'/').count(),
        };
        Self {
            path_segments,
            store,
        }
    }

    fn resolve(&self, url: &Url) -> Result<(Arc<dyn ObjectStore>, Path), Error> {
        let segments = match url.path_segments() {
            Some(segments) => segments.skip(self.path_segments),
            None => return Ok((Arc::clone(&self.store), Path::default())),
        };

        let path = segments.map(PathPart::parse).collect::<Result<_, _>>()?;
        Ok((Arc::clone(&self.store), path))
    }
}

impl DefaultObjectStoreRegistry {
    /// Create a new [`DefaultObjectStoreRegistry`]
    pub fn new() -> Self {
        Self::default()
    }
}

impl ObjectStoreRegistry for DefaultObjectStoreRegistry {
    fn register(&self, url: Url, store: Arc<dyn ObjectStore>) -> Option<Arc<dyn ObjectStore>> {
        let entry = RegistryEntry::new(&url, store);
        Some(self.map.write().insert(StrUrl(url), entry)?.store)
    }

    fn unregister(&self, store: &Url) -> Option<Arc<dyn ObjectStore>> {
        Some(self.map.write().remove(store.as_str())?.store)
    }

    fn resolve(&self, to_resolve: &Url) -> crate::Result<(Arc<dyn ObjectStore>, Path)> {
        {
            let map = self.map.read();
            let start = &to_resolve[..url::Position::BeforePath];

            let candidates = map
                .range::<str, _>((Bound::Included(start), Bound::Unbounded))
                .take_while(|&(base, _)| &base.0[..url::Position::BeforePath] == start);

            let mut longest_len = 0;
            let mut longest = None;
            for (base, entry) in candidates {
                let len = base.0.as_str().len();
                if len > longest_len && to_resolve.as_str().starts_with(base.0.as_str()) {
                    longest = Some(entry);
                    longest_len = len;
                }
            }

            if let Some(entry) = longest {
                return Ok(entry.resolve(to_resolve)?);
            }
        }

        if let Ok((store, path)) = parse_url_opts(to_resolve, std::env::vars()) {
            let mut store_url = to_resolve.clone();
            {
                let mut segments = store_url.path_segments_mut().unwrap();
                for _ in path.parts() {
                    segments.pop();
                }
            }
            let candidate = RegistryEntry::new(&store_url, store.into());

            return Ok(match self.map.write().entry(StrUrl(store_url)) {
                Entry::Vacant(vacant) => vacant.insert(candidate).resolve(to_resolve)?,
                Entry::Occupied(o) => o.get().resolve(to_resolve)?,
            });
        }

        Err(Error::NotFound.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::InMemory;
    use crate::prefix::PrefixStore;

    #[test]
    fn test_default_registry() {
        let registry = DefaultObjectStoreRegistry::new();

        // Should automatically register in memory store
        let object = Url::parse("memory:///banana").unwrap();
        let (resolved, path) = registry.resolve(&object).unwrap();
        assert_eq!(path.as_ref(), "banana");

        // Should replace store
        let base = Url::parse("memory:///").unwrap();
        let replaced = registry.register(base, Arc::new(InMemory::new())).unwrap();
        assert!(Arc::ptr_eq(&resolved, &replaced));

        // Should not replace store
        let base = Url::parse("memory:///banana").unwrap();
        let banana = Arc::new(PrefixStore::new(InMemory::new(), "banana")) as Arc<dyn ObjectStore>;
        assert!(registry.register(base, Arc::clone(&banana)).is_none());

        let (resolved, path) = registry.resolve(&object).unwrap();
        assert_eq!(path.as_ref(), "");
        assert!(Arc::ptr_eq(&resolved, &banana));

        let base = Url::parse("memory:///apples").unwrap();
        let apples = Arc::new(PrefixStore::new(InMemory::new(), "apples")) as Arc<dyn ObjectStore>;
        assert!(registry.register(base, Arc::clone(&apples)).is_none());

        let (resolved, path) = registry.resolve(&object).unwrap();
        assert_eq!(path.as_ref(), "");
        assert!(Arc::ptr_eq(&resolved, &banana));
    }
}
