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

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{Instrument, trace, trace_span};

/// A temporary authentication token with an associated expiry
#[derive(Debug, Clone)]
pub(crate) struct TemporaryToken<T> {
    /// The temporary credential
    pub token: T,
    /// The instant at which this credential is no longer valid
    /// None means the credential does not expire
    pub expiry: Option<Instant>,
}

/// Provides [`TokenCache::get_or_insert_with`] which can be used to cache a
/// [`TemporaryToken`] based on its expiry
#[derive(Debug)]
pub(crate) struct TokenCache<T> {
    id: OnceLock<usize>,
    cache: RwLock<Option<CacheEntry<T>>>,
    min_ttl: Duration,
    fetch_backoff: Duration,
}

#[derive(Debug)]
struct CacheEntry<T> {
    token: TemporaryToken<T>,
    fetched_at: Instant,
}

impl<T> Default for TokenCache<T> {
    fn default() -> Self {
        Self {
            id: OnceLock::new(),
            cache: Default::default(),
            min_ttl: Duration::from_secs(300),
            // How long to wait before re-attempting a token fetch after receiving one that
            // is still within the min-ttl
            fetch_backoff: Duration::from_millis(100),
        }
    }
}

impl<T: Clone + Send + Sync> TokenCache<T> {
    /// Override the minimum remaining TTL for a cached token to be used
    #[cfg(any(feature = "aws-base", feature = "azure-base", feature = "gcp-base"))]
    pub(crate) fn with_min_ttl(self, min_ttl: Duration) -> Self {
        Self { min_ttl, ..self }
    }

    pub(crate) async fn get_or_insert_with<F, Fut, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<TemporaryToken<T>, E>> + Send,
    {
        // Allocate an opaque identifier only when tracing is enabled. Store clones that
        // share this cache keep the same ID across lookups and task migrations.
        let lookup = trace_span!(
            "token_cache_lookup",
            cache_id = *self.id.get_or_init(|| {
                static NEXT_ID: AtomicUsize = AtomicUsize::new(1);
                NEXT_ID.fetch_add(1, Ordering::Relaxed)
            })
        );
        let observed = !lookup.is_disabled();
        async {
            let result = self.lookup(f, observed).await;
            trace!(stage = "lookup_returned", success = result.is_ok());
            result
        }
        .instrument(lookup)
        .await
    }

    async fn lookup<F, Fut, E>(&self, f: F, observed: bool) -> Result<T, E>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<TemporaryToken<T>, E>> + Send,
    {
        let now = Instant::now();
        let is_token_valid = |entry: &CacheEntry<T>| {
            entry.token.expiry.is_none_or(|ttl| {
                ttl.checked_duration_since(now).unwrap_or_default() > self.min_ttl ||
                // if we've recently attempted to fetch this token and it's not actually
                // expired, we'll wait to re-fetch it and return the cached one
                (entry.fetched_at.elapsed() < self.fetch_backoff && ttl > now)
            })
        };

        if let Some(cache) = acquire(self.cache.read(), observed, "read").await.as_ref() {
            if is_token_valid(cache) {
                return Ok(cache.token.token.clone());
            }
        }

        let mut guard = acquire(self.cache.write(), observed, "write").await;
        trace!(stage = "write_acquired");
        if let Some(cache) = guard.as_ref() {
            if is_token_valid(cache) {
                // Refresh race
                return Ok(cache.token.token.clone());
            }
        }

        trace!(stage = "refresh_started");
        let result = f().await;
        trace!(stage = "refresh_returned", success = result.is_ok());
        let cached = result?;
        let token = cached.token.clone();
        *guard = Some(CacheEntry {
            token: cached,
            fetched_at: Instant::now(),
        });

        Ok(token)
    }
}

// Poll the original acquisition future throughout, retaining its place in the lock queue.
async fn acquire<F: Future>(lock: F, observed: bool, mode: &'static str) -> F::Output {
    if !observed {
        return lock.await;
    }
    let mut lock = pin!(lock);
    let mut waiting = false;
    poll_fn(|cx| match lock.as_mut().poll(cx) {
        Poll::Pending => {
            if !waiting {
                trace!(stage = "lock_wait", mode);
                waiting = true;
            }
            Poll::Pending
        }
        ready => ready,
    })
    .await
}

#[cfg(test)]
mod test {
    use crate::client::token::{TemporaryToken, TokenCache};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::{Duration, Instant};

    // Helper function to create a token with a specific expiry duration from now
    fn create_token(expiry_duration: Option<Duration>) -> TemporaryToken<String> {
        TemporaryToken {
            token: "test_token".to_string(),
            expiry: expiry_duration.map(|d| Instant::now() + d),
        }
    }

    #[tokio::test]
    async fn test_expired_token_is_refreshed() {
        let cache = TokenCache::default();
        static COUNTER: AtomicU32 = AtomicU32::new(0);

        async fn get_token() -> Result<TemporaryToken<String>, String> {
            COUNTER.fetch_add(1, Ordering::SeqCst);
            Ok::<_, String>(create_token(Some(Duration::from_secs(0))))
        }

        // Should fetch initial token
        let _ = cache.get_or_insert_with(get_token).await.unwrap();
        assert_eq!(COUNTER.load(Ordering::SeqCst), 1);

        tokio::time::sleep(Duration::from_millis(2)).await;

        // Token is expired, so should fetch again
        let _ = cache.get_or_insert_with(get_token).await.unwrap();
        assert_eq!(COUNTER.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_min_ttl_causes_refresh() {
        let cache = TokenCache {
            min_ttl: Duration::from_secs(1),
            fetch_backoff: Duration::from_millis(1),
            ..Default::default()
        };

        static COUNTER: AtomicU32 = AtomicU32::new(0);

        async fn get_token() -> Result<TemporaryToken<String>, String> {
            COUNTER.fetch_add(1, Ordering::SeqCst);
            Ok::<_, String>(create_token(Some(Duration::from_millis(100))))
        }

        // Initial fetch
        let _ = cache.get_or_insert_with(get_token).await.unwrap();
        assert_eq!(COUNTER.load(Ordering::SeqCst), 1);

        // Should not fetch again since not expired and within fetch_backoff
        let _ = cache.get_or_insert_with(get_token).await.unwrap();
        assert_eq!(COUNTER.load(Ordering::SeqCst), 1);

        tokio::time::sleep(Duration::from_millis(2)).await;

        // Should fetch, since we've passed fetch_backoff
        let _ = cache.get_or_insert_with(get_token).await.unwrap();
        assert_eq!(COUNTER.load(Ordering::SeqCst), 2);
    }
}
