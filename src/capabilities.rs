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

//! Capability advertisement for [`ObjectStore`](crate::ObjectStore) implementations.
//!
//! See [`Capabilities`] and [`Capability`] for details.
use crate::Error;
use std::collections::HashSet;

const ORDERED_LISTING: &str = "ordered_listing";

/// An individual capability that an [`ObjectStore`] implementation may support.
///
/// Used together with [`Capabilities`] to advertise optional backend features.
/// Get the set of supported capabilities via [`ObjectStore::capabilities`].
#[derive(Hash, Eq, PartialEq, Copy, Clone, Debug)]
#[non_exhaustive]
pub enum Capability {
    /// List results from [`ObjectStore::list`] and
    /// [`ObjectStore::list_with_offset`] are returned in ascending
    /// lexicographic order by [`Path`].
    ///
    /// When this capability is present, callers may rely on the ordering and
    /// avoid buffering all results solely for sorting purposes.
    OrderedListing,
}

impl std::str::FromStr for Capability {
    type Err = Error;

    /// Parses a capability from its snake_case string representation.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            ORDERED_LISTING => Ok(Self::OrderedListing),
            cap => Err(Error::Generic {
                store: "Config",
                source: format!("invalid capability: {cap}").into(),
            }),
        }
    }
}

impl std::fmt::Display for Capability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OrderedListing => write!(f, "{}", ORDERED_LISTING),
        }
    }
}

/// Optional features supported by an [`ObjectStore`] implementation.
///
/// Get the capabilities of a store by calling [`ObjectStore::capabilities`].
/// Check whether [`Capability`] is supported by calling [`Capabilities::has`] method.
///
/// The struct is `#[non_exhaustive]` so that new capability flags can be added
/// in future versions without breaking existing code.
///
/// # Example
///
/// ```
/// # use object_store::{ObjectStore, memory::InMemory, Capability};
/// let store = InMemory::new();
/// if store.capabilities().has(Capability::OrderedListing) {
///     println!("list() results are in lexicographic order — no need to sort");
/// }
/// ```
#[derive(Debug, PartialEq, Clone, Default)]
pub struct Capabilities {
    supported: HashSet<Capability>,
}

impl Capabilities {
    /// Create a [`Capabilities`] from an explicit list of supported [`Capability`] values.
    ///
    /// Any capability not included in `capabilities` is considered unsupported.
    ///
    /// # Example
    ///
    /// ```
    /// # use object_store::{Capabilities, Capability};
    /// let caps = Capabilities::new([Capability::OrderedListing]);
    /// assert!(caps.has(Capability::OrderedListing));
    /// ```
    pub fn new(capabilities: impl IntoIterator<Item = Capability>) -> Self {
        Self {
            supported: capabilities.into_iter().collect(),
        }
    }

    /// Returns `true` if the given [`Capability`] is supported by this store.
    pub fn has(&self, capability: Capability) -> bool {
        self.supported.contains(&capability)
    }
}

impl std::str::FromStr for Capabilities {
    type Err = Error;

    /// Parses a comma-separated list of capability names into a [`Capabilities`].
    fn from_str(s: &str) -> crate::Result<Self> {
        let mut capabilities: Vec<Capability> = Vec::new();
        for mut cap in s.split(',') {
            cap = cap.trim();
            if cap.is_empty() {
                continue;
            }
            capabilities.push(cap.parse::<Capability>()?);
        }
        Ok(Self::new(capabilities))
    }
}

impl std::fmt::Display for Capabilities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut caps: Vec<_> = self.supported.iter().map(ToString::to_string).collect();
        caps.sort();
        write!(f, "{}", caps.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::{Capabilities, Capability};

    #[test]
    fn test_capability() {
        assert_eq!(format!("{}", Capability::OrderedListing), "ordered_listing");
        assert_eq!(
            Capability::OrderedListing,
            "ordered_listing".parse::<Capability>().unwrap()
        );
        assert_eq!("invalid".parse::<Capability>().is_ok(), false);
    }

    #[test]
    fn test_capabilities() {
        assert_eq!("invalid".parse::<Capabilities>().is_err(), true);
        assert_eq!(
            "".parse::<Capabilities>()
                .unwrap()
                .has(Capability::OrderedListing),
            false
        );
        assert_eq!(
            "ordered_listing"
                .parse::<Capabilities>()
                .unwrap()
                .has(Capability::OrderedListing),
            true
        );
    }
}
