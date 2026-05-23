use crate::Error;
use std::collections::HashSet;

/// An individual capability that an [`ObjectStore`] implementation may support.
///
/// Used together with [`Capabilities`] to advertise optional backend features.
/// Obtain the set of supported capabilities via [`ObjectStore::capabilities`].
///
/// # String representation
///
/// Each variant has a stable kebab-case string form accessible via
/// [`Capability::as_str`] and parseable via [`Capability::from_str`].
/// These strings are intended for configuration, logging, and serialisation.
#[derive(Hash, Eq, PartialEq, Copy, Clone, Debug)]
pub enum Capability {
    /// List results from [`ObjectStore::list`] and
    /// [`ObjectStore::list_with_offset`] are returned in ascending
    /// lexicographic order by [`Path`].
    ///
    /// When this capability is present, callers may rely on the ordering and
    /// avoid buffering all results solely for sorting purposes.
    OrderedListing,
}

impl Capability {
    /// Returns the stable kebab-case string representation of this capability.
    ///
    /// The returned string can be round-tripped through [`Capability::from_str`].
    pub fn as_str(&self) -> &'static str {
        match self {
            Capability::OrderedListing => "ordered-listing",
        }
    }

    /// Parses a capability from its kebab-case string representation.
    ///
    /// Returns `None` if `s` does not correspond to any known capability.
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "ordered-listing" => Some(Capability::OrderedListing),
            _ => None,
        }
    }
}

/// Optional features supported by an [`ObjectStore`] implementation.
///
/// Obtain the capabilities of a store by calling [`ObjectStore::capabilities`].
/// All fields default to `false`; a store sets a field to `true` when it
/// natively supports that feature.
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
#[derive(Debug, PartialEq)]
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

    pub fn from_str(s: &str) -> crate::Result<Self> {
        let mut capabilities: Vec<Capability> = Vec::new();
        for mut cap in s.split(',') {
            cap = cap.trim();
            if cap.is_empty() {
                continue;
            }
            match Capability::from_str(cap) {
                Some(cap) => capabilities.push(cap),
                None => {
                    return Err(Error::Generic {
                        store: "Config",
                        source: format!("invalid capability: {cap}").into(),
                    });
                }
            }
        }
        Ok(Self::new(capabilities))
    }
}

#[cfg(test)]
mod tests {
    use super::{Capabilities, Capability};

    #[test]
    fn test_capability() {
        assert_eq!(Capability::OrderedListing.as_str(), "ordered-listing");
        assert_eq!(
            Capability::OrderedListing,
            Capability::from_str("ordered-listing").unwrap()
        );
        assert_eq!(Capability::from_str("invalid").is_some(), false);
    }

    #[test]
    fn test_capabilities() {
        assert_eq!(Capabilities::from_str("invalid").is_err(), true);
        assert_eq!(
            Capabilities::from_str("")
                .unwrap()
                .has(Capability::OrderedListing),
            false
        );
        assert_eq!(
            Capabilities::from_str("ordered-listing")
                .unwrap()
                .has(Capability::OrderedListing),
            true
        );
    }
}
