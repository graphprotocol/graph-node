use std::{collections::HashMap, sync::Arc};

use heck::ToSnakeCase;

use crate::{cheap_clone::CheapClone, derive::CheapClone};

/// Provides case-insensitive string comparison through normalization.
///
/// Normalizes names and stores them in memory for fast access.
pub(super) struct NameCache {
    cache: HashMap<Box<str>, NormalizedName>,
}

/// Contains a normalized name.
///
/// A normalized name is a list of lowercase tokens from the original name.
/// A token is a sequence of characters between case format separators.
#[derive(Debug, Clone, CheapClone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct NormalizedName(Arc<[Box<str>]>);

impl NameCache {
    /// Creates a new empty cache.
    pub(super) fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    /// Takes a `name` with any case format and returns a normalized version.
    ///
    /// A normalized version is a list of lowercase tokens from the original input.
    /// A token is a sequence of characters between case format separators.
    ///
    /// # Example
    ///
    /// ```no_run
    /// let mut name_cache = NameCache::new();
    ///
    /// assert_eq!(
    ///     name_cache.normalized("blockNumber"),
    ///     vec!["block".into(), "number".into()].into(),
    /// );
    /// assert_eq!(
    ///     name_cache.normalized("block number"),
    ///     vec!["block".into(), "number".into()].into(),
    /// );
    /// assert_eq!(
    ///     name_cache.normalized("block_number"),
    ///     vec!["block".into(), "number".into()].into(),
    /// );
    /// ```
    pub(super) fn normalized(&mut self, name: &str) -> NormalizedName {
        if let Some(normalized_name) = self.cache.get(name) {
            return normalized_name.cheap_clone();
        }

        let normalized_name = NormalizedName::new(name);

        self.cache
            .insert(name.into(), normalized_name.cheap_clone());

        normalized_name
    }
}

impl NormalizedName {
    /// Creates a normalized name from the input string.
    fn new(name: &str) -> Self {
        Self(
            name.to_snake_case()
                .split('_')
                .map(Into::into)
                .collect::<Vec<_>>()
                .into(),
        )
    }
}
