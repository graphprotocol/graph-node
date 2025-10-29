use std::collections::HashMap;

use anyhow::Result;

use crate::{amp::common::Ident, cheap_clone::CheapClone};

/// Caches identifiers that are used to match Arrow columns and subgraph entity fields.
pub(super) struct NameCache {
    cache: HashMap<Box<str>, Ident>,
}

impl NameCache {
    /// Creates a new empty cache.
    pub(super) fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    /// Returns the identifier for the given name.
    ///
    /// If the identifier exists in the cache, returns the cached version.
    /// Otherwise, creates a new identifier, caches it, and returns it.
    pub(super) fn ident(&mut self, name: &str) -> Result<Ident> {
        if let Some(ident) = self.cache.get(name) {
            return Ok(ident.cheap_clone());
        }

        let ident = Ident::new(name)?;
        self.cache.insert(name.into(), ident.cheap_clone());

        Ok(ident)
    }
}
