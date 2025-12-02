use std::{collections::HashMap, sync::Arc};

use inflector::Inflector;

use crate::cheap_clone::CheapClone;

/// Normalizes and caches identifiers that are used to match Arrow columns and subgraph entity fields.
pub(super) struct NameCache {
    cache: HashMap<Box<str>, Arc<str>>,
}

impl NameCache {
    /// Creates a new empty cache.
    pub(super) fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    /// Normalizes and returns the identifier for the given name.
    ///
    /// If the identifier exists in the cache, returns the cached version.
    /// Otherwise, creates a new normalized identifier, caches it, and returns it.
    pub(super) fn ident(&mut self, name: &str) -> Arc<str> {
        if let Some(ident) = self.cache.get(name) {
            return ident.cheap_clone();
        }

        let ident: Arc<str> = name.to_camel_case().to_lowercase().into();
        self.cache.insert(name.into(), ident.cheap_clone());

        ident
    }
}
