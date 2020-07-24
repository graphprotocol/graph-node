use futures03::future::FutureExt;
use futures03::future::Shared;
use graph::prelude::{futures03, CheapClone};
use stable_hash::crypto::SetHasher;
use stable_hash::prelude::*;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

type Hash = <SetHasher as StableHasher>::Out;

type PinFut<R> = Pin<Box<dyn Future<Output = R> + 'static + Send>>;
/// Cache that keeps a result around as long as it is still being processed.
/// The cache ensures that the query is not re-entrant, so multiple consumers
/// of identical queries will not execute them in parallel.
///
/// This has a lot in common with AsyncCache in the network-services repo,
/// but more specialized.
pub struct QueryCache<R> {
    cache: Arc<Mutex<HashMap<Hash, Shared<PinFut<R>>>>>,
}

impl<R: CheapClone> QueryCache<R> {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Assumption: Whatever F is passed in consistently returns the same
    /// value for any input - for all values of F used with this Cache.
    ///
    /// Returns `(value, cached)`, where `cached` is true if the value was
    /// already in the cache and false otherwise.
    pub async fn cached_query<F: Future<Output = R> + Send + 'static>(
        &self,
        hash: Hash,
        f: F,
    ) -> (R, bool) {
        let f = f.boxed();

        let (work, cached) = {
            let mut cache = self.cache.lock().unwrap();

            match cache.entry(hash) {
                Entry::Occupied(entry) => {
                    // This is already being worked on.
                    let entry = entry.get().cheap_clone();
                    (entry, true)
                }
                Entry::Vacant(entry) => {
                    // New work, put it in the in-flight list.
                    let uncached = f.shared();
                    entry.insert(uncached.clone());
                    (uncached, false)
                }
            }
        };

        let _remove_guard = if !cached {
            // Make sure to remove this from the in-flight list, even if `poll` panics.
            Some(defer::defer(|| {
                self.cache.lock().unwrap().remove(&hash);
            }))
        } else {
            None
        };

        (work.await, cached)
    }
}
