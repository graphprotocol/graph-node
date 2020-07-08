use graph::prelude::CheapClone;
use once_cell::sync::OnceCell;
use stable_hash::crypto::SetHasher;
use stable_hash::prelude::*;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};

type Hash = <SetHasher as StableHasher>::Out;

/// The 'true' cache entry that lives inside the Arc.
/// When the last Arc is dropped, this is dropped, and the cache is removed.
#[derive(Debug)]
struct CacheEntryInner<R> {
    // Considered using once_cell::sync::Lazy,
    // but that quickly becomes a mess of generics
    // or runs into the issue that Box<dyn FnOnce> can't be
    // called at all, so doesn't impl FnOnce as Lazy requires.
    result: OnceCell<Option<R>>,

    // Temporary to implement OnceCell.wait
    condvar: Condvar,
    lock: Mutex<bool>,
}

impl<R> CacheEntryInner<R> {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            result: OnceCell::new(),
            condvar: Condvar::new(),
            lock: Mutex::new(false),
        })
    }

    fn set_inner(&self, value: Option<R>) {
        // Store the cached value
        self.result
            .set(value)
            .unwrap_or_else(|_| panic!("Cache set should only be called once"));
        // Wakeup consumers of the cache
        let mut is_set = self.lock.lock().unwrap();
        *is_set = true;
        self.condvar.notify_all();
    }

    fn set(&self, value: R) {
        self.set_inner(Some(value));
    }

    fn set_panic(&self) {
        self.set_inner(None);
    }

    fn wait(&self) -> &R {
        // Happy path - already cached.
        if let Some(r) = self.result.get() {
            match r.as_ref() {
                Some(r) => r,
                // TODO: Instead of having an Option,
                // retain panic information and propagate it.
                None => panic!("Query panicked"),
            }
        } else {
            // Wait for the item to be placed in the cache.
            let mut is_set = self.lock.lock().unwrap();
            while !*is_set {
                is_set = self.condvar.wait(is_set).unwrap();
            }

            self.wait()
        }
    }
}

/// On drop, call set_panic on self.value,
/// unless set was called.
struct PanicHelper<R> {
    value: Option<Arc<CacheEntryInner<R>>>,
}

impl<R> Drop for PanicHelper<R> {
    fn drop(&mut self) {
        if let Some(inner) = self.value.take() {
            inner.set_panic();
        }
    }
}

impl<R> PanicHelper<R> {
    fn new(value: Arc<CacheEntryInner<R>>) -> Self {
        Self { value: Some(value) }
    }
    fn set(mut self, r: R) -> Arc<CacheEntryInner<R>> {
        let value = self.value.take().unwrap();
        value.set(r);
        value
    }
}

/// Cache that keeps a result around as long as it is still being processed.
/// The cache ensures that the query is not re-entrant, so multiple consumers
/// of identical queries will not execute them in parallel.
///
/// This has a lot in common with AsyncCache in the network-services repo,
/// but is sync instead of async, and more specialized.
pub struct QueryCache<R> {
    cache: Arc<Mutex<HashMap<Hash, Arc<CacheEntryInner<R>>>>>,
}

impl<R: CheapClone> QueryCache<R> {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Assumption: Whatever F is passed in consistently returns the same
    /// value for any input - for all values of F used with this Cache.
    pub fn cached_query<F: FnOnce() -> R>(&self, hash: Hash, f: F) -> R {
        let work = {
            let mut cache = self.cache.lock().unwrap();

            // Try to pull the item out of the cache and return it.
            // If we get past this expr, it means this thread will do
            // the work and fullfil that 'promise' in this work variable.
            match cache.entry(hash) {
                Entry::Occupied(entry) => {
                    // Another thread is doing the work, release the lock and wait for it.
                    let entry = entry.get().cheap_clone();
                    drop(cache);
                    return entry.wait().cheap_clone();
                }
                Entry::Vacant(entry) => {
                    let uncached = CacheEntryInner::new();
                    entry.insert(uncached.clone());
                    uncached
                }
            }
        };

        let _remove_guard = defer::defer(|| {
            // Remove this from the list of in-flight work.
            self.cache.lock().unwrap().remove(&hash);
        });

        // Now that we have taken on the responsibility, propagate panics to
        // make sure that no threads wait forever on a result that will never
        // come.
        let work = PanicHelper::new(work);

        // Actually compute the value and then share it with waiters.
        let value = f();
        work.set(value.cheap_clone());
        value
    }
}
