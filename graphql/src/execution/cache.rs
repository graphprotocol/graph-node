use graph::prelude::CheapClone;
use once_cell::sync::OnceCell;
use stable_hash::crypto::SetHasher;
use stable_hash::prelude::*;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::ops::Deref;
use std::sync::{Arc, Condvar, Mutex, Weak};

type Hash = <SetHasher as StableHasher>::Out;

/// A queue of items which (may) have expired from the cache.
/// This is kept separate to avoid circular references. The way
/// the code is implemented ensure that this does not grow without
/// bound, and generally cleanup stays ahead of insertion.
#[derive(Default, Clone, Debug)]
struct CleanupQueue {
    inner: Arc<Mutex<VecDeque<Hash>>>,
}

impl CleanupQueue {
    /// Schedule an item for cleanup later
    fn push(&self, value: Hash) {
        let mut inner = self.inner.lock().unwrap();
        inner.push_back(value);
    }
    /// Take an item to clean up. The consumer MUST
    /// deal with this without fail or memory will leak.
    fn pop(&self) -> Option<Hash> {
        let mut inner = self.inner.lock().unwrap();
        inner.pop_front()
    }
}

// Implemented on top of Arc, so this is ok.
impl CheapClone for CleanupQueue {}

/// A handle to a cached item. As long as this handle is kept alive,
/// the value remains in the cache.
///
/// The cached value may not be immediately available when used.
/// In this case this will block until the value is available.
#[derive(Debug)]
pub struct CachedResponse<R> {
    inner: Arc<CacheEntryInner<R>>,
}

impl<R> Deref for CachedResponse<R> {
    type Target = R;
    fn deref(&self) -> &R {
        self.inner.wait()
    }
}

// Manual impl required because of generic parameter.
impl<R> Clone for CachedResponse<R> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

// Ok, because implemented on top of Arc
impl<R> CheapClone for CachedResponse<R> {}

/// The 'true' cache entry that lives inside the Arc.
/// When the last Arc is dropped, this is dropped,
/// and the cache is removed.
#[derive(Debug)]
struct CacheEntryInner<R> {
    cleanup: CleanupQueue,
    hash: Hash,
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
    fn new(hash: Hash, cleanup: &CleanupQueue) -> Arc<Self> {
        Arc::new(Self {
            cleanup: cleanup.cheap_clone(),
            hash,
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

/// Once the last reference is removed, schedule for cleanup in the cache.
impl<R> Drop for CacheEntryInner<R> {
    fn drop(&mut self) {
        self.cleanup.push(self.hash);
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

/// Cache that keeps a result around as long as it is still in use somewhere.
/// The cache ensures that the query is not re-entrant, so multiple consumers
/// of identical queries will not execute them in parallel.
///
/// This has a lot in common with AsyncCache in the network-services repo,
/// but is sync instead of async, and more specialized.
pub struct QueryCache<R> {
    cleanup: CleanupQueue,
    cache: Arc<Mutex<HashMap<Hash, Weak<CacheEntryInner<R>>>>>,
}

impl<R> QueryCache<R> {
    pub fn new() -> Self {
        Self {
            cleanup: CleanupQueue::default(),
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    /// Assumption: Whatever F is passed in consistently returns the same
    /// value for any input - for all values of F used with this Cache.
    pub fn cached_query<F: FnOnce() -> R>(&self, hash: Hash, f: F) -> CachedResponse<R> {
        // This holds it's own lock so make sure that this happens outside of
        // holding any other lock.
        let cleanup = self.cleanup.pop();

        let mut cache = self.cache.lock().unwrap();

        // Execute the amortized cleanup step, checking that the content is
        // still missing since it may have been re-inserted. By always cleaning
        // up one item before potentially inserting another item we ensure that
        // the memory usage stays bounded. There is no need to stay ahead of
        // this work, because this step doesn't actually free any real memory,
        // it just ensures the memory doesn't grow unnecessarily when inserting.
        if let Some(cleanup) = cleanup {
            if let Entry::Occupied(entry) = cache.entry(cleanup) {
                if entry.get().strong_count() == 0 {
                    entry.remove_entry();
                }
            }
        }

        // Try to pull the item out of the cache and return it.
        // If we get past this expr, it means this thread will do
        // the work and fullfil that 'promise' in this work variable.
        let work = match cache.entry(hash) {
            Entry::Occupied(mut entry) => {
                // Cache hit!
                if let Some(cached) = entry.get().upgrade() {
                    return CachedResponse { inner: cached };
                }
                // Need to re-add to cache
                let uncached = CacheEntryInner::new(hash, &self.cleanup);
                *entry.get_mut() = Arc::downgrade(&uncached);
                uncached
            }
            Entry::Vacant(entry) => {
                let uncached = CacheEntryInner::new(hash, &self.cleanup);
                entry.insert(Arc::downgrade(&uncached));
                uncached
            }
        };

        // Don't hold the lock.
        drop(cache);

        // Now that we have taken on the responsibility, propagate panics to
        // make sure that no threads wait forever on a result that will never
        // come.
        let work = PanicHelper::new(work);

        // After all that ceremony, this part is easy enough.
        CachedResponse {
            inner: work.set(f()),
        }
    }
}
