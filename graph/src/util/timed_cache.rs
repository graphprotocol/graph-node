use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

/// Caching of values for a specified amount of time
#[derive(Debug)]
struct CacheEntry<T> {
    value: Arc<T>,
    expires: Instant,
}

/// A cache that keeps entries live for a fixed amount of time. It is assumed
/// that all that data that could possibly wind up in the cache is very small,
/// and that expired entries are replaced by an updated entry whenever expiry
/// is detected. In other words, the cache does not ever remove entries.
#[derive(Debug)]
pub struct TimedCache<T> {
    ttl: Duration,
    entries: RwLock<HashMap<String, CacheEntry<T>>>,
}

impl<T> TimedCache<T> {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Return the entry for `key` if it exists and is not expired yet, and
    /// return `None` otherwise. Note that expired entries stay in the cache
    /// as it is assumed that, after returning `None`, the caller will
    /// immediately overwrite that entry with a call to `set`
    pub fn get(&self, key: &str) -> Option<Arc<T>> {
        self.get_at(key, Instant::now())
    }

    fn get_at(&self, key: &str, now: Instant) -> Option<Arc<T>> {
        match self.entries.read().unwrap().get(key) {
            Some(CacheEntry { value, expires }) if *expires >= now => Some(value.clone()),
            _ => None,
        }
    }

    /// Associate `key` with `value` in the cache. The `value` will be
    /// valid for `self.ttl` duration
    pub fn set(&self, key: String, value: Arc<T>) {
        self.set_at(key, value, Instant::now())
    }

    fn set_at(&self, key: String, value: Arc<T>, now: Instant) {
        let entry = CacheEntry {
            value,
            expires: now + self.ttl,
        };
        self.entries.write().unwrap().insert(key, entry);
    }
}

#[test]
fn cache() {
    const KEY: &str = "one";
    let cache = TimedCache::<String>::new(Duration::from_millis(10));
    let now = Instant::now();
    cache.set_at(KEY.to_string(), Arc::new("value".to_string()), now);
    assert!(cache.get_at(KEY, now + Duration::from_millis(5)).is_some());
    assert!(cache.get_at(KEY, now + Duration::from_millis(15)).is_none());
}
