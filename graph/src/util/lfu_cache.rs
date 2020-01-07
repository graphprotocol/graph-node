use priority_queue::PriorityQueue;
use std::cmp::Reverse;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

// The number of `evict` calls without access after which an entry is considered stale.
const STALE_PERIOD: u64 = 100;

pub trait CacheWeight {
    fn weight(&self) -> u64;
}

impl<T: CacheWeight> CacheWeight for Option<T> {
    fn weight(&self) -> u64 {
        match self {
            Some(x) => x.weight(),
            None => 0,
        }
    }
}

/// `PartialEq` and `Hash` are delegated to the `key`.
#[derive(Clone, Debug)]
pub struct CacheEntry<K, V> {
    weight: u64,
    key: K,
    value: V,
    will_stale: bool,
}

impl<K: Eq, V> PartialEq for CacheEntry<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl<K: Eq, V> Eq for CacheEntry<K, V> {}

impl<K: Hash, V> Hash for CacheEntry<K, V> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state)
    }
}

impl<K, V: Default> CacheEntry<K, V> {
    fn cache_key(key: K) -> Self {
        // Only the key matters for finding an entry in the cache.
        CacheEntry {
            key,
            value: V::default(),
            weight: 0,
            will_stale: false,
        }
    }
}

// The priorities are `(stale, frequency)` tuples, first all stale entries will be popped and
// then non-stale entries by least frequency.
type Priority = (bool, Reverse<u64>);

/// Each entry in the cache has a frequency, which is incremented by 1 on access. Entries also have
/// a weight, upon eviction first stale entries will be removed and then non-stale entries by order
/// of least frequency until the max weight is respected. This cache only removes entries on calls
/// to `evict`, so the max weight may be exceeded until `evict` is called. Every STALE_PERIOD
/// evictions entities are checked for staleness.
#[derive(Clone, Debug)]
pub struct LfuCache<K: Eq + Hash, V> {
    queue: PriorityQueue<CacheEntry<K, V>, Priority>,
    total_weight: u64,
    stale_counter: u64,
}

impl<K: Ord + Eq + Hash, V> Default for LfuCache<K, V> {
    fn default() -> Self {
        LfuCache {
            queue: PriorityQueue::new(),
            total_weight: 0,
            stale_counter: 0,
        }
    }
}

impl<K: Clone + Ord + Eq + Hash + Debug, V: CacheWeight + Default> LfuCache<K, V> {
    pub fn new() -> Self {
        LfuCache {
            queue: PriorityQueue::new(),
            total_weight: 0,
            stale_counter: 0,
        }
    }

    /// Updates and bumps freceny if already present.
    pub fn insert(&mut self, key: K, value: V) {
        let weight = value.weight();
        match self.get_mut(key.clone()) {
            None => {
                self.total_weight += weight;
                self.queue.push(
                    CacheEntry {
                        weight,
                        key,
                        value,
                        will_stale: false,
                    },
                    (false, Reverse(1)),
                );
            }
            Some(entry) => {
                entry.weight = weight;
                entry.value = value;
                entry.will_stale = false;
                self.total_weight += weight - entry.weight;
            }
        }
    }

    fn get_mut(&mut self, key: K) -> Option<&mut CacheEntry<K, V>> {
        // Increment the frequency by 1
        let key_entry = CacheEntry::cache_key(key);
        self.queue
            .change_priority_by(&key_entry, |(s, Reverse(f))| (s, Reverse(f + 1)));
        self.queue.get_mut(&key_entry).map(|x| x.0)
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.get_mut(key.clone()).map(|x| &x.value)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        // `PriorityQueue` doesn't have a remove method, so emulate that by setting the priority to
        // the absolute minimum and popping.
        let key_entry = CacheEntry::cache_key(key.clone());
        self.queue
            .change_priority(&key_entry, (true, Reverse(u64::min_value())))
            .and_then(|_| {
                self.queue.pop().map(|(e, _)| {
                    assert_eq!(e.key, key_entry.key);
                    self.total_weight -= e.weight;
                    e.value
                })
            })
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.queue
            .get(&CacheEntry::cache_key(key.clone()))
            .is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn evict(&mut self, max_weight: u64) {
        if self.total_weight <= max_weight {
            return;
        }

        self.stale_counter += 1;
        if self.stale_counter == STALE_PERIOD {
            self.stale_counter = 0;

            // Entries marked `will_stale` are effectively set as stale.
            // All entries are then marked `will_stale`.
            for (e, p) in self.queue.iter_mut() {
                p.0 = e.will_stale;
                e.will_stale = true;
            }
        }

        while self.total_weight > max_weight {
            // Unwrap: If there were no entries left, `total_weight` would be 0.
            let entry = self.queue.pop().unwrap().0;
            self.total_weight -= entry.weight;
        }
    }
}

impl<K: Ord + Eq + Hash + 'static, V: 'static> IntoIterator for LfuCache<K, V> {
    type Item = (CacheEntry<K, V>, Priority);
    type IntoIter = Box<dyn Iterator<Item = Self::Item>>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.queue.into_iter())
    }
}

impl<K: Ord + Eq + Hash, V> Extend<(CacheEntry<K, V>, Priority)> for LfuCache<K, V> {
    fn extend<T: IntoIterator<Item = (CacheEntry<K, V>, Priority)>>(&mut self, iter: T) {
        self.queue.extend(iter);
    }
}
