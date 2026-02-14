use crate::env::ENV_VARS;
use crate::prelude::CacheWeight;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::time::{Duration, Instant};

// The number of `evict` calls without access after which an entry is considered stale.
const STALE_PERIOD: u64 = 100;

#[derive(Clone, Debug)]
pub struct CacheEntry<V> {
    pub value: V,
    weight: usize,
    freq: u64,
    stale: bool,
    will_stale: bool,
}

/// Statistics about what happened during cache eviction
pub struct EvictStats {
    /// The weight of the cache after eviction
    pub new_weight: usize,
    /// The weight of the items that were evicted
    pub evicted_weight: usize,
    /// The number of entries after eviction
    pub new_count: usize,
    /// The number if entries that were evicted
    pub evicted_count: usize,
    /// Whether we updated the stale status of entries
    pub stale_update: bool,
    /// How long eviction took
    pub evict_time: Duration,
    /// The total number of cache accesses during this stale period
    pub accesses: usize,
    /// The total number of cache hits during this stale period
    pub hits: usize,
}

impl EvictStats {
    /// The cache hit rate in percent. The underlying counters are reset at
    /// the end of each stale period.
    pub fn hit_rate_pct(&self) -> f64 {
        if self.accesses > 0 {
            self.hits as f64 / self.accesses as f64 * 100.0
        } else {
            100.0
        }
    }
}
/// Each entry in the cache has a frequency, which is incremented by 1 on access. Entries also have
/// a weight, upon eviction first stale entries will be removed and then non-stale entries by order
/// of least frequency until the max weight is respected. This cache only removes entries on calls
/// to `evict`, so the max weight may be exceeded until `evict` is called. Every STALE_PERIOD
/// evictions entities are checked for staleness.
#[derive(Debug)]
pub struct LfuCache<K: Eq + Hash, V> {
    entries: HashMap<K, CacheEntry<V>>,
    total_weight: usize,
    stale_counter: u64,
    dead_weight: bool,
    accesses: usize,
    hits: usize,
}

impl<K: Eq + Hash, V> Default for LfuCache<K, V> {
    fn default() -> Self {
        LfuCache {
            entries: HashMap::new(),
            total_weight: 0,
            stale_counter: 0,
            dead_weight: false,
            accesses: 0,
            hits: 0,
        }
    }
}

impl<K: Eq + Hash + Debug + CacheWeight, V: CacheWeight + Default> LfuCache<K, V> {
    pub fn new() -> Self {
        LfuCache {
            entries: HashMap::new(),
            total_weight: 0,
            stale_counter: 0,
            dead_weight: ENV_VARS.mappings.entity_cache_dead_weight,
            accesses: 0,
            hits: 0,
        }
    }

    fn entry_weight(key: &K, value: &V) -> usize {
        value.indirect_weight() + key.indirect_weight()
    }

    /// Updates and bumps frequency if already present.
    pub fn insert(&mut self, key: K, value: V) {
        let weight = Self::entry_weight(&key, &value);
        match self.entries.get_mut(&key) {
            Some(entry) => {
                self.total_weight -= entry.weight;
                self.total_weight += weight;
                entry.weight = weight;
                entry.value = value;
                entry.freq += 1;
                entry.will_stale = false;
            }
            None => {
                self.total_weight += weight;
                self.entries.insert(
                    key,
                    CacheEntry {
                        value,
                        weight,
                        freq: 1,
                        stale: false,
                        will_stale: false,
                    },
                );
            }
        }
    }

    #[cfg(test)]
    fn weight(&self, key: &K) -> usize {
        self.entries.get(key).map(|e| e.weight).unwrap_or(0)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.entries.iter().map(|(k, e)| (k, &e.value))
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.accesses += 1;
        self.entries.get_mut(key).map(|entry| {
            self.hits += 1;
            entry.freq += 1;
            entry.will_stale = false;
            &entry.value
        })
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.accesses += 1;
        self.entries.get_mut(key).map(|entry| {
            self.hits += 1;
            entry.freq += 1;
            entry.will_stale = false;
            &mut entry.value
        })
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.entries.remove(key).map(|entry| {
            self.total_weight -= entry.weight;
            entry.value
        })
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.entries.contains_key(key)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn evict_and_stats(&mut self, max_weight: usize) -> EvictStats {
        self.evict_with_period(max_weight, STALE_PERIOD)
            .unwrap_or_else(|| EvictStats {
                new_weight: self.total_weight,
                evicted_weight: 0,
                new_count: self.len(),
                evicted_count: 0,
                stale_update: false,
                evict_time: Duration::from_millis(0),
                accesses: 0,
                hits: 0,
            })
    }

    /// Same as `evict_with_period(max_weight, STALE_PERIOD)`
    pub fn evict(&mut self, max_weight: usize) -> Option<EvictStats> {
        self.evict_with_period(max_weight, STALE_PERIOD)
    }

    /// Evict entries in the cache until the total weight of the cache is
    /// equal to or smaller than `max_weight`.
    ///
    /// The return value is mostly useful for testing and diagnostics and can
    /// safely ignored in normal use. It gives the sum of the weight of all
    /// evicted entries, the weight before anything was evicted and the new
    /// total weight of the cache, in that order, if anything was evicted
    /// at all. If there was no reason to evict, `None` is returned.
    pub fn evict_with_period(
        &mut self,
        max_weight: usize,
        stale_period: u64,
    ) -> Option<EvictStats> {
        if self.total_weight <= max_weight {
            return None;
        }

        let start = Instant::now();

        let accesses = self.accesses;
        let hits = self.hits;

        self.stale_counter += 1;
        if self.stale_counter == stale_period {
            self.stale_counter = 0;

            self.accesses = 0;
            self.hits = 0;

            // Entries marked `will_stale` were not accessed in this period. Properly mark them as
            // stale. Also mark all entities as `will_stale` for the _next_ period so that they
            // will be marked stale next time unless they are updated or looked up between now and
            // then.
            for entry in self.entries.values_mut() {
                entry.stale = entry.will_stale;
                entry.will_stale = true;
            }
        }

        let old_len = self.len();
        let dead_weight = if self.dead_weight {
            old_len * (std::mem::size_of::<CacheEntry<V>>() + 40)
        } else {
            0
        };

        // Determine a frequency threshold below which entries should be
        // evicted. We sort (stale, freq, weight) tuples to find the cutoff,
        // then use `retain` to remove entries that fall at or below it.
        let mut evict_order: Vec<(bool, u64, usize)> = self
            .entries
            .values()
            .map(|e| (e.stale, e.freq, e.weight))
            .collect();
        // Evict priority: stale entries first, then lowest frequency first.
        evict_order.sort_unstable_by(|a, b| {
            b.0.cmp(&a.0) // stale=true before stale=false
                .then(a.1.cmp(&b.1)) // lowest freq first
        });

        let target_evict = self.total_weight + dead_weight - max_weight;
        let mut accumulated = 0;
        let mut evict_count = 0;
        for &(_, _, w) in &evict_order {
            if accumulated >= target_evict {
                break;
            }
            accumulated += w;
            evict_count += 1;
        }

        let mut evicted_weight = 0;
        if evict_count > 0 {
            let threshold_stale = evict_order[evict_count - 1].0;
            let threshold_freq = evict_order[evict_count - 1].1;
            let mut remaining = evict_count;

            self.entries.retain(|_, entry| {
                if remaining == 0 {
                    return true;
                }
                // An entry should be evicted if it sorts at or before the
                // threshold in eviction order (stale first, low freq first).
                let dominated = (entry.stale, threshold_stale) == (true, false)
                    || (entry.stale == threshold_stale && entry.freq <= threshold_freq);
                if dominated {
                    remaining -= 1;
                    evicted_weight += entry.weight;
                    false
                } else {
                    true
                }
            });
        }
        self.total_weight -= evicted_weight;

        Some(EvictStats {
            new_weight: self.total_weight,
            evicted_weight,
            new_count: self.len(),
            evicted_count: old_len - self.len(),
            stale_update: self.stale_counter == 0,
            evict_time: start.elapsed(),
            accesses,
            hits,
        })
    }
}

impl<K: Eq + Hash + 'static, V: 'static> IntoIterator for LfuCache<K, V> {
    type Item = (K, CacheEntry<V>);
    type IntoIter = std::collections::hash_map::IntoIter<K, CacheEntry<V>>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

impl<K: Eq + Hash + CacheWeight, V: CacheWeight + Default> Extend<(K, CacheEntry<V>)>
    for LfuCache<K, V>
{
    fn extend<T: IntoIterator<Item = (K, CacheEntry<V>)>>(&mut self, iter: T) {
        for (key, entry) in iter {
            self.total_weight += entry.weight;
            self.entries.insert(key, entry);
        }
    }
}

#[test]
fn entity_lru_cache() {
    #[derive(Default, Debug, PartialEq, Eq)]
    struct Weight(usize);

    impl CacheWeight for Weight {
        fn weight(&self) -> usize {
            self.indirect_weight()
        }

        fn indirect_weight(&self) -> usize {
            self.0
        }
    }

    let mut cache: LfuCache<&'static str, Weight> = LfuCache::new();
    cache.insert("panda", Weight(2));
    cache.insert("cow", Weight(1));
    let panda_weight = cache.weight(&"panda");
    let cow_weight = cache.weight(&"cow");

    assert_eq!(cache.get(&"cow"), Some(&Weight(1)));
    assert_eq!(cache.get(&"panda"), Some(&Weight(2)));

    // Nothing is evicted.
    cache.evict(panda_weight + cow_weight);
    assert_eq!(cache.len(), 2);

    // "cow" was accessed twice, so "panda" is evicted.
    cache.get(&"cow");
    cache.evict(cow_weight);
    assert!(cache.get(&"panda").is_none());

    cache.insert("alligator", Weight(2));
    let alligator_weight = cache.weight(&"alligator");

    // Give "cow" and "alligator" a high frequency.
    for _ in 0..1000 {
        cache.get(&"cow");
        cache.get(&"alligator");
    }

    // Insert a lion and make it weigh the same as the cow and the alligator
    // together.
    cache.insert("lion", Weight(0));
    let lion_weight = cache.weight(&"lion");
    let lion_inner_weight = cow_weight + alligator_weight - lion_weight;
    cache.insert("lion", Weight(lion_inner_weight));
    let lion_weight = cache.weight(&"lion");

    // Make "cow" and "alligator" stale and remove them.
    for _ in 0..(2 * STALE_PERIOD) {
        cache.get(&"lion");

        // The "whale" is something to evict so the stale counter moves.
        cache.insert("whale", Weight(100 * lion_weight));
        cache.evict(2 * lion_weight);
    }

    // Either "cow" and "alligator" fit in the cache, or just "lion".
    // "lion" will be kept, it had lower frequency but was not stale.
    assert!(cache.get(&"cow").is_none());
    assert!(cache.get(&"alligator").is_none());
    assert_eq!(cache.get(&"lion"), Some(&Weight(lion_inner_weight)));
}
