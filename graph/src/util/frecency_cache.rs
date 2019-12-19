use std::collections::{btree_map, BTreeMap};

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

#[derive(Clone, Debug)]
pub struct CacheEntry<V> {
    weight: u64,
    frecency: ordered_float::OrderedFloat<f32>,
    value: V,
}

/// Each entry in the cache has a frecency, which is incremented by 1 on access and multiplied by
/// 0.9 on each evict so that entries age. Entries also have a weight, upon eviction entries will
/// be removed by order of least frecency until the max weight is respected. This cache only
/// removes entries on calls to `evict`, so the max weight may be exceeded until `evict` is called.
#[derive(Clone, Debug)]
pub struct FrecencyCache<K, V>(BTreeMap<K, CacheEntry<V>>);

impl<K: Ord, V> Default for FrecencyCache<K, V> {
    fn default() -> Self {
        FrecencyCache(BTreeMap::new())
    }
}

impl<K: Ord, V: CacheWeight> FrecencyCache<K, V> {
    pub fn new() -> Self {
        FrecencyCache(BTreeMap::new())
    }

    /// Updates and bumps freceny if already present.
    pub fn insert(&mut self, key: K, value: V) {
        match self.0.entry(key) {
            btree_map::Entry::Vacant(entry) => {
                entry.insert(CacheEntry {
                    frecency: 1.0.into(),
                    weight: value.weight(),
                    value,
                });
            }
            btree_map::Entry::Occupied(mut entry) => {
                let entry = entry.get_mut();
                *entry.frecency += 1.0;
                entry.weight = value.weight();
                entry.value = value;
            }
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.0.get_mut(key).map(|e| {
            *e.frecency += 1.0;
            &e.value
        })
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.0.remove(key).map(|e| e.value)
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.0.contains_key(key)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn evict(&mut self, max_weight: u64) {
        let total_weight: i64 = self.0.values().map(|e| e.weight as i64).sum();
        if total_weight > max_weight as i64 {
            let mut excess_weight: i64 = total_weight - max_weight as i64;
            let mut entries: Vec<_> = std::mem::take(&mut self.0).into_iter().collect();
            entries.sort_by_key(|(_, e)| std::cmp::Reverse(e.frecency));

            // This loop halts because `excess_weight <= sum(entries.weight)` and this will evict
            // all entries if necessary.
            while excess_weight > 0 {
                // Unwrap: If there were no entries left, `excess_weight` would be 0.
                let evicted = entries.pop().unwrap();
                excess_weight -= evicted.1.weight as i64;
            }

            self.0 = entries
                .into_iter()
                .map(|(k, mut e)| {
                    // Age the entries.
                    *e.frecency *= 0.9;
                    (k, e)
                })
                .collect();
        }
    }
}

impl<K: Ord, V> IntoIterator for FrecencyCache<K, V> {
    type Item = (K, CacheEntry<V>);
    type IntoIter = btree_map::IntoIter<K, CacheEntry<V>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<K: Ord, V> Extend<(K, CacheEntry<V>)> for FrecencyCache<K, V> {
    fn extend<T: IntoIterator<Item = (K, CacheEntry<V>)>>(&mut self, iter: T) {
        self.0.extend(iter);
    }
}
