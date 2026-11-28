//! Regression test for https://github.com/graphprotocol/graph-node/issues/6706
//!
//! Eviction ties on the `(stale, frequency)` priority are the normal case, not
//! the exception. Which of the tied entries is evicted must not depend on the
//! order in which they were inserted: a caller can insert entries in an order
//! that is nondeterministic per process (e.g. when iterating a `HashMap` whose
//! iteration order is randomized per process), and that would otherwise make
//! the eviction set — and therefore cache hit rates / store read counts —
//! differ between otherwise identical runs.

use graph::prelude::CacheWeight;
use graph::util::lfu_cache::LfuCache;

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

/// Insert the same six entries, each of equal weight and frequency 1, so every
/// eviction candidate is tied on the priority, evict down to a max weight that
/// leaves two entries, and return the surviving keys.
fn survivors_in_order(order: &[&str]) -> Vec<String> {
    let mut cache: LfuCache<String, Weight> = LfuCache::new();
    for &key in order {
        cache.insert(key.to_string(), Weight(1));
    }
    cache.evict(6);
    let mut survivors: Vec<String> = cache.iter().map(|(k, _)| k.clone()).collect();
    survivors.sort();
    survivors
}

#[test]
fn eviction_is_independent_of_insertion_order() {
    let order_a = ["k1", "k2", "k3", "k4", "k5", "k6"];
    let order_b = ["k6", "k5", "k4", "k3", "k2", "k1"];
    let order_c = ["k3", "k6", "k2", "k5", "k1", "k4"];

    let reference = survivors_in_order(&order_a);
    assert_eq!(survivors_in_order(&order_b), reference);
    assert_eq!(survivors_in_order(&order_c), reference);
}
