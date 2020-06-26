//! Utilities to keep moving statistics about queries

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::util::stats::{MovingStats, BIN_SIZE, WINDOW_SIZE};

pub struct QueryEffort {
    inner: Arc<RwLock<QueryEffortInner>>,
}

/// Track the effort for queries (identified by their ShapeHash) over a
/// time window.
struct QueryEffortInner {
    window_size: Duration,
    bin_size: Duration,
    effort: HashMap<u64, MovingStats>,
    total: MovingStats,
}

/// Create a `QueryEffort` that uses the window and bin sizes configured in
/// the environment
impl Default for QueryEffort {
    fn default() -> Self {
        Self::new(*WINDOW_SIZE, *BIN_SIZE)
    }
}

impl QueryEffort {
    pub fn new(window_size: Duration, bin_size: Duration) -> Self {
        Self {
            inner: Arc::new(RwLock::new(QueryEffortInner::new(window_size, bin_size))),
        }
    }

    pub fn add(&self, shape_hash: u64, duration: Duration) {
        let mut inner = self.inner.write().unwrap();
        inner.add(shape_hash, duration);
    }
}

impl QueryEffortInner {
    fn new(window_size: Duration, bin_size: Duration) -> Self {
        Self {
            window_size,
            bin_size,
            effort: HashMap::default(),
            total: MovingStats::new(window_size, bin_size),
        }
    }

    fn add(&mut self, shape_hash: u64, duration: Duration) {
        let window_size = self.window_size;
        let bin_size = self.bin_size;
        let now = Instant::now();
        self.effort
            .entry(shape_hash)
            .or_insert_with(|| MovingStats::new(window_size, bin_size))
            .add_at(now, duration);
        self.total.add_at(now, duration);
    }
}
