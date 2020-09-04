use std::fmt;

/// Used for checking if a response hit the cache.
#[derive(Copy, Clone)]
pub enum CacheStatus {
    /// Hit is a hit in the generational cache.
    Hit,

    /// Shared is a hit in the herd cache.
    Shared,

    /// Insert is a miss that inserted in the generational cache.
    Insert,

    /// A miss is none of the above.
    Miss,
}

impl Default for CacheStatus {
    fn default() -> Self {
        CacheStatus::Miss
    }
}

impl fmt::Display for CacheStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CacheStatus::Hit => f.write_str("hit"),
            CacheStatus::Shared => f.write_str("shared"),
            CacheStatus::Insert => f.write_str("insert"),
            CacheStatus::Miss => f.write_str("miss"),
        }
    }
}
