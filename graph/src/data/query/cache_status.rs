use std::fmt;
use std::slice::Iter;

use serde::Serialize;

use crate::derive::CacheWeight;

/// Used for checking if a response hit the cache.
#[derive(Copy, Clone, CacheWeight, Debug, PartialEq, Eq, Hash)]
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
        f.write_str(self.as_str())
    }
}

impl CacheStatus {
    pub fn iter() -> Iter<'static, CacheStatus> {
        use CacheStatus::*;
        static STATUSES: [CacheStatus; 4] = [Hit, Shared, Insert, Miss];
        STATUSES.iter()
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            CacheStatus::Hit => "hit",
            CacheStatus::Shared => "shared",
            CacheStatus::Insert => "insert",
            CacheStatus::Miss => "miss",
        }
    }

    pub fn uses_database(&self) -> bool {
        match self {
            CacheStatus::Hit | CacheStatus::Shared => false,
            CacheStatus::Insert | CacheStatus::Miss => true,
        }
    }
}

impl Serialize for CacheStatus {
    fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        ser.serialize_str(self.as_str())
    }
}
