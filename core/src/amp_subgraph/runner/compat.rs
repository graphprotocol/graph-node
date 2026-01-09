//! This is a temporary compatibility module until the graph-node is fully migrated to `alloy`.

use alloy::primitives::{BlockHash, BlockNumber};
use chrono::{DateTime, Utc};

mod legacy {
    pub(super) use graph::{
        blockchain::{BlockHash, BlockPtr, BlockTime},
        components::store::BlockNumber,
        data::store::scalar::Timestamp,
    };
}

pub(super) trait Compat<T> {
    fn compat(&self) -> T;
}

impl Compat<legacy::BlockNumber> for BlockNumber {
    fn compat(&self) -> legacy::BlockNumber {
        (*self).try_into().unwrap()
    }
}

impl Compat<BlockNumber> for legacy::BlockNumber {
    fn compat(&self) -> BlockNumber {
        (*self).try_into().unwrap()
    }
}

impl Compat<legacy::BlockHash> for BlockHash {
    fn compat(&self) -> legacy::BlockHash {
        legacy::BlockHash(self.0.into())
    }
}

impl Compat<BlockHash> for legacy::BlockHash {
    fn compat(&self) -> BlockHash {
        BlockHash::from_slice(&self.0)
    }
}

impl Compat<legacy::BlockTime> for DateTime<Utc> {
    fn compat(&self) -> legacy::BlockTime {
        legacy::Timestamp(*self).into()
    }
}

impl Compat<legacy::BlockPtr> for (BlockNumber, BlockHash) {
    fn compat(&self) -> legacy::BlockPtr {
        legacy::BlockPtr::new(self.1.compat(), self.0.compat())
    }
}
