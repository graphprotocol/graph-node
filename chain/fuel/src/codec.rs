pub use crate::protobuf::pbcodec::*;

use graph::{
    blockchain::{Block as BlockchainBlock, BlockPtr},
    prelude::BlockNumber,
};

use std::convert::TryFrom;
use graph::blockchain::BlockTime;

impl Block {
    pub fn parent_ptr(&self) -> Option<BlockPtr> {
        match self.height {
            0 => None,
            _ => Some(BlockPtr {
                hash: self.prev_id.clone().into(),
                number: self.number().saturating_sub(1),
            }),
        }
    }
    fn block_time(&self) -> BlockTime {
        let ts = i64::try_from(self.timestamp).unwrap();
        let secs = ts / 1_000_000_000;
        let ns: u32 = (ts % 1_000_000_000) as u32;
        BlockTime::since_epoch(secs, ns)
    }
}



impl<'a> From<&'a Block> for BlockPtr {
    fn from(b: &'a Block) -> BlockPtr {
        BlockPtr::new(b.id.clone().into(), b.number())
    }
}

impl BlockchainBlock for Block {
    fn ptr(&self) -> BlockPtr {
        BlockPtr::try_from(self).unwrap()
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        self.parent_ptr()
    }

    fn number(&self) -> i32 {
        BlockNumber::try_from(self.height).unwrap()
    }

    fn timestamp(&self) -> BlockTime {
        self.block_time()
    }
}