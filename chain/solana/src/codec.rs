#[path = "protobuf/sf.solana.codec.v1.rs"]
mod pbcodec;

use graph::{blockchain::Block as BlockchainBlock, blockchain::BlockPtr, prelude::BlockNumber};
use std::convert::TryFrom;

pub use pbcodec::*;

impl Block {
    pub fn parent_ptr(&self) -> Option<BlockPtr> {
        if self.previous_id.len() == 0 {
            return None;
        }

        let hash = String::from_utf8(self.previous_id.clone()).expect("could not decode block hash");
        Some(
            BlockPtr::try_from((hash.as_str(), self.number))
                .expect("invalid block hash"),
        )
    }
}

impl<'a> From<&'a Block> for BlockPtr {
    fn from(b: &'a Block) -> BlockPtr {
        BlockPtr::try_from((b.id.as_slice(), i64::try_from(b.number).unwrap()))
            .expect("invalid block's hash")
    }
}

impl BlockchainBlock for Block {
    fn ptr(&self) -> BlockPtr {
        self.into()
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        self.parent_ptr()
    }

    fn number(&self) -> i32 {
        BlockNumber::try_from(self.number).expect("invalid block's height")
    }
}
