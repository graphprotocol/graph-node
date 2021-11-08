#[path = "protobuf/sf.solana.codec.v1.rs"]
mod pbcodec;

use graph::{blockchain::Block as BlockchainBlock, blockchain::BlockPtr, prelude::BlockNumber};
use std::convert::TryFrom;

pub use pbcodec::*;

impl Block {
    pub fn parent_ptr(&self) -> Option<BlockPtr> {
        return None;
        // if self.previous_id.len() == 0 {
        //     return None;
        // }
        //
        // Some(
        //     BlockPtr::try_from((self.previous_id.as_ref(), self.number))
        //         .expect("invalid block's hash"),
        // )
        //todo: ^^^^^
    }
}

impl From<Block> for BlockPtr {
    fn from(b: Block) -> BlockPtr {
        (&b).into()
    }
}

impl<'a> From<&'a Block> for BlockPtr {
    fn from(b: &'a Block) -> BlockPtr {
        // let hash = BlockHash::from(b.id)
        //     .expect(&format!("id {} should be a valid BlockHash", &b.id,));

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
