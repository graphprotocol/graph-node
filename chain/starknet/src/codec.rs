#[rustfmt::skip]
#[path = "protobuf/zklend.starknet.r#type.v1.rs"]
mod pbcodec;

use graph::blockchain::{Block as BlockchainBlock, BlockHash, BlockPtr};

pub use pbcodec::*;

impl BlockchainBlock for Block {
    fn number(&self) -> i32 {
        self.height as i32
    }

    fn ptr(&self) -> BlockPtr {
        BlockPtr {
            hash: BlockHash {
                value: String::from_utf8(self.hash.clone()).unwrap(),
            },
            number: self.height as i32,
        }
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        if self.height == 0 {
            None
        } else {
            Some(BlockPtr {
                hash: BlockHash {
                    value: String::from_utf8(self.prev_hash.clone()).unwrap(),
                },
                number: (self.height - 1) as i32,
            })
        }
    }
}
