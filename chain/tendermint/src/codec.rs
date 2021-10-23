#[path = "protobuf/fig.tendermint.codec.v1.rs"]
mod pbcodec;

pub use pbcodec::*;

use graph::{blockchain::BlockPtr, prelude::BlockNumber};
use graph::blockchain::Block as BBlock;

use std::convert::TryFrom;

/*
impl From<&CryptoHash> for H256 {
    fn from(input: &CryptoHash) -> Self {
        H256::from_slice(&input.bytes)
    }
}

impl LowerHex for &CryptoHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&hex::encode(&self.bytes))
    }
}
*/

impl EventList {
    pub fn block(&self) -> &pbcodec::EventDataNewBlock {
        self.newblock.as_ref().unwrap()
    }

    pub fn header(&self) -> &Header {
        self.block().block.as_ref().unwrap().header.as_ref().unwrap()
    }

    pub fn parent_ptr(&self) -> Option<BlockPtr> {
        let last_block_ID = self.header().last_block_id.as_ref().unwrap();

        match (last_block_ID.hash.clone(), self.header().height -1) {
            (hash, number) => Some(BlockPtr::from((hash, number))),
            _ => None,
        }
    }
}

impl From<EventList> for BlockPtr {
    fn from(b: EventList) -> BlockPtr {
        (&b).into()
    }
}

impl<'a> From<&'a EventList> for BlockPtr {
    fn from(b: &'a EventList) -> BlockPtr {
        BlockPtr::from((b.header().data_hash.clone(), b.header().height))
    }
}

impl BBlock for EventList {
    fn number(&self) -> i32 {
        BlockNumber::try_from(self.header().height).unwrap()
    }

    fn ptr(&self) -> BlockPtr {
        self.into()
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        self.parent_ptr()
    }
}

