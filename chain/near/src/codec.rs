#[path = "protobuf/sf.near.codec.v1.rs"]
mod pbcodec;

use graph::{
    blockchain::Block as Blockchainblock,
    blockchain::BlockPtr,
    prelude::{hex, web3::types::H256, BlockNumber},
};
use std::convert::TryFrom;
use std::fmt::LowerHex;

pub use pbcodec::*;

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

impl Block {
    pub fn header(&self) -> &BlockHeader {
        self.header.as_ref().unwrap()
    }

    pub fn parent_ptr(&self) -> Option<BlockPtr> {
        let header = self.header();

        match (header.prev_hash.as_ref(), header.prev_height) {
            (Some(hash), number) => Some(BlockPtr::from((hash.into(), number))),
            _ => None,
        }
    }
}

impl From<Block> for BlockPtr {
    fn from(b: Block) -> BlockPtr {
        (&b).into()
    }
}

impl<'a> From<&'a Block> for BlockPtr {
    fn from(b: &'a Block) -> BlockPtr {
        let header = b.header();
        let hash: H256 = header.hash.as_ref().unwrap().into();

        BlockPtr::from((hash, header.height))
    }
}

impl Blockchainblock for Block {
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

impl execution_outcome::Status {
    pub fn is_success(&self) -> bool {
        use execution_outcome::Status::*;
        match self {
            Unknown(_) | Failure(_) => false,
            SuccessValue(_) | SuccessReceiptId(_) => true,
        }
    }
}
