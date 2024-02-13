#[rustfmt::skip]
#[path = "protobuf/sf.near.codec.v1.rs"]
pub mod pbcodec;

#[rustfmt::skip]
#[path = "protobuf/receipts.v1.rs"]
pub mod substreams_triggers;

use graph::{
    blockchain::Block as BlockchainBlock,
    blockchain::{BlockPtr, BlockTime},
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

impl BlockHeader {
    pub fn parent_ptr(&self) -> Option<BlockPtr> {
        match (self.prev_hash.as_ref(), self.prev_height) {
            (Some(hash), number) => Some(BlockPtr::from((H256::from(hash), number))),
            _ => None,
        }
    }
}

impl<'a> From<&'a BlockHeader> for BlockPtr {
    fn from(b: &'a BlockHeader) -> BlockPtr {
        BlockPtr::from((H256::from(b.hash.as_ref().unwrap()), b.height))
    }
}

impl Block {
    pub fn header(&self) -> &BlockHeader {
        self.header.as_ref().unwrap()
    }

    pub fn ptr(&self) -> BlockPtr {
        BlockPtr::from(self.header())
    }

    pub fn parent_ptr(&self) -> Option<BlockPtr> {
        self.header().parent_ptr()
    }
}

impl<'a> From<&'a Block> for BlockPtr {
    fn from(b: &'a Block) -> BlockPtr {
        BlockPtr::from(b.header())
    }
}

impl BlockchainBlock for Block {
    fn number(&self) -> i32 {
        BlockNumber::try_from(self.header().height).unwrap()
    }

    fn ptr(&self) -> BlockPtr {
        self.into()
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        self.parent_ptr()
    }

    fn timestamp(&self) -> BlockTime {
        block_time_from_header(self.header())
    }
}

impl HeaderOnlyBlock {
    pub fn header(&self) -> &BlockHeader {
        self.header.as_ref().unwrap()
    }
}

impl<'a> From<&'a HeaderOnlyBlock> for BlockPtr {
    fn from(b: &'a HeaderOnlyBlock) -> BlockPtr {
        BlockPtr::from(b.header())
    }
}

impl BlockchainBlock for HeaderOnlyBlock {
    fn number(&self) -> i32 {
        BlockNumber::try_from(self.header().height).unwrap()
    }

    fn ptr(&self) -> BlockPtr {
        self.into()
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        self.header().parent_ptr()
    }

    fn timestamp(&self) -> BlockTime {
        block_time_from_header(self.header())
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

fn block_time_from_header(header: &BlockHeader) -> BlockTime {
    // The timstamp is in ns since the epoch
    let ts = i64::try_from(header.timestamp_nanosec).unwrap();
    let secs = ts / 1_000_000_000;
    let ns: u32 = (ts % 1_000_000_000) as u32;
    BlockTime::since_epoch(secs, ns)
}

#[test]
fn timestamp_conversion() {
    // 2020-07-21T21:50:10Z in ns
    let ts = 1_595_368_210_762_782_796;
    let header = BlockHeader {
        timestamp_nanosec: ts,
        ..Default::default()
    };
    assert_eq!(
        1595368210,
        block_time_from_header(&header).as_secs_since_epoch()
    );
}
