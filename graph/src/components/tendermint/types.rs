use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use web3::types::H256;

use crate::{
    blockchain::{Block, BlockPtr},
    prelude::BlockNumber,
};

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct TendermintBlock {
    pub hash: H256,
    pub number: u64,
    pub parent_hash: Option<H256>,
    pub parent_number: Option<u64>,
}

impl From<TendermintBlock> for BlockPtr {
    fn from(b: TendermintBlock) -> BlockPtr {
        BlockPtr::from((b.hash, b.number))
    }
}

impl<'a> From<&'a TendermintBlock> for BlockPtr {
    fn from(b: &'a TendermintBlock) -> BlockPtr {
        BlockPtr::from((b.hash, b.number))
    }
}

impl Block for TendermintBlock {
    fn ptr(&self) -> BlockPtr {
        BlockPtr::from((self.hash, self.number))
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        match (self.parent_hash, self.parent_number) {
            (Some(hash), Some(number)) => Some(BlockPtr::from((hash, number))),
            _ => None,
        }
    }
}

pub trait TendermintBlockExt {
    fn number(&self) -> BlockNumber;
    fn parent_ptr(&self) -> Option<BlockPtr>;
    fn format(&self) -> String;
    fn block_ptr(&self) -> BlockPtr;
}

impl TendermintBlockExt for TendermintBlock {
    fn number(&self) -> BlockNumber {
        BlockNumber::try_from(self.number).unwrap()
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        match (self.parent_hash, self.parent_number) {
            (Some(hash), Some(number)) => Some(BlockPtr::from((hash, number))),
            _ => None,
        }
    }

    fn format(&self) -> String {
        format!("#{} ({:x})", self.number, self.hash)
    }

    fn block_ptr(&self) -> BlockPtr {
        BlockPtr::from((self.hash, self.number))
    }
}
