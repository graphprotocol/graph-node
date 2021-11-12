use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

use crate::{
    blockchain::{Block, BlockPtr},
    components::tendermint::hash::Hash,
    prelude::BlockNumber,
};

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct TendermintBlock {
    pub hash: Hash,
    pub number: u64,
    pub parent_hash: Option<Hash>,
    pub parent_number: Option<u64>,

    pub header: TendermintBlockHeader,
    pub data: TendermintBlockTxData,
    //pub evidence:  Option<TendermintEvidenceList>,
    //pub last_commit:  Option<TendermintCommit>,
}

impl From<TendermintBlock> for BlockPtr {
    fn from(b: TendermintBlock) -> BlockPtr {
        BlockPtr::from((b.hash.as_vec(), b.number))
    }
}

impl<'a> From<&'a TendermintBlock> for BlockPtr {
    fn from(b: &'a TendermintBlock) -> BlockPtr {
        BlockPtr::from((b.hash.as_vec(), b.number))
    }
}

impl Block for TendermintBlock {
    fn ptr(&self) -> BlockPtr {
        BlockPtr::from((self.hash.as_vec(), self.number))
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        match (self.parent_hash.clone(), self.parent_number) {
            (Some(hash), Some(number)) => Some(BlockPtr::from((hash.as_vec(), number))),
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
        match (self.parent_hash.clone(), self.parent_number) {
            (Some(hash), Some(number)) => Some(BlockPtr::from((hash.as_vec(), number))),
            _ => None,
        }
    }

    fn format(&self) -> String {
        format!("#{} (#{})", self.number, self.hash.to_string())
    }

    fn block_ptr(&self) -> BlockPtr {
        BlockPtr::from((self.hash.as_vec(), self.number))
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct TendermintBlockHeader {
    pub version: Option<TendermintConsensus>,
    pub chain_id: String,
    pub height: u64,
    pub time_sec: i64,
    pub time_nano: i32,
    pub last_block_id: Option<TendermintBlockId>,
    pub last_commit_hash: Hash,
    pub data_hash: Hash,
    pub validators_hash: Hash,
    pub next_validators_hash: Hash,
    pub consensus_hash: Hash,
    pub app_hash: Hash,
    pub last_results_hash: Hash,
    pub evidence_hash: Hash,
    pub proposer_address: Hash,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct TendermintConsensus {
    pub block: u64,
    pub app: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct TendermintBlockId {
    pub hash: Hash,
    pub part_set_header: Option<TendermintPartSetHeader>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct TendermintPartSetHeader {
    pub total: u32,
    pub hash: Hash
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct TendermintBlockTxData {
    pub txs: Vec<Vec<u8>>,
}
