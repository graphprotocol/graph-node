use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use web3::types::H256;

use crate::{
    blockchain::{Block, BlockPtr},
    prelude::BlockNumber,
};

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct NearBlock {
    // FIXME (NEAR): Should be a thin wrapper over NEAR protobuf generated code with accessor
    //               to formalize which files are required and which not.
    pub hash: H256,
    pub number: u64,
    pub parent_hash: Option<H256>,
    pub parent_number: Option<u64>,
}

impl From<NearBlock> for BlockPtr {
    fn from(b: NearBlock) -> BlockPtr {
        BlockPtr::from((b.hash, b.number))
    }
}

impl<'a> From<&'a NearBlock> for BlockPtr {
    fn from(b: &'a NearBlock) -> BlockPtr {
        BlockPtr::from((b.hash, b.number))
    }
}

impl Block for NearBlock {
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

pub trait NearBlockExt {
    fn number(&self) -> BlockNumber;
    fn parent_ptr(&self) -> Option<BlockPtr>;
    fn format(&self) -> String;
    fn block_ptr(&self) -> BlockPtr;
}

impl NearBlockExt for NearBlock {
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

// FIXME (NEAR): Schedule for deletion, already provided elsewhere, should the other location be moved
//               out to a more chain agnostic location?
// impl ToEntityKey for BlockPtr {
//     fn to_entity_key(&self, subgraph: DeploymentHash) -> EntityKey {
//         EntityKey::data(subgraph, "Block".into(), self.hash_hex())
//     }
// }
