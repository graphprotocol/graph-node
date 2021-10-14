use graph::blockchain;
use graph::blockchain::Block;
use graph::blockchain::TriggerData;
use graph::cheap_clone::CheapClone;
use graph::prelude::web3::types::H256;
use graph::prelude::web3::types::U64;
use graph::prelude::BlockNumber;
use graph::runtime::asc_new;
use graph::runtime::AscHeap;
use graph::runtime::AscPtr;
use graph::runtime::DeterministicHostError;
use std::{cmp::Ordering, sync::Arc};

use crate::codec;

// Logging the block is too verbose, so this strips the block from the trigger for Debug.
impl std::fmt::Debug for NearTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[derive(Debug)]
        pub enum MappingTriggerWithoutBlock {
            Block,
        }

        let trigger_without_block = match self {
            NearTrigger::Block(_) => MappingTriggerWithoutBlock::Block,
        };

        write!(f, "{:?}", trigger_without_block)
    }
}

impl blockchain::MappingTrigger for NearTrigger {
    fn to_asc_ptr<H: AscHeap>(self, heap: &mut H) -> Result<AscPtr<()>, DeterministicHostError> {
        Ok(match self {
            NearTrigger::Block(block) => {
                let block = NearBlockData::from(block.as_ref());
                asc_new(heap, &block)?.erase()
            }
        })
    }
}

#[derive(Clone)]
pub enum NearTrigger {
    Block(Arc<codec::BlockWrapper>),
}

impl CheapClone for NearTrigger {
    fn cheap_clone(&self) -> NearTrigger {
        match self {
            NearTrigger::Block(block) => NearTrigger::Block(block.cheap_clone()),
        }
    }
}

impl PartialEq for NearTrigger {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Block(a_ptr), Self::Block(b_ptr)) => a_ptr == b_ptr,
        }
    }
}

impl Eq for NearTrigger {}

impl NearTrigger {
    pub fn block_number(&self) -> BlockNumber {
        match self {
            NearTrigger::Block(block) => block.number(),
        }
    }

    pub fn block_hash(&self) -> H256 {
        match self {
            NearTrigger::Block(block) => block.ptr().hash_as_h256(),
        }
    }
}

impl Ord for NearTrigger {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // Keep the order when comparing two block triggers
            (Self::Block(..), Self::Block(..)) => Ordering::Equal,
        }
    }
}

impl PartialOrd for NearTrigger {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl TriggerData for NearTrigger {
    fn error_context(&self) -> std::string::String {
        match self {
            NearTrigger::Block(..) => {
                format!("block #{} ({})", self.block_number(), self.block_hash(),)
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct NearBlockData {
    pub hash: H256,
    pub parent_hash: Option<H256>,
    pub number: U64,
    pub timestamp: U64,
}

impl<'a> From<&'a codec::BlockWrapper> for NearBlockData {
    fn from(block: &'a codec::BlockWrapper) -> NearBlockData {
        let block = block.block.as_ref().unwrap();
        let header = block.header.as_ref().unwrap();

        NearBlockData {
            hash: header.hash.as_ref().unwrap().into(),
            parent_hash: header.prev_hash.as_ref().map(Into::into),
            number: U64::from(header.height),
            // FIXME (NEAR): Fix timestamp
            timestamp: U64::from(0),
        }
    }
}
