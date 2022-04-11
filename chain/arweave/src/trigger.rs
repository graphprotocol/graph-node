use graph::blockchain;
use graph::blockchain::Block;
use graph::blockchain::TriggerData;
use graph::cheap_clone::CheapClone;
use graph::prelude::web3::types::H256;
use graph::prelude::BlockNumber;
use graph::runtime::asc_new;
use graph::runtime::gas::GasCounter;
use graph::runtime::AscHeap;
use graph::runtime::AscPtr;
use graph::runtime::DeterministicHostError;
use std::{cmp::Ordering, sync::Arc};

use crate::codec;

// Logging the block is too verbose, so this strips the block from the trigger for Debug.
impl std::fmt::Debug for ArweaveTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[derive(Debug)]
        pub enum MappingTriggerWithoutBlock {
            Block,
        }

        let trigger_without_block = match self {
            ArweaveTrigger::Block(_) => MappingTriggerWithoutBlock::Block,
        };

        write!(f, "{:?}", trigger_without_block)
    }
}

impl blockchain::MappingTrigger for ArweaveTrigger {
    fn to_asc_ptr<H: AscHeap>(
        self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscPtr<()>, DeterministicHostError> {
        Ok(match self {
            ArweaveTrigger::Block(block) => asc_new(heap, block.as_ref(), gas)?.erase(),
        })
    }
}

#[derive(Clone)]
pub enum ArweaveTrigger {
    Block(Arc<codec::Block>),
}

impl CheapClone for ArweaveTrigger {
    fn cheap_clone(&self) -> ArweaveTrigger {
        match self {
            ArweaveTrigger::Block(block) => ArweaveTrigger::Block(block.cheap_clone()),
        }
    }
}

impl PartialEq for ArweaveTrigger {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Block(a_ptr), Self::Block(b_ptr)) => a_ptr == b_ptr,
        }
    }
}

impl Eq for ArweaveTrigger {}

impl ArweaveTrigger {
    pub fn block_number(&self) -> BlockNumber {
        match self {
            ArweaveTrigger::Block(block) => block.number(),
        }
    }

    pub fn block_hash(&self) -> H256 {
        match self {
            ArweaveTrigger::Block(block) => block.ptr().hash_as_h256(),
        }
    }
}

impl Ord for ArweaveTrigger {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // Keep the order when comparing two block triggers
            (Self::Block(..), Self::Block(..)) => Ordering::Equal,
        }
    }
}

impl PartialOrd for ArweaveTrigger {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl TriggerData for ArweaveTrigger {
    fn error_context(&self) -> std::string::String {
        match self {
            ArweaveTrigger::Block(..) => {
                format!("Block #{} ({})", self.block_number(), self.block_hash())
            }
        }
    }
}
