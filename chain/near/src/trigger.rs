use graph::blockchain;
use graph::blockchain::Block;
use graph::blockchain::TriggerData;
use graph::cheap_clone::CheapClone;
use graph::prelude::hex;
use graph::prelude::web3::types::H256;
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
        pub enum MappingTriggerWithoutBlock<'a> {
            Block,

            Receipt {
                outcome: &'a codec::ExecutionOutcomeWithIdView,
                receipt: &'a codec::Receipt,
            },
        }

        let trigger_without_block = match self {
            NearTrigger::Block(_) => MappingTriggerWithoutBlock::Block,
            NearTrigger::Receipt(receipt) => MappingTriggerWithoutBlock::Receipt {
                outcome: &receipt.outcome,
                receipt: &receipt.receipt,
            },
        };

        write!(f, "{:?}", trigger_without_block)
    }
}

impl blockchain::MappingTrigger for NearTrigger {
    fn to_asc_ptr<H: AscHeap>(self, heap: &mut H) -> Result<AscPtr<()>, DeterministicHostError> {
        Ok(match self {
            NearTrigger::Block(block) => asc_new(heap, block.as_ref())?.erase(),
            NearTrigger::Receipt(receipt) => asc_new(heap, receipt.as_ref())?.erase(),
        })
    }
}

#[derive(Clone)]
pub enum NearTrigger {
    Block(Arc<codec::BlockWrapper>),
    Receipt(Arc<ReceiptWithOutcome>),
}

impl CheapClone for NearTrigger {
    fn cheap_clone(&self) -> NearTrigger {
        match self {
            NearTrigger::Block(block) => NearTrigger::Block(block.cheap_clone()),
            NearTrigger::Receipt(receipt) => NearTrigger::Receipt(receipt.cheap_clone()),
        }
    }
}

impl PartialEq for NearTrigger {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Block(a_ptr), Self::Block(b_ptr)) => a_ptr == b_ptr,
            (Self::Receipt(a), Self::Receipt(b)) => a.receipt.receipt_id == b.receipt.receipt_id,

            (Self::Block(_), Self::Receipt(_)) | (Self::Receipt(_), Self::Block(_)) => false,
        }
    }
}

impl Eq for NearTrigger {}

impl NearTrigger {
    pub fn block_number(&self) -> BlockNumber {
        match self {
            NearTrigger::Block(block) => block.number(),
            NearTrigger::Receipt(receipt) => receipt.block.number(),
        }
    }

    pub fn block_hash(&self) -> H256 {
        match self {
            NearTrigger::Block(block) => block.ptr().hash_as_h256(),
            NearTrigger::Receipt(receipt) => receipt.block.ptr().hash_as_h256(),
        }
    }
}

impl Ord for NearTrigger {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // Keep the order when comparing two block triggers
            (Self::Block(..), Self::Block(..)) => Ordering::Equal,

            // Block triggers always come last
            (Self::Block(..), _) => Ordering::Greater,
            (_, Self::Block(..)) => Ordering::Less,

            // Execution outcomes have no intrinsic ordering information, so we keep the order in
            // which they are included in the `receipt_execution_outcomes` field of `IndexerShard`.
            (Self::Receipt(..), Self::Receipt(..)) => Ordering::Equal,
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
                format!("Block #{} ({})", self.block_number(), self.block_hash())
            }
            NearTrigger::Receipt(receipt) => {
                format!(
                    "receipt id {}, block #{} ({})",
                    hex::encode(&receipt.receipt.receipt_id.as_ref().unwrap().bytes),
                    self.block_number(),
                    self.block_hash()
                )
            }
        }
    }
}

pub struct ReceiptWithOutcome {
    // REVIEW: Do we want to actually also have those two below behind an `Arc` wrapper?
    pub outcome: codec::ExecutionOutcomeWithIdView,
    pub receipt: codec::Receipt,
    pub block: Arc<codec::BlockWrapper>,
}
