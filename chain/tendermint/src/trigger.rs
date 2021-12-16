use std::{cmp::Ordering, sync::Arc};

use graph::blockchain;
use graph::blockchain::Block;
use graph::blockchain::TriggerData;
use graph::cheap_clone::CheapClone;
use graph::prelude::BlockNumber;
use graph::runtime::asc_new;
use graph::runtime::AscHeap;
use graph::runtime::AscPtr;
use graph::runtime::DeterministicHostError;

use crate::codec;

// Logging the block is too verbose, so this strips the block from the trigger for Debug.
impl std::fmt::Debug for TendermintTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[derive(Debug)]
        pub enum MappingTriggerWithoutBlock {
            Block,
            Event,
        }

        let trigger_without_block = match self {
            TendermintTrigger::Block(_) => MappingTriggerWithoutBlock::Block,
            TendermintTrigger::Event(_) => MappingTriggerWithoutBlock::Event,
        };

        write!(f, "{:?}", trigger_without_block)
    }
}

impl blockchain::MappingTrigger for TendermintTrigger {
    fn to_asc_ptr<H: AscHeap>(self, heap: &mut H) -> Result<AscPtr<()>, DeterministicHostError> {
        Ok(match self {
            TendermintTrigger::Block(block) => {
                //let block = TendermintBlockData::from(block.as_ref());
                //b.header()
                asc_new(heap, block.as_ref())?.erase()
            }
            TendermintTrigger::Event(data) => {
                asc_new(heap, data.as_ref())?.erase()
            }
        })
    }
}

#[derive(Clone)]
pub enum TendermintTrigger {
    Block(Arc<codec::EventList>),
    Event(Arc<EventData>),
}

impl CheapClone for TendermintTrigger {
    fn cheap_clone(&self) -> TendermintTrigger {
        match self {
            TendermintTrigger::Block(block) => TendermintTrigger::Block(block.cheap_clone()),
            TendermintTrigger::Event(data) => TendermintTrigger::Event(data.cheap_clone()),
        }
    }
}

impl PartialEq for TendermintTrigger {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Block(a_ptr), Self::Block(b_ptr)) => a_ptr == b_ptr,
            (Self::Event(a), Self::Event(b)) => a.event.eventtype == b.event.eventtype,
            _ => false,
        }
    }
}

impl Eq for TendermintTrigger {}

impl TendermintTrigger {
    pub fn block_number(&self) -> BlockNumber {
        match self {
            TendermintTrigger::Block(block_ptr) => block_ptr.number(),
            TendermintTrigger::Event(data) => data.block.number(),
        }
    }

    // pub fn block_hash(&self) -> H256 {
    /* pub fn block_hash(&self) -> Vec<u8> {
           match self {
               TendermintTrigger::Block(block_ptr, _) => block_ptr; // .hash_as_h256(),
           }
    }*/
}

impl Ord for TendermintTrigger {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // Keep the order when comparing two block triggers
            (Self::Block(..), Self::Block(..)) => Ordering::Equal,

            // Block triggers always come last
            (Self::Block(..), _) => Ordering::Greater,
            (_, Self::Block(..)) => Ordering::Less,

            // Events have no intrinsic ordering information, so we keep the order in
            // which they are included in the `events` field
            (Self::Event(..), Self::Event(..)) => Ordering::Equal,
        }
    }
}

impl PartialOrd for TendermintTrigger {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl TriggerData for TendermintTrigger {
    fn error_context(&self) -> std::string::String {
        match self {
            TendermintTrigger::Block(..) => {
                format!("block #{}", self.block_number()) // TODO: Add `self.block_hash()`
            }
            TendermintTrigger::Event(data) => {
                format!(
                    "event type {}, block #{}",
                    data.event.eventtype,
                    self.block_number(),
                    // TODO: Add `self.block_hash()`
                )
            }
        }
    }
}

pub struct EventData {
    pub event: codec::Event, // REVIEW: Do we want to have this behind an `Arc` wrapper?
    pub block: Arc<codec::EventList>,
}
