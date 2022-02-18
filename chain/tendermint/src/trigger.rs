use std::{cmp::Ordering, sync::Arc};

use graph::blockchain::{Block, BlockHash, MappingTrigger, TriggerData};
use graph::cheap_clone::CheapClone;
use graph::prelude::BlockNumber;
use graph::runtime::{asc_new, AscHeap, AscPtr, DeterministicHostError};

use crate::codec;

// Logging the block is too verbose, so this strips the block from the trigger for Debug.
impl std::fmt::Debug for TendermintTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[derive(Debug)]
        pub enum MappingTriggerWithoutBlock<'e> {
            Block,
            Event { event_type: &'e str },
        }

        let trigger_without_block = match self {
            TendermintTrigger::Block(_) => MappingTriggerWithoutBlock::Block,
            TendermintTrigger::Event(event) => MappingTriggerWithoutBlock::Event {
                event_type: &event.event().event_type,
            },
        };

        write!(f, "{:?}", trigger_without_block)
    }
}

impl MappingTrigger for TendermintTrigger {
    fn to_asc_ptr<H: AscHeap>(self, heap: &mut H) -> Result<AscPtr<()>, DeterministicHostError> {
        Ok(match self {
            TendermintTrigger::Block(event_list) => asc_new(heap, event_list.as_ref())?.erase(),
            TendermintTrigger::Event(event_data) => asc_new(heap, event_data.as_ref())?.erase(),
        })
    }
}

#[derive(Clone, PartialOrd)]
pub enum TendermintTrigger {
    Block(Arc<codec::EventList>),
    Event(Arc<codec::EventData>),
}

impl CheapClone for TendermintTrigger {
    fn cheap_clone(&self) -> TendermintTrigger {
        match self {
            TendermintTrigger::Block(event_list) => {
                TendermintTrigger::Block(event_list.cheap_clone())
            }
            TendermintTrigger::Event(event_data) => {
                TendermintTrigger::Event(event_data.cheap_clone())
            }
        }
    }
}

impl PartialEq for TendermintTrigger {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Block(a_ptr), Self::Block(b_ptr)) => a_ptr == b_ptr,
            (Self::Event(a), Self::Event(b)) => a.event().event_type == b.event().event_type,
            _ => false,
        }
    }
}

impl Eq for TendermintTrigger {}

impl TendermintTrigger {
    pub fn block_number(&self) -> BlockNumber {
        match self {
            TendermintTrigger::Block(event_list) => event_list.block().number(),
            TendermintTrigger::Event(event_data) => event_data.block().number(),
        }
    }

    pub fn block_hash(&self) -> BlockHash {
        match self {
            TendermintTrigger::Block(event_list) => event_list.block().hash(),
            TendermintTrigger::Event(event_data) => event_data.block().hash(),
        }
    }
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

impl TriggerData for TendermintTrigger {
    fn error_context(&self) -> std::string::String {
        match self {
            TendermintTrigger::Block(..) => {
                format!("block #{}, hash {}", self.block_number(), self.block_hash())
            }
            TendermintTrigger::Event(event_data) => {
                format!(
                    "event type {}, block #{}, hash {}",
                    event_data.event().event_type,
                    self.block_number(),
                    self.block_hash(),
                )
            }
        }
    }
}
