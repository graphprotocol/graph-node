use std::{cmp::Ordering, sync::Arc};

use graph::blockchain::{Block, BlockHash, MappingTrigger, TriggerData};
use graph::cheap_clone::CheapClone;
use graph::prelude::BlockNumber;
use graph::runtime::{asc_new, gas::GasCounter, AscHeap, AscPtr, DeterministicHostError};

use crate::codec;
use crate::data_source::EventOrigin;

// Logging the block is too verbose, so this strips the block from the trigger for Debug.
impl std::fmt::Debug for CosmosTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[derive(Debug)]
        pub enum MappingTriggerWithoutBlock<'e> {
            Block,
            Event {
                event_type: &'e str,
                origin: EventOrigin,
            },
            Transaction,
        }

        let trigger_without_block = match self {
            CosmosTrigger::Block(_) => MappingTriggerWithoutBlock::Block,
            CosmosTrigger::Event { event_data, origin } => MappingTriggerWithoutBlock::Event {
                event_type: &event_data.event().event_type,
                origin: *origin,
            },
            CosmosTrigger::Transaction(_) => MappingTriggerWithoutBlock::Transaction,
        };

        write!(f, "{:?}", trigger_without_block)
    }
}

impl MappingTrigger for CosmosTrigger {
    fn to_asc_ptr<H: AscHeap>(
        self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscPtr<()>, DeterministicHostError> {
        Ok(match self {
            CosmosTrigger::Block(block) => asc_new(heap, block.as_ref(), gas)?.erase(),
            CosmosTrigger::Event { event_data, .. } => {
                asc_new(heap, event_data.as_ref(), gas)?.erase()
            }
            CosmosTrigger::Transaction(transaction_data) => {
                asc_new(heap, transaction_data.as_ref(), gas)?.erase()
            }
        })
    }
}

#[derive(Clone)]
pub enum CosmosTrigger {
    Block(Arc<codec::Block>),
    Event {
        event_data: Arc<codec::EventData>,
        origin: EventOrigin,
    },
    Transaction(Arc<codec::TransactionData>),
}

impl CheapClone for CosmosTrigger {
    fn cheap_clone(&self) -> CosmosTrigger {
        match self {
            CosmosTrigger::Block(block) => CosmosTrigger::Block(block.cheap_clone()),
            CosmosTrigger::Event { event_data, origin } => CosmosTrigger::Event {
                event_data: event_data.cheap_clone(),
                origin: *origin,
            },
            CosmosTrigger::Transaction(transaction_data) => {
                CosmosTrigger::Transaction(transaction_data.cheap_clone())
            }
        }
    }
}

impl PartialEq for CosmosTrigger {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Block(a_ptr), Self::Block(b_ptr)) => a_ptr == b_ptr,
            (
                Self::Event {
                    event_data: a_event_data,
                    origin: a_origin,
                },
                Self::Event {
                    event_data: b_event_data,
                    origin: b_origin,
                },
            ) => {
                a_event_data.event().event_type == b_event_data.event().event_type
                    && a_origin == b_origin
            }
            (Self::Transaction(a_ptr), Self::Transaction(b_ptr)) => a_ptr == b_ptr,
            _ => false,
        }
    }
}

impl Eq for CosmosTrigger {}

impl CosmosTrigger {
    pub(crate) fn with_event(
        event: codec::Event,
        block: codec::HeaderOnlyBlock,
        origin: EventOrigin,
    ) -> CosmosTrigger {
        CosmosTrigger::Event {
            event_data: Arc::new(codec::EventData {
                event: Some(event),
                block: Some(block),
            }),
            origin,
        }
    }

    pub(crate) fn with_transaction(
        tx_result: codec::TxResult,
        block: codec::HeaderOnlyBlock,
    ) -> CosmosTrigger {
        CosmosTrigger::Transaction(Arc::new(codec::TransactionData {
            tx: Some(tx_result),
            block: Some(block),
        }))
    }

    pub fn block_number(&self) -> BlockNumber {
        match self {
            CosmosTrigger::Block(block) => block.number(),
            CosmosTrigger::Event { event_data, .. } => event_data.block().number(),
            CosmosTrigger::Transaction(transaction_data) => transaction_data.block().number(),
        }
    }

    pub fn block_hash(&self) -> BlockHash {
        match self {
            CosmosTrigger::Block(block) => block.hash(),
            CosmosTrigger::Event { event_data, .. } => event_data.block().hash(),
            CosmosTrigger::Transaction(transaction_data) => transaction_data.block().hash(),
        }
    }
}

impl Ord for CosmosTrigger {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // Keep the order when comparing two block triggers
            (Self::Block(..), Self::Block(..)) => Ordering::Equal,

            // Block triggers always come last
            (Self::Block(..), _) => Ordering::Greater,
            (_, Self::Block(..)) => Ordering::Less,

            // Events have no intrinsic ordering information, so we keep the order in
            // which they are included in the `events` field
            (Self::Event { .. }, Self::Event { .. }) => Ordering::Equal,

            // Transactions are ordered by their index inside the block
            (Self::Transaction(a), Self::Transaction(b)) => {
                a.tx_result().index.cmp(&b.tx_result().index)
            }

            // When comparing events and transactions, transactions go first
            (Self::Transaction(..), Self::Event { .. }) => Ordering::Less,
            (Self::Event { .. }, Self::Transaction(..)) => Ordering::Greater,
        }
    }
}

impl PartialOrd for CosmosTrigger {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl TriggerData for CosmosTrigger {
    fn error_context(&self) -> std::string::String {
        match self {
            CosmosTrigger::Block(..) => {
                format!("block #{}, hash {}", self.block_number(), self.block_hash())
            }
            CosmosTrigger::Event { event_data, origin } => {
                format!(
                    "event type {}, origin: {:?}, block #{}, hash {}",
                    event_data.event().event_type,
                    origin,
                    self.block_number(),
                    self.block_hash(),
                )
            }
            CosmosTrigger::Transaction(transaction_data) => {
                format!(
                    "block #{}, hash {}, transaction log: {}",
                    self.block_number(),
                    self.block_hash(),
                    transaction_data.response_deliver_tx().log
                )
            }
        }
    }
}
