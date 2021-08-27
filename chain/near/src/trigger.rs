use graph::blockchain;
use graph::blockchain::TriggerData;
use graph::components::near::NearBlock;
use graph::prelude::web3::types::Address;
use graph::prelude::web3::types::H256;
use graph::prelude::web3::types::U64;
use graph::prelude::BlockNumber;
use graph::prelude::BlockPtr;
use graph::prelude::MappingBlockHandler;
use graph::runtime::asc_new;
use graph::runtime::AscHeap;
use graph::runtime::AscPtr;
use graph::runtime::DeterministicHostError;
use graph::slog::{o, SendSyncRefUnwindSafeKV};
use std::{cmp::Ordering, sync::Arc};

pub enum MappingTrigger {
    Block {
        block: Arc<NearBlock>,
        handler: MappingBlockHandler,
    },
}

// Logging the block is too verbose, so this strips the block from the trigger for Debug.
impl std::fmt::Debug for MappingTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[derive(Debug)]
        pub enum MappingTriggerWithoutBlock {
            Block { handler: MappingBlockHandler },
        }

        let trigger_without_block = match self {
            MappingTrigger::Block { block: _, handler } => MappingTriggerWithoutBlock::Block {
                handler: handler.clone(),
            },
        };

        write!(f, "{:?}", trigger_without_block)
    }
}

impl blockchain::MappingTrigger for MappingTrigger {
    fn handler_name(&self) -> &str {
        match self {
            MappingTrigger::Block { handler, .. } => &handler.handler,
        }
    }

    fn logging_extras(&self) -> Box<dyn SendSyncRefUnwindSafeKV> {
        match self {
            MappingTrigger::Block { .. } => Box::new(o! {}),
        }
    }

    fn to_asc_ptr<H: AscHeap>(self, heap: &mut H) -> Result<AscPtr<()>, DeterministicHostError> {
        Ok(match self {
            MappingTrigger::Block { block, handler: _ } => {
                let block = NearBlockData::from(block.as_ref());
                asc_new(heap, &block)?.erase()
            }
        })
    }
}

#[derive(Clone, Debug)]
pub enum NearTrigger {
    Block(BlockPtr, NearBlockTriggerType),
}

impl PartialEq for NearTrigger {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Block(a_ptr, a_kind), Self::Block(b_ptr, b_kind)) => {
                a_ptr == b_ptr && a_kind == b_kind
            }
        }
    }
}

impl Eq for NearTrigger {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NearBlockTriggerType {
    Every,
    WithCallTo(Address),
}

impl NearTrigger {
    pub fn block_number(&self) -> BlockNumber {
        match self {
            NearTrigger::Block(block_ptr, _) => block_ptr.number,
        }
    }

    pub fn block_hash(&self) -> H256 {
        match self {
            NearTrigger::Block(block_ptr, _) => block_ptr.hash_as_h256(),
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

impl<'a> From<&'a NearBlock> for NearBlockData {
    fn from(block: &'a NearBlock) -> NearBlockData {
        NearBlockData {
            hash: block.hash.clone(),
            parent_hash: block.parent_hash.clone(),
            number: U64::from(block.number),
            // FIXME (NEAR): Fix timestamp
            timestamp: U64::from(0),
        }
    }
}
