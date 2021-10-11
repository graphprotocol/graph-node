use crate::data_source::MappingBlockHandler;
use graph::blockchain;
use graph::blockchain::TriggerData;
use graph::components::tendermint::hash::Hash;
use graph::components::tendermint::{TendermintBlock, TendermintBlockHeader, TendermintBlockTxData};
use graph::prelude::BlockNumber;
use graph::prelude::BlockPtr;
use graph::runtime::asc_new;
use graph::runtime::AscHeap;
use graph::runtime::AscPtr;
use graph::runtime::DeterministicHostError;
use graph::slog::{o, SendSyncRefUnwindSafeKV};
use std::{cmp::Ordering, sync::Arc};

pub enum MappingTrigger {
    Block {
        block: Arc<TendermintBlock>,
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
                let block = TendermintBlockData::from(block.as_ref());
                asc_new(heap, &block)?.erase()
            }
        })
    }
}

#[derive(Clone, Debug)]
pub enum TendermintTrigger {
    Block(BlockPtr, TendermintBlockTriggerType),
}

impl PartialEq for TendermintTrigger {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Block(a_ptr, a_kind), Self::Block(b_ptr, b_kind)) => {
                a_ptr == b_ptr && a_kind == b_kind
            }
        }
    }
}

impl Eq for TendermintTrigger {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TendermintBlockTriggerType {
    Every,
}

impl TendermintTrigger {
    pub fn block_number(&self) -> BlockNumber {
        match self {
            TendermintTrigger::Block(block_ptr, _) => block_ptr.number,
        }
    }

    //pub fn block_hash(&self) -> H256 {
  /*   pub fn block_hash(&self) -> Vec<u8> {
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
                format!("block #{}", self.block_number() ) //, self.block_hash(),)
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct TendermintBlockData {
    pub hash: Hash,
    pub parent_hash:  Option<Hash>,
    pub number: u64,
    pub time_sec: i64,
    pub time_nano: i32,


    pub header: TendermintBlockHeader,
    pub data: TendermintBlockTxData,
}

impl<'a> From<&'a TendermintBlock> for TendermintBlockData {
    fn from(block: &'a TendermintBlock) -> TendermintBlockData {
        //let t = block.header.time.as_ref().unwrap();

        TendermintBlockData {
            hash: block.hash,
            parent_hash: block.parent_hash.clone(),
            number: block.number,
            time_sec: block.header.time_sec,
            time_nano: block.header.time_nano,
            header: block.header.clone(),
            data: block.data.clone(),
        }
    }
}

