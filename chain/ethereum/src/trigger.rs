use ethabi::LogParam;
use graph::blockchain;
use graph::blockchain::TriggerData;
use graph::prelude::BlockNumber;
use graph::prelude::BlockPtr;
use graph::prelude::{
    CheapClone, EthereumCall, MappingBlockHandler, MappingCallHandler, MappingEventHandler,
};
use graph::slog::{o, SendSyncRefUnwindSafeKV};
use std::convert::TryFrom;
use std::{cmp::Ordering, sync::Arc};
use web3::types::{Address, Block, Log, Transaction, H256};

// ETHDEP: This should be defined in only one place.
type LightEthereumBlock = Block<Transaction>;

pub enum MappingTrigger {
    Log {
        block: Arc<LightEthereumBlock>,
        transaction: Arc<Transaction>,
        log: Arc<Log>,
        params: Vec<LogParam>,
        handler: MappingEventHandler,
    },
    Call {
        block: Arc<LightEthereumBlock>,
        transaction: Arc<Transaction>,
        call: Arc<EthereumCall>,
        inputs: Vec<LogParam>,
        outputs: Vec<LogParam>,
        handler: MappingCallHandler,
    },
    Block {
        block: Arc<LightEthereumBlock>,
        handler: MappingBlockHandler,
    },
}

// Logging the block is too verbose, so this strips the block from the trigger for Debug.
impl std::fmt::Debug for MappingTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[derive(Debug)]
        pub enum MappingTriggerWithoutBlock {
            Log {
                transaction: Arc<Transaction>,
                log: Arc<Log>,
                params: Vec<LogParam>,
                handler: MappingEventHandler,
            },
            Call {
                transaction: Arc<Transaction>,
                call: Arc<EthereumCall>,
                inputs: Vec<LogParam>,
                outputs: Vec<LogParam>,
                handler: MappingCallHandler,
            },
            Block {
                handler: MappingBlockHandler,
            },
        }

        let trigger_without_block = match self {
            MappingTrigger::Log {
                block: _,
                transaction,
                log,
                params,
                handler,
            } => MappingTriggerWithoutBlock::Log {
                transaction: transaction.cheap_clone(),
                log: log.cheap_clone(),
                params: params.clone(),
                handler: handler.clone(),
            },
            MappingTrigger::Call {
                block: _,
                transaction,
                call,
                inputs,
                outputs,
                handler,
            } => MappingTriggerWithoutBlock::Call {
                transaction: transaction.cheap_clone(),
                call: call.cheap_clone(),
                inputs: inputs.clone(),
                outputs: outputs.clone(),
                handler: handler.clone(),
            },
            MappingTrigger::Block { block: _, handler } => MappingTriggerWithoutBlock::Block {
                handler: handler.clone(),
            },
        };

        write!(f, "{:?}", trigger_without_block)
    }
}

impl MappingTrigger {
    pub fn logging_extras(&self) -> Box<dyn SendSyncRefUnwindSafeKV> {
        match self {
            MappingTrigger::Log { handler, log, .. } => Box::new(o! {
                "signature" => handler.event.to_string(),
                "address" => format!("{}", &log.address),
            }),
            MappingTrigger::Call { handler, call, .. } => Box::new(o! {
                "function" => handler.function.to_string(),
                "to" => format!("{}", &call.to),
            }),
            MappingTrigger::Block { .. } => Box::new(o! {}),
        }
    }
}

impl blockchain::MappingTrigger for MappingTrigger {
    fn handler_name(&self) -> &str {
        match self {
            MappingTrigger::Log { handler, .. } => &handler.handler,
            MappingTrigger::Call { handler, .. } => &handler.handler,
            MappingTrigger::Block { handler, .. } => &handler.handler,
        }
    }
}

#[derive(Clone, Debug)]
pub enum EthereumTrigger {
    Block(BlockPtr, EthereumBlockTriggerType),
    Call(Arc<EthereumCall>),
    Log(Arc<Log>),
}

impl PartialEq for EthereumTrigger {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Block(a_ptr, a_kind), Self::Block(b_ptr, b_kind)) => {
                a_ptr == b_ptr && a_kind == b_kind
            }

            (Self::Call(a), Self::Call(b)) => a == b,

            (Self::Log(a), Self::Log(b)) => {
                a.transaction_hash == b.transaction_hash && a.log_index == b.log_index
            }

            _ => false,
        }
    }
}

impl Eq for EthereumTrigger {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EthereumBlockTriggerType {
    Every,
    WithCallTo(Address),
}

impl EthereumTrigger {
    pub fn block_number(&self) -> BlockNumber {
        match self {
            EthereumTrigger::Block(block_ptr, _) => block_ptr.number,
            EthereumTrigger::Call(call) => call.block_number,
            EthereumTrigger::Log(log) => i32::try_from(log.block_number.unwrap().as_u64()).unwrap(),
        }
    }

    pub fn block_hash(&self) -> H256 {
        match self {
            EthereumTrigger::Block(block_ptr, _) => block_ptr.hash_as_h256(),
            EthereumTrigger::Call(call) => call.block_hash,
            EthereumTrigger::Log(log) => log.block_hash.unwrap(),
        }
    }
}

impl Ord for EthereumTrigger {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // Keep the order when comparing two block triggers
            (Self::Block(..), Self::Block(..)) => Ordering::Equal,

            // Block triggers always come last
            (Self::Block(..), _) => Ordering::Greater,
            (_, Self::Block(..)) => Ordering::Less,

            // Calls are ordered by their tx indexes
            (Self::Call(a), Self::Call(b)) => a.transaction_index.cmp(&b.transaction_index),

            // Events are ordered by their log index
            (Self::Log(a), Self::Log(b)) => a.log_index.cmp(&b.log_index),

            // Calls vs. events are logged by their tx index;
            // if they are from the same transaction, events come first
            (Self::Call(a), Self::Log(b))
                if a.transaction_index == b.transaction_index.unwrap().as_u64() =>
            {
                Ordering::Greater
            }
            (Self::Log(a), Self::Call(b))
                if a.transaction_index.unwrap().as_u64() == b.transaction_index =>
            {
                Ordering::Less
            }
            (Self::Call(a), Self::Log(b)) => a
                .transaction_index
                .cmp(&b.transaction_index.unwrap().as_u64()),
            (Self::Log(a), Self::Call(b)) => a
                .transaction_index
                .unwrap()
                .as_u64()
                .cmp(&b.transaction_index),
        }
    }
}

impl PartialOrd for EthereumTrigger {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl TriggerData for EthereumTrigger {
    fn error_context(&self) -> std::string::String {
        let transaction_id = match self {
            EthereumTrigger::Log(log) => log.transaction_hash,
            EthereumTrigger::Call(call) => call.transaction_hash,
            EthereumTrigger::Block(..) => None,
        };

        match transaction_id {
            Some(tx_hash) => format!(
                "block #{} ({}), transaction {:x}",
                self.block_number(),
                self.block_hash(),
                tx_hash
            ),
            None => String::new(),
        }
    }
}
