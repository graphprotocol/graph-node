use graph::blockchain;
use graph::blockchain::TriggerData;
use graph::prelude::ethabi::ethereum_types::H160;
use graph::prelude::ethabi::ethereum_types::H256;
use graph::prelude::ethabi::ethereum_types::U128;
use graph::prelude::ethabi::ethereum_types::U256;
use graph::prelude::ethabi::ethereum_types::U64;
use graph::prelude::ethabi::Address;
use graph::prelude::ethabi::Bytes;
use graph::prelude::ethabi::LogParam;
use graph::prelude::web3::types::Block;
use graph::prelude::web3::types::Log;
use graph::prelude::web3::types::Transaction;
use graph::prelude::BlockNumber;
use graph::prelude::BlockPtr;
use graph::prelude::{CheapClone, EthereumCall};
use graph::runtime::asc_new;
use graph::runtime::AscHeap;
use graph::runtime::AscPtr;
use graph::runtime::DeterministicHostError;
use graph::semver::Version;
use std::convert::TryFrom;
use std::ops::Deref;
use std::{cmp::Ordering, sync::Arc};

use crate::runtime::abi::AscEthereumBlock;
use crate::runtime::abi::AscEthereumBlock_0_0_6;
use crate::runtime::abi::AscEthereumCall;
use crate::runtime::abi::AscEthereumCall_0_0_3;
use crate::runtime::abi::AscEthereumEvent;
use crate::runtime::abi::AscEthereumTransaction_0_0_1;
use crate::runtime::abi::AscEthereumTransaction_0_0_2;
use crate::runtime::abi::AscEthereumTransaction_0_0_6;

// ETHDEP: This should be defined in only one place.
type LightEthereumBlock = Block<Transaction>;

pub enum MappingTrigger {
    Log {
        block: Arc<LightEthereumBlock>,
        transaction: Arc<Transaction>,
        log: Arc<Log>,
        params: Vec<LogParam>,
    },
    Call {
        block: Arc<LightEthereumBlock>,
        transaction: Arc<Transaction>,
        call: Arc<EthereumCall>,
        inputs: Vec<LogParam>,
        outputs: Vec<LogParam>,
    },
    Block {
        block: Arc<LightEthereumBlock>,
    },
}

// Logging the block is too verbose, so this strips the block from the trigger for Debug.
impl std::fmt::Debug for MappingTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[derive(Debug)]
        enum MappingTriggerWithoutBlock {
            Log {
                _transaction: Arc<Transaction>,
                _log: Arc<Log>,
                _params: Vec<LogParam>,
            },
            Call {
                _transaction: Arc<Transaction>,
                _call: Arc<EthereumCall>,
                _inputs: Vec<LogParam>,
                _outputs: Vec<LogParam>,
            },
            Block,
        }

        let trigger_without_block = match self {
            MappingTrigger::Log {
                block: _,
                transaction,
                log,
                params,
            } => MappingTriggerWithoutBlock::Log {
                _transaction: transaction.cheap_clone(),
                _log: log.cheap_clone(),
                _params: params.clone(),
            },
            MappingTrigger::Call {
                block: _,
                transaction,
                call,
                inputs,
                outputs,
            } => MappingTriggerWithoutBlock::Call {
                _transaction: transaction.cheap_clone(),
                _call: call.cheap_clone(),
                _inputs: inputs.clone(),
                _outputs: outputs.clone(),
            },
            MappingTrigger::Block { block: _ } => MappingTriggerWithoutBlock::Block,
        };

        write!(f, "{:?}", trigger_without_block)
    }
}

impl blockchain::MappingTrigger for MappingTrigger {
    fn to_asc_ptr<H: AscHeap>(self, heap: &mut H) -> Result<AscPtr<()>, DeterministicHostError> {
        Ok(match self {
            MappingTrigger::Log {
                block,
                transaction,
                log,
                params,
            } => {
                let ethereum_event_data = EthereumEventData {
                    block: EthereumBlockData::from(block.as_ref()),
                    transaction: EthereumTransactionData::from(transaction.deref()),
                    address: log.address,
                    log_index: log.log_index.unwrap_or(U256::zero()),
                    transaction_log_index: log.log_index.unwrap_or(U256::zero()),
                    log_type: log.log_type.clone(),
                    params,
                };
                let api_version = heap.api_version();
                if api_version >= Version::new(0, 0, 6) {
                    asc_new::<
                        AscEthereumEvent<AscEthereumTransaction_0_0_6, AscEthereumBlock_0_0_6>,
                        _,
                        _,
                    >(heap, &ethereum_event_data)?
                    .erase()
                } else if api_version >= Version::new(0, 0, 2) {
                    asc_new::<AscEthereumEvent<AscEthereumTransaction_0_0_2, AscEthereumBlock>, _, _>(
                        heap,
                        &ethereum_event_data,
                    )?
                    .erase()
                } else {
                    asc_new::<AscEthereumEvent<AscEthereumTransaction_0_0_1, AscEthereumBlock>, _, _>(
                        heap,
                        &ethereum_event_data,
                    )?
                    .erase()
                }
            }
            MappingTrigger::Call {
                block,
                transaction,
                call,
                inputs,
                outputs,
            } => {
                let call = EthereumCallData {
                    to: call.to,
                    from: call.from,
                    block: EthereumBlockData::from(block.as_ref()),
                    transaction: EthereumTransactionData::from(transaction.deref()),
                    inputs,
                    outputs,
                };
                if heap.api_version() >= Version::new(0, 0, 6) {
                    asc_new::<
                        AscEthereumCall_0_0_3<AscEthereumTransaction_0_0_6, AscEthereumBlock_0_0_6>,
                        _,
                        _,
                    >(heap, &call)?
                    .erase()
                } else if heap.api_version() >= Version::new(0, 0, 3) {
                    asc_new::<
                        AscEthereumCall_0_0_3<AscEthereumTransaction_0_0_2, AscEthereumBlock>,
                        _,
                        _,
                    >(heap, &call)?
                    .erase()
                } else {
                    asc_new::<AscEthereumCall, _, _>(heap, &call)?.erase()
                }
            }
            MappingTrigger::Block { block } => {
                let block = EthereumBlockData::from(block.as_ref());
                if heap.api_version() >= Version::new(0, 0, 6) {
                    asc_new::<AscEthereumBlock_0_0_6, _, _>(heap, &block)?.erase()
                } else {
                    asc_new::<AscEthereumBlock, _, _>(heap, &block)?.erase()
                }
            }
        })
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

/// Ethereum block data.
#[derive(Clone, Debug, Default)]
pub struct EthereumBlockData {
    pub hash: H256,
    pub parent_hash: H256,
    pub uncles_hash: H256,
    pub author: H160,
    pub state_root: H256,
    pub transactions_root: H256,
    pub receipts_root: H256,
    pub number: U64,
    pub gas_used: U256,
    pub gas_limit: U256,
    pub timestamp: U256,
    pub difficulty: U256,
    pub total_difficulty: U256,
    pub size: Option<U256>,
    pub base_fee_per_gas: Option<U256>,
}

impl<'a, T> From<&'a Block<T>> for EthereumBlockData {
    fn from(block: &'a Block<T>) -> EthereumBlockData {
        EthereumBlockData {
            hash: block.hash.unwrap(),
            parent_hash: block.parent_hash,
            uncles_hash: block.uncles_hash,
            author: block.author,
            state_root: block.state_root,
            transactions_root: block.transactions_root,
            receipts_root: block.receipts_root,
            number: block.number.unwrap(),
            gas_used: block.gas_used,
            gas_limit: block.gas_limit,
            timestamp: block.timestamp,
            difficulty: block.difficulty,
            total_difficulty: block.total_difficulty.unwrap_or_default(),
            size: block.size,
            base_fee_per_gas: block.base_fee_per_gas,
        }
    }
}

/// Ethereum transaction data.
#[derive(Clone, Debug)]
pub struct EthereumTransactionData {
    pub hash: H256,
    pub index: U128,
    pub from: H160,
    pub to: Option<H160>,
    pub value: U256,
    pub gas_limit: U256,
    pub gas_price: U256,
    pub input: Bytes,
    pub nonce: U256,
}

impl From<&'_ Transaction> for EthereumTransactionData {
    fn from(tx: &Transaction) -> EthereumTransactionData {
        // unwrap: this is always `Some` for txns that have been mined
        //         (see https://github.com/tomusdrw/rust-web3/pull/407)
        let from = tx.from.unwrap();
        EthereumTransactionData {
            hash: tx.hash,
            index: tx.transaction_index.unwrap().as_u64().into(),
            from,
            to: tx.to,
            value: tx.value,
            gas_limit: tx.gas,
            gas_price: tx.gas_price,
            input: tx.input.0.clone(),
            nonce: tx.nonce.clone(),
        }
    }
}

/// An Ethereum event logged from a specific contract address and block.
#[derive(Debug)]
pub struct EthereumEventData {
    pub address: Address,
    pub log_index: U256,
    pub transaction_log_index: U256,
    pub log_type: Option<String>,
    pub block: EthereumBlockData,
    pub transaction: EthereumTransactionData,
    pub params: Vec<LogParam>,
}

impl Clone for EthereumEventData {
    fn clone(&self) -> Self {
        EthereumEventData {
            address: self.address,
            log_index: self.log_index,
            transaction_log_index: self.transaction_log_index,
            log_type: self.log_type.clone(),
            block: self.block.clone(),
            transaction: self.transaction.clone(),
            params: self
                .params
                .iter()
                .map(|log_param| LogParam {
                    name: log_param.name.clone(),
                    value: log_param.value.clone(),
                })
                .collect(),
        }
    }
}

/// An Ethereum call executed within a transaction within a block to a contract address.
#[derive(Debug)]
pub struct EthereumCallData {
    pub from: Address,
    pub to: Address,
    pub block: EthereumBlockData,
    pub transaction: EthereumTransactionData,
    pub inputs: Vec<LogParam>,
    pub outputs: Vec<LogParam>,
}

impl Clone for EthereumCallData {
    fn clone(&self) -> Self {
        EthereumCallData {
            to: self.to,
            from: self.from,
            block: self.block.clone(),
            transaction: self.transaction.clone(),
            inputs: self
                .inputs
                .iter()
                .map(|log_param| LogParam {
                    name: log_param.name.clone(),
                    value: log_param.value.clone(),
                })
                .collect(),
            outputs: self
                .outputs
                .iter()
                .map(|log_param| LogParam {
                    name: log_param.name.clone(),
                    value: log_param.value.clone(),
                })
                .collect(),
        }
    }
}
