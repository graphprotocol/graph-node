use graph::abi;
use graph::blockchain::MappingTriggerTrait;
use graph::blockchain::TriggerData;
use graph::data::subgraph::API_VERSION_0_0_2;
use graph::data::subgraph::API_VERSION_0_0_6;
use graph::data::subgraph::API_VERSION_0_0_7;
use graph::data_source::common::DeclaredCall;
use graph::prelude::web3::types::Address;
use graph::prelude::web3::types::Block;
use graph::prelude::web3::types::Log;
use graph::prelude::web3::types::Transaction;
use graph::prelude::web3::types::TransactionReceipt;
use graph::prelude::web3::types::H160;
use graph::prelude::web3::types::H256;
use graph::prelude::web3::types::U128;
use graph::prelude::web3::types::U256;
use graph::prelude::web3::types::U64;
use graph::prelude::BlockNumber;
use graph::prelude::BlockPtr;
use graph::prelude::{CheapClone, EthereumCall};
use graph::runtime::asc_new;
use graph::runtime::gas::GasCounter;
use graph::runtime::AscHeap;
use graph::runtime::AscPtr;
use graph::runtime::HostExportError;
use graph::semver::Version;
use graph_runtime_wasm::module::ToAscPtr;
use std::{cmp::Ordering, sync::Arc};

use crate::runtime::abi::AscEthereumBlock;
use crate::runtime::abi::AscEthereumBlock_0_0_6;
use crate::runtime::abi::AscEthereumCall;
use crate::runtime::abi::AscEthereumCall_0_0_3;
use crate::runtime::abi::AscEthereumEvent;
use crate::runtime::abi::AscEthereumEvent_0_0_7;
use crate::runtime::abi::AscEthereumTransaction_0_0_1;
use crate::runtime::abi::AscEthereumTransaction_0_0_2;
use crate::runtime::abi::AscEthereumTransaction_0_0_6;

// ETHDEP: This should be defined in only one place.
type LightEthereumBlock = Block<Transaction>;

static U256_DEFAULT: U256 = U256::zero();

pub enum MappingTrigger {
    Log {
        block: Arc<LightEthereumBlock>,
        transaction: Arc<Transaction>,
        log: Arc<Log>,
        params: Vec<abi::DynSolParam>,
        receipt: Option<Arc<TransactionReceipt>>,
        calls: Vec<DeclaredCall>,
    },
    Call {
        block: Arc<LightEthereumBlock>,
        transaction: Arc<Transaction>,
        call: Arc<EthereumCall>,
        inputs: Vec<abi::DynSolParam>,
        outputs: Vec<abi::DynSolParam>,
    },
    Block {
        block: Arc<LightEthereumBlock>,
    },
}

impl MappingTriggerTrait for MappingTrigger {
    fn error_context(&self) -> String {
        let transaction_id = match self {
            MappingTrigger::Log { log, .. } => log.transaction_hash,
            MappingTrigger::Call { call, .. } => call.transaction_hash,
            MappingTrigger::Block { .. } => None,
        };

        match transaction_id {
            Some(tx_hash) => format!("transaction {:x}", tx_hash),
            None => String::new(),
        }
    }
}

// Logging the block is too verbose, so this strips the block from the trigger for Debug.
impl std::fmt::Debug for MappingTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[derive(Debug)]
        enum MappingTriggerWithoutBlock {
            Log {
                _transaction: Arc<Transaction>,
                _log: Arc<Log>,
                _params: Vec<abi::DynSolParam>,
            },
            Call {
                _transaction: Arc<Transaction>,
                _call: Arc<EthereumCall>,
                _inputs: Vec<abi::DynSolParam>,
                _outputs: Vec<abi::DynSolParam>,
            },
            Block,
        }

        let trigger_without_block = match self {
            MappingTrigger::Log {
                block: _,
                transaction,
                log,
                params,
                receipt: _,
                calls: _,
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

impl ToAscPtr for MappingTrigger {
    fn to_asc_ptr<H: AscHeap>(
        self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscPtr<()>, HostExportError> {
        Ok(match self {
            MappingTrigger::Log {
                block,
                transaction,
                log,
                params,
                receipt,
                calls: _,
            } => {
                let api_version = heap.api_version();
                let ethereum_event_data = EthereumEventData::new(
                    block.as_ref(),
                    transaction.as_ref(),
                    log.as_ref(),
                    &params,
                );
                if api_version >= API_VERSION_0_0_7 {
                    asc_new::<
                        AscEthereumEvent_0_0_7<
                            AscEthereumTransaction_0_0_6,
                            AscEthereumBlock_0_0_6,
                        >,
                        _,
                        _,
                    >(heap, &(ethereum_event_data, receipt.as_deref()), gas)?
                    .erase()
                } else if api_version >= API_VERSION_0_0_6 {
                    asc_new::<
                        AscEthereumEvent<AscEthereumTransaction_0_0_6, AscEthereumBlock_0_0_6>,
                        _,
                        _,
                    >(heap, &ethereum_event_data, gas)?
                    .erase()
                } else if api_version >= API_VERSION_0_0_2 {
                    asc_new::<
                            AscEthereumEvent<AscEthereumTransaction_0_0_2, AscEthereumBlock>,
                            _,
                            _,
                        >(heap, &ethereum_event_data, gas)?
                        .erase()
                } else {
                    asc_new::<
                        AscEthereumEvent<AscEthereumTransaction_0_0_1, AscEthereumBlock>,
                        _,
                        _,
                    >(heap, &ethereum_event_data, gas)?
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
                let call = EthereumCallData::new(&block, &transaction, &call, &inputs, &outputs);
                if heap.api_version() >= Version::new(0, 0, 6) {
                    asc_new::<
                        AscEthereumCall_0_0_3<AscEthereumTransaction_0_0_6, AscEthereumBlock_0_0_6>,
                        _,
                        _,
                    >(heap, &call, gas)?
                    .erase()
                } else if heap.api_version() >= Version::new(0, 0, 3) {
                    asc_new::<
                        AscEthereumCall_0_0_3<AscEthereumTransaction_0_0_2, AscEthereumBlock>,
                        _,
                        _,
                    >(heap, &call, gas)?
                    .erase()
                } else {
                    asc_new::<AscEthereumCall, _, _>(heap, &call, gas)?.erase()
                }
            }
            MappingTrigger::Block { block } => {
                let block = EthereumBlockData::from(block.as_ref());
                if heap.api_version() >= Version::new(0, 0, 6) {
                    asc_new::<AscEthereumBlock_0_0_6, _, _>(heap, &block, gas)?.erase()
                } else {
                    asc_new::<AscEthereumBlock, _, _>(heap, &block, gas)?.erase()
                }
            }
        })
    }
}

#[derive(Clone, Debug)]
pub enum LogRef {
    FullLog(Arc<Log>, Option<Arc<TransactionReceipt>>),
    LogPosition(usize, Arc<TransactionReceipt>),
}

impl LogRef {
    pub fn log(&self) -> &Log {
        match self {
            LogRef::FullLog(log, _) => log.as_ref(),
            LogRef::LogPosition(index, receipt) => receipt.logs.get(*index).unwrap(),
        }
    }

    pub fn receipt(&self) -> Option<&Arc<TransactionReceipt>> {
        match self {
            LogRef::FullLog(_, receipt) => receipt.as_ref(),
            LogRef::LogPosition(_, receipt) => Some(receipt),
        }
    }

    pub fn log_index(&self) -> Option<U256> {
        self.log().log_index
    }

    pub fn transaction_index(&self) -> Option<U64> {
        self.log().transaction_index
    }

    fn transaction_hash(&self) -> Option<H256> {
        self.log().transaction_hash
    }

    pub fn block_hash(&self) -> Option<H256> {
        self.log().block_hash
    }

    pub fn block_number(&self) -> Option<U64> {
        self.log().block_number
    }

    pub fn address(&self) -> &H160 {
        &self.log().address
    }
}

#[derive(Clone, Debug)]
pub enum EthereumTrigger {
    Block(BlockPtr, EthereumBlockTriggerType),
    Call(Arc<EthereumCall>),
    Log(LogRef),
}

impl PartialEq for EthereumTrigger {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Block(a_ptr, a_kind), Self::Block(b_ptr, b_kind)) => {
                a_ptr == b_ptr && a_kind == b_kind
            }

            (Self::Call(a), Self::Call(b)) => a == b,

            (Self::Log(a), Self::Log(b)) => {
                a.transaction_hash() == b.transaction_hash() && a.log_index() == b.log_index()
            }
            _ => false,
        }
    }
}

impl Eq for EthereumTrigger {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EthereumBlockTriggerType {
    Start,
    End,
    WithCallTo(Address),
}

impl EthereumTrigger {
    pub fn block_number(&self) -> BlockNumber {
        match self {
            EthereumTrigger::Block(block_ptr, _) => block_ptr.number,
            EthereumTrigger::Call(call) => call.block_number,
            EthereumTrigger::Log(log_ref) => {
                i32::try_from(log_ref.block_number().unwrap().as_u64()).unwrap()
            }
        }
    }

    pub fn block_hash(&self) -> H256 {
        match self {
            EthereumTrigger::Block(block_ptr, _) => block_ptr.hash_as_h256(),
            EthereumTrigger::Call(call) => call.block_hash,
            EthereumTrigger::Log(log_ref) => log_ref.block_hash().unwrap(),
        }
    }

    /// `None` means the trigger matches any address.
    pub fn address(&self) -> Option<&Address> {
        match self {
            EthereumTrigger::Block(_, EthereumBlockTriggerType::WithCallTo(address)) => {
                Some(address)
            }
            EthereumTrigger::Call(call) => Some(&call.to),
            EthereumTrigger::Log(log_ref) => Some(&log_ref.address()),
            // Unfiltered block triggers match any data source address.
            EthereumTrigger::Block(_, EthereumBlockTriggerType::End) => None,
            EthereumTrigger::Block(_, EthereumBlockTriggerType::Start) => None,
        }
    }
}

impl Ord for EthereumTrigger {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // Block triggers with `EthereumBlockTriggerType::Start` always come
            (Self::Block(_, EthereumBlockTriggerType::Start), _) => Ordering::Less,
            (_, Self::Block(_, EthereumBlockTriggerType::Start)) => Ordering::Greater,

            // Keep the order when comparing two block triggers
            (Self::Block(..), Self::Block(..)) => Ordering::Equal,

            // Block triggers with `EthereumBlockTriggerType::End` always come last
            (Self::Block(..), _) => Ordering::Greater,
            (_, Self::Block(..)) => Ordering::Less,

            // Calls are ordered by their tx indexes
            (Self::Call(a), Self::Call(b)) => a.transaction_index.cmp(&b.transaction_index),

            // Events are ordered by their log index
            (Self::Log(a), Self::Log(b)) => a.log_index().cmp(&b.log_index()),

            // Calls vs. events are logged by their tx index;
            // if they are from the same transaction, events come first
            (Self::Call(a), Self::Log(b))
                if a.transaction_index == b.transaction_index().unwrap().as_u64() =>
            {
                Ordering::Greater
            }
            (Self::Log(a), Self::Call(b))
                if a.transaction_index().unwrap().as_u64() == b.transaction_index =>
            {
                Ordering::Less
            }
            (Self::Call(a), Self::Log(b)) => a
                .transaction_index
                .cmp(&b.transaction_index().unwrap().as_u64()),
            (Self::Log(a), Self::Call(b)) => a
                .transaction_index()
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
            EthereumTrigger::Log(log) => log.transaction_hash(),
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

    fn address_match(&self) -> Option<&[u8]> {
        self.address().map(|address| address.as_bytes())
    }
}

/// Ethereum block data.
#[derive(Clone, Debug)]
pub struct EthereumBlockData<'a> {
    block: &'a Block<Transaction>,
}

impl<'a> From<&'a Block<Transaction>> for EthereumBlockData<'a> {
    fn from(block: &'a Block<Transaction>) -> EthereumBlockData<'a> {
        EthereumBlockData { block }
    }
}

impl<'a> EthereumBlockData<'a> {
    pub fn hash(&self) -> &H256 {
        self.block.hash.as_ref().unwrap()
    }

    pub fn parent_hash(&self) -> &H256 {
        &self.block.parent_hash
    }

    pub fn uncles_hash(&self) -> &H256 {
        &self.block.uncles_hash
    }

    pub fn author(&self) -> &H160 {
        &self.block.author
    }

    pub fn state_root(&self) -> &H256 {
        &self.block.state_root
    }

    pub fn transactions_root(&self) -> &H256 {
        &self.block.transactions_root
    }

    pub fn receipts_root(&self) -> &H256 {
        &self.block.receipts_root
    }

    pub fn number(&self) -> U64 {
        self.block.number.unwrap()
    }

    pub fn gas_used(&self) -> &U256 {
        &self.block.gas_used
    }

    pub fn gas_limit(&self) -> &U256 {
        &self.block.gas_limit
    }

    pub fn timestamp(&self) -> &U256 {
        &self.block.timestamp
    }

    pub fn difficulty(&self) -> &U256 {
        &self.block.difficulty
    }

    pub fn total_difficulty(&self) -> &U256 {
        self.block
            .total_difficulty
            .as_ref()
            .unwrap_or(&U256_DEFAULT)
    }

    pub fn size(&self) -> &Option<U256> {
        &self.block.size
    }

    pub fn base_fee_per_gas(&self) -> &Option<U256> {
        &self.block.base_fee_per_gas
    }
}

/// Ethereum transaction data.
#[derive(Clone, Debug)]
pub struct EthereumTransactionData<'a> {
    tx: &'a Transaction,
}

impl<'a> EthereumTransactionData<'a> {
    // We don't implement `From` because it causes confusion with the `from`
    // accessor method
    fn new(tx: &'a Transaction) -> EthereumTransactionData<'a> {
        EthereumTransactionData { tx }
    }

    pub fn hash(&self) -> &H256 {
        &self.tx.hash
    }

    pub fn index(&self) -> U128 {
        self.tx.transaction_index.unwrap().as_u64().into()
    }

    pub fn from(&self) -> &H160 {
        // unwrap: this is always `Some` for txns that have been mined
        //         (see https://github.com/tomusdrw/rust-web3/pull/407)
        self.tx.from.as_ref().unwrap()
    }

    pub fn to(&self) -> &Option<H160> {
        &self.tx.to
    }

    pub fn value(&self) -> &U256 {
        &self.tx.value
    }

    pub fn gas_limit(&self) -> &U256 {
        &self.tx.gas
    }

    pub fn gas_price(&self) -> &U256 {
        // EIP-1559 made this optional.
        self.tx.gas_price.as_ref().unwrap_or(&U256_DEFAULT)
    }

    pub fn input(&self) -> &[u8] {
        &self.tx.input.0
    }

    pub fn nonce(&self) -> &U256 {
        &self.tx.nonce
    }
}

/// An Ethereum event logged from a specific contract address and block.
#[derive(Debug, Clone)]
pub struct EthereumEventData<'a> {
    pub block: EthereumBlockData<'a>,
    pub transaction: EthereumTransactionData<'a>,
    pub params: &'a [abi::DynSolParam],
    log: &'a Log,
}

impl<'a> EthereumEventData<'a> {
    pub fn new(
        block: &'a Block<Transaction>,
        tx: &'a Transaction,
        log: &'a Log,
        params: &'a [abi::DynSolParam],
    ) -> Self {
        EthereumEventData {
            block: EthereumBlockData::from(block),
            transaction: EthereumTransactionData::new(tx),
            log,
            params,
        }
    }

    pub fn address(&self) -> &Address {
        &self.log.address
    }

    pub fn log_index(&self) -> &U256 {
        self.log.log_index.as_ref().unwrap_or(&U256_DEFAULT)
    }

    pub fn transaction_log_index(&self) -> &U256 {
        self.log
            .transaction_log_index
            .as_ref()
            .unwrap_or(&U256_DEFAULT)
    }

    pub fn log_type(&self) -> &Option<String> {
        &self.log.log_type
    }
}

/// An Ethereum call executed within a transaction within a block to a contract address.
#[derive(Debug, Clone)]
pub struct EthereumCallData<'a> {
    pub block: EthereumBlockData<'a>,
    pub transaction: EthereumTransactionData<'a>,
    pub inputs: &'a [abi::DynSolParam],
    pub outputs: &'a [abi::DynSolParam],
    call: &'a EthereumCall,
}

impl<'a> EthereumCallData<'a> {
    fn new(
        block: &'a Block<Transaction>,
        transaction: &'a Transaction,
        call: &'a EthereumCall,
        inputs: &'a [abi::DynSolParam],
        outputs: &'a [abi::DynSolParam],
    ) -> EthereumCallData<'a> {
        EthereumCallData {
            block: EthereumBlockData::from(block),
            transaction: EthereumTransactionData::new(transaction),
            inputs,
            outputs,
            call,
        }
    }

    pub fn from(&self) -> &Address {
        &self.call.from
    }

    pub fn to(&self) -> &Address {
        &self.call.to
    }
}
