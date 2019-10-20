use ethabi::LogParam;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use web3::types::*;

pub type LightEthereumBlock = Block<Transaction>;

pub trait LightEthereumBlockExt {
    fn number(&self) -> u64;
    fn transaction_for_log(&self, log: &Log) -> Option<Transaction>;
    fn transaction_for_call(&self, call: &EthereumCall) -> Option<Transaction>;
    fn parent_ptr(&self) -> Option<EthereumBlockPointer>;
}

impl LightEthereumBlockExt for LightEthereumBlock {
    fn number(&self) -> u64 {
        self.number.unwrap().as_u64()
    }

    fn transaction_for_log(&self, log: &Log) -> Option<Transaction> {
        log.transaction_hash
            .and_then(|hash| self.transactions.iter().find(|tx| tx.hash == hash))
            .cloned()
    }

    fn transaction_for_call(&self, call: &EthereumCall) -> Option<Transaction> {
        call.transaction_hash
            .and_then(|hash| self.transactions.iter().find(|tx| tx.hash == hash))
            .cloned()
    }

    fn parent_ptr(&self) -> Option<EthereumBlockPointer> {
        match self.number() {
            0 => None,
            n => Some(EthereumBlockPointer {
                hash: self.parent_hash,
                number: n - 1,
            }),
        }
    }
}

/// This is used in `EthereumAdapter::triggers_in_block`, called when re-processing a block for
/// newly created data sources. This allows the re-processing to be reorg safe without having to
/// always fetch the full block data.
#[derive(Clone, Debug)]
pub enum BlockFinality {
    /// If a block is final, we only need the header and the triggers.
    Final(LightEthereumBlock),

    // If a block may still be reorged, we need to work with more local data.
    NonFinal(EthereumBlockWithCalls),
}

impl BlockFinality {
    pub fn light_block(&self) -> LightEthereumBlock {
        match self {
            BlockFinality::Final(block) => block.clone(),
            BlockFinality::NonFinal(block) => block.ethereum_block.block.clone(),
        }
    }

    pub fn number(&self) -> u64 {
        match self {
            BlockFinality::Final(block) => block.number(),
            BlockFinality::NonFinal(block) => block.ethereum_block.block.number(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct EthereumBlockWithTriggers {
    pub ethereum_block: BlockFinality,
    pub triggers: Vec<EthereumTrigger>,
}

impl EthereumBlockWithTriggers {
    pub fn new(mut triggers: Vec<EthereumTrigger>, ethereum_block: BlockFinality) -> Self {
        // Sort the triggers
        triggers.sort_by(|a, b| {
            let a_tx_index = a.transaction_index();
            let b_tx_index = b.transaction_index();
            if a_tx_index.is_none() && b_tx_index.is_none() {
                return Ordering::Equal;
            }
            if a_tx_index.is_none() {
                return Ordering::Greater;
            }
            if b_tx_index.is_none() {
                return Ordering::Less;
            }
            a_tx_index.unwrap().cmp(&b_tx_index.unwrap())
        });

        EthereumBlockWithTriggers {
            ethereum_block,
            triggers,
        }
    }
}

#[derive(Clone, Debug)]
pub struct EthereumBlockWithCalls {
    pub ethereum_block: EthereumBlock,
    pub calls: Option<Vec<EthereumCall>>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct EthereumBlock {
    pub block: LightEthereumBlock,
    pub transaction_receipts: Vec<TransactionReceipt>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EthereumCall {
    pub from: Address,
    pub to: Address,
    pub value: U256,
    pub gas_used: U256,
    pub input: Bytes,
    pub output: Bytes,
    pub block_number: u64,
    pub block_hash: H256,
    pub transaction_hash: Option<H256>,
    transaction_index: u64,
}

impl EthereumCall {
    pub fn try_from_trace(trace: &Trace) -> Option<Self> {
        // The parity-ethereum tracing api returns traces for operations which had execution errors.
        // Filter errorful traces out, since call handlers should only run on successful CALLs.
        if trace.error.is_some() {
            return None;
        }
        // We are only interested in traces from CALLs
        let call = match &trace.action {
            // Contract to contract value transfers compile to the CALL opcode
            // and have no input. Call handlers are for triggering on explicit method calls right now.
            Action::Call(call) if call.input.0.len() >= 4 => call,
            _ => return None,
        };
        let (output, gas_used) = match &trace.result {
            Some(Res::Call(result)) => (result.output.clone(), result.gas_used),
            _ => return None,
        };

        // The only traces without transactions are those from Parity block reward contracts, we
        // don't support triggering on that.
        let transaction_index = trace.transaction_position? as u64;

        Some(EthereumCall {
            from: call.from,
            to: call.to,
            value: call.value,
            gas_used,
            input: call.input.clone(),
            output: output,
            block_number: trace.block_number,
            block_hash: trace.block_hash,
            transaction_hash: trace.transaction_hash,
            transaction_index,
        })
    }
}

#[derive(Clone, Debug)]
pub enum EthereumTrigger {
    Block(EthereumBlockPointer, EthereumBlockTriggerType),
    Call(EthereumCall),
    Log(Log),
}

#[derive(Clone, Debug)]
pub enum EthereumBlockTriggerType {
    Every,
    WithCallTo(Address),
}

impl EthereumTrigger {
    fn transaction_index(&self) -> Option<u64> {
        match self {
            // We only handle logs that are in a block and therefore have a `transaction_index`.
            EthereumTrigger::Log(log) => Some(log.transaction_index.unwrap().as_u64()),
            EthereumTrigger::Call(call) => Some(call.transaction_index),
            EthereumTrigger::Block(_, _) => None,
        }
    }

    pub fn block_number(&self) -> u64 {
        match self {
            EthereumTrigger::Block(block_ptr, _) => block_ptr.number,
            EthereumTrigger::Call(call) => call.block_number,
            EthereumTrigger::Log(log) => log.block_number.unwrap().as_u64(),
        }
    }

    pub fn block_hash(&self) -> H256 {
        match self {
            EthereumTrigger::Block(block_ptr, _) => block_ptr.hash,
            EthereumTrigger::Call(call) => call.block_hash,
            EthereumTrigger::Log(log) => log.block_hash.unwrap(),
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
    pub number: U128,
    pub gas_used: U256,
    pub gas_limit: U256,
    pub timestamp: U256,
    pub difficulty: U256,
    pub total_difficulty: U256,
    pub size: Option<U256>,
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
            total_difficulty: block.total_difficulty,
            size: block.size,
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
    pub gas_used: U256,
    pub gas_price: U256,
    pub input: Bytes,
}

impl<'a> From<&'a Transaction> for EthereumTransactionData {
    fn from(tx: &'a Transaction) -> EthereumTransactionData {
        EthereumTransactionData {
            hash: tx.hash,
            index: tx.transaction_index.unwrap(),
            from: tx.from,
            to: tx.to,
            value: tx.value,
            gas_used: tx.gas,
            gas_price: tx.gas_price,
            input: tx.input.clone(),
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

/// A block hash and block number from a specific Ethereum block.
///
/// Maximum block number supported: 2^63 - 1
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EthereumBlockPointer {
    pub hash: H256,
    pub number: u64,
}

impl EthereumBlockPointer {
    /// Encodes the block hash into a hexadecimal string **without** a "0x" prefix.
    /// Hashes are stored in the database in this format.
    ///
    /// This mainly exists because of backwards incompatible changes in how the Web3 library
    /// implements `H256::to_string`.
    pub fn hash_hex(&self) -> String {
        format!("{:x}", self.hash)
    }
}

impl<T> From<Block<T>> for EthereumBlockPointer {
    fn from(b: Block<T>) -> EthereumBlockPointer {
        EthereumBlockPointer {
            hash: b.hash.unwrap(),
            number: b.number.unwrap().as_u64(),
        }
    }
}

impl<'a, T> From<&'a Block<T>> for EthereumBlockPointer {
    fn from(b: &'a Block<T>) -> EthereumBlockPointer {
        EthereumBlockPointer {
            hash: b.hash.unwrap(),
            number: b.number.unwrap().as_u64(),
        }
    }
}

impl From<EthereumBlock> for EthereumBlockPointer {
    fn from(b: EthereumBlock) -> EthereumBlockPointer {
        EthereumBlockPointer {
            hash: b.block.hash.unwrap(),
            number: b.block.number.unwrap().as_u64(),
        }
    }
}

impl<'a> From<&'a EthereumBlock> for EthereumBlockPointer {
    fn from(b: &'a EthereumBlock) -> EthereumBlockPointer {
        EthereumBlockPointer {
            hash: b.block.hash.unwrap(),
            number: b.block.number.unwrap().as_u64(),
        }
    }
}

impl From<(H256, u64)> for EthereumBlockPointer {
    fn from((hash, number): (H256, u64)) -> EthereumBlockPointer {
        if number >= (1 << 63) {
            panic!("block number out of range: {}", number);
        }

        EthereumBlockPointer { hash, number }
    }
}

impl From<(H256, i64)> for EthereumBlockPointer {
    fn from((hash, number): (H256, i64)) -> EthereumBlockPointer {
        if number < 0 {
            panic!("block number out of range: {}", number);
        }

        EthereumBlockPointer {
            hash,
            number: number as u64,
        }
    }
}

impl<'a> From<&'a EthereumCall> for EthereumBlockPointer {
    fn from(call: &'a EthereumCall) -> EthereumBlockPointer {
        EthereumBlockPointer {
            hash: call.block_hash,
            number: call.block_number,
        }
    }
}

impl<'a> From<&'a BlockFinality> for EthereumBlockPointer {
    fn from(block: &'a BlockFinality) -> EthereumBlockPointer {
        match block {
            BlockFinality::Final(b) => b.into(),
            BlockFinality::NonFinal(b) => EthereumBlockPointer::from(&b.ethereum_block),
        }
    }
}

impl From<EthereumBlockPointer> for H256 {
    fn from(ptr: EthereumBlockPointer) -> Self {
        ptr.hash
    }
}

impl From<EthereumBlockPointer> for u64 {
    fn from(ptr: EthereumBlockPointer) -> Self {
        ptr.number
    }
}
