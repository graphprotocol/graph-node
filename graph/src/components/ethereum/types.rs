use ethabi::LogParam;
use serde::{Deserialize, Serialize};
use std::{convert::TryFrom, sync::Arc};
use web3::types::{
    Action, Address, Block, Bytes, Log, Res, Trace, Transaction, TransactionReceipt, H160, H256,
    U128, U256, U64,
};

use crate::{
    blockchain::BlockPtr,
    cheap_clone::CheapClone,
    prelude::{BlockNumber, DeploymentHash, EntityKey, ToEntityKey},
};

pub type LightEthereumBlock = Block<Transaction>;

pub trait LightEthereumBlockExt {
    fn number(&self) -> BlockNumber;
    fn transaction_for_log(&self, log: &Log) -> Option<Transaction>;
    fn transaction_for_call(&self, call: &EthereumCall) -> Option<Transaction>;
    fn parent_ptr(&self) -> Option<BlockPtr>;
    fn format(&self) -> String;
    fn block_ptr(&self) -> BlockPtr;
}

impl LightEthereumBlockExt for LightEthereumBlock {
    fn number(&self) -> BlockNumber {
        BlockNumber::try_from(self.number.unwrap().as_u64()).unwrap()
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

    fn parent_ptr(&self) -> Option<BlockPtr> {
        match self.number() {
            0 => None,
            n => Some(BlockPtr::from((self.parent_hash, n - 1))),
        }
    }

    fn format(&self) -> String {
        format!(
            "{} ({})",
            self.number
                .map_or(String::from("none"), |number| format!("#{}", number)),
            self.hash
                .map_or(String::from("-"), |hash| format!("{:x}", hash))
        )
    }

    fn block_ptr(&self) -> BlockPtr {
        BlockPtr::from((self.hash.unwrap(), self.number.unwrap().as_u64()))
    }
}

/// This is used in `EthereumAdapter::triggers_in_block`, called when re-processing a block for
/// newly created data sources. This allows the re-processing to be reorg safe without having to
/// always fetch the full block data.
#[derive(Clone, Debug)]
pub enum BlockFinality {
    /// If a block is final, we only need the header and the triggers.
    Final(Arc<LightEthereumBlock>),

    // If a block may still be reorged, we need to work with more local data.
    NonFinal(EthereumBlockWithCalls),
}

impl BlockFinality {
    pub fn light_block(&self) -> Arc<LightEthereumBlock> {
        match self {
            BlockFinality::Final(block) => block.cheap_clone(),
            BlockFinality::NonFinal(block) => block.ethereum_block.block.cheap_clone(),
        }
    }

    pub fn number(&self) -> BlockNumber {
        match self {
            BlockFinality::Final(block) => block.number(),
            BlockFinality::NonFinal(block) => block.ethereum_block.block.number(),
        }
    }

    pub fn ptr(&self) -> BlockPtr {
        match self {
            BlockFinality::Final(block) => block.block_ptr(),
            BlockFinality::NonFinal(block) => block.ethereum_block.block.block_ptr(),
        }
    }

    pub fn parent_ptr(&self) -> Option<BlockPtr> {
        match self {
            BlockFinality::Final(block) => block.parent_ptr(),
            BlockFinality::NonFinal(block) => block.ethereum_block.block.parent_ptr(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct EthereumBlockWithCalls {
    pub ethereum_block: EthereumBlock,
    /// The calls in this block; `None` means we haven't checked yet,
    /// `Some(vec![])` means that we checked and there were none
    pub calls: Option<Vec<EthereumCall>>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct EthereumBlock {
    pub block: Arc<LightEthereumBlock>,
    pub transaction_receipts: Vec<TransactionReceipt>,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct EthereumCall {
    pub from: Address,
    pub to: Address,
    pub value: U256,
    pub gas_used: U256,
    pub input: Bytes,
    pub output: Bytes,
    pub block_number: BlockNumber,
    pub block_hash: H256,
    pub transaction_hash: Option<H256>,
    pub transaction_index: u64,
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
            output,
            block_number: trace.block_number as BlockNumber,
            block_hash: trace.block_hash,
            transaction_hash: trace.transaction_hash,
            transaction_index,
        })
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

impl From<&'_ Transaction> for EthereumTransactionData {
    fn from(tx: &Transaction) -> EthereumTransactionData {
        EthereumTransactionData {
            hash: tx.hash,
            index: tx.transaction_index.unwrap().as_u64().into(),
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

impl From<EthereumBlock> for BlockPtr {
    fn from(b: EthereumBlock) -> BlockPtr {
        BlockPtr::from((b.block.hash.unwrap(), b.block.number.unwrap().as_u64()))
    }
}

impl<'a> From<&'a EthereumBlock> for BlockPtr {
    fn from(b: &'a EthereumBlock) -> BlockPtr {
        BlockPtr::from((b.block.hash.unwrap(), b.block.number.unwrap().as_u64()))
    }
}

impl<'a> From<&'a EthereumCall> for BlockPtr {
    fn from(call: &'a EthereumCall) -> BlockPtr {
        BlockPtr::from((call.block_hash, call.block_number))
    }
}

impl<'a> From<&'a BlockFinality> for BlockPtr {
    fn from(block: &'a BlockFinality) -> BlockPtr {
        match block {
            BlockFinality::Final(b) => BlockPtr::from(&**b),
            BlockFinality::NonFinal(b) => BlockPtr::from(&b.ethereum_block),
        }
    }
}

impl ToEntityKey for BlockPtr {
    fn to_entity_key(&self, subgraph: DeploymentHash) -> EntityKey {
        EntityKey::data(subgraph, "Block".into(), self.hash_hex())
    }
}
