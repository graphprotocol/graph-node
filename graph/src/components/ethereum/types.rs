use ethabi::LogParam;
use serde::{Deserialize, Serialize};
use stable_hash::prelude::*;
use stable_hash::utils::AsBytes;
use std::cmp::Ordering;
use std::fmt;
use web3::types::*;

use crate::prelude::{EntityKey, SubgraphDeploymentId, ToEntityKey};

pub type LightEthereumBlock = Block<Transaction>;

pub trait LightEthereumBlockExt {
    fn number(&self) -> u64;
    fn transaction_for_log(&self, log: &Log) -> Option<Transaction>;
    fn transaction_for_call(&self, call: &EthereumCall) -> Option<Transaction>;
    fn parent_ptr(&self) -> Option<EthereumBlockPointer>;
    fn format(&self) -> String;
    fn block_ptr(&self) -> EthereumBlockPointer;
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

    fn format(&self) -> String {
        format!(
            "{} ({})",
            self.number
                .map_or(String::from("none"), |number| format!("#{}", number)),
            self.hash
                .map_or(String::from("-"), |hash| format!("{:x}", hash))
        )
    }

    fn block_ptr(&self) -> EthereumBlockPointer {
        EthereumBlockPointer {
            hash: self.hash.unwrap(),
            number: self.number.unwrap().as_u64(),
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
        triggers.sort();

        EthereumBlockWithTriggers {
            ethereum_block,
            triggers,
        }
    }
}

#[derive(Clone, Debug)]
pub struct EthereumBlockWithCalls {
    pub ethereum_block: EthereumBlock,
    pub calls: Vec<EthereumCall>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct EthereumBlock {
    pub block: LightEthereumBlock,
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
            output,
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

/// A block hash and block number from a specific Ethereum block.
///
/// Maximum block number supported: 2^63 - 1
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EthereumBlockPointer {
    pub hash: H256,
    pub number: u64,
}

impl StableHash for EthereumBlockPointer {
    fn stable_hash<H: StableHasher>(&self, mut sequence_number: H::Seq, state: &mut H) {
        AsBytes(self.hash.as_bytes()).stable_hash(sequence_number.next_child(), state);
        self.number.stable_hash(sequence_number.next_child(), state);
    }
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

impl fmt::Display for EthereumBlockPointer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "#{} ({:x})", self.number, self.hash)
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

impl ToEntityKey for EthereumBlockPointer {
    fn to_entity_key(&self, subgraph: SubgraphDeploymentId) -> EntityKey {
        EntityKey {
            subgraph_id: subgraph,
            entity_type: "Block".into(),
            entity_id: format!("{:x}", self.hash),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{EthereumBlockPointer, EthereumBlockTriggerType, EthereumCall, EthereumTrigger};
    use web3::types::*;

    #[test]
    fn test_trigger_ordering() {
        let block1 = EthereumTrigger::Block(
            EthereumBlockPointer {
                number: 1,
                hash: H256::random(),
            },
            EthereumBlockTriggerType::Every,
        );

        let block2 = EthereumTrigger::Block(
            EthereumBlockPointer {
                number: 0,
                hash: H256::random(),
            },
            EthereumBlockTriggerType::WithCallTo(Address::random()),
        );

        let mut call1 = EthereumCall::default();
        call1.transaction_index = 1;
        let call1 = EthereumTrigger::Call(call1);

        let mut call2 = EthereumCall::default();
        call2.transaction_index = 2;
        let call2 = EthereumTrigger::Call(call2);

        let mut call3 = EthereumCall::default();
        call3.transaction_index = 3;
        let call3 = EthereumTrigger::Call(call3);

        // Call with the same tx index as call2
        let mut call4 = EthereumCall::default();
        call4.transaction_index = 2;
        let call4 = EthereumTrigger::Call(call4);

        fn create_log(tx_index: u64, log_index: u64) -> Log {
            Log {
                address: H160::default(),
                topics: vec![],
                data: Bytes::default(),
                block_hash: Some(H256::zero()),
                block_number: Some(U64::zero()),
                transaction_hash: Some(H256::zero()),
                transaction_index: Some(tx_index.into()),
                log_index: Some(log_index.into()),
                transaction_log_index: Some(log_index.into()),
                log_type: Some("".into()),
                removed: Some(false),
            }
        }

        // Event with transaction_index 1 and log_index 0;
        // should be the first element after sorting
        let log1 = EthereumTrigger::Log(create_log(1, 0));

        // Event with transaction_index 1 and log_index 1;
        // should be the second element after sorting
        let log2 = EthereumTrigger::Log(create_log(1, 1));

        // Event with transaction_index 2 and log_index 5;
        // should come after call1 and before call2 after sorting
        let log3 = EthereumTrigger::Log(create_log(2, 5));

        let mut triggers = vec![
            // Call triggers; these should be in the order 1, 2, 4, 3 after sorting
            call3.clone(),
            call1.clone(),
            call2.clone(),
            call4.clone(),
            // Block triggers; these should appear at the end after sorting
            // but with their order unchanged
            block2.clone(),
            block1.clone(),
            // Event triggers
            log3.clone(),
            log2.clone(),
            log1.clone(),
        ];
        triggers.sort();

        assert_eq!(
            triggers,
            vec![log1, log2, call1, log3, call2, call4, call3, block2, block1]
        );
    }
}
