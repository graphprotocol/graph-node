use ethabi::LogParam;
use serde::{Deserialize, Serialize};
use slog::{o, SendSyncRefUnwindSafeKV};
use std::sync::Arc;
use std::{cmp::Ordering, convert::TryFrom};
use strum_macros::AsStaticStr;
use web3::types::{
    Action, Address, Block, Bytes, Log, Res, Trace, Transaction, TransactionReceipt, H160, H256,
    U128, U256, U64,
};

use crate::{
    blockchain::BlockPtr,
    cheap_clone::CheapClone,
    prelude::{
        BlockNumber, DeploymentHash, EntityKey, MappingBlockHandler, MappingCallHandler,
        MappingEventHandler, ToEntityKey,
    },
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
    pub block_number: BlockNumber,
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
            block_number: trace.block_number as BlockNumber,
            block_hash: trace.block_hash,
            transaction_hash: trace.transaction_hash,
            transaction_index,
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

#[derive(AsStaticStr)]
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
    pub fn handler_name(&self) -> &str {
        match self {
            MappingTrigger::Log { handler, .. } => &handler.handler,
            MappingTrigger::Call { handler, .. } => &handler.handler,
            MappingTrigger::Block { handler, .. } => &handler.handler,
        }
    }

    pub fn logging_extras(&self) -> impl SendSyncRefUnwindSafeKV {
        match self {
            MappingTrigger::Log { handler, log, .. } => o! {
                "signature" => handler.event.to_string(),
                "address" => format!("{}", &log.address),
            },
            MappingTrigger::Call { handler, call, .. } => o! {
                "function" => handler.function.to_string(),
                "to" => format!("{}", &call.to),
            },
            MappingTrigger::Block { .. } => o! { "" => String::new(), "" => String::new() },
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
            BlockFinality::Final(b) => b.into(),
            BlockFinality::NonFinal(b) => BlockPtr::from(&b.ethereum_block),
        }
    }
}

impl ToEntityKey for BlockPtr {
    fn to_entity_key(&self, subgraph: DeploymentHash) -> EntityKey {
        EntityKey::data(subgraph, "Block".into(), self.hash_hex())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::{BlockPtr, EthereumBlockTriggerType, EthereumCall, EthereumTrigger};
    use web3::types::*;

    #[test]
    fn test_trigger_ordering() {
        let block1 = EthereumTrigger::Block(
            BlockPtr::from((H256::random(), 1u64)),
            EthereumBlockTriggerType::Every,
        );

        let block2 = EthereumTrigger::Block(
            BlockPtr::from((H256::random(), 0u64)),
            EthereumBlockTriggerType::WithCallTo(Address::random()),
        );

        let mut call1 = EthereumCall::default();
        call1.transaction_index = 1;
        let call1 = EthereumTrigger::Call(Arc::new(call1));

        let mut call2 = EthereumCall::default();
        call2.transaction_index = 2;
        let call2 = EthereumTrigger::Call(Arc::new(call2));

        let mut call3 = EthereumCall::default();
        call3.transaction_index = 3;
        let call3 = EthereumTrigger::Call(Arc::new(call3));

        // Call with the same tx index as call2
        let mut call4 = EthereumCall::default();
        call4.transaction_index = 2;
        let call4 = EthereumTrigger::Call(Arc::new(call4));

        fn create_log(tx_index: u64, log_index: u64) -> Arc<Log> {
            Arc::new(Log {
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
            })
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
