use alloy::{
    network::{
        AnyHeader, AnyReceiptEnvelope, AnyRpcHeader, AnyTxEnvelope, ReceiptResponse,
        TransactionResponse,
    },
    primitives::{Address, B256, Bytes, U256},
    rpc::types::{
        Block, Header, Log, Transaction, TransactionReceipt,
        trace::parity::{Action, LocalizedTransactionTrace, TraceOutput},
    },
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{
    blockchain::{BlockPtr, BlockTime},
    prelude::BlockNumber,
};

use super::json_patch;

pub type AnyTransaction = Transaction<AnyTxEnvelope>;
pub type AnyBlock = Block<AnyTransaction, Header<AnyHeader>>;
/// Like alloy's `AnyTransactionReceipt` but without the `WithOtherFields` wrapper,
/// avoiding `#[serde(flatten)]` overhead during deserialization.
pub type AnyTransactionReceiptBare = TransactionReceipt<AnyReceiptEnvelope<Log>>;

#[allow(dead_code)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LightEthereumBlock(AnyBlock);

impl Default for LightEthereumBlock {
    fn default() -> Self {
        use alloy::rpc::types::BlockTransactions;

        Self(Block {
            header: AnyRpcHeader::default(),
            transactions: BlockTransactions::Full(vec![]),
            uncles: vec![],
            withdrawals: None,
        })
    }
}

impl LightEthereumBlock {
    pub fn new(block: AnyBlock) -> Self {
        Self(block)
    }

    pub fn hash(&self) -> B256 {
        self.0.header.hash
    }

    pub fn number_u64(&self) -> u64 {
        self.0.header.number
    }

    pub fn timestamp_u64(&self) -> u64 {
        self.0.header.timestamp
    }

    pub fn transactions(&self) -> Option<&[AnyTransaction]> {
        self.0.transactions.as_transactions()
    }

    pub fn inner(&self) -> &AnyBlock {
        &self.0
    }

    pub fn base_fee_per_gas(&self) -> Option<u64> {
        self.0.header.base_fee_per_gas
    }
}

pub trait LightEthereumBlockExt {
    fn number(&self) -> BlockNumber;
    fn transaction_for_log(&self, log: &Log) -> Option<AnyTransaction>;
    fn transaction_for_call(&self, call: &EthereumCall) -> Option<AnyTransaction>;
    fn parent_ptr(&self) -> Option<BlockPtr>;
    fn format(&self) -> String;
    fn block_ptr(&self) -> BlockPtr;
    fn timestamp(&self) -> BlockTime;
}

impl LightEthereumBlockExt for AnyBlock {
    fn number(&self) -> BlockNumber {
        BlockNumber::try_from(self.header.number).unwrap()
    }

    fn timestamp(&self) -> BlockTime {
        let time = self.header.timestamp;
        let time = i64::try_from(time).unwrap();
        BlockTime::since_epoch(time, 0)
    }

    fn transaction_for_log(&self, log: &Log) -> Option<AnyTransaction> {
        log.transaction_hash.and_then(|hash| {
            self.transactions
                .txns()
                .find(|tx| tx.tx_hash() == hash)
                .cloned()
        })
    }

    fn transaction_for_call(&self, call: &EthereumCall) -> Option<AnyTransaction> {
        call.transaction_hash.and_then(|hash| {
            self.transactions
                .txns()
                .find(|tx| tx.tx_hash() == hash)
                .cloned()
        })
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        match self.header.number {
            0 => None,
            n => {
                let number = i32::try_from(n - 1).unwrap();
                Some(BlockPtr::new(self.header.parent_hash.into(), number))
            }
        }
    }

    fn format(&self) -> String {
        format!("{} ({})", self.header.number, self.header.hash)
    }

    fn block_ptr(&self) -> BlockPtr {
        BlockPtr::from((self.header.hash, self.header.number))
    }
}

impl LightEthereumBlockExt for LightEthereumBlock {
    fn number(&self) -> BlockNumber {
        self.0.header.number.try_into().unwrap()
    }

    fn transaction_for_log(&self, log: &alloy::rpc::types::Log) -> Option<AnyTransaction> {
        self.0.transaction_for_log(log)
    }

    fn transaction_for_call(&self, call: &EthereumCall) -> Option<AnyTransaction> {
        self.0.transaction_for_call(call)
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        self.0.parent_ptr()
    }

    fn format(&self) -> String {
        self.0.format()
    }

    fn block_ptr(&self) -> BlockPtr {
        self.0.block_ptr()
    }

    fn timestamp(&self) -> BlockTime {
        self.0.timestamp()
    }
}

#[derive(Clone, Debug)]
pub struct EthereumBlockWithCalls {
    pub ethereum_block: EthereumBlock,
    /// The calls in this block; `None` means we haven't checked yet,
    /// `Some(vec![])` means that we checked and there were none
    pub calls: Option<Vec<EthereumCall>>,
}

impl EthereumBlockWithCalls {
    /// Given an `EthereumCall`, check within receipts if that transaction was successful.
    pub fn transaction_for_call_succeeded(&self, call: &EthereumCall) -> anyhow::Result<bool> {
        let call_transaction_hash = call.transaction_hash.ok_or(anyhow::anyhow!(
            "failed to find a transaction for this call"
        ))?;

        let receipt = self
            .ethereum_block
            .transaction_receipts
            .iter()
            .find(|txn| txn.transaction_hash == call_transaction_hash)
            .ok_or(anyhow::anyhow!(
                "failed to find the receipt for this transaction"
            ))?;

        Ok(receipt.status())
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct EthereumBlock {
    pub block: Arc<LightEthereumBlock>,
    pub transaction_receipts: Vec<Arc<AnyTransactionReceiptBare>>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EthereumCall {
    pub from: Address,
    pub to: Address,
    pub value: U256,
    pub gas_used: u64,
    pub input: Bytes,
    pub output: Bytes,
    pub block_number: BlockNumber,
    pub block_hash: B256,
    pub transaction_hash: Option<B256>,
    pub transaction_index: u64,
}

impl EthereumCall {
    pub fn try_from_trace(trace: &LocalizedTransactionTrace) -> Option<Self> {
        // The parity-ethereum tracing api returns traces for operations which had execution errors.
        // Filter errorful traces out, since call handlers should only run on successful CALLs.

        let tx_trace = &trace.trace;

        if tx_trace.error.is_some() {
            return None;
        }
        // We are only interested in traces from CALLs
        let call = match &tx_trace.action {
            // Contract to contract value transfers compile to the CALL opcode
            // and have no input. Call handlers are for triggering on explicit method calls right now.
            Action::Call(call) if call.input.0.len() >= 4 => call,
            _ => return None,
        };
        let (output, gas_used) = match &tx_trace.result {
            Some(TraceOutput::Call(result)) => (result.output.clone(), result.gas_used),
            _ => return None,
        };

        // The only traces without transactions are those from Parity block reward contracts, we
        // don't support triggering on that.
        let transaction_index = trace.transaction_position?;

        Some(EthereumCall {
            from: call.from,
            to: call.to,
            value: call.value,
            gas_used,
            input: call.input.clone(),
            output,
            block_number: BlockNumber::try_from(
                trace
                    .block_number
                    .expect("localized trace must have block_number"),
            )
            .unwrap(),
            block_hash: trace
                .block_hash
                .expect("localized trace must have block_hash"),
            transaction_hash: trace.transaction_hash,
            transaction_index,
        })
    }
}

impl<'a> From<&'a EthereumCall> for BlockPtr {
    fn from(call: &'a EthereumCall) -> BlockPtr {
        BlockPtr::from((call.block_hash, call.block_number))
    }
}

/// Typed cached block for Ethereum. Stores the deserialized block so that
/// repeated reads from the in-memory cache avoid `serde_json::from_value()`.
#[derive(Clone, Debug)]
pub enum CachedBlock {
    Full(EthereumBlock),
    Light(Arc<LightEthereumBlock>),
}

impl CachedBlock {
    pub fn light_block(&self) -> &LightEthereumBlock {
        match self {
            CachedBlock::Full(block) => &block.block,
            CachedBlock::Light(block) => block,
        }
    }

    pub fn to_light_block(&self) -> Arc<LightEthereumBlock> {
        match self {
            CachedBlock::Full(block) => block.block.clone(),
            CachedBlock::Light(block) => block.clone(),
        }
    }

    pub fn into_full_block(self) -> Option<EthereumBlock> {
        match self {
            CachedBlock::Full(block) => Some(block),
            CachedBlock::Light(_) => None,
        }
    }

    /// Serializes the block data directly. Note: the output shape differs
    /// from the store format (which wraps in a `{"block": ..}` envelope).
    pub fn to_json(&self) -> serde_json::Result<serde_json::Value> {
        match self {
            CachedBlock::Full(b) => serde_json::to_value(b),
            CachedBlock::Light(b) => serde_json::to_value(b),
        }
    }

    /// Deserializes a JSON block from the store into a typed `CachedBlock`.
    /// Returns `None` for shallow (header-only) blocks.
    ///
    /// Patches missing `type` fields in transactions/receipts before
    /// deserialization for compatibility with blocks cached by older
    /// graph-node versions.
    pub fn from_json(mut value: serde_json::Value) -> Option<Self> {
        // Shallow blocks have no body, only a null `data` field.
        if value.get("data") == Some(&serde_json::Value::Null) {
            return None;
        }

        let has_receipts = value
            .get("transaction_receipts")
            .is_some_and(|v| !v.is_null());

        if has_receipts {
            // Full block: patch in place then deserialize the entire value.
            if let Some(block) = value.get_mut("block") {
                json_patch::patch_block_transactions(block);
            }
            if let Some(receipts) = value.get_mut("transaction_receipts") {
                json_patch::patch_receipts(receipts);
            }
            serde_json::from_value(value).ok().map(CachedBlock::Full)
        } else {
            // Light block: extract the inner `block` field, patch, and deserialize.
            let mut inner = value
                .as_object_mut()
                .and_then(|obj| obj.remove("block"))
                .unwrap_or(value);
            json_patch::patch_block_transactions(&mut inner);
            serde_json::from_value(inner)
                .ok()
                .map(|b| CachedBlock::Light(Arc::new(b)))
        }
    }

    pub fn timestamp(&self) -> Option<u64> {
        Some(self.light_block().timestamp_u64())
    }

    pub fn parent_ptr(&self) -> Option<BlockPtr> {
        self.light_block().parent_ptr()
    }

    pub fn ptr(&self) -> BlockPtr {
        self.light_block().block_ptr()
    }
}
