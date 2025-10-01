use alloy::{
    primitives::{Address, Bytes, B256, U256, U64},
    rpc::types::{
        trace::parity::{Action, LocalizedTransactionTrace, TraceOutput},
        Log, Transaction,
    },
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{
    blockchain::{BlockPtr, BlockTime},
    prelude::{alloy::rpc::types::Block as AlloyBlock, BlockNumber},
};

#[allow(dead_code)]
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct BlockWrapper(AlloyBlock);

impl BlockWrapper {
    pub fn new(block: AlloyBlock) -> Self {
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

    pub fn transactions(&self) -> Option<&[Transaction]> {
        self.0.transactions.as_transactions()
    }

    pub fn inner(&self) -> &AlloyBlock {
        &self.0
    }
}

pub type LightEthereumBlock = BlockWrapper;

pub trait LightEthereumBlockExt {
    fn number(&self) -> BlockNumber;
    fn transaction_for_log(&self, log: &Log) -> Option<Transaction>;
    fn transaction_for_call(&self, call: &EthereumCall) -> Option<Transaction>;
    fn parent_ptr(&self) -> Option<BlockPtr>;
    fn format(&self) -> String;
    fn block_ptr(&self) -> BlockPtr;
    fn timestamp(&self) -> BlockTime;
}

impl LightEthereumBlockExt for AlloyBlock {
    fn number(&self) -> BlockNumber {
        self.header.number as BlockNumber
    }

    fn timestamp(&self) -> BlockTime {
        let time = self.header.timestamp;
        let time = i64::try_from(time).unwrap();
        BlockTime::since_epoch(time, 0)
    }

    fn transaction_for_log(&self, log: &Log) -> Option<Transaction> {
        log.transaction_hash.and_then(|hash| {
            self.transactions
                .txns()
                .find(|tx| tx.inner.hash() == &hash)
                .cloned()
        })
    }

    fn transaction_for_call(&self, call: &EthereumCall) -> Option<Transaction> {
        call.transaction_hash.and_then(|hash| {
            self.transactions
                .txns()
                .find(|tx| tx.inner.hash() == &hash)
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
        BlockPtr::new(self.header.hash.into(), self.header.number as i32)
    }
}

impl LightEthereumBlockExt for LightEthereumBlock {
    fn number(&self) -> BlockNumber {
        self.0.header.number.try_into().unwrap()
    }

    fn transaction_for_log(&self, log: &alloy::rpc::types::Log) -> Option<Transaction> {
        self.0.transaction_for_log(log)
    }

    fn transaction_for_call(&self, call: &EthereumCall) -> Option<Transaction> {
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

/// Evaluates if a given transaction was successful.
///
/// Returns `true` on success and `false` on failure.
/// If a receipt does not have a status value (EIP-658), assume the transaction was successful.
pub fn evaluate_transaction_status(receipt_status: Option<U64>) -> bool {
    receipt_status
        .map(|status| !status.is_zero())
        .unwrap_or(true)
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct EthereumBlock {
    pub block: Arc<LightEthereumBlock>,
    pub transaction_receipts: Vec<Arc<alloy::rpc::types::TransactionReceipt>>,
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
        let transaction_index = trace.transaction_position? as u64;

        Some(EthereumCall {
            from: call.from,
            to: call.to,
            value: call.value,
            gas_used: gas_used,
            input: call.input.clone(),
            output: output,
            block_number: trace.block_number? as BlockNumber,
            block_hash: trace.block_hash?,
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
