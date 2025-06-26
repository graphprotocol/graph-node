use alloy::rpc::types::trace::parity::{Action, LocalizedTransactionTrace, TraceOutput};
use serde::{Deserialize, Serialize};
use std::{convert::TryFrom, sync::Arc};
use web3::types::{Address, Bytes, Log, Transaction, TransactionReceipt, H256, U256, U64};

use crate::{
    alloy_todo,
    blockchain::{BlockPtr, BlockTime},
    prelude::{
        alloy::rpc::types::Block as AlloyBlock, alloy_address_to_h160, b256_to_h256, BlockNumber,
    },
    util::conversions::{alloy_bytes_to_web3_bytes, alloy_u256_to_web3_u256, u64_to_web3_u256},
};

#[allow(dead_code)]
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct BlockWrapper(AlloyBlock);

impl BlockWrapper {
    pub fn new(block: AlloyBlock) -> Self {
        Self(block)
    }

    pub fn hash_h256(&self) -> Option<H256> {
        alloy_todo!()
        // self.0.hash
    }

    pub fn number_u64(&self) -> Option<u64> {
        alloy_todo!()
        // self.0.number.map(|n| n.as_u64())
    }

    pub fn number_web3_u64(&self) -> Option<U64> {
        alloy_todo!()
        // Some(u64_to_web3_u64(self.0.header.number))
    }

    pub fn timestamp_u64(&self) -> u64 {
        alloy_todo!()
        // self.0.timestamp.as_u64()
    }

    pub fn transactions(&self) -> &[Transaction] {
        alloy_todo!()
        // &self.0.transactions
    }

    pub fn inner(&self) -> &web3::types::Block<Transaction> {
        alloy_todo!()
        // &self.0
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
        alloy_todo!()
    }

    fn timestamp(&self) -> BlockTime {
        alloy_todo!()
    }

    fn transaction_for_log(&self, _log: &Log) -> Option<Transaction> {
        alloy_todo!()
    }

    fn transaction_for_call(&self, _call: &EthereumCall) -> Option<Transaction> {
        alloy_todo!()
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        match self.header.number {
            0 => None,
            n => Some(BlockPtr::new(
                self.header.parent_hash.into(),
                (n - 1) as i32,
            )),
        }
    }

    fn format(&self) -> String {
        alloy_todo!()
    }

    fn block_ptr(&self) -> BlockPtr {
        alloy_todo!()
    }
}

impl LightEthereumBlockExt for LightEthereumBlock {
    fn number(&self) -> BlockNumber {
        self.0.number()
    }

    fn transaction_for_log(&self, log: &Log) -> Option<Transaction> {
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

impl LightEthereumBlockExt for web3::types::Block<Transaction> {
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

    fn timestamp(&self) -> BlockTime {
        let ts = i64::try_from(self.timestamp.as_u64()).unwrap();
        BlockTime::since_epoch(ts, 0)
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

        Ok(evaluate_transaction_status(receipt.status))
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
    pub transaction_receipts: Vec<Arc<TransactionReceipt>>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
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
            from: alloy_address_to_h160(call.from),
            to: alloy_address_to_h160(call.to),
            value: alloy_u256_to_web3_u256(call.value),
            gas_used: u64_to_web3_u256(gas_used),
            input: alloy_bytes_to_web3_bytes(call.input.clone()),
            output: alloy_bytes_to_web3_bytes(output),
            block_number: trace.block_number? as BlockNumber,
            block_hash: trace.block_hash.map(|h| b256_to_h256(h))?,
            transaction_hash: trace.transaction_hash.map(|h| b256_to_h256(h)),
            transaction_index,
        })
    }
}

impl From<EthereumBlock> for BlockPtr {
    fn from(b: EthereumBlock) -> BlockPtr {
        BlockPtr::from((b.block.hash_h256().unwrap(), b.block.number()))
    }
}

impl<'a> From<&'a EthereumBlock> for BlockPtr {
    fn from(b: &'a EthereumBlock) -> BlockPtr {
        BlockPtr::from((b.block.hash_h256().unwrap(), b.block.number()))
    }
}

impl<'a> From<&'a EthereumCall> for BlockPtr {
    fn from(call: &'a EthereumCall) -> BlockPtr {
        BlockPtr::from((call.block_hash, call.block_number))
    }
}
