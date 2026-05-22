//! Code for retrieving transaction receipts from the database.
//!
//! This module exposes the [`LightTransactionReceipt`] type, which holds basic information about
//! the retrieved transaction receipts.

use alloy::network::ReceiptResponse;
use alloy::primitives::B256;

#[derive(Debug, PartialEq, Eq)]
pub struct LightTransactionReceipt {
    pub transaction_hash: B256,
    pub transaction_index: u64,
    pub block_hash: Option<B256>,
    pub block_number: Option<u64>,
    pub gas_used: u64,
    pub status: bool,
}

impl From<super::ethereum::AnyTransactionReceiptBare> for LightTransactionReceipt {
    fn from(receipt: super::ethereum::AnyTransactionReceiptBare) -> Self {
        LightTransactionReceipt {
            transaction_hash: receipt.transaction_hash,
            transaction_index: receipt.transaction_index.unwrap(), // unwrap is safe because its None only for pending transactions, graph-node does not ingest pending transactions
            block_hash: receipt.block_hash,
            block_number: receipt.block_number,
            gas_used: receipt.gas_used,
            status: receipt.status(),
        }
    }
}
