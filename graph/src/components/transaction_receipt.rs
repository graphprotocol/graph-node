//! Code for retrieving transaction receipts from the database.
//!
//! This module exposes the [`LightTransactionReceipt`] type, which holds basic information about
//! the retrieved transaction receipts.

use alloy::primitives::B256;
use alloy::rpc::types::TransactionReceipt;

/// Like web3::types::Receipt, but with fewer fields.
#[derive(Debug, PartialEq, Eq)]
pub struct LightTransactionReceipt {
    pub transaction_hash: B256,
    pub transaction_index: u64,
    pub block_hash: Option<B256>,
    pub block_number: Option<u64>,
    pub gas_used: u64,
    pub status: bool,
}

impl From<TransactionReceipt> for LightTransactionReceipt {
    fn from(receipt: alloy::rpc::types::TransactionReceipt) -> Self {
        LightTransactionReceipt {
            transaction_hash: receipt.transaction_hash,
            transaction_index: receipt.transaction_index.unwrap(),
            block_hash: receipt.block_hash,
            block_number: receipt.block_number,
            gas_used: receipt.gas_used,
            status: receipt.status(),
        }
    }
}
