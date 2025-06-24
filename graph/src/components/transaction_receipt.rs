//! Code for retrieving transaction receipts from the database.
//!
//! This module exposes the [`LightTransactionReceipt`] type, which holds basic information about
//! the retrieved transaction receipts.

use web3::types::{TransactionReceipt, H256, U256, U64};

use crate::{
    prelude::b256_to_h256,
    util::conversions::{bool_to_web3_u64, u64_to_web3_u256, web3_u64_from_option},
};

/// Like web3::types::Receipt, but with fewer fields.
#[derive(Debug, PartialEq, Eq)]
pub struct LightTransactionReceipt {
    pub transaction_hash: H256,
    pub transaction_index: U64,
    pub block_hash: Option<H256>,
    pub block_number: Option<U64>,
    pub gas_used: Option<U256>,
    pub status: Option<U64>,
}

impl From<TransactionReceipt> for LightTransactionReceipt {
    fn from(receipt: TransactionReceipt) -> Self {
        let TransactionReceipt {
            transaction_hash,
            transaction_index,
            block_hash,
            block_number,
            gas_used,
            status,
            ..
        } = receipt;
        LightTransactionReceipt {
            transaction_hash,
            transaction_index,
            block_hash,
            block_number,
            gas_used,
            status,
        }
    }
}

impl From<alloy::rpc::types::TransactionReceipt> for LightTransactionReceipt {
    fn from(receipt: alloy::rpc::types::TransactionReceipt) -> Self {
        LightTransactionReceipt {
            transaction_hash: b256_to_h256(receipt.transaction_hash),
            transaction_index: web3_u64_from_option(receipt.transaction_index),
            block_hash: receipt.block_hash.map(b256_to_h256),
            block_number: Some(web3_u64_from_option(receipt.block_number)),
            gas_used: Some(u64_to_web3_u256(receipt.gas_used)),
            status: Some(bool_to_web3_u64(receipt.status())),
        }
    }
}
