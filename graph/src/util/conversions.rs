use std::sync::Arc;

use crate::prelude::alloy::primitives::{Address as AlloyAddress, B256};
use crate::prelude::alloy::rpc::types::{
    Block as AlloyBlock, Log as AlloyLog, TransactionReceipt as AlloyTransactionReceipt,
};
/// Type conversion utilities between web3 and alloy types
use crate::prelude::web3::types::{
    Address as Web3Address, Block as Web3Block, Log as Web3Log, Transaction as Web3Transaction,
    TransactionReceipt as Web3TransactionReceipt, H160, H256, U256, U64,
};

/// Converts H256 to alloy B256
pub fn h256_to_b256(h: H256) -> B256 {
    B256::from_slice(h.as_bytes())
}

/// Converts alloy B256 to H256
pub fn b256_to_h256(b: B256) -> H256 {
    H256::from_slice(b.as_slice())
}

/// Converts web3 H160 to alloy Address
pub fn h160_to_alloy_address(h: H160) -> AlloyAddress {
    AlloyAddress::from_slice(h.as_bytes())
}

/// Converts alloy Address to web3 H160
pub fn alloy_address_to_h160(addr: AlloyAddress) -> H160 {
    H160::from_slice(addr.as_slice())
}

/// Converts web3 Address to alloy Address
pub fn web3_address_to_alloy_address(addr: Web3Address) -> AlloyAddress {
    h160_to_alloy_address(addr)
}

/// Converts alloy Address to web3 Address
pub fn alloy_address_to_web3_address(addr: AlloyAddress) -> Web3Address {
    alloy_address_to_h160(addr)
}

/// Converts alloy Log to web3 Log
pub fn alloy_log_to_web3_log(log: AlloyLog) -> Web3Log {
    Web3Log {
        address: alloy_address_to_h160(log.address()),
        topics: log.topics().iter().map(|t| b256_to_h256(*t)).collect(),
        data: log.data().data.clone().into(),
        block_hash: log.block_hash.map(b256_to_h256),
        block_number: log.block_number.map(|n| U64::from(n)),
        transaction_hash: log.transaction_hash.map(b256_to_h256),
        transaction_index: log.transaction_index.map(|i| U64::from(i)),
        log_index: log.log_index.map(|i| U256::from(i)),
        transaction_log_index: None, // alloy Log doesn't have transaction_log_index
        log_type: None,              // alloy Log doesn't have log_type
        removed: Some(log.removed),
    }
}

pub fn alloy_transaction_receipt_to_web3_transaction_receipt(
    _receipt: Arc<AlloyTransactionReceipt>,
) -> Arc<Web3TransactionReceipt> {
    unimplemented!("TransactionReceipt conversion not yet implemented - will be done when needed")
}

/// Converts alloy Block to web3 Block
pub fn alloy_block_to_web3_block(_block: AlloyBlock) -> Web3Block<Web3Transaction> {
    unimplemented!(
        "Block conversion from alloy to web3 not yet implemented - will be done when needed"
    )
}

/// Converts web3 Block to alloy Block
pub fn web3_block_to_alloy_block(_block: Web3Block<Web3Transaction>) -> AlloyBlock {
    unimplemented!(
        "Block conversion from web3 to alloy not yet implemented - will be done when needed"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_h256_to_b256_conversion() {
        let h = H256::from([1u8; 32]);
        let b = h256_to_b256(h);
        assert_eq!(b.as_slice(), &[1u8; 32]);
    }

    #[test]
    fn test_b256_to_h256_conversion() {
        let b = B256::from([2u8; 32]);
        let h = b256_to_h256(b);
        assert_eq!(h.as_bytes(), &[2u8; 32]);
    }

    #[test]
    fn test_round_trip_conversion() {
        let original_h = H256::from([42u8; 32]);
        let b = h256_to_b256(original_h);
        let converted_h = b256_to_h256(b);
        assert_eq!(original_h, converted_h);
    }

    #[test]
    fn test_h160_to_alloy_address_conversion() {
        let h = H160::from([1u8; 20]);
        let addr = h160_to_alloy_address(h);
        assert_eq!(addr.as_slice(), &[1u8; 20]);
    }

    #[test]
    fn test_alloy_address_to_h160_conversion() {
        let addr = AlloyAddress::from([2u8; 20]);
        let h = alloy_address_to_h160(addr);
        assert_eq!(h.as_bytes(), &[2u8; 20]);
    }

    #[test]
    fn test_address_round_trip_conversion() {
        let original_h = H160::from([42u8; 20]);
        let addr = h160_to_alloy_address(original_h);
        let converted_h = alloy_address_to_h160(addr);
        assert_eq!(original_h, converted_h);
    }
}
