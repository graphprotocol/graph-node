use alloy::consensus::{TxEnvelope, TxLegacy};
use alloy::primitives::Address;
use alloy::rpc::types::Transaction;

use crate::prelude::alloy::consensus::Header as ConsensusHeader;
use crate::prelude::alloy::primitives::B256;
use crate::prelude::alloy::rpc::types::{Block, Header};

/// Creates a minimal Alloy Block for testing purposes.
pub fn create_minimal_block_for_test(block_number: u64, block_hash: B256) -> Block {
    // Create consensus header with defaults, but set the specific number
    let mut consensus_header = ConsensusHeader::default();
    consensus_header.number = block_number;

    // Create RPC header with the specific hash
    let rpc_header = Header {
        hash: block_hash,
        inner: consensus_header,
        total_difficulty: None,
        size: None,
    };

    // Create an empty block with this header
    Block::empty(rpc_header)
}

/// Generic function that creates a mock legacy Transaction from ANY log
pub fn create_dummy_transaction(
    block_number: u64,
    block_hash: B256,
    transaction_index: Option<u64>,
    transaction_hash: B256,
) -> Transaction<TxEnvelope> {
    use alloy::{
        consensus::transaction::Recovered,
        consensus::Signed,
        primitives::{Signature, U256},
    };

    let tx = TxLegacy::default();

    // Create a dummy signature
    let signature = Signature::new(U256::from(0x1111), U256::from(0x2222), false);

    let signed_tx = Signed::new_unchecked(tx, signature, transaction_hash);
    let envelope = TxEnvelope::Legacy(signed_tx);

    let recovered = Recovered::new_unchecked(envelope, Address::ZERO);

    Transaction {
        inner: recovered,
        block_hash: Some(block_hash),
        block_number: Some(block_number),
        transaction_index: transaction_index,
        effective_gas_price: None,
    }
}
