use alloy::consensus::TxLegacy;
use alloy::primitives::Address;
use alloy::rpc::types::Transaction;

use crate::components::ethereum::{AnyBlock, AnyHeader, AnyTransaction, AnyTxEnvelope};
use crate::prelude::alloy::consensus::Header as ConsensusHeader;
use crate::prelude::alloy::primitives::B256;
use crate::prelude::alloy::rpc::types::{Block, Header};

/// Creates a minimal Alloy Block for testing purposes.
pub fn create_minimal_block_for_test(block_number: u64, block_hash: B256) -> AnyBlock {
    // Create consensus header with defaults, but set the specific number
    let consensus_header = ConsensusHeader {
        number: block_number,
        ..Default::default()
    };

    // Create RPC header with the specific hash and wrap in AnyHeader
    let rpc_header = Header {
        hash: block_hash,
        inner: AnyHeader::from(consensus_header),
        total_difficulty: None,
        size: None,
    };

    Block::empty(rpc_header)
}

/// Generic function that creates a mock legacy Transaction for testing
pub fn create_dummy_transaction(
    block_number: u64,
    block_hash: B256,
    transaction_index: Option<u64>,
    transaction_hash: B256,
) -> AnyTransaction {
    use alloy::{
        consensus::transaction::Recovered,
        consensus::{Signed, TxEnvelope},
        primitives::{Signature, U256},
    };

    let tx = TxLegacy::default();

    // Create a dummy signature
    let signature = Signature::new(U256::from(0x1111), U256::from(0x2222), false);

    let signed_tx = Signed::new_unchecked(tx, signature, transaction_hash);
    let envelope = TxEnvelope::Legacy(signed_tx);
    let any_envelope = AnyTxEnvelope::Ethereum(envelope);

    let recovered = Recovered::new_unchecked(any_envelope, Address::ZERO);

    Transaction {
        inner: recovered,
        block_hash: Some(block_hash),
        block_number: Some(block_number),
        transaction_index,
        effective_gas_price: None,
    }
}
