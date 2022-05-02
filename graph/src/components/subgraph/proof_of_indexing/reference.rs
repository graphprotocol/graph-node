use super::ProofOfIndexingEvent;
use crate::prelude::DeploymentHash;
use stable_hash::{utils::AsBytes, FieldAddress, StableHash};
use stable_hash_legacy::SequenceNumber;
use std::collections::HashMap;
use web3::types::{Address, H256};

/// The PoI is the StableHash of this struct. This reference implementation is
/// mostly here just to make sure that the online implementation is
/// well-implemented (without conflicting sequence numbers, or other oddities).
/// It's just way easier to check that this works, and serves as a kind of
/// documentation as a side-benefit.
pub struct PoI<'a> {
    pub causality_regions: HashMap<String, CausalityRegion<'a>>,
    pub subgraph_id: DeploymentHash,
    pub block_hash: H256,
    pub indexer: Option<Address>,
}

impl stable_hash_legacy::StableHash for PoI<'_> {
    fn stable_hash<H: stable_hash_legacy::StableHasher>(
        &self,
        mut sequence_number: H::Seq,
        state: &mut H,
    ) {
        let PoI {
            causality_regions,
            subgraph_id,
            block_hash,
            indexer,
        } = self;

        stable_hash_legacy::StableHash::stable_hash(
            causality_regions,
            sequence_number.next_child(),
            state,
        );
        stable_hash_legacy::StableHash::stable_hash(
            subgraph_id,
            sequence_number.next_child(),
            state,
        );
        stable_hash_legacy::utils::AsBytes(block_hash.as_bytes())
            .stable_hash(sequence_number.next_child(), state);
        indexer
            .as_ref()
            .map(|i| stable_hash_legacy::utils::AsBytes(i.as_bytes()))
            .stable_hash(sequence_number.next_child(), state);
    }
}

impl StableHash for PoI<'_> {
    fn stable_hash<H: stable_hash::StableHasher>(&self, field_address: H::Addr, state: &mut H) {
        let PoI {
            causality_regions,
            subgraph_id,
            block_hash,
            indexer: _,
        } = self;

        StableHash::stable_hash(causality_regions, field_address.child(0), state);
        subgraph_id.stable_hash(field_address.child(1), state);
        AsBytes(block_hash.as_bytes()).stable_hash(field_address.child(2), state);
        self.indexer
            .as_ref()
            .map(|i| AsBytes(i.as_bytes()))
            .stable_hash(field_address.child(3), state);
    }
}

pub struct CausalityRegion<'a> {
    pub blocks: Vec<Block<'a>>,
}

impl stable_hash_legacy::StableHash for CausalityRegion<'_> {
    fn stable_hash<H: stable_hash_legacy::StableHasher>(
        &self,
        mut sequence_number: H::Seq,
        state: &mut H,
    ) {
        let CausalityRegion { blocks } = self;

        stable_hash_legacy::StableHash::stable_hash(blocks, sequence_number.next_child(), state);
    }
}

impl StableHash for CausalityRegion<'_> {
    fn stable_hash<H: stable_hash::StableHasher>(&self, field_address: H::Addr, state: &mut H) {
        let CausalityRegion { blocks } = self;

        StableHash::stable_hash(blocks, field_address.child(0), state);
    }
}

impl CausalityRegion<'_> {
    pub fn from_network(network: &str) -> String {
        format!("ethereum/{}", network)
    }
}

#[derive(Default)]
pub struct Block<'a> {
    pub events: Vec<ProofOfIndexingEvent<'a>>,
}

impl stable_hash_legacy::StableHash for Block<'_> {
    fn stable_hash<H: stable_hash_legacy::StableHasher>(
        &self,
        mut sequence_number: H::Seq,
        state: &mut H,
    ) {
        let Block { events } = self;

        stable_hash_legacy::StableHash::stable_hash(events, sequence_number.next_child(), state);
    }
}

impl StableHash for Block<'_> {
    fn stable_hash<H: stable_hash::StableHasher>(&self, field_address: H::Addr, state: &mut H) {
        let Block { events } = self;

        StableHash::stable_hash(events, field_address.child(0), state);
    }
}
