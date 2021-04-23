use super::ProofOfIndexingEvent;
use crate::prelude::DeploymentHash;
use stable_hash::prelude::*;
use stable_hash::utils::AsBytes;
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

impl StableHash for PoI<'_> {
    fn stable_hash<H: StableHasher>(&self, mut sequence_number: H::Seq, state: &mut H) {
        self.causality_regions
            .stable_hash(sequence_number.next_child(), state);
        self.subgraph_id
            .stable_hash(sequence_number.next_child(), state);
        AsBytes(self.block_hash.as_bytes()).stable_hash(sequence_number.next_child(), state);
        self.indexer
            .as_ref()
            .map(|i| AsBytes(i.as_bytes()))
            .stable_hash(sequence_number.next_child(), state);
    }
}

pub struct CausalityRegion<'a> {
    pub blocks: Vec<Block<'a>>,
}

impl StableHash for CausalityRegion<'_> {
    fn stable_hash<H: StableHasher>(&self, mut sequence_number: H::Seq, state: &mut H) {
        self.blocks.stable_hash(sequence_number.next_child(), state);
    }
}

#[derive(Default)]
pub struct Block<'a> {
    pub events: Vec<ProofOfIndexingEvent<'a>>,
}

impl StableHash for Block<'_> {
    fn stable_hash<H: StableHasher>(&self, mut sequence_number: H::Seq, state: &mut H) {
        self.events.stable_hash(sequence_number.next_child(), state);
    }
}
