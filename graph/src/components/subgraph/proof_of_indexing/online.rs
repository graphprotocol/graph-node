//! This is an online (streaming) implementation of the reference implementation
//! Any hash constructed from here should be the same as if the same data was given
//! to the reference implementation, but this is updated incrementally

use super::ProofOfIndexingEvent;
use crate::{
    blockchain::BlockPtr,
    prelude::{debug, BlockNumber, DeploymentHash, Logger},
};
use lazy_static::lazy_static;
use stable_hash::crypto::{Blake3SeqNo, SetHasher};
use stable_hash::prelude::*;
use stable_hash::utils::AsBytes;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt;
use web3::types::Address;

lazy_static! {
    static ref LOG_EVENTS: bool = std::env::var("GRAPH_LOG_POI_EVENTS")
        .unwrap_or_else(|_| "false".into())
        .parse::<bool>()
        .expect("invalid GRAPH_LOG_POI_EVENTS");
}

pub struct BlockEventStream {
    vec_length: usize,
    seq_no: Blake3SeqNo,
    digest: SetHasher,
}

/// Go directly to a SequenceNumber identifying a field within a struct.
/// This is best understood by example. Consider the struct:
///
/// struct Outer {
///    inners: Vec<Inner>,
///    outer_num: i32
/// }
/// struct Inner {
///    inner_num: i32,
///    inner_str: String,
/// }
///
/// Let's say that we have the following data:
/// Outer {
///    inners: vec![
///       Inner {
///           inner_num: 10,
///           inner_str: "THIS",
///       },
///    ],
///    outer_num: 0,
/// }
///
/// And we need to identify the string "THIS", at outer.inners[0].inner_str;
/// This would require the following:
/// traverse_seq_no(&[
///    0, // Outer.inners
///    0, // Vec<Inner>[0]
///    1, // Inner.inner_str
///])
// Performance: Could write a specialized function for this easily, avoiding a bunch of clones of Blake3SeqNo
fn traverse_seq_no(counts: &[usize]) -> Blake3SeqNo {
    counts.iter().fold(Blake3SeqNo::root(), |mut s, i| {
        s.skip(*i);
        s.next_child()
    })
}

impl BlockEventStream {
    fn new(block_number: BlockNumber) -> Self {
        let events = traverse_seq_no(&[
            1,                                // kvp -> v
            0,                                // CausalityRegion.blocks: Vec<Block>
            block_number.try_into().unwrap(), // Vec<Block> -> [i]
            0,                                // Block.events -> Vec<ProofOfIndexingEvent>
        ]);
        Self {
            vec_length: 0,
            seq_no: events,
            digest: SetHasher::new(),
        }
    }

    /// Finishes the current block and returns the serialized hash function to
    /// be resumed later. Cases in which the hash function is resumed include
    /// when asking for the final PoI, or when combining with the next modified
    /// block via the argument `prev`
    pub fn pause(mut self, prev: Option<&[u8]>) -> Vec<u8> {
        self.vec_length.stable_hash(self.seq_no, &mut self.digest);
        let mut state = self.digest;
        if let Some(prev) = prev {
            let prev = SetHasher::from_bytes(prev);
            state.finish_unordered(prev, SequenceNumber::root());
        }
        state.to_bytes()
    }

    fn write(&mut self, event: &ProofOfIndexingEvent<'_>) {
        self.vec_length += 1;
        event.stable_hash(self.seq_no.next_child(), &mut self.digest);
    }
}

#[derive(Default)]
pub struct ProofOfIndexing {
    block_number: BlockNumber,
    /// The POI is updated for each data source independently. This is necessary because
    /// some data sources (eg: IPFS files) may be unreliable and therefore cannot mix
    /// state with other data sources. This may also give us some freedom to change
    /// the order of triggers in the future.
    per_causality_region: HashMap<String, BlockEventStream>,
}

impl fmt::Debug for ProofOfIndexing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ProofOfIndexing").field(&"...").finish()
    }
}

impl ProofOfIndexing {
    pub fn new(block_number: BlockNumber) -> Self {
        Self {
            block_number,
            per_causality_region: HashMap::new(),
        }
    }
    /// Adds an event to the digest of the ProofOfIndexingStream local to the causality region
    pub fn write(
        &mut self,
        logger: &Logger,
        causality_region: &str,
        event: &ProofOfIndexingEvent<'_>,
    ) {
        if *LOG_EVENTS {
            debug!(
                logger,
                "Proof of indexing event";
                "event" => &event,
                "causality_region" => causality_region
            );
        }

        // This may be better with the raw_entry API, once that is stabilized
        if let Some(causality_region) = self.per_causality_region.get_mut(causality_region) {
            causality_region.write(event);
        } else {
            let mut entry = BlockEventStream::new(self.block_number);
            entry.write(event);
            self.per_causality_region
                .insert(causality_region.to_owned(), entry);
        }
    }
    pub fn take(self) -> HashMap<String, BlockEventStream> {
        self.per_causality_region
    }
}

pub struct ProofOfIndexingFinisher {
    block_number: BlockNumber,
    state: SetHasher,
    causality_count: usize,
}

impl ProofOfIndexingFinisher {
    pub fn new(block: &BlockPtr, subgraph_id: &DeploymentHash, indexer: &Option<Address>) -> Self {
        let mut state = SetHasher::new();

        // Add the subgraph id
        let subgraph_id_seq_no = traverse_seq_no(&[
            1, // PoI.subgraph_id
        ]);
        subgraph_id.stable_hash(subgraph_id_seq_no, &mut state);

        // Add the block hash
        let block_hash_seq_no = traverse_seq_no(&[
            2, // PoI.block_hash
        ]);
        AsBytes(block.hash_slice()).stable_hash(block_hash_seq_no, &mut state);

        // Add the indexer
        let indexer_seq_no = traverse_seq_no(&[
            3, // PoI.indexer
        ]);
        indexer
            .as_ref()
            .map(|i| AsBytes(i.as_bytes()))
            .stable_hash(indexer_seq_no, &mut state);

        ProofOfIndexingFinisher {
            block_number: block.number,
            state,
            causality_count: 0,
        }
    }

    pub fn add_causality_region(&mut self, name: &str, region: &[u8]) {
        let mut state = SetHasher::from_bytes(region);

        // Finish the blocks vec
        let blocks_seq_no = traverse_seq_no(&[
            1, // kvp -> v
            0, // CausalityRegion.blocks: Vec<Block>
        ]);
        // + 1 is to account that the length of the blocks array for the genesis block is 1, not 0.
        (self.block_number + 1).stable_hash(blocks_seq_no, &mut state);

        // Add the name.
        let name_seq_no = traverse_seq_no(&[
            0, // kvp -> k
        ]);
        name.stable_hash(name_seq_no, &mut state);

        let state = state.finish();

        // Mixin the region with the final value
        let causality_regions_member_seq_no = traverse_seq_no(&[
            0, // Poi.causality_regions
            1, // unordered collection member
        ]);

        self.state.write(causality_regions_member_seq_no, &state);
        self.causality_count += 1;
    }

    pub fn finish(mut self) -> <SetHasher as StableHasher>::Out {
        let causality_regions_count_seq_no = traverse_seq_no(&[
            0, // Poi.causality_regions
            2, // unordered collection count
        ]);

        // Note that technically to get the same sequence number one would need
        // to call causality_regions_count_seq_no.skip(self.causality_count);
        // but it turns out that the result happens to be the same for
        // non-negative numbers.

        self.causality_count
            .stable_hash(causality_regions_count_seq_no, &mut self.state);

        self.state.finish()
    }
}
