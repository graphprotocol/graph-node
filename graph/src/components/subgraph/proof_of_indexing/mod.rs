mod event;
mod online;
mod reference;

pub use event::ProofOfIndexingEvent;
pub use online::{ProofOfIndexing, ProofOfIndexingFinisher};
pub use reference::CausalityRegion;

use atomic_refcell::AtomicRefCell;
use std::sync::Arc;

#[derive(Copy, Clone, Debug)]
pub enum ProofOfIndexingVersion {
    Fast,
    Legacy,
}

/// This concoction of types is to allow MappingContext to be static, yet still
/// have shared mutable data for derive_with_empty_block_state. The static
/// requirement is so that host exports can be static for wasmtime.
/// AtomicRefCell is chosen over Mutex because concurrent access is
/// intentionally disallowed - PoI requires sequential access to the hash
/// function within a given causality region even if ownership is shared across
/// multiple mapping contexts.
///
/// The Option<_> is because not all subgraphs support PoI until re-deployed.
/// Eventually this can be removed.
///
/// This is not a great place to define this type, since the ProofOfIndexing
/// shouldn't "know" these details about wasmtime and subgraph re-deployments,
/// but the APIs that would make use of this are in graph/components so this
/// lives here for lack of a better choice.
pub type SharedProofOfIndexing = Option<Arc<AtomicRefCell<ProofOfIndexing>>>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::{BlockPtr, DeploymentHash, Value};
    use maplit::hashmap;
    use online::ProofOfIndexingFinisher;
    use reference::*;
    use slog::{o, Discard, Logger};
    use stable_hash::{fast_stable_hash, utils::check_for_child_errors};
    use stable_hash_legacy::crypto::SetHasher;
    use stable_hash_legacy::utils::stable_hash as stable_hash_legacy;
    use std::collections::HashMap;
    use std::convert::TryInto;
    use web3::types::{Address, H256};

    /// Verify that the stable hash of a reference and online implementation match
    fn check(reference: &PoI, cache: &mut HashMap<String, &str>, name: &'static str) {
        let logger = Logger::root(Discard, o!());

        // Does a sanity check to ensure that the schema itself is correct,
        // which is separate to verifying that the online/offline version
        // return the same result.
        check_for_child_errors(reference).expect("Found child errors");

        let offline_fast = tiny_keccak::keccak256(&fast_stable_hash(&reference).to_le_bytes());
        let offline_legacy = stable_hash_legacy::<SetHasher, _>(reference);

        for (version, offline) in [
            (ProofOfIndexingVersion::Legacy, offline_legacy),
            (ProofOfIndexingVersion::Fast, offline_fast),
        ] {
            // The code is meant to approximate what happens during indexing as
            // close as possible. The API for the online PoI is meant to be
            // pretty foolproof so that the actual usage will also match.

            // Create a database which stores intermediate PoIs
            let mut db = HashMap::<String, Vec<u8>>::new();

            let mut block_count = 1;
            for causality_region in reference.causality_regions.values() {
                block_count = causality_region.blocks.len();
                break;
            }

            for block_i in 0..block_count {
                let mut stream = ProofOfIndexing::new(block_i.try_into().unwrap(), version);

                for (name, region) in reference.causality_regions.iter() {
                    let block = &region.blocks[block_i];

                    for evt in block.events.iter() {
                        stream.write(&logger, name, evt);
                    }
                }

                for (name, region) in stream.take() {
                    let prev = db.get(&name);
                    let update = region.pause(prev.map(|v| &v[..]));
                    db.insert(name, update);
                }
            }

            let block_number = (block_count - 1) as u64;
            let block_ptr = BlockPtr::from((reference.block_hash, block_number));

            // This region emulates the request
            let mut finisher = ProofOfIndexingFinisher::new(
                &block_ptr,
                &reference.subgraph_id,
                &reference.indexer,
                version,
            );
            for (name, region) in db.iter() {
                finisher.add_causality_region(name, region);
            }

            let online = hex::encode(finisher.finish());
            let offline = hex::encode(&offline);
            assert_eq!(&online, &offline);

            if let Some(prev) = cache.insert(offline, name) {
                panic!("Found conflict for case: {} == {}", name, prev);
            }
        }
    }

    /// This test checks that each case resolves to a unique hash, and that
    /// in each case the reference and online versions match
    #[test]
    fn online_vs_reference() {
        let data = hashmap! {
            "val".to_owned() => Value::Int(1)
        };
        let data_empty = hashmap! {};
        let data2 = hashmap! {
            "key".to_owned() => Value::String("s".to_owned()),
            "null".to_owned() => Value::Null,
        };

        let mut cases = hashmap! {
            // Simple case of basically nothing
            "genesis" => PoI {
                subgraph_id: DeploymentHash::new("test").unwrap(),
                block_hash: H256::repeat_byte(1),
                causality_regions: HashMap::new(),
                indexer: None,
            },

            // Add an event
            "one_event" => PoI {
                subgraph_id: DeploymentHash::new("test").unwrap(),
                block_hash: H256::repeat_byte(1),
                causality_regions: hashmap! {
                    "eth".to_owned() => CausalityRegion {
                        blocks: vec! [
                            Block::default(),
                            Block {
                                events: vec![
                                    ProofOfIndexingEvent::SetEntity {
                                        entity_type: "t",
                                        id: "id",
                                        data: &data_empty,
                                    }
                                ]
                            }
                        ],
                    },
                },
                indexer: Some(Address::repeat_byte(1)),
            },

            // Try adding a couple more blocks, including an empty block on the end
            "multiple_blocks" => PoI {
                subgraph_id: DeploymentHash::new("b").unwrap(),
                block_hash: H256::repeat_byte(3),
                causality_regions: hashmap! {
                    "eth".to_owned() => CausalityRegion {
                        blocks: vec! [
                            Block::default(),
                            Block {
                                events: vec![
                                    ProofOfIndexingEvent::SetEntity {
                                        entity_type: "type",
                                        id: "id",
                                        data: &data,
                                    }
                                ]
                            },
                            Block::default(),
                            Block {
                                events: vec![
                                    ProofOfIndexingEvent::SetEntity {
                                        entity_type: "type",
                                        id: "id",
                                        data: &data_empty,
                                    }
                                ]
                            },
                            Block::default(),
                        ],
                    },
                },
                indexer: Some(Address::repeat_byte(1)),
            },

            // Try adding another causality region
            "causality_regions" => PoI {
                subgraph_id: DeploymentHash::new("b").unwrap(),
                block_hash: H256::repeat_byte(3),
                causality_regions: hashmap! {
                    "eth".to_owned() => CausalityRegion {
                        blocks: vec! [
                            Block::default(),
                            Block {
                                events: vec![
                                    ProofOfIndexingEvent::SetEntity {
                                        entity_type: "type",
                                        id: "id",
                                        data: &data2,
                                    }
                                ]
                            },
                            Block::default(),
                            Block {
                                events: vec![
                                    ProofOfIndexingEvent::RemoveEntity {
                                        entity_type: "type",
                                        id: "id",
                                    }
                                ]
                            },
                            Block::default(),
                        ],
                    },
                    "ipfs".to_owned() => CausalityRegion {
                        blocks: vec! [
                            Block::default(),
                            Block {
                                events: vec![
                                    ProofOfIndexingEvent::SetEntity {
                                        entity_type: "type",
                                        id: "id",
                                        data: &data,
                                    }
                                ]
                            },
                            Block::default(),
                            Block {
                                events: vec![
                                    ProofOfIndexingEvent::SetEntity {
                                        entity_type: "type",
                                        id: "id",
                                        data: &data,
                                    }
                                ]
                            },
                            Block::default(),
                        ],
                    },
                },
                indexer: Some(Address::repeat_byte(1)),
            },

            // Back to the one event case, but try adding some data.
            "data" => PoI {
                subgraph_id: DeploymentHash::new("test").unwrap(),
                block_hash: H256::repeat_byte(1),
                causality_regions: hashmap! {
                    "eth".to_owned() => CausalityRegion {
                        blocks: vec! [
                            Block::default(),
                            Block {
                                events: vec![
                                    ProofOfIndexingEvent::SetEntity {
                                        entity_type: "type",
                                        id: "id",
                                        data: &data,
                                    }
                                ]
                            }
                        ],
                    },
                },
                indexer: Some(Address::repeat_byte(4)),
            },
        };

        // Lots of data up there ⬆️ to test. Finally, loop over each case, comparing the reference and
        // online version, then checking that there are no conflicts for the reference versions.
        let mut results = HashMap::new();
        for (name, data) in cases.drain() {
            check(&data, &mut results, name);
        }
    }
}
