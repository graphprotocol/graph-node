mod event;
mod online;
mod reference;

pub use event::ProofOfIndexingEvent;
pub use online::{ProofOfIndexing, ProofOfIndexingFinisher};
pub use reference::PoICausalityRegion;

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
    fn check(case: Case, cache: &mut HashMap<String, &str>) {
        let logger = Logger::root(Discard, o!());

        // Does a sanity check to ensure that the schema itself is correct,
        // which is separate to verifying that the online/offline version
        // return the same result.
        check_for_child_errors(&case.data).expect("Found child errors");

        let offline_fast = tiny_keccak::keccak256(&fast_stable_hash(&case.data).to_le_bytes());
        let offline_legacy = stable_hash_legacy::<SetHasher, _>(&case.data);

        for (version, offline, hardcoded) in [
            (ProofOfIndexingVersion::Legacy, offline_legacy, case.legacy),
            (ProofOfIndexingVersion::Fast, offline_fast, case.fast),
        ] {
            // The code is meant to approximate what happens during indexing as
            // close as possible. The API for the online PoI is meant to be
            // pretty foolproof so that the actual usage will also match.

            // Create a database which stores intermediate PoIs
            let mut db = HashMap::<String, Vec<u8>>::new();

            let mut block_count = 1;
            for causality_region in case.data.causality_regions.values() {
                block_count = causality_region.blocks.len();
                break;
            }

            for block_i in 0..block_count {
                let mut stream = ProofOfIndexing::new(block_i.try_into().unwrap(), version);

                for (name, region) in case.data.causality_regions.iter() {
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
            let block_ptr = BlockPtr::from((case.data.block_hash, block_number));

            // This region emulates the request
            let mut finisher = ProofOfIndexingFinisher::new(
                &block_ptr,
                &case.data.subgraph_id,
                &case.data.indexer,
                version,
            );
            for (name, region) in db.iter() {
                finisher.add_causality_region(name, region);
            }

            let online = hex::encode(finisher.finish());
            let offline = hex::encode(offline);
            assert_eq!(&online, &offline);
            assert_eq!(&online, hardcoded);

            if let Some(prev) = cache.insert(offline, case.name) {
                panic!("Found conflict for case: {} == {}", case.name, prev);
            }
        }
    }

    struct Case<'a> {
        name: &'static str,
        legacy: &'static str,
        fast: &'static str,
        data: PoI<'a>,
    }

    /// This test checks that each case resolves to a unique hash, and that
    /// in each case the reference and online versions match
    #[test]
    fn online_vs_reference() {
        let data = hashmap! {
            "val".into() => Value::Int(1)
        };
        let data_empty = hashmap! {};
        let data2 = hashmap! {
            "key".into() => Value::String("s".to_owned()),
            "null".into() => Value::Null,
        };

        let mut cases = vec![
            // Simple case of basically nothing
            Case {
                name: "genesis",
                legacy: "401e5bef572bc3a56b0ced0eb6cb4619d2ca748db6af8855828d16ff3446cfdd",
                fast: "dced49c45eac68e8b3d8f857928e7be6c270f2db8b56b0d7f27ce725100bae01",
                data: PoI {
                    subgraph_id: DeploymentHash::new("test").unwrap(),
                    block_hash: H256::repeat_byte(1),
                    causality_regions: HashMap::new(),
                    indexer: None,
                },
            },
            // Add an event
            Case {
                name: "one_event",
                legacy: "9241634bfc8a9a12c796a0de6f326326a74967cd477ee7ce78fbac20a9e9c303",
                fast: "bb3c37659d4bc799b9dcf3d17b1b1e93847f5fc0b2c50ee6a27f13b5c07f7e97",
                data: PoI {
                    subgraph_id: DeploymentHash::new("test").unwrap(),
                    block_hash: H256::repeat_byte(1),
                    causality_regions: hashmap! {
                        "eth".to_owned() => PoICausalityRegion {
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
            },
            // Try adding a couple more blocks, including an empty block on the end
            Case {
                name: "multiple_blocks",
                legacy: "775fa30bbaef2a8659456a317923a36f46e3715e6c9cf43203dd3486af4e361f",
                fast: "3bb882049e8f4a11cd4a7a005c6ce3b3c779a0e90057a9556c595660e626268d",
                data: PoI {
                    subgraph_id: DeploymentHash::new("b").unwrap(),
                    block_hash: H256::repeat_byte(3),
                    causality_regions: hashmap! {
                        "eth".to_owned() => PoICausalityRegion {
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
            },
            // Try adding another causality region
            Case {
                name: "causality_regions",
                legacy: "13e6fd2b581911c80d935d4f098b40ef3d87cbc564b5a635c81b06091a381e54",
                fast: "b2cb70acd4a1337a67df810fe4c5c2fb3d3a3b2b8eb137dbb592bd6014869362",
                data: PoI {
                    subgraph_id: DeploymentHash::new("b").unwrap(),
                    block_hash: H256::repeat_byte(3),
                    causality_regions: hashmap! {
                        "eth".to_owned() => PoICausalityRegion {
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
                        "ipfs".to_owned() => PoICausalityRegion {
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
            },
            // Back to the one event case, but try adding some data.
            Case {
                name: "data",
                legacy: "cd3020511cf4c88dd2be542aca4f95bb2a67b06e29f444bcdf44009933b8ff31",
                fast: "a992ba24702615a3f591014f7351acf85a35b75e1f8646fc8d77509c4b5d31ed",
                data: PoI {
                    subgraph_id: DeploymentHash::new("test").unwrap(),
                    block_hash: H256::repeat_byte(1),
                    causality_regions: hashmap! {
                        "eth".to_owned() => PoICausalityRegion {
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
            },
        ];

        // Lots of data up there ⬆️ to test. Finally, loop over each case, comparing the reference and
        // online version, then checking that there are no conflicts for the reference versions.
        let mut results = HashMap::new();
        for case in cases.drain(..) {
            check(case, &mut results);
        }
    }
}
