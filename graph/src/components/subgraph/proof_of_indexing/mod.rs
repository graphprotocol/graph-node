mod event;
mod online;
mod reference;

pub use event::ProofOfIndexingEvent;
use graph_derive::CheapClone;
pub use online::{ProofOfIndexing, ProofOfIndexingFinisher};
pub use reference::PoICausalityRegion;

use atomic_refcell::AtomicRefCell;
use slog::Logger;
use std::{ops::Deref, sync::Arc};

use crate::prelude::BlockNumber;

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
#[derive(Clone, CheapClone)]
pub struct SharedProofOfIndexing {
    poi: Option<Arc<AtomicRefCell<ProofOfIndexing>>>,
}

impl SharedProofOfIndexing {
    pub fn new(block: BlockNumber, version: ProofOfIndexingVersion) -> Self {
        SharedProofOfIndexing {
            poi: Some(Arc::new(AtomicRefCell::new(ProofOfIndexing::new(
                block, version,
            )))),
        }
    }

    pub fn ignored() -> Self {
        SharedProofOfIndexing { poi: None }
    }

    pub fn write_event(
        &self,
        poi_event: &ProofOfIndexingEvent,
        causality_region: &str,
        logger: &Logger,
    ) {
        if let Some(poi) = &self.poi {
            let mut poi = poi.deref().borrow_mut();
            poi.write(logger, causality_region, poi_event);
        }
    }

    pub fn start_handler(&self, causality_region: &str) {
        if let Some(poi) = &self.poi {
            let mut poi = poi.deref().borrow_mut();
            poi.start_handler(causality_region);
        }
    }

    pub fn write_deterministic_error(&self, logger: &Logger, causality_region: &str) {
        if let Some(proof_of_indexing) = &self.poi {
            proof_of_indexing
                .deref()
                .borrow_mut()
                .write_deterministic_error(logger, causality_region);
        }
    }

    pub fn into_inner(self) -> Option<ProofOfIndexing> {
        self.poi
            .map(|poi| Arc::try_unwrap(poi).unwrap().into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::stable_hash_glue::{impl_stable_hash, AsBytes};
    use crate::{
        data::store::Id,
        prelude::{BlockPtr, DeploymentHash, Value},
        schema::InputSchema,
    };
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

    /// The PoI is the StableHash of this struct. This reference implementation is
    /// mostly here just to make sure that the online implementation is
    /// well-implemented (without conflicting sequence numbers, or other oddities).
    /// It's just way easier to check that this works, and serves as a kind of
    /// documentation as a side-benefit.
    pub struct PoI<'a> {
        pub causality_regions: HashMap<String, PoICausalityRegion<'a>>,
        pub subgraph_id: DeploymentHash,
        pub block_hash: H256,
        pub indexer: Option<Address>,
    }

    fn h256_as_bytes(val: &H256) -> AsBytes<&[u8]> {
        AsBytes(val.as_bytes())
    }

    fn indexer_opt_as_bytes(val: &Option<Address>) -> Option<AsBytes<&[u8]>> {
        val.as_ref().map(|v| AsBytes(v.as_bytes()))
    }

    impl_stable_hash!(PoI<'_> {
        causality_regions,
        subgraph_id,
        block_hash: h256_as_bytes,
        indexer: indexer_opt_as_bytes
    });

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
            let mut db = HashMap::<Id, Vec<u8>>::new();

            let block_count = match case.data.causality_regions.values().next() {
                Some(causality_region) => causality_region.blocks.len(),
                None => 1,
            };

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
            assert_eq!(&online, &offline, "case: {}", case.name);
            assert_eq!(&online, hardcoded, "case: {}", case.name);

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
        let id = DeploymentHash::new("Qm123").unwrap();

        let data_schema =
            InputSchema::parse_latest("type User @entity { id: String!, val: Int }", id.clone())
                .unwrap();
        let data = data_schema
            .make_entity(hashmap! {
                "id".into() => Value::String("id".to_owned()),
                "val".into() => Value::Int(1)
            })
            .unwrap();

        let empty_schema =
            InputSchema::parse_latest("type User @entity { id: String! }", id.clone()).unwrap();
        let data_empty = empty_schema
            .make_entity(hashmap! { "id".into() => Value::String("id".into())})
            .unwrap();

        let data2_schema = InputSchema::parse_latest(
            "type User @entity { id: String!, key: String!, null: String }",
            id,
        )
        .unwrap();
        let data2 = data2_schema
            .make_entity(hashmap! {
                "id".into() => Value::String("id".to_owned()),
                "key".into() => Value::String("s".to_owned()),
                "null".into() => Value::Null,
            })
            .unwrap();

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
                legacy: "96640d7a35405524bb21da8d86f7a51140634f44568cf9f7df439d0b2b01a435",
                fast: "8bb3373fb55e02bde3202bac0eeecf1bd9a676856a4dd6667bd809aceda41885",
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
                legacy: "a0346ee0d7e0518f73098b6f9dc020f1cf564fb88e09779abfdf5da736de5e82",
                fast: "8b0097ad96b21f7e4bd8dcc41985e6e5506b808f1185016ab1073dd8745238ce",
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
                legacy: "cc9449860e5b19b76aa39d6e05c5a560d1cb37a93d4bf64669feb47cfeb452fa",
                fast: "2041af28678e68406247a5cfb5fe336947da75256c79b35c2f61fc7985091c0e",
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
                legacy: "d304672a249293ee928d99d9cb0576403bdc4b6dbadeb49b98f527277297cdcc",
                fast: "421ef30a03be64014b9eef2b999795dcabfc601368040df855635e7886eb3822",
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
