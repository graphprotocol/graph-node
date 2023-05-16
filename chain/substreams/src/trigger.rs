use std::{collections::HashMap, str::FromStr, sync::Arc};

use anyhow::Error;
use graph::{
    blockchain::{self, block_stream::BlockWithTriggers, BlockPtr, EmptyNodeCapabilities},
    components::{
        store::{DeploymentLocator, EntityKey, EntityType, SubgraphFork},
        subgraph::{MappingError, ProofOfIndexingEvent, SharedProofOfIndexing},
    },
    data::{store::scalar::Bytes, value::Word},
    data_source::{self, CausalityRegion},
    prelude::{
        anyhow, async_trait, BigDecimal, BigInt, BlockHash, BlockNumber, BlockState,
        RuntimeHostBuilder, Value,
    },
    slog::Logger,
    substreams::Modules,
};
use graph_runtime_wasm::module::ToAscPtr;
use lazy_static::__Deref;

use crate::codec;
use crate::{codec::entity_change::Operation, Block, Chain, NoopDataSourceTemplate};

#[derive(Eq, PartialEq, PartialOrd, Ord, Debug)]
pub struct TriggerData {}

impl blockchain::TriggerData for TriggerData {
    // TODO(filipe): Can this be improved with some data from the block?
    fn error_context(&self) -> String {
        "Failed to process substreams block".to_string()
    }
}

impl ToAscPtr for TriggerData {
    // substreams doesn't rely on wasm on the graph-node so this is not needed.
    fn to_asc_ptr<H: graph::runtime::AscHeap>(
        self,
        _heap: &mut H,
        _gas: &graph::runtime::gas::GasCounter,
    ) -> Result<graph::runtime::AscPtr<()>, graph::runtime::HostExportError> {
        unimplemented!()
    }
}

#[derive(Debug, Clone, Default)]
pub struct TriggerFilter {
    pub(crate) modules: Option<Modules>,
    pub(crate) module_name: String,
    pub(crate) start_block: Option<BlockNumber>,
    pub(crate) data_sources_len: u8,
}

// TriggerFilter should bypass all triggers and just rely on block since all the data received
// should already have been processed.
impl blockchain::TriggerFilter<Chain> for TriggerFilter {
    fn extend_with_template(&mut self, _data_source: impl Iterator<Item = NoopDataSourceTemplate>) {
    }

    /// this function is not safe to call multiple times, only one DataSource is supported for
    ///
    fn extend<'a>(
        &mut self,
        mut data_sources: impl Iterator<Item = &'a crate::DataSource> + Clone,
    ) {
        let Self {
            modules,
            module_name,
            start_block,
            data_sources_len,
        } = self;

        if *data_sources_len >= 1 {
            return;
        }

        if let Some(ds) = data_sources.next() {
            *data_sources_len = 1;
            *modules = ds.source.package.modules.clone();
            *module_name = ds.source.module_name.clone();
            *start_block = ds.initial_block;
        }
    }

    fn node_capabilities(&self) -> EmptyNodeCapabilities<Chain> {
        EmptyNodeCapabilities::default()
    }

    fn to_firehose_filter(self) -> Vec<prost_types::Any> {
        unimplemented!("this should never be called for this type")
    }
}

pub struct TriggersAdapter {}

#[async_trait]
impl blockchain::TriggersAdapter<Chain> for TriggersAdapter {
    async fn ancestor_block(
        &self,
        _ptr: BlockPtr,
        _offset: BlockNumber,
    ) -> Result<Option<Block>, Error> {
        unimplemented!()
    }

    async fn scan_triggers(
        &self,
        _from: BlockNumber,
        _to: BlockNumber,
        _filter: &TriggerFilter,
    ) -> Result<Vec<BlockWithTriggers<Chain>>, Error> {
        unimplemented!()
    }

    async fn triggers_in_block(
        &self,
        _logger: &Logger,
        _block: Block,
        _filter: &TriggerFilter,
    ) -> Result<BlockWithTriggers<Chain>, Error> {
        unimplemented!()
    }

    async fn is_on_main_chain(&self, _ptr: BlockPtr) -> Result<bool, Error> {
        unimplemented!()
    }

    async fn parent_ptr(&self, block: &BlockPtr) -> Result<Option<BlockPtr>, Error> {
        // This seems to work for a lot of the firehose chains.
        Ok(Some(BlockPtr {
            hash: BlockHash::from(vec![0xff; 32]),
            number: block.number.saturating_sub(1),
        }))
    }
}

fn write_poi_event(
    proof_of_indexing: &SharedProofOfIndexing,
    poi_event: &ProofOfIndexingEvent,
    causality_region: &str,
    logger: &Logger,
) {
    if let Some(proof_of_indexing) = proof_of_indexing {
        let mut proof_of_indexing = proof_of_indexing.deref().borrow_mut();
        proof_of_indexing.write(logger, causality_region, poi_event);
    }
}

pub struct TriggerProcessor {
    pub locator: DeploymentLocator,
}

impl TriggerProcessor {
    pub fn new(locator: DeploymentLocator) -> Self {
        Self { locator }
    }
}

#[async_trait]
impl<T> graph::prelude::TriggerProcessor<Chain, T> for TriggerProcessor
where
    T: RuntimeHostBuilder<Chain>,
{
    async fn process_trigger(
        &self,
        logger: &Logger,
        _hosts: &[Arc<T::Host>],
        block: &Arc<Block>,
        _trigger: &data_source::TriggerData<Chain>,
        mut state: BlockState<Chain>,
        proof_of_indexing: &SharedProofOfIndexing,
        causality_region: &str,
        _debug_fork: &Option<Arc<dyn SubgraphFork>>,
        _subgraph_metrics: &Arc<graph::prelude::SubgraphInstanceMetrics>,
        _instrument: bool,
    ) -> Result<BlockState<Chain>, MappingError> {
        for entity_change in block.changes.entity_changes.iter() {
            match entity_change.operation() {
                Operation::Unset => {
                    // Potentially an issue with the server side or
                    // we are running an outdated version. In either case we should abort.
                    return Err(MappingError::Unknown(anyhow!("Detected UNSET entity operation, either a server error or there's a new type of operation and we're running an outdated protobuf")));
                }
                Operation::Create | Operation::Update => {
                    let entity_type: &str = &entity_change.entity;
                    let entity_id: String = entity_change.id.clone();
                    let key = EntityKey {
                        entity_type: EntityType::new(entity_type.to_string()),
                        entity_id: entity_id.clone().into(),
                        causality_region: CausalityRegion::ONCHAIN, // Substreams don't currently support offchain data
                    };
                    let mut data: HashMap<Word, Value> = HashMap::from_iter(vec![]);

                    for field in entity_change.fields.iter() {
                        let new_value: &codec::value::Typed = match &field.new_value {
                            Some(codec::Value {
                                typed: Some(new_value),
                            }) => new_value,
                            _ => continue,
                        };

                        let value: Value = decode_value(new_value)?;
                        *data
                            .entry(Word::from(field.name.clone()))
                            .or_insert(Value::Null) = value;
                    }

                    write_poi_event(
                        proof_of_indexing,
                        &ProofOfIndexingEvent::SetEntity {
                            entity_type,
                            id: &entity_id,
                            // TODO: This should be an entity so we do not have to build the intermediate HashMap
                            data: &data,
                        },
                        causality_region,
                        logger,
                    );

                    let id = state.entity_cache.schema.id_value(&key)?;
                    data.insert(Word::from("id"), id);

                    let entity = state.entity_cache.make_entity(data)?;
                    state.entity_cache.set(key, entity)?;
                }
                Operation::Delete => {
                    let entity_type: &str = &entity_change.entity;
                    let entity_id: String = entity_change.id.clone();
                    let key = EntityKey {
                        entity_type: EntityType::new(entity_type.to_string()),
                        entity_id: entity_id.clone().into(),
                        causality_region: CausalityRegion::ONCHAIN, // Substreams don't currently support offchain data
                    };

                    state.entity_cache.remove(key);

                    write_poi_event(
                        proof_of_indexing,
                        &ProofOfIndexingEvent::RemoveEntity {
                            entity_type,
                            id: &entity_id,
                        },
                        causality_region,
                        logger,
                    )
                }
            }
        }

        Ok(state)
    }
}

fn decode_value(value: &crate::codec::value::Typed) -> Result<Value, MappingError> {
    use codec::value::Typed;

    match value {
        Typed::Int32(new_value) => Ok(Value::Int(*new_value)),

        Typed::Bigdecimal(new_value) => BigDecimal::from_str(new_value)
            .map(Value::BigDecimal)
            .map_err(|err| MappingError::Unknown(anyhow::Error::from(err))),

        Typed::Bigint(new_value) => BigInt::from_str(new_value)
            .map(Value::BigInt)
            .map_err(|err| MappingError::Unknown(anyhow::Error::from(err))),

        Typed::String(new_value) => {
            let mut string = new_value.clone();

            // Strip null characters since they are not accepted by Postgres.
            if string.contains('\u{0000}') {
                string = string.replace('\u{0000}', "");
            }
            Ok(Value::String(string))
        }

        Typed::Bytes(new_value) => base64::decode(new_value)
            .map(|bs| Value::Bytes(Bytes::from(bs)))
            .map_err(|err| MappingError::Unknown(anyhow::Error::from(err))),

        Typed::Bool(new_value) => Ok(Value::Bool(*new_value)),

        Typed::Array(arr) => arr
            .value
            .iter()
            .filter_map(|item| item.typed.as_ref().map(decode_value))
            .collect::<Result<Vec<Value>, MappingError>>()
            .map(Value::List),
    }
}

#[cfg(test)]
mod test {
    use std::{ops::Add, str::FromStr};

    use crate::codec::value::Typed;
    use crate::codec::{Array, Value};
    use crate::trigger::decode_value;
    use graph::{
        data::store::scalar::Bytes,
        prelude::{BigDecimal, BigInt, Value as GraphValue},
    };

    #[test]
    fn validate_substreams_field_types() {
        struct Case {
            name: String,
            value: Value,
            expected_value: GraphValue,
        }

        let cases = vec![
            Case {
                name: "string value".to_string(),
                value: Value {
                    typed: Some(Typed::String(
                        "d4325ee72c39999e778a9908f5fb0803f78e30c441a5f2ce5c65eee0e0eba59d"
                            .to_string(),
                    )),
                },
                expected_value: GraphValue::String(
                    "d4325ee72c39999e778a9908f5fb0803f78e30c441a5f2ce5c65eee0e0eba59d".to_string(),
                ),
            },
            Case {
                name: "bytes value".to_string(),
                value: Value {
                    typed: Some(Typed::Bytes(
                        base64::encode(
                            hex::decode(
                                "445247fe150195bd866516594e087e1728294aa831613f4d48b8ec618908519f",
                            )
                            .unwrap(),
                        )
                        .into_bytes(),
                    )),
                },
                expected_value: GraphValue::Bytes(
                    Bytes::from_str(
                        "0x445247fe150195bd866516594e087e1728294aa831613f4d48b8ec618908519f",
                    )
                    .unwrap(),
                ),
            },
            Case {
                name: "int value for block".to_string(),
                value: Value {
                    typed: Some(Typed::Int32(12369760)),
                },
                expected_value: GraphValue::Int(12369760),
            },
            Case {
                name: "negative int value".to_string(),
                value: Value {
                    typed: Some(Typed::Int32(-12369760)),
                },
                expected_value: GraphValue::Int(-12369760),
            },
            Case {
                name: "big int".to_string(),
                value: Value {
                    typed: Some(Typed::Bigint("123".to_string())),
                },
                expected_value: GraphValue::BigInt(BigInt::from(123u64)),
            },
            Case {
                name: "big int > u64".to_string(),
                value: Value {
                    typed: Some(Typed::Bigint(
                        BigInt::from(u64::MAX).add(BigInt::from(1)).to_string(),
                    )),
                },
                expected_value: GraphValue::BigInt(BigInt::from(u64::MAX).add(BigInt::from(1))),
            },
            Case {
                name: "big decimal value".to_string(),
                value: Value {
                    typed: Some(Typed::Bigdecimal("3133363633312e35".to_string())),
                },
                expected_value: GraphValue::BigDecimal(BigDecimal::new(
                    BigInt::from(3133363633312u64),
                    35,
                )),
            },
            Case {
                name: "bool value".to_string(),
                value: Value {
                    typed: Some(Typed::Bool(true)),
                },
                expected_value: GraphValue::Bool(true),
            },
            Case {
                name: "string array".to_string(),
                value: Value {
                    typed: Some(Typed::Array(Array {
                        value: vec![
                            Value {
                                typed: Some(Typed::String("1".to_string())),
                            },
                            Value {
                                typed: Some(Typed::String("2".to_string())),
                            },
                            Value {
                                typed: Some(Typed::String("3".to_string())),
                            },
                        ],
                    })),
                },
                expected_value: GraphValue::List(vec!["1".into(), "2".into(), "3".into()]),
            },
        ];

        for case in cases.into_iter() {
            let value: GraphValue = decode_value(&case.value.typed.unwrap()).unwrap();
            assert_eq!(case.expected_value, value, "failed case: {}", case.name)
        }
    }
}
