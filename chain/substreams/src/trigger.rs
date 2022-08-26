use std::{collections::HashMap, sync::Arc};

use anyhow::Error;
use graph::{
    blockchain::{self, block_stream::BlockWithTriggers, BlockPtr},
    components::{
        store::{DeploymentLocator, EntityKey, SubgraphFork},
        subgraph::{MappingError, ProofOfIndexingEvent, SharedProofOfIndexing},
    },
    data::store::scalar::Bytes,
    data_source,
    prelude::{
        anyhow, async_trait, BigDecimal, BigInt, BlockHash, BlockNumber, BlockState, Entity,
        RuntimeHostBuilder, Value,
    },
    slog::Logger,
    substreams::Modules,
};
use graph_runtime_wasm::module::ToAscPtr;
use lazy_static::__Deref;

use crate::codec::Field;
use crate::{
    codec::{entity_change::Operation, field::Type},
    Block, Chain, NodeCapabilities, NoopDataSourceTemplate,
};

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
    ) -> Result<graph::runtime::AscPtr<()>, graph::runtime::DeterministicHostError> {
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

        if let Some(ref ds) = data_sources.next() {
            *data_sources_len = 1;
            *modules = ds.source.package.modules.clone();
            *module_name = ds.source.module_name.clone();
            *start_block = ds.initial_block;
        }
    }

    fn node_capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {}
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
    ) -> Result<BlockState<Chain>, MappingError> {
        for entity_change in block.entity_changes.iter() {
            match entity_change.operation() {
                Operation::Unset => {
                    // Potentially an issue with the server side or
                    // we are running an outdated version. In either case we should abort.
                    return Err(MappingError::Unknown(anyhow!("Detected UNSET entity operation, either a server error or there's a new type of operation and we're running an outdated protobuf")));
                }
                Operation::Create | Operation::Update => {
                    // TODO(filipe): Remove this once the substreams GRPC has been fixed.
                    let entity_type: &str = {
                        let letter: String = entity_change.entity[0..1].to_uppercase();
                        &(letter + &entity_change.entity[1..])
                    };
                    let entity_id: String = String::from_utf8(entity_change.id.clone())
                        .map_err(|e| MappingError::Unknown(anyhow::Error::from(e)))?;
                    let key = EntityKey::data(entity_type.to_string(), entity_id.clone());
                    let mut data: HashMap<String, Value> = HashMap::from_iter(vec![]);
                    for field in entity_change.fields.iter() {
                        let value: Value = decode_entity_change(&field, &entity_change.entity)?;
                        *data
                            .entry(field.name.as_str().to_owned())
                            .or_insert(Value::Null) = value;
                    }

                    write_poi_event(
                        proof_of_indexing,
                        &ProofOfIndexingEvent::SetEntity {
                            entity_type: &entity_type,
                            id: &entity_id,
                            data: &data,
                        },
                        causality_region,
                        logger,
                    );

                    state.entity_cache.set(key, Entity::from(data))?;
                }
                Operation::Delete => {
                    let entity_type: &str = &entity_change.entity;
                    let entity_id: String = String::from_utf8(entity_change.id.clone())
                        .map_err(|e| MappingError::Unknown(anyhow::Error::from(e)))?;
                    let key = EntityKey::data(entity_type.to_string(), entity_id.clone());

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

fn decode_entity_change(field: &Field, entity: &String) -> Result<Value, MappingError> {
    return match field.value_type() {
        Type::Unset => {
            return Err(MappingError::Unknown(anyhow!(
                "Invalid field type, the protobuf probably needs updating"
            )))
        }
        Type::Bigdecimal => match BigDecimal::parse_bytes(field.new_value.as_ref()) {
            Some(bd) => Ok(Value::BigDecimal(bd)),
            None => {
                return Err(MappingError::Unknown(anyhow!(
                    "Unable to parse BigDecimal for entity {}",
                    entity
                )))
            }
        },
        Type::Bigint => Ok(Value::BigInt(BigInt::from_signed_bytes_be(
            field.new_value.as_ref(),
        ))),
        Type::Int => {
            let mut bytes: [u8; 8] = [0; 8];
            bytes.copy_from_slice(field.new_value.as_ref());
            Ok(Value::Int(i64::from_be_bytes(bytes) as i32))
        }
        Type::Bytes => Ok(Value::Bytes(Bytes::from(field.new_value.as_ref()))),
        Type::String => Ok(Value::String(
            String::from_utf8(field.new_value.clone())
                .map_err(|e| MappingError::Unknown(anyhow::Error::from(e)))?,
        )),
    };
}

#[cfg(test)]
mod test {
    use crate::codec::field::Type as FieldType;
    use crate::codec::Field;
    use crate::trigger::decode_entity_change;
    use graph::prelude::Value;

    #[test]
    fn validate_substreams_field_types() {
        struct Case {
            field: Field,
            entity: String,
            expected_new_value: String,
        }

        let cases = vec![
            Case {
                field: Field {
                    name: "setting string value".to_string(),
                    value_type: FieldType::String as i32,
                    new_value: Vec::from(
                        "d4325ee72c39999e778a9908f5fb0803f78e30c441a5f2ce5c65eee0e0eba59d",
                    ),
                    new_value_null: false,
                    old_value: Vec::from("".to_string()),
                    old_value_null: true,
                },
                entity: "Block".to_string(),
                expected_new_value:
                    "d4325ee72c39999e778a9908f5fb0803f78e30c441a5f2ce5c65eee0e0eba59d".to_string(),
            },
            Case {
                field: Field {
                    name: "settings bytes value".to_string(),
                    value_type: FieldType::Bytes as i32,
                    new_value: hex::decode(
                        "445247fe150195bd866516594e087e1728294aa831613f4d48b8ec618908519f",
                    )
                    .unwrap(),
                    new_value_null: false,
                    old_value: Vec::from("".to_string()),
                    old_value_null: true,
                },
                entity: "Block".to_string(),
                expected_new_value:
                    "0x445247fe150195bd866516594e087e1728294aa831613f4d48b8ec618908519f".to_string(),
            },
            Case {
                field: Field {
                    name: "setting int value for block 12369760".to_string(),
                    value_type: FieldType::Int as i32,
                    new_value: hex::decode("0000000000bcbf60").unwrap(),
                    new_value_null: false,
                    old_value: vec![],
                    old_value_null: true,
                },
                entity: "Block".to_string(),
                expected_new_value: "12369760".to_string(),
            },
            Case {
                field: Field {
                    name: "setting int value for block 12369622".to_string(),
                    value_type: FieldType::Int as i32,
                    new_value: hex::decode("0000000000bcbed6").unwrap(),
                    new_value_null: false,
                    old_value: vec![],
                    old_value_null: true,
                },
                entity: "Block".to_string(),
                expected_new_value: "12369622".to_string(),
            },
            Case {
                field: Field {
                    name: "setting int value for block 12369623".to_string(),
                    value_type: FieldType::Int as i32,
                    new_value: hex::decode("0000000000bcbed7").unwrap(),
                    new_value_null: false,
                    old_value: vec![],
                    old_value_null: true,
                },
                entity: "Block".to_string(),
                expected_new_value: "12369623".to_string(),
            },
            Case {
                field: Field {
                    name: "setting big int transactions count of 123".to_string(),
                    value_type: FieldType::Bigint as i32,
                    new_value: hex::decode("7b").unwrap(),
                    new_value_null: false,
                    old_value: vec![],
                    old_value_null: true,
                },
                entity: "Block".to_string(),
                expected_new_value: "123".to_string(),
            },
            Case {
                field: Field {
                    name: "setting big int transactions count of 302".to_string(),
                    value_type: FieldType::Bigint as i32,
                    new_value: hex::decode("012e").unwrap(),
                    new_value_null: false,
                    old_value: vec![],
                    old_value_null: true,
                },
                entity: "Block".to_string(),
                expected_new_value: "302".to_string(),
            },
            Case {
                field: Field {
                    name: "setting big int transactions count of 209".to_string(),
                    value_type: FieldType::Bigint as i32,
                    new_value: hex::decode("00d1").unwrap(),
                    new_value_null: false,
                    old_value: vec![],
                    old_value_null: true,
                },
                entity: "Block".to_string(),
                expected_new_value: "209".to_string(),
            },
            Case {
                field: Field {
                    name: "setting big decimal value".to_string(),
                    value_type: FieldType::Bigdecimal as i32,
                    new_value: hex::decode("3133363633312e35").unwrap(),
                    new_value_null: false,
                    old_value: vec![],
                    old_value_null: true,
                },
                entity: "Block".to_string(),
                expected_new_value: "136631.5".to_string(),
            },
            Case {
                field: Field {
                    name: "setting big decimal value 2".to_string(),
                    value_type: FieldType::Bigdecimal as i32,
                    new_value: hex::decode("3133303730392e30").unwrap(),
                    new_value_null: false,
                    old_value: vec![],
                    old_value_null: true,
                },
                entity: "Block".to_string(),
                expected_new_value: "130709.0".to_string(),
            },
            Case {
                field: Field {
                    name: "setting big decimal value 3".to_string(),
                    value_type: FieldType::Bigdecimal as i32,
                    new_value: hex::decode("39373839322e36").unwrap(),
                    new_value_null: false,
                    old_value: vec![],
                    old_value_null: true,
                },
                entity: "Block".to_string(),
                expected_new_value: "97892.6".to_string(),
            },
        ];

        for case in cases.into_iter() {
            let value: Value = decode_entity_change(&case.field, &case.entity).unwrap();
            let mut actual_value: String = String::new();
            match case.field.value_type {
                1 => actual_value = value.as_big_decimal().unwrap().to_string(),
                2 => actual_value = value.as_bigint().unwrap().to_string(),
                3 => actual_value = value.as_int().unwrap().to_string(),
                4 => actual_value = value.as_bytes().unwrap().to_string(),
                5 => actual_value = value.as_string().unwrap(),
                _ => {}
            }
            assert_eq!(case.expected_new_value.as_str(), actual_value)
        }
    }
}
