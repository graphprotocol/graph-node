use std::{collections::HashMap, sync::Arc};

use anyhow::Error;
use graph::{
    blockchain::{self, block_stream::BlockWithTriggers, BlockPtr},
    components::{
        store::{DeploymentLocator, EntityType, SubgraphFork},
        subgraph::{MappingError, ProofOfIndexingEvent, SharedProofOfIndexing},
    },
    data::store::scalar::Bytes,
    prelude::{
        anyhow, async_trait, BigDecimal, BigInt, BlockNumber, BlockState, Entity, EntityKey,
        RuntimeHostBuilder, Value,
    },
    slog::Logger,
};
use lazy_static::__Deref;

use crate::{
    codec::{entity_change::Operation, field::Type},
    Block, Chain, DataSource, NodeCapabilities, NoopDataSourceTemplate,
};

#[derive(Eq, PartialEq, PartialOrd, Ord, Debug)]
pub struct TriggerData {}

impl blockchain::TriggerData for TriggerData {
    fn error_context(&self) -> String {
        unimplemented!()
    }
}

impl blockchain::MappingTrigger for TriggerData {
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
pub struct TriggerFilter {}

// TriggerFilter should bypass all triggers and just rely on block since all the data received
// should already have been processed.
impl blockchain::TriggerFilter<Chain> for TriggerFilter {
    fn extend_with_template(&mut self, _data_source: impl Iterator<Item = NoopDataSourceTemplate>) {
    }

    fn extend<'a>(&mut self, _data_sources: impl Iterator<Item = &'a DataSource> + Clone) {}

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

    async fn parent_ptr(&self, _block: &BlockPtr) -> Result<Option<BlockPtr>, Error> {
        unimplemented!()
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
        _trigger: &TriggerData,
        mut state: BlockState<Chain>,
        proof_of_indexing: &SharedProofOfIndexing,
        causality_region: &str,
        _debug_fork: &Option<Arc<dyn SubgraphFork>>,
        _subgraph_metrics: &Arc<graph::prelude::SubgraphInstanceMetrics>,
    ) -> Result<BlockState<Chain>, MappingError> {
        for entity_change in block.entities_changes.entity_changes.iter() {
            match entity_change.operation() {
                Operation::Unset => {
                    // Potentially an issue with the server side or
                    // we are running an outdated version. In either case we should abort.
                    return Err(MappingError::Unknown(anyhow!("Detected UNSET entity operation, either a server error or there's a new type of operation and we're running an outdated protobuf")));
                }
                Operation::Create | Operation::Update => {
                    let entity_type: &str = &entity_change.entity;
                    let entity_id: String = String::from_utf8(entity_change.id.clone())
                        .map_err(|e| MappingError::Unknown(anyhow::Error::from(e)))?;
                    let key = EntityKey {
                        subgraph_id: self.locator.hash.clone(),
                        entity_type: EntityType::new(entity_type.into()),
                        entity_id: entity_id.clone(),
                    };

                    let mut data: HashMap<String, Value> = HashMap::from_iter(vec![]);
                    for field in entity_change.fields.iter() {
                        let value: Value = match field.value_type() {
                            Type::Unset => {
                                return Err(MappingError::Unknown(anyhow!(
                                    "Invalid field type, the protobuf probably needs updating"
                                )))
                            }
                            Type::Bigdecimal => {
                                match BigDecimal::parse_bytes(field.new_value.as_ref()) {
                                    Some(bd) => Value::BigDecimal(bd),
                                    None => {
                                        return Err(MappingError::Unknown(anyhow!(
                                            "Unable to parse BigDecimal for entity {}",
                                            entity_change.entity
                                        )))
                                    }
                                }
                            }
                            Type::Bigint => Value::BigInt(BigInt::from_signed_bytes_be(
                                field.new_value.as_ref(),
                            )),
                            Type::Int => {
                                let mut bytes: [u8; 4] = [0; 4];
                                bytes.copy_from_slice(field.new_value.as_ref());
                                Value::Int(i32::from_be_bytes(bytes))
                            }
                            Type::Bytes => Value::Bytes(Bytes::from(field.new_value.as_ref())),
                            Type::String => Value::String(
                                String::from_utf8(field.new_value.clone())
                                    .map_err(|e| MappingError::Unknown(anyhow::Error::from(e)))?,
                            ),
                        };
                        *data.entry(field.name.clone()).or_insert(Value::Null) = value;
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
                    let key = EntityKey {
                        subgraph_id: self.locator.hash.clone(),
                        entity_type: EntityType::new(entity_type.into()),
                        entity_id: entity_id.clone(),
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
