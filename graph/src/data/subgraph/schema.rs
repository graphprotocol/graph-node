//! See `subgraphs.graphql` in the store for corresponding graphql schema.

use super::SubgraphId;
use components::store::{EntityKey, EntityOperation};
use data::store::{Entity, Value};
use std::collections::HashMap;

/// ID of the subgraph of subgraphs.
lazy_static! {
    pub static ref SUBGRAPHS_ID: SubgraphId = SubgraphId::new("subgraphs").unwrap();
}

/// Type name of the root entity in the subgraph of subgraphs.
pub const SUBGRAPH_ENTITY_TYPENAME: &str = "Subgraph";

/// Type name of manifests in the subgraph of subgraphs.
pub const MANIFEST_ENTITY_TYPENAME: &str = "SubgraphManifest";

#[derive(Debug)]
pub struct SubgraphEntity {
    id: SubgraphId,
    manifest: SubgraphManifestEntity,
    created_at: u64,
}

impl SubgraphEntity {
    pub fn new(source_manifest: &super::SubgraphManifest, created_at: u64) -> Self {
        Self {
            id: source_manifest.id.clone(),
            manifest: SubgraphManifestEntity::from(source_manifest),
            created_at,
        }
    }

    pub fn write_operations(self) -> Vec<EntityOperation> {
        let mut ops = vec![];

        let manifest_id = SubgraphManifestEntity::id(&self.id);
        ops.append(&mut self.manifest.write_operations(&manifest_id));

        let mut entity = HashMap::new();
        entity.insert("id".to_owned(), self.id.to_string().into());
        entity.insert("manifest".to_owned(), manifest_id.into());
        entity.insert("createdAt".to_owned(), self.created_at.into());
        ops.push(set_entity_operation(
            SUBGRAPH_ENTITY_TYPENAME,
            self.id.to_string(),
            entity,
        ));

        ops
    }
}

#[derive(Debug)]
pub struct SubgraphManifestEntity {
    spec_version: String,
    description: Option<String>,
    repository: Option<String>,
    schema: String,
    data_sources: Vec<EthereumContractDataSourceEntity>,
}

impl SubgraphManifestEntity {
    pub fn id(subgraph_id: &SubgraphId) -> String {
        format!("{}-manifest", subgraph_id)
    }

    fn write_operations(self, id: &str) -> Vec<EntityOperation> {
        let mut ops = vec![];

        let mut data_source_ids: Vec<Value> = vec![];
        for (i, data_source) in self.data_sources.into_iter().enumerate() {
            let data_source_id = format!("{}-data-source-{}", id, i);
            ops.append(&mut data_source.write_operations(&data_source_id));
            data_source_ids.push(data_source_id.into());
        }

        let mut entity = HashMap::new();
        entity.insert("id".to_owned(), id.clone().into());
        entity.insert("specVersion".to_owned(), self.spec_version.into());
        entity.insert("description".to_owned(), self.description.into());
        entity.insert("repository".to_owned(), self.repository.into());
        entity.insert("schema".to_owned(), self.schema.into());
        entity.insert("dataSources".to_owned(), data_source_ids.into());
        ops.push(set_entity_operation("SubgraphManifest", id, entity));

        ops
    }
}

impl<'a> From<&'a super::SubgraphManifest> for SubgraphManifestEntity {
    fn from(manifest: &'a super::SubgraphManifest) -> Self {
        Self {
            spec_version: manifest.spec_version.clone(),
            description: manifest.description.clone(),
            repository: manifest.repository.clone(),
            schema: manifest.schema.document.clone().to_string(),
            data_sources: manifest.data_sources.iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug)]
struct EthereumContractDataSourceEntity {
    kind: String,
    network: Option<String>,
    name: String,
    source: EthereumContractSourceEntity,
    mapping: EthereumContractMappingEntity,
}

impl EthereumContractDataSourceEntity {
    fn write_operations(self, id: &str) -> Vec<EntityOperation> {
        let mut ops = vec![];

        let source_id = format!("{}-source", id);
        ops.append(&mut self.source.write_operations(&source_id));

        let mapping_id = format!("{}-mapping", id);
        ops.append(&mut self.mapping.write_operations(&mapping_id));

        let mut entity = HashMap::new();
        entity.insert("id".to_owned(), id.clone().into());
        entity.insert("kind".to_owned(), self.kind.into());
        entity.insert("network".to_owned(), self.network.into());
        entity.insert("name".to_owned(), self.name.into());
        entity.insert("source".to_owned(), source_id.into());
        entity.insert("mapping".to_owned(), mapping_id.into());
        ops.push(set_entity_operation(
            "EthereumContractDataSource",
            id,
            entity,
        ));

        ops
    }
}

impl<'a> From<&'a super::DataSource> for EthereumContractDataSourceEntity {
    fn from(data_source: &'a super::DataSource) -> Self {
        Self {
            kind: data_source.kind.clone(),
            name: data_source.name.clone(),
            network: data_source.network.clone(),
            source: data_source.source.clone().into(),
            mapping: EthereumContractMappingEntity::from(&data_source.mapping),
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
struct EthereumContractSourceEntity {
    address: super::Address,
    abi: String,
}

impl EthereumContractSourceEntity {
    fn write_operations(self, id: &str) -> Vec<EntityOperation> {
        let mut entity = HashMap::new();
        entity.insert("id".to_owned(), id.clone().into());
        entity.insert("address".to_owned(), self.address.into());
        entity.insert("abi".to_owned(), self.abi.into());
        vec![set_entity_operation("EthereumContractSource", id, entity)]
    }
}

impl From<super::Source> for EthereumContractSourceEntity {
    fn from(source: super::Source) -> Self {
        Self {
            address: source.address,
            abi: source.abi,
        }
    }
}

#[derive(Debug)]
struct EthereumContractMappingEntity {
    kind: String,
    api_version: String,
    language: String,
    file: String,
    entities: Vec<String>,
    abis: Vec<EthereumContractAbiEntity>,
    event_handlers: Vec<EthereumContractEventHandlerEntity>,
}

impl EthereumContractMappingEntity {
    fn write_operations(self, id: &str) -> Vec<EntityOperation> {
        let mut ops = vec![];

        let mut abi_ids: Vec<Value> = vec![];
        for (i, abi) in self.abis.into_iter().enumerate() {
            let abi_id = format!("{}-abi-{}", id, i);
            ops.append(&mut abi.write_operations(&abi_id));
            abi_ids.push(abi_id.into());
        }

        let mut event_handler_ids: Vec<Value> = vec![];
        for (i, event_handler) in self.event_handlers.into_iter().enumerate() {
            let handler_id = format!("{}-event-handler-{}", id, i);
            ops.append(&mut event_handler.write_operations(&handler_id));
            event_handler_ids.push(handler_id.into());
        }

        let mut entity = HashMap::new();
        entity.insert("id".to_owned(), id.clone().into());
        entity.insert("kind".to_owned(), self.kind.into());
        entity.insert("apiVersion".to_owned(), self.api_version.into());
        entity.insert("language".to_owned(), self.language.into());
        entity.insert("file".to_owned(), self.file.into());
        entity.insert("abis".to_owned(), abi_ids.into());
        entity.insert(
            "entities".to_owned(),
            self.entities
                .into_iter()
                .map(Value::from)
                .collect::<Vec<Value>>()
                .into(),
        );
        entity.insert("eventHandlers".to_owned(), event_handler_ids.into());
        ops.push(set_entity_operation("EthereumContractMapping", id, entity));

        ops
    }
}

impl<'a> From<&'a super::Mapping> for EthereumContractMappingEntity {
    fn from(mapping: &'a super::Mapping) -> Self {
        Self {
            kind: mapping.kind.clone(),
            api_version: mapping.api_version.clone(),
            language: mapping.language.clone(),
            file: mapping.link.link.clone(),
            entities: mapping.entities.clone(),
            abis: mapping.abis.iter().map(Into::into).collect(),
            event_handlers: mapping
                .event_handlers
                .clone()
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}

#[derive(Debug)]
struct EthereumContractAbiEntity {
    name: String,
    file: String,
}

impl EthereumContractAbiEntity {
    fn write_operations(self, id: &str) -> Vec<EntityOperation> {
        let mut entity = HashMap::new();
        entity.insert("id".to_owned(), id.clone().into());
        entity.insert("name".to_owned(), self.name.into());
        entity.insert("file".to_owned(), self.file.into());
        vec![set_entity_operation("EthereumContractAbi", id, entity)]
    }
}

impl<'a> From<&'a super::MappingABI> for EthereumContractAbiEntity {
    fn from(abi: &'a super::MappingABI) -> Self {
        Self {
            name: abi.name.clone(),
            file: abi.link.link.clone(),
        }
    }
}

#[derive(Debug)]
struct EthereumContractEventHandlerEntity {
    event: String,
    handler: String,
}

impl EthereumContractEventHandlerEntity {
    fn write_operations(self, id: &str) -> Vec<EntityOperation> {
        let mut entity = HashMap::new();
        entity.insert("id".to_owned(), id.clone().into());
        entity.insert("event".to_owned(), self.event.into());
        entity.insert("handler".to_owned(), self.handler.into());
        vec![set_entity_operation(
            "EthereumContractEventHandler",
            id,
            entity,
        )]
    }
}

impl From<super::MappingEventHandler> for EthereumContractEventHandlerEntity {
    fn from(event_handler: super::MappingEventHandler) -> Self {
        Self {
            event: event_handler.event,
            handler: event_handler.handler,
        }
    }
}

fn set_entity_operation(
    entity_type_name: impl Into<String>,
    entity_id: impl Into<String>,
    data: impl Into<Entity>,
) -> EntityOperation {
    EntityOperation::Set {
        key: EntityKey {
            subgraph_id: SUBGRAPHS_ID.clone(),
            entity_type: entity_type_name.into(),
            entity_id: entity_id.into(),
        },
        data: data.into(),
    }
}
