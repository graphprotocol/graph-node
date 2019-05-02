//! Entity types that contain the graph-node state.
//!
//! Entity type methods follow these naming conventions:
//! - `*_operations`: Method does not have any side effects, but returns a sequence of operations
//!   to be provided to `Store::apply_entity_operations`.
//! - `create_*_operations`: Create an entity, unless the entity already exists (in which case the
//!   transaction is aborted).
//! - `update_*_operations`: Update an entity, unless the entity does not exist (in which case the
//!   transaction is aborted).
//! - `write_*_operations`: Create an entity or update an existing entity.
//!
//! See `subgraphs.graphql` in the store for corresponding graphql schema.

use failure::Error;
use graphql_parser::query as q;
use graphql_parser::schema::{Definition, Document, Type, TypeDefinition};
use hex;
use rand::rngs::OsRng;
use rand::Rng;
use std::str::FromStr;
use web3::types::*;

use super::SubgraphDeploymentId;
use crate::components::ethereum::EthereumBlockPointer;
use crate::components::store::{
    AttributeIndexDefinition, EntityFilter, EntityKey, EntityOperation, EntityQuery, EntityRange,
};
use crate::data::graphql::{TryFromValue, ValueMap};
use crate::data::store::{Entity, NodeId, SubgraphEntityPair, Value, ValueType};
use crate::data::subgraph::{SubgraphManifest, SubgraphName};

/// ID of the subgraph of subgraphs.
lazy_static! {
    pub static ref SUBGRAPHS_ID: SubgraphDeploymentId =
        SubgraphDeploymentId::new("subgraphs").unwrap();
}

/// Generic type for the entity types defined below.
pub trait TypedEntity {
    const TYPENAME: &'static str;
    type IdType: ToString;

    fn query() -> EntityQuery {
        let range = EntityRange {
            first: None,
            skip: 0,
        };
        EntityQuery::new(SUBGRAPHS_ID.clone(), vec![Self::TYPENAME.to_owned()], range)
    }

    fn subgraph_entity_pair() -> SubgraphEntityPair {
        (SUBGRAPHS_ID.clone(), Self::TYPENAME.to_owned())
    }

    fn key(entity_id: Self::IdType) -> EntityKey {
        let (subgraph_id, entity_type) = Self::subgraph_entity_pair();
        EntityKey {
            subgraph_id,
            entity_type,
            entity_id: entity_id.to_string(),
        }
    }
}

#[derive(Debug)]
pub struct SubgraphEntity {
    name: SubgraphName,
    current_version_id: Option<String>,
    pending_version_id: Option<String>,
    created_at: u64,
}

impl TypedEntity for SubgraphEntity {
    const TYPENAME: &'static str = "Subgraph";
    type IdType = String;
}

impl SubgraphEntity {
    pub fn new(
        name: SubgraphName,
        current_version_id: Option<String>,
        pending_version_id: Option<String>,
        created_at: u64,
    ) -> SubgraphEntity {
        SubgraphEntity {
            name: name,
            current_version_id,
            pending_version_id,
            created_at,
        }
    }

    pub fn write_operations(self, id: &str) -> Vec<EntityOperation> {
        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("name", self.name.to_string());
        entity.set("currentVersion", self.current_version_id);
        entity.set("pendingVersion", self.pending_version_id);
        entity.set("createdAt", self.created_at);
        vec![set_entity_operation(Self::TYPENAME, id, entity)]
    }

    pub fn update_current_version_operations(
        id: &str,
        version_id_opt: Option<String>,
    ) -> Vec<EntityOperation> {
        let mut entity = Entity::new();
        entity.set("currentVersion", version_id_opt);

        vec![EntityOperation::Update {
            key: Self::key(id.to_owned()),
            data: entity,
            guard: None,
        }]
    }

    pub fn update_pending_version_operations(
        id: &str,
        version_id_opt: Option<String>,
    ) -> Vec<EntityOperation> {
        let mut entity = Entity::new();
        entity.set("pendingVersion", version_id_opt);

        vec![EntityOperation::Update {
            key: Self::key(id.to_owned()),
            data: entity,
            guard: None,
        }]
    }
}

#[derive(Debug)]
pub struct SubgraphVersionEntity {
    subgraph_id: String,
    deployment_id: SubgraphDeploymentId,
    created_at: u64,
}

impl TypedEntity for SubgraphVersionEntity {
    const TYPENAME: &'static str = "SubgraphVersion";
    type IdType = String;
}

impl SubgraphVersionEntity {
    pub fn new(subgraph_id: String, deployment_id: SubgraphDeploymentId, created_at: u64) -> Self {
        Self {
            subgraph_id,
            deployment_id,
            created_at,
        }
    }

    pub fn write_operations(self, id: &str) -> Vec<EntityOperation> {
        let mut entity = Entity::new();
        entity.set("id", id.to_owned());
        entity.set("subgraph", self.subgraph_id);
        entity.set("deployment", self.deployment_id.to_string());
        entity.set("createdAt", self.created_at);
        vec![set_entity_operation(Self::TYPENAME, id, entity)]
    }
}

#[derive(Debug)]
pub struct SubgraphDeploymentEntity {
    manifest: SubgraphManifestEntity,
    failed: bool,
    synced: bool,
    latest_ethereum_block_hash: H256,
    latest_ethereum_block_number: u64,
    total_ethereum_blocks_count: u64,
}

impl TypedEntity for SubgraphDeploymentEntity {
    const TYPENAME: &'static str = "SubgraphDeployment";
    type IdType = SubgraphDeploymentId;
}

impl SubgraphDeploymentEntity {
    pub fn new(
        source_manifest: &SubgraphManifest,
        failed: bool,
        synced: bool,
        latest_ethereum_block: EthereumBlockPointer,
        total_ethereum_blocks_count: u64,
    ) -> Self {
        Self {
            manifest: SubgraphManifestEntity::from(source_manifest),
            failed,
            synced,
            latest_ethereum_block_hash: latest_ethereum_block.hash,
            latest_ethereum_block_number: latest_ethereum_block.number,
            total_ethereum_blocks_count,
        }
    }

    // Overwrite entity if it exists. Only in debug builds so it's not used outside tests.
    #[cfg(debug_assertions)]
    pub fn create_operations_replace(self, id: &SubgraphDeploymentId) -> Vec<EntityOperation> {
        self.private_create_operations(id)
    }

    pub fn create_operations(self, id: &SubgraphDeploymentId) -> Vec<EntityOperation> {
        let mut ops = vec![];

        // Abort unless no entity exists with this ID
        ops.push(EntityOperation::AbortUnless {
            description: "Subgraph deployment entity must not exist yet to be created".to_owned(),
            query: Self::query().filter(EntityFilter::new_equal("id", id.to_string())),
            entity_ids: vec![],
        });

        ops.extend(self.private_create_operations(id));
        ops
    }

    fn private_create_operations(self, id: &SubgraphDeploymentId) -> Vec<EntityOperation> {
        let mut ops = vec![];

        let manifest_id = SubgraphManifestEntity::id(&id);
        ops.extend(self.manifest.write_operations(&manifest_id));

        let mut entity = Entity::new();
        entity.set("id", id.to_string());
        entity.set("manifest", manifest_id);
        entity.set("failed", self.failed);
        entity.set("synced", self.synced);
        entity.set(
            "latestEthereumBlockHash",
            format!("{:x}", self.latest_ethereum_block_hash),
        );
        entity.set(
            "latestEthereumBlockNumber",
            self.latest_ethereum_block_number,
        );
        entity.set("totalEthereumBlocksCount", self.total_ethereum_blocks_count);
        ops.push(set_entity_operation(Self::TYPENAME, id.to_string(), entity));

        ops
    }

    pub fn update_ethereum_block_pointer_operations(
        id: &SubgraphDeploymentId,
        block_ptr_from: EthereumBlockPointer,
        block_ptr_to: EthereumBlockPointer,
    ) -> Vec<EntityOperation> {
        let mut entity = Entity::new();
        entity.set("latestEthereumBlockHash", block_ptr_to.hash_hex());
        entity.set("latestEthereumBlockNumber", block_ptr_to.number);

        let guard = EntityFilter::And(vec![
            EntityFilter::new_equal("latestEthereumBlockHash", block_ptr_from.hash_hex()),
            EntityFilter::new_equal("latestEthereumBlockNumber", block_ptr_from.number),
        ]);
        vec![EntityOperation::Update {
            key: Self::key(id.clone()),
            data: entity,
            guard: Some(guard),
        }]
    }

    pub fn update_ethereum_blocks_count_operations(
        id: &SubgraphDeploymentId,
        total_blocks_count: u64,
    ) -> Vec<EntityOperation> {
        let mut entity = Entity::new();
        entity.set("totalEthereumBlocksCount", total_blocks_count);

        vec![EntityOperation::Update {
            key: Self::key(id.clone()),
            data: entity,
            guard: None,
        }]
    }

    pub fn update_failed_operations(
        id: &SubgraphDeploymentId,
        failed: bool,
    ) -> Vec<EntityOperation> {
        let mut entity = Entity::new();
        entity.set("failed", failed);

        vec![EntityOperation::Update {
            key: Self::key(id.clone()),
            data: entity,
            guard: None,
        }]
    }

    pub fn update_synced_operations(
        id: &SubgraphDeploymentId,
        synced: bool,
    ) -> Vec<EntityOperation> {
        let mut entity = Entity::new();
        entity.set("synced", synced);

        vec![EntityOperation::Update {
            key: Self::key(id.clone()),
            data: entity,
            guard: None,
        }]
    }
}

#[derive(Debug)]
pub struct SubgraphDeploymentAssignmentEntity {
    node_id: NodeId,
    cost: u64,
}

impl TypedEntity for SubgraphDeploymentAssignmentEntity {
    const TYPENAME: &'static str = "SubgraphDeploymentAssignment";
    type IdType = SubgraphDeploymentId;
}

impl SubgraphDeploymentAssignmentEntity {
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id, cost: 1 }
    }

    pub fn write_operations(self, id: &SubgraphDeploymentId) -> Vec<EntityOperation> {
        let mut entity = Entity::new();
        entity.set("id", id.to_string());
        entity.set("nodeId", self.node_id.to_string());
        entity.set("cost", self.cost);
        vec![set_entity_operation(Self::TYPENAME, id.to_string(), entity)]
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

impl TypedEntity for SubgraphManifestEntity {
    const TYPENAME: &'static str = "SubgraphManifest";
    type IdType = String;
}

impl SubgraphManifestEntity {
    pub fn id(subgraph_id: &SubgraphDeploymentId) -> String {
        format!("{}-manifest", subgraph_id)
    }

    fn write_operations(self, id: &str) -> Vec<EntityOperation> {
        let mut ops = vec![];

        let mut data_source_ids: Vec<Value> = vec![];
        for (i, data_source) in self.data_sources.into_iter().enumerate() {
            let data_source_id = format!("{}-data-source-{}", id, i);
            ops.extend(data_source.write_operations(&data_source_id));
            data_source_ids.push(data_source_id.into());
        }

        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("specVersion", self.spec_version);
        entity.set("description", self.description);
        entity.set("repository", self.repository);
        entity.set("schema", self.schema);
        entity.set("dataSources", data_source_ids);
        ops.push(set_entity_operation(Self::TYPENAME, id, entity));

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
pub struct EthereumContractDataSourceEntity {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub source: EthereumContractSourceEntity,
    pub mapping: EthereumContractMappingEntity,
    pub templates: Option<Vec<EthereumContractDataSourceTemplateEntity>>,
}

impl TypedEntity for EthereumContractDataSourceEntity {
    const TYPENAME: &'static str = "EthereumContractDataSource";
    type IdType = String;
}

impl EthereumContractDataSourceEntity {
    pub fn write_operations(self, id: &str) -> Vec<EntityOperation> {
        let mut ops = vec![];

        let source_id = format!("{}-source", id);
        ops.extend(self.source.write_operations(&source_id));

        let mapping_id = format!("{}-mapping", id);
        ops.extend(self.mapping.write_operations(&mapping_id));

        let template_ids: Option<Vec<Value>> = self.templates.map(|templates| {
            templates
                .into_iter()
                .enumerate()
                .map(|(i, template)| {
                    let template_id = format!("{}-templates-{}", id, i);
                    ops.extend(template.write_operations(&template_id));
                    template_id.into()
                })
                .collect()
        });

        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("kind", self.kind);
        entity.set("network", self.network);
        entity.set("name", self.name);
        entity.set("source", source_id);
        entity.set("mapping", mapping_id);
        match template_ids {
            Some(ids) => {
                entity.set("templates", ids);
            }
            None => {}
        }
        ops.push(set_entity_operation(Self::TYPENAME, id, entity));

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
            templates: data_source.templates.as_ref().map(|templates| {
                templates
                    .iter()
                    .map(|template| EthereumContractDataSourceTemplateEntity::from(template))
                    .collect()
            }),
        }
    }
}

impl TryFromValue for EthereumContractDataSourceEntity {
    fn try_from_value(value: &q::Value) -> Result<Self, Error> {
        let map = match value {
            q::Value::Object(map) => Ok(map),
            _ => Err(format_err!(
                "Cannot parse value into a data source entity: {:?}",
                value
            )),
        }?;

        Ok(Self {
            kind: map.get_required("kind")?,
            name: map.get_required("name")?,
            network: map.get_optional("network")?,
            source: map.get_required("source")?,
            mapping: map.get_required("mapping")?,
            templates: map.get_optional("templates")?,
        })
    }
}

#[derive(Debug)]
pub struct DynamicEthereumContractDataSourceEntity {
    kind: String,
    deployment: String,
    ethereum_block_hash: H256,
    ethereum_block_number: u64,
    network: Option<String>,
    name: String,
    source: EthereumContractSourceEntity,
    mapping: EthereumContractMappingEntity,
    templates: Option<Vec<EthereumContractDataSourceTemplateEntity>>,
}

impl TypedEntity for DynamicEthereumContractDataSourceEntity {
    const TYPENAME: &'static str = "DynamicEthereumContractDataSource";
    type IdType = String;
}

impl DynamicEthereumContractDataSourceEntity {
    pub fn write_operations(self, id: &str) -> Vec<EntityOperation> {
        let mut ops = vec![];

        let source_id = format!("{}-source", id);
        ops.extend(self.source.write_operations(&source_id));

        let mapping_id = format!("{}-mapping", id);
        ops.extend(self.mapping.write_operations(&mapping_id));

        let template_ids: Option<Vec<Value>> = self.templates.map(|templates| {
            templates
                .into_iter()
                .enumerate()
                .map(|(i, template)| {
                    let template_id = format!("{}-templates-{}", id, i);
                    ops.extend(template.write_operations(&template_id));
                    template_id.into()
                })
                .collect()
        });

        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("kind", self.kind);
        entity.set("network", self.network);
        entity.set("name", self.name);
        entity.set("source", source_id);
        entity.set("mapping", mapping_id);
        match template_ids {
            Some(ids) => {
                entity.set("templates", ids);
            }
            None => {}
        }
        entity.set("deployment", self.deployment);
        entity.set("ethereumBlockHash", self.ethereum_block_hash);
        entity.set("ethereumBlockNumber", self.ethereum_block_number);
        ops.push(set_entity_operation(Self::TYPENAME, id, entity));

        ops
    }
}

impl<'a, 'b, 'c>
    From<(
        &'a SubgraphDeploymentId,
        &'b super::DataSource,
        &'c EthereumBlockPointer,
    )> for DynamicEthereumContractDataSourceEntity
{
    fn from(
        data: (
            &'a SubgraphDeploymentId,
            &'b super::DataSource,
            &'c EthereumBlockPointer,
        ),
    ) -> Self {
        let (deployment_id, data_source, block_ptr) = data;

        Self {
            kind: data_source.kind.clone(),
            deployment: deployment_id.to_string(),
            ethereum_block_hash: block_ptr.hash.clone(),
            ethereum_block_number: block_ptr.number,
            name: data_source.name.clone(),
            network: data_source.network.clone(),
            source: data_source.source.clone().into(),
            mapping: EthereumContractMappingEntity::from(&data_source.mapping),
            templates: data_source.templates.as_ref().map(|templates| {
                templates
                    .iter()
                    .map(|template| EthereumContractDataSourceTemplateEntity::from(template))
                    .collect()
            }),
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct EthereumContractSourceEntity {
    pub address: Option<super::Address>,
    pub abi: String,
}

impl TypedEntity for EthereumContractSourceEntity {
    const TYPENAME: &'static str = "EthereumContractSource";
    type IdType = String;
}

impl EthereumContractSourceEntity {
    fn write_operations(self, id: &str) -> Vec<EntityOperation> {
        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("address", self.address);
        entity.set("abi", self.abi);
        vec![set_entity_operation(Self::TYPENAME, id, entity)]
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

impl TryFromValue for EthereumContractSourceEntity {
    fn try_from_value(value: &q::Value) -> Result<Self, Error> {
        let map = match value {
            q::Value::Object(map) => Ok(map),
            _ => Err(format_err!(
                "Cannot parse value into a contract source entity: {:?}",
                value
            )),
        }?;

        Ok(Self {
            address: map.get_optional("address")?,
            abi: map.get_required("abi")?,
        })
    }
}

#[derive(Debug)]
pub struct EthereumContractMappingEntity {
    pub kind: String,
    pub api_version: String,
    pub language: String,
    pub file: String,
    pub entities: Vec<String>,
    pub abis: Vec<EthereumContractAbiEntity>,
    pub block_handlers: Option<Vec<EthereumBlockHandlerEntity>>,
    pub call_handlers: Option<Vec<EthereumCallHandlerEntity>>,
    pub event_handlers: Option<Vec<EthereumContractEventHandlerEntity>>,
}

impl TypedEntity for EthereumContractMappingEntity {
    const TYPENAME: &'static str = "EthereumContractMapping";
    type IdType = String;
}

impl EthereumContractMappingEntity {
    fn write_operations(self, id: &str) -> Vec<EntityOperation> {
        let mut ops = vec![];

        let mut abi_ids: Vec<Value> = vec![];
        for (i, abi) in self.abis.into_iter().enumerate() {
            let abi_id = format!("{}-abi-{}", id, i);
            ops.extend(abi.write_operations(&abi_id));
            abi_ids.push(abi_id.into());
        }

        let event_handler_ids: Option<Vec<Value>> = self.event_handlers.map(|event_handlers| {
            event_handlers
                .into_iter()
                .enumerate()
                .map(|(i, event_handler)| {
                    let handler_id = format!("{}-event-handler-{}", id, i);
                    ops.extend(event_handler.write_operations(&handler_id));
                    handler_id
                })
                .map(Into::into)
                .collect()
        });
        let call_handler_ids: Option<Vec<Value>> = self.call_handlers.map(|call_handlers| {
            call_handlers
                .into_iter()
                .enumerate()
                .map(|(i, call_handler)| {
                    let handler_id = format!("{}-call-handler-{}", id, i);
                    ops.extend(call_handler.write_operations(&handler_id));
                    handler_id
                })
                .map(Into::into)
                .collect()
        });

        let block_handler_ids: Option<Vec<Value>> = self.block_handlers.map(|block_handlers| {
            block_handlers
                .into_iter()
                .enumerate()
                .map(|(i, block_handler)| {
                    let handler_id = format!("{}-block-handler-{}", id, i);
                    ops.extend(block_handler.write_operations(&handler_id));
                    handler_id
                })
                .map(Into::into)
                .collect()
        });

        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("kind", self.kind);
        entity.set("apiVersion", self.api_version);
        entity.set("language", self.language);
        entity.set("file", self.file);
        entity.set("abis", abi_ids);
        entity.set(
            "entities",
            self.entities
                .into_iter()
                .map(Value::from)
                .collect::<Vec<Value>>(),
        );
        event_handler_ids.map(|event_handler_ids| entity.set("eventHandlers", event_handler_ids));
        call_handler_ids.map(|call_handler_ids| entity.set("callHandlers", call_handler_ids));
        block_handler_ids.map(|block_handler_ids| entity.set("blockHandlers", block_handler_ids));

        ops.push(set_entity_operation(Self::TYPENAME, id, entity));

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
            block_handlers: mapping
                .block_handlers
                .clone()
                .map(|block_handlers| block_handlers.clone().into_iter().map(Into::into).collect()),
            call_handlers: mapping
                .call_handlers
                .clone()
                .map(|call_handlers| call_handlers.clone().into_iter().map(Into::into).collect()),
            event_handlers: mapping
                .event_handlers
                .clone()
                .map(|event_handlers| event_handlers.clone().into_iter().map(Into::into).collect()),
        }
    }
}

impl TryFromValue for EthereumContractMappingEntity {
    fn try_from_value(value: &q::Value) -> Result<Self, Error> {
        let map = match value {
            q::Value::Object(map) => Ok(map),
            _ => Err(format_err!(
                "Cannot parse value into a mapping entity: {:?}",
                value
            )),
        }?;

        Ok(Self {
            kind: map.get_required("kind")?,
            api_version: map.get_required("apiVersion")?,
            language: map.get_required("language")?,
            file: map.get_required("file")?,
            entities: map.get_required("entities")?,
            abis: map.get_required("abis")?,
            event_handlers: map.get_optional("eventHandlers")?,
            call_handlers: map.get_optional("callHandlers")?,
            block_handlers: map.get_optional("blockHandlers")?,
        })
    }
}

#[derive(Debug)]
pub struct EthereumContractAbiEntity {
    pub name: String,
    pub file: String,
}

impl TypedEntity for EthereumContractAbiEntity {
    const TYPENAME: &'static str = "EthereumContractAbi";
    type IdType = String;
}

impl EthereumContractAbiEntity {
    fn write_operations(self, id: &str) -> Vec<EntityOperation> {
        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("name", self.name);
        entity.set("file", self.file);
        vec![set_entity_operation(Self::TYPENAME, id, entity)]
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

impl TryFromValue for EthereumContractAbiEntity {
    fn try_from_value(value: &q::Value) -> Result<Self, Error> {
        let map = match value {
            q::Value::Object(map) => Ok(map),
            _ => Err(format_err!(
                "Cannot parse value into ABI entity: {:?}",
                value
            )),
        }?;

        Ok(Self {
            name: map.get_required("name")?,
            file: map.get_required("file")?,
        })
    }
}

#[derive(Debug)]
pub struct EthereumBlockHandlerEntity {
    pub handler: String,
    pub filter: Option<EthereumBlockHandlerFilterEntity>,
}

impl EthereumBlockHandlerEntity {
    fn write_operations(self, id: &str) -> Vec<EntityOperation> {
        let mut ops = vec![];

        let filter_id: Option<Value> = self.filter.map(|filter| {
            let filter_id = format!("{}-filter", id);
            ops.extend(filter.write_operations(&filter_id));
            filter_id.into()
        });

        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("handler", self.handler);
        match filter_id {
            Some(filter_id) => {
                entity.set("filter", filter_id);
            }
            None => {}
        }
        ops.push(set_entity_operation(Self::TYPENAME, id, entity));

        ops
    }
}

impl TypedEntity for EthereumBlockHandlerEntity {
    const TYPENAME: &'static str = "EthereumBlockHandlerEntity";
    type IdType = String;
}

impl From<super::MappingBlockHandler> for EthereumBlockHandlerEntity {
    fn from(block_handler: super::MappingBlockHandler) -> Self {
        let filter = match block_handler.filter {
            Some(filter) => match filter {
                // TODO: Figure out how to use serde to get lowercase spelling here
                super::BlockHandlerFilter::Call => Some(EthereumBlockHandlerFilterEntity {
                    kind: "call".to_string(),
                }),
            },
            None => None,
        };
        EthereumBlockHandlerEntity {
            handler: block_handler.handler,
            filter: filter,
        }
    }
}

impl TryFromValue for EthereumBlockHandlerEntity {
    fn try_from_value(value: &q::Value) -> Result<Self, Error> {
        let map = match value {
            q::Value::Object(map) => Ok(map),
            _ => Err(format_err!(
                "Cannot parse value into block handler entity: {:?}",
                value
            )),
        }?;

        Ok(EthereumBlockHandlerEntity {
            handler: map.get_required("handler")?,
            filter: map.get_optional("filter")?,
        })
    }
}

#[derive(Debug)]
pub struct EthereumBlockHandlerFilterEntity {
    pub kind: String,
}

impl TypedEntity for EthereumBlockHandlerFilterEntity {
    const TYPENAME: &'static str = "EthereumBlockHandlerFilterEntity";
    type IdType = String;
}

impl EthereumBlockHandlerFilterEntity {
    fn write_operations(self, id: &str) -> Vec<EntityOperation> {
        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("kind", self.kind);
        vec![set_entity_operation(Self::TYPENAME, id, entity)]
    }
}

impl TryFromValue for EthereumBlockHandlerFilterEntity {
    fn try_from_value(value: &q::Value) -> Result<Self, Error> {
        let map = match value {
            q::Value::Object(map) => Ok(map),
            _ => Err(format_err!(
                "Cannot parse value into block handler filter entity: {:?}",
                value,
            )),
        }?;

        Ok(Self {
            kind: map.get_required("kind")?,
        })
    }
}

#[derive(Debug)]
pub struct EthereumCallHandlerEntity {
    pub function: String,
    pub handler: String,
}

impl TypedEntity for EthereumCallHandlerEntity {
    const TYPENAME: &'static str = "EthereumCallHandlerEntity";
    type IdType = String;
}

impl EthereumCallHandlerEntity {
    fn write_operations(self, id: &str) -> Vec<EntityOperation> {
        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("function", self.function);
        entity.set("handler", self.handler);
        vec![set_entity_operation(Self::TYPENAME, id, entity)]
    }
}

impl From<super::MappingCallHandler> for EthereumCallHandlerEntity {
    fn from(call_handler: super::MappingCallHandler) -> Self {
        Self {
            function: call_handler.function,
            handler: call_handler.handler,
        }
    }
}

impl TryFromValue for EthereumCallHandlerEntity {
    fn try_from_value(value: &q::Value) -> Result<Self, Error> {
        let map = match value {
            q::Value::Object(map) => Ok(map),
            _ => Err(format_err!(
                "Cannot parse value into call handler entity: {:?}",
                value
            )),
        }?;

        Ok(Self {
            function: map.get_required("function")?,
            handler: map.get_required("handler")?,
        })
    }
}

#[derive(Debug)]
pub struct EthereumContractEventHandlerEntity {
    pub event: String,
    pub topic0: Option<H256>,
    pub handler: String,
}

impl TypedEntity for EthereumContractEventHandlerEntity {
    const TYPENAME: &'static str = "EthereumContractEventHandler";
    type IdType = String;
}

impl EthereumContractEventHandlerEntity {
    fn write_operations(self, id: &str) -> Vec<EntityOperation> {
        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("event", self.event);
        entity.set("topic0", self.topic0.map_or(Value::Null, Value::from));
        entity.set("handler", self.handler);
        vec![set_entity_operation(Self::TYPENAME, id, entity)]
    }
}

impl From<super::MappingEventHandler> for EthereumContractEventHandlerEntity {
    fn from(event_handler: super::MappingEventHandler) -> Self {
        Self {
            event: event_handler.event,
            topic0: event_handler.topic0,
            handler: event_handler.handler,
        }
    }
}

impl TryFromValue for EthereumContractEventHandlerEntity {
    fn try_from_value(value: &q::Value) -> Result<Self, Error> {
        let map = match value {
            q::Value::Object(map) => Ok(map),
            _ => Err(format_err!(
                "Cannot parse value into event handler entity: {:?}",
                value
            )),
        }?;

        Ok(Self {
            event: map.get_required("event")?,
            topic0: map.get_optional("topic0")?,
            handler: map.get_required("handler")?,
        })
    }
}

#[derive(Debug)]
pub struct EthereumContractDataSourceTemplateEntity {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub source: EthereumContractDataSourceTemplateSourceEntity,
    pub mapping: EthereumContractMappingEntity,
}

impl TypedEntity for EthereumContractDataSourceTemplateEntity {
    const TYPENAME: &'static str = "EthereumContractDataSourceTemplate";
    type IdType = String;
}

impl EthereumContractDataSourceTemplateEntity {
    fn write_operations(self, id: &str) -> Vec<EntityOperation> {
        let mut ops = vec![];

        let source_id = format!("{}-source", id);
        ops.extend(self.source.write_operations(&source_id));

        let mapping_id = format!("{}-mapping", id);
        ops.extend(self.mapping.write_operations(&mapping_id));

        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("kind", self.kind);
        entity.set("network", self.network);
        entity.set("name", self.name);
        entity.set("source", source_id);
        entity.set("mapping", mapping_id);
        ops.push(set_entity_operation(Self::TYPENAME, id, entity));

        ops
    }
}

impl From<&super::DataSourceTemplate> for EthereumContractDataSourceTemplateEntity {
    fn from(template: &super::DataSourceTemplate) -> Self {
        Self {
            kind: template.kind.clone(),
            name: template.name.clone(),
            network: template.network.clone(),
            source: template.source.clone().into(),
            mapping: EthereumContractMappingEntity::from(&template.mapping),
        }
    }
}

impl TryFromValue for EthereumContractDataSourceTemplateEntity {
    fn try_from_value(value: &q::Value) -> Result<Self, Error> {
        let map = match value {
            q::Value::Object(map) => Ok(map),
            _ => Err(format_err!(
                "Cannot parse value into a data source template entity: {:?}",
                value
            )),
        }?;

        Ok(Self {
            kind: map.get_required("kind")?,
            name: map.get_required("name")?,
            network: map.get_optional("network")?,
            source: map.get_required("source")?,
            mapping: map.get_required("mapping")?,
        })
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct EthereumContractDataSourceTemplateSourceEntity {
    pub abi: String,
}

impl TypedEntity for EthereumContractDataSourceTemplateSourceEntity {
    const TYPENAME: &'static str = "EthereumContractDataSourceTemplateSource";
    type IdType = String;
}

impl EthereumContractDataSourceTemplateSourceEntity {
    fn write_operations(self, id: &str) -> Vec<EntityOperation> {
        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("abi", self.abi);
        vec![set_entity_operation(Self::TYPENAME, id, entity)]
    }
}

impl From<super::TemplateSource> for EthereumContractDataSourceTemplateSourceEntity {
    fn from(source: super::TemplateSource) -> Self {
        Self { abi: source.abi }
    }
}

impl TryFromValue for EthereumContractDataSourceTemplateSourceEntity {
    fn try_from_value(value: &q::Value) -> Result<Self, Error> {
        let map = match value {
            q::Value::Object(map) => Ok(map),
            _ => Err(format_err!(
                "Cannot parse value into a template source entity: {:?}",
                value
            )),
        }?;

        Ok(Self {
            abi: map.get_required("abi")?,
        })
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

pub fn generate_entity_id() -> String {
    // Fast crypto RNG from operating system
    let mut rng = OsRng::new().unwrap();

    // 128 random bits
    let id_bytes: [u8; 16] = rng.gen();

    // 32 hex chars
    // Comparable to uuidv4, but without the hyphens,
    // and without spending bits on a version identifier.
    hex::encode(id_bytes)
}

pub fn attribute_index_definitions(
    subgraph_id: SubgraphDeploymentId,
    document: Document,
) -> Vec<AttributeIndexDefinition> {
    let mut indexing_ops = vec![];
    for (entity_number, schema_type) in document.definitions.clone().into_iter().enumerate() {
        if let Definition::TypeDefinition(definition) = schema_type {
            if let TypeDefinition::Object(schema_object) = definition {
                for (attribute_number, entity_field) in schema_object
                    .fields
                    .into_iter()
                    .filter(|f| f.name != "id")
                    .enumerate()
                {
                    indexing_ops.push(AttributeIndexDefinition {
                        subgraph_id: subgraph_id.clone(),
                        entity_number,
                        attribute_number,
                        field_value_type: match inner_type_name(&entity_field.field_type) {
                            Ok(value_type) => value_type,
                            Err(_) => continue,
                        },
                        attribute_name: entity_field.name,
                        entity_name: schema_object.name.clone(),
                    });
                }
            }
        }
    }
    indexing_ops
}

/// Returns the value type for a GraphQL field type.
pub fn inner_type_name(field_type: &Type) -> Result<ValueType, Error> {
    match field_type {
        Type::NamedType(ref name) => ValueType::from_str(&name),
        Type::NonNullType(inner) => inner_type_name(&inner),
        Type::ListType(inner) => inner_type_name(inner).and(Ok(ValueType::List)),
    }
}
