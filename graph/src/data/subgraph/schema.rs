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

use graphql_parser::query as q;
use graphql_parser::schema::{Definition, Document, Field, Name, Type, TypeDefinition};
use hex;
use lazy_static::lazy_static;
use rand::rngs::OsRng;
use rand::Rng;
use std::collections::BTreeMap;
use std::str::FromStr;
use uuid::Uuid;
use web3::types::*;

use super::SubgraphDeploymentId;
use crate::components::ethereum::EthereumBlockPointer;
use crate::components::store::{
    AttributeIndexDefinition, EntityCollection, EntityFilter, EntityKey, EntityOperation,
    EntityQuery, EntityRange, MetadataOperation,
};
use crate::data::graphql::{TryFromValue, ValueMap};
use crate::data::store::{Entity, NodeId, SubgraphEntityPair, Value, ValueType};
use crate::data::subgraph::{SubgraphManifest, SubgraphName};
use crate::prelude::*;

lazy_static! {
    /// ID of the subgraph of subgraphs.
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
        EntityQuery::new(
            SUBGRAPHS_ID.clone(),
            BLOCK_NUMBER_MAX,
            EntityCollection::All(vec![Self::TYPENAME.to_owned()]),
        )
        .range(range)
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

// See also: ed42d219c6704a4aab57ce1ea66698e7.
// Note: The types here need to be in sync with the metadata GraphQL schema.

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

trait OperationList {
    fn add(&mut self, entity: &str, id: String, data: Entity);
}

struct MetadataOperationList(Vec<MetadataOperation>);

impl OperationList for MetadataOperationList {
    fn add(&mut self, entity: &str, id: String, data: Entity) {
        self.0.push(MetadataOperation::Set {
            entity: entity.to_owned(),
            id,
            data,
        })
    }
}

struct EntityOperationList(Vec<EntityOperation>);

impl OperationList for EntityOperationList {
    fn add(&mut self, entity: &str, id: String, data: Entity) {
        self.0.push(EntityOperation::Set {
            key: EntityKey {
                subgraph_id: SUBGRAPHS_ID.clone(),
                entity_type: entity.to_owned(),
                entity_id: id.to_owned(),
            },
            data,
        })
    }
}

trait WriteOperations: Sized {
    fn generate(self, id: &str, ops: &mut dyn OperationList);

    fn write_operations(self, id: &str) -> Vec<MetadataOperation> {
        let mut ops = MetadataOperationList(Vec::new());
        self.generate(id, &mut ops);
        ops.0
    }

    fn write_entity_operations(self, id: &str) -> Vec<EntityOperation> {
        let mut ops = EntityOperationList(Vec::new());
        self.generate(id, &mut ops);
        ops.0
    }
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

    pub fn write_operations(self, id: &str) -> Vec<MetadataOperation> {
        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("name", self.name.to_string());
        entity.set("currentVersion", self.current_version_id);
        entity.set("pendingVersion", self.pending_version_id);
        entity.set("createdAt", self.created_at);
        vec![set_metadata_operation(Self::TYPENAME, id, entity)]
    }

    pub fn update_current_version_operations(
        id: &str,
        version_id_opt: Option<String>,
    ) -> Vec<MetadataOperation> {
        let mut entity = Entity::new();
        entity.set("currentVersion", version_id_opt);

        vec![update_metadata_operation(Self::TYPENAME, id, entity)]
    }

    pub fn update_pending_version_operations(
        id: &str,
        version_id_opt: Option<String>,
    ) -> Vec<MetadataOperation> {
        let mut entity = Entity::new();
        entity.set("pendingVersion", version_id_opt);

        vec![update_metadata_operation(Self::TYPENAME, id, entity)]
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

    pub fn write_operations(self, id: &str) -> Vec<MetadataOperation> {
        let mut entity = Entity::new();
        entity.set("id", id.to_owned());
        entity.set("subgraph", self.subgraph_id);
        entity.set("deployment", self.deployment_id.to_string());
        entity.set("createdAt", self.created_at);
        vec![set_metadata_operation(Self::TYPENAME, id, entity)]
    }
}

#[derive(Debug)]
pub struct SubgraphDeploymentEntity {
    manifest: SubgraphManifestEntity,
    failed: bool,
    synced: bool,
    earliest_ethereum_block_hash: Option<H256>,
    earliest_ethereum_block_number: Option<u64>,
    latest_ethereum_block_hash: Option<H256>,
    latest_ethereum_block_number: Option<u64>,
    ethereum_head_block_hash: Option<H256>,
    ethereum_head_block_number: Option<u64>,
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
        earliest_ethereum_block: Option<EthereumBlockPointer>,
        chain_head_block: Option<EthereumBlockPointer>,
    ) -> Self {
        Self {
            manifest: SubgraphManifestEntity::from(source_manifest),
            failed,
            synced,
            earliest_ethereum_block_hash: earliest_ethereum_block.map(Into::into),
            earliest_ethereum_block_number: earliest_ethereum_block.map(Into::into),
            latest_ethereum_block_hash: earliest_ethereum_block.map(Into::into),
            latest_ethereum_block_number: earliest_ethereum_block.map(Into::into),
            ethereum_head_block_hash: chain_head_block.map(Into::into),
            ethereum_head_block_number: chain_head_block.map(Into::into),
            total_ethereum_blocks_count: chain_head_block.map_or(0, |block| block.number + 1),
        }
    }

    // Overwrite entity if it exists. Only in debug builds so it's not used outside tests.
    #[cfg(debug_assertions)]
    pub fn create_operations_replace(self, id: &SubgraphDeploymentId) -> Vec<MetadataOperation> {
        self.private_create_operations(id)
    }

    pub fn create_operations(self, id: &SubgraphDeploymentId) -> Vec<MetadataOperation> {
        let mut ops: Vec<MetadataOperation> = vec![];

        // Abort unless no entity exists with this ID
        ops.push(MetadataOperation::AbortUnless {
            description: "Subgraph deployment entity must not exist yet to be created".to_owned(),
            query: Self::query().filter(EntityFilter::new_equal("id", id.to_string())),
            entity_ids: vec![],
        });

        ops.extend(self.private_create_operations(id));
        ops
    }

    fn private_create_operations(self, id: &SubgraphDeploymentId) -> Vec<MetadataOperation> {
        let mut ops = vec![];

        let manifest_id = SubgraphManifestEntity::id(&id);
        ops.extend(self.manifest.write_operations(&manifest_id));

        let mut entity = Entity::new();
        entity.set("id", id.to_string());
        entity.set("manifest", manifest_id);
        entity.set("failed", self.failed);
        entity.set("synced", self.synced);
        entity.set(
            "earliestEthereumBlockHash",
            Value::from(self.earliest_ethereum_block_hash),
        );
        entity.set(
            "earliestEthereumBlockNumber",
            Value::from(self.earliest_ethereum_block_number),
        );
        entity.set(
            "latestEthereumBlockHash",
            Value::from(self.latest_ethereum_block_hash),
        );
        entity.set(
            "latestEthereumBlockNumber",
            Value::from(self.latest_ethereum_block_number),
        );
        entity.set(
            "ethereumHeadBlockHash",
            Value::from(self.ethereum_head_block_hash),
        );
        entity.set(
            "ethereumHeadBlockNumber",
            Value::from(self.ethereum_head_block_number),
        );
        entity.set("totalEthereumBlocksCount", self.total_ethereum_blocks_count);
        entity.set("entityCount", 0 as u64);
        ops.push(set_metadata_operation(
            Self::TYPENAME,
            id.to_string(),
            entity,
        ));

        ops
    }

    pub fn update_ethereum_block_pointer_operations(
        id: &SubgraphDeploymentId,
        block_ptr_to: EthereumBlockPointer,
    ) -> Vec<MetadataOperation> {
        let mut entity = Entity::new();
        entity.set("latestEthereumBlockHash", block_ptr_to.hash);
        entity.set("latestEthereumBlockNumber", block_ptr_to.number);

        vec![update_metadata_operation(
            Self::TYPENAME,
            id.to_string(),
            entity,
        )]
    }

    pub fn update_ethereum_head_block_operations(
        id: &SubgraphDeploymentId,
        block_ptr: EthereumBlockPointer,
    ) -> Vec<MetadataOperation> {
        let mut entity = Entity::new();
        entity.set("totalEthereumBlocksCount", block_ptr.number);
        entity.set("ethereumHeadBlockHash", block_ptr.hash);
        entity.set("ethereumHeadBlockNumber", block_ptr.number);

        vec![update_metadata_operation(
            Self::TYPENAME,
            id.to_string(),
            entity,
        )]
    }

    pub fn update_failed_operations(
        id: &SubgraphDeploymentId,
        failed: bool,
    ) -> Vec<MetadataOperation> {
        let mut entity = Entity::new();
        entity.set("failed", failed);

        vec![update_metadata_operation(
            Self::TYPENAME,
            id.as_str(),
            entity,
        )]
    }

    pub fn update_synced_operations(
        id: &SubgraphDeploymentId,
        synced: bool,
    ) -> Vec<MetadataOperation> {
        let mut entity = Entity::new();
        entity.set("synced", synced);

        vec![update_metadata_operation(
            Self::TYPENAME,
            id.as_str(),
            entity,
        )]
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

    pub fn write_operations(self, id: &SubgraphDeploymentId) -> Vec<MetadataOperation> {
        let mut entity = Entity::new();
        entity.set("id", id.to_string());
        entity.set("nodeId", self.node_id.to_string());
        entity.set("cost", self.cost);
        vec![set_metadata_operation(Self::TYPENAME, id.as_str(), entity)]
    }
}

#[derive(Debug)]
pub struct SubgraphManifestEntity {
    spec_version: String,
    description: Option<String>,
    repository: Option<String>,
    schema: String,
    data_sources: Vec<EthereumContractDataSourceEntity>,
    templates: Vec<EthereumContractDataSourceTemplateEntity>,
}

impl TypedEntity for SubgraphManifestEntity {
    const TYPENAME: &'static str = "SubgraphManifest";
    type IdType = String;
}

impl SubgraphManifestEntity {
    pub fn id(subgraph_id: &SubgraphDeploymentId) -> String {
        format!("{}-manifest", subgraph_id)
    }

    fn write_operations(self, id: &str) -> Vec<MetadataOperation> {
        let mut ops = vec![];

        let mut data_source_ids: Vec<Value> = vec![];
        for (i, data_source) in self.data_sources.into_iter().enumerate() {
            let data_source_id = format!("{}-data-source-{}", id, i);
            ops.extend(data_source.write_operations(&data_source_id));
            data_source_ids.push(data_source_id.into());
        }

        let template_ids: Vec<Value> = self
            .templates
            .into_iter()
            .enumerate()
            .map(|(i, template)| {
                let template_id = format!("{}-templates-{}", id, i);
                ops.extend(template.write_operations(&template_id));
                template_id.into()
            })
            .collect();

        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("specVersion", self.spec_version);
        entity.set("description", self.description);
        entity.set("repository", self.repository);
        entity.set("schema", self.schema);
        entity.set("dataSources", data_source_ids);
        entity.set("templates", template_ids);

        ops.push(set_metadata_operation(Self::TYPENAME, id, entity));

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
            templates: manifest
                .templates
                .iter()
                .map(EthereumContractDataSourceTemplateEntity::from)
                .collect(),
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
    pub templates: Vec<EthereumContractDataSourceTemplateEntity>,
}

impl TypedEntity for EthereumContractDataSourceEntity {
    const TYPENAME: &'static str = "EthereumContractDataSource";
    type IdType = String;
}

impl EthereumContractDataSourceEntity {
    pub fn write_operations(self, id: &str) -> Vec<MetadataOperation> {
        let mut ops = vec![];

        let source_id = format!("{}-source", id);
        ops.extend(self.source.write_operations(&source_id));

        let mapping_id = format!("{}-mapping", id);
        ops.extend(self.mapping.write_operations(&mapping_id));

        let template_ids: Vec<Value> = self
            .templates
            .into_iter()
            .enumerate()
            .map(|(i, template)| {
                let template_id = format!("{}-templates-{}", id, i);
                ops.extend(template.write_operations(&template_id));
                template_id.into()
            })
            .collect();

        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("kind", self.kind);
        entity.set("network", self.network);
        entity.set("name", self.name);
        entity.set("source", source_id);
        entity.set("mapping", mapping_id);
        entity.set("templates", template_ids);

        ops.push(set_metadata_operation(Self::TYPENAME, id, entity));

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
            templates: data_source
                .templates
                .iter()
                .map(|template| EthereumContractDataSourceTemplateEntity::from(template))
                .collect(),
        }
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
    templates: Vec<EthereumContractDataSourceTemplateEntity>,
    context: Option<DataSourceContext>,
}

impl DynamicEthereumContractDataSourceEntity {
    pub fn write_entity_operations(self, id: &str) -> Vec<EntityOperation> {
        WriteOperations::write_entity_operations(self, id)
    }

    pub fn make_id() -> String {
        format!("{}-dynamic", Uuid::new_v4().to_simple())
    }
}

impl TypedEntity for DynamicEthereumContractDataSourceEntity {
    const TYPENAME: &'static str = "DynamicEthereumContractDataSource";
    type IdType = String;
}

impl WriteOperations for DynamicEthereumContractDataSourceEntity {
    fn generate(self, id: &str, ops: &mut dyn OperationList) {
        let source_id = format!("{}-source", id);
        self.source.generate(&source_id, ops);

        let mapping_id = format!("{}-mapping", id);
        self.mapping.generate(&mapping_id, ops);

        let Self {
            kind,
            deployment,
            ethereum_block_hash,
            ethereum_block_number,
            name,
            network,
            source: _,
            mapping: _,
            templates,
            context,
        } = self;

        let template_ids: Vec<Value> = templates
            .into_iter()
            .enumerate()
            .map(|(i, template)| {
                let template_id = format!("{}-templates-{}", id, i);
                template.generate(&template_id, ops);
                template_id.into()
            })
            .collect();

        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("kind", kind);
        entity.set("network", network);
        entity.set("name", name);
        entity.set("source", source_id);
        entity.set("mapping", mapping_id);
        entity.set("templates", template_ids);
        entity.set("deployment", deployment);
        entity.set("ethereumBlockHash", ethereum_block_hash);
        entity.set("ethereumBlockNumber", ethereum_block_number);
        entity.set(
            "context",
            context
                .as_ref()
                .map(|ctx| serde_json::to_string(&ctx).unwrap()),
        );
        ops.add(Self::TYPENAME, id.to_owned(), entity);
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
        let DataSource {
            kind,
            network,
            name,
            source,
            mapping,
            templates,
            context,
        } = data_source;

        Self {
            kind: kind.clone(),
            deployment: deployment_id.to_string(),
            ethereum_block_hash: block_ptr.hash.clone(),
            ethereum_block_number: block_ptr.number,
            name: name.clone(),
            network: network.clone(),
            source: source.clone().into(),
            mapping: EthereumContractMappingEntity::from(mapping),
            templates: templates
                .iter()
                .map(|template| EthereumContractDataSourceTemplateEntity::from(template))
                .collect(),
            context: context.clone(),
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct EthereumContractSourceEntity {
    pub address: Option<super::Address>,
    pub abi: String,
    pub start_block: u64,
}

impl TypedEntity for EthereumContractSourceEntity {
    const TYPENAME: &'static str = "EthereumContractSource";
    type IdType = String;
}

impl WriteOperations for EthereumContractSourceEntity {
    fn generate(self, id: &str, ops: &mut dyn OperationList) {
        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("address", self.address);
        entity.set("abi", self.abi);
        entity.set("startBlock", self.start_block);
        ops.add(Self::TYPENAME, id.to_owned(), entity);
    }
}

impl From<super::Source> for EthereumContractSourceEntity {
    fn from(source: super::Source) -> Self {
        Self {
            address: source.address,
            abi: source.abi,
            start_block: source.start_block,
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
            start_block: map.get_optional("startBlock")?.unwrap_or_default(),
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
    pub block_handlers: Vec<EthereumBlockHandlerEntity>,
    pub call_handlers: Vec<EthereumCallHandlerEntity>,
    pub event_handlers: Vec<EthereumContractEventHandlerEntity>,
}

impl TypedEntity for EthereumContractMappingEntity {
    const TYPENAME: &'static str = "EthereumContractMapping";
    type IdType = String;
}

impl WriteOperations for EthereumContractMappingEntity {
    fn generate(self, id: &str, ops: &mut dyn OperationList) {
        let mut abi_ids: Vec<Value> = vec![];
        for (i, abi) in self.abis.into_iter().enumerate() {
            let abi_id = format!("{}-abi-{}", id, i);
            abi.generate(&abi_id, ops);
            abi_ids.push(abi_id.into());
        }

        let event_handler_ids: Vec<Value> = self
            .event_handlers
            .into_iter()
            .enumerate()
            .map(|(i, event_handler)| {
                let handler_id = format!("{}-event-handler-{}", id, i);
                event_handler.generate(&handler_id, ops);
                handler_id
            })
            .map(Into::into)
            .collect();
        let call_handler_ids: Vec<Value> = self
            .call_handlers
            .into_iter()
            .enumerate()
            .map(|(i, call_handler)| {
                let handler_id = format!("{}-call-handler-{}", id, i);
                call_handler.generate(&handler_id, ops);
                handler_id
            })
            .map(Into::into)
            .collect();

        let block_handler_ids: Vec<Value> = self
            .block_handlers
            .into_iter()
            .enumerate()
            .map(|(i, block_handler)| {
                let handler_id = format!("{}-block-handler-{}", id, i);
                block_handler.generate(&handler_id, ops);
                handler_id
            })
            .map(Into::into)
            .collect();

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
        entity.set("eventHandlers", event_handler_ids);
        entity.set("callHandlers", call_handler_ids);
        entity.set("blockHandlers", block_handler_ids);

        ops.add(Self::TYPENAME, id.to_owned(), entity);
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
                .into_iter()
                .map(Into::into)
                .collect(),
            call_handlers: mapping
                .call_handlers
                .clone()
                .into_iter()
                .map(Into::into)
                .collect(),
            event_handlers: mapping
                .event_handlers
                .clone()
                .into_iter()
                .map(Into::into)
                .collect(),
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
            event_handlers: map.get_optional("eventHandlers")?.unwrap_or_default(),
            call_handlers: map.get_optional("callHandlers")?.unwrap_or_default(),
            block_handlers: map.get_optional("blockHandlers")?.unwrap_or_default(),
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

impl WriteOperations for EthereumContractAbiEntity {
    fn generate(self, id: &str, ops: &mut dyn OperationList) {
        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("name", self.name);
        entity.set("file", self.file);
        ops.add(Self::TYPENAME, id.to_owned(), entity)
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

impl WriteOperations for EthereumBlockHandlerEntity {
    fn generate(self, id: &str, ops: &mut dyn OperationList) {
        let filter_id: Option<Value> = self.filter.map(|filter| {
            let filter_id = format!("{}-filter", id);
            filter.generate(&filter_id, ops);
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
        ops.add(Self::TYPENAME, id.to_owned(), entity);
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
                    kind: Some("call".to_string()),
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
    pub kind: Option<String>,
}

impl TypedEntity for EthereumBlockHandlerFilterEntity {
    const TYPENAME: &'static str = "EthereumBlockHandlerFilterEntity";
    type IdType = String;
}

impl WriteOperations for EthereumBlockHandlerFilterEntity {
    fn generate(self, id: &str, ops: &mut dyn OperationList) {
        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("kind", self.kind);
        ops.add(Self::TYPENAME, id.to_owned(), entity)
    }
}

impl TryFromValue for EthereumBlockHandlerFilterEntity {
    fn try_from_value(value: &q::Value) -> Result<Self, Error> {
        let empty_map = BTreeMap::new();
        let map = match value {
            q::Value::Object(map) => Ok(map),
            q::Value::Null => Ok(&empty_map),
            _ => Err(format_err!(
                "Cannot parse value into block handler filter entity: {:?}",
                value,
            )),
        }?;

        Ok(Self {
            kind: map.get_optional("kind")?,
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

impl WriteOperations for EthereumCallHandlerEntity {
    fn generate(self, id: &str, ops: &mut dyn OperationList) {
        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("function", self.function);
        entity.set("handler", self.handler);
        ops.add(Self::TYPENAME, id.to_owned(), entity);
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

impl WriteOperations for EthereumContractEventHandlerEntity {
    fn generate(self, id: &str, ops: &mut dyn OperationList) {
        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("event", self.event);
        entity.set("topic0", self.topic0.map_or(Value::Null, Value::from));
        entity.set("handler", self.handler);
        ops.add(Self::TYPENAME, id.to_owned(), entity);
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

impl WriteOperations for EthereumContractDataSourceTemplateEntity {
    fn generate(self, id: &str, ops: &mut dyn OperationList) {
        let source_id = format!("{}-source", id);
        self.source.generate(&source_id, ops);

        let mapping_id = format!("{}-mapping", id);
        self.mapping.generate(&mapping_id, ops);

        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("kind", self.kind);
        entity.set("network", self.network);
        entity.set("name", self.name);
        entity.set("source", source_id);
        entity.set("mapping", mapping_id);
        ops.add(Self::TYPENAME, id.to_owned(), entity);
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

impl WriteOperations for EthereumContractDataSourceTemplateSourceEntity {
    fn generate(self, id: &str, ops: &mut dyn OperationList) {
        let mut entity = Entity::new();
        entity.set("id", id);
        entity.set("abi", self.abi);
        ops.add(Self::TYPENAME, id.to_owned(), entity);
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

fn set_metadata_operation(
    entity_type_name: impl Into<String>,
    entity_id: impl Into<String>,
    data: impl Into<Entity>,
) -> MetadataOperation {
    MetadataOperation::Set {
        entity: entity_type_name.into(),
        id: entity_id.into(),
        data: data.into(),
    }
}

fn update_metadata_operation(
    entity_type_name: impl Into<String>,
    entity_id: impl Into<String>,
    data: impl Into<Entity>,
) -> MetadataOperation {
    MetadataOperation::Update {
        entity: entity_type_name.into(),
        id: entity_id.into(),
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
                    // Skip derived fields since they are not stored in objects
                    // of this type. We can not put this check into the filter
                    // above since that changes how indexes are numbered
                    if is_derived_field(&entity_field) {
                        continue;
                    }
                    indexing_ops.push(AttributeIndexDefinition {
                        subgraph_id: subgraph_id.clone(),
                        entity_number,
                        attribute_number,
                        field_value_type: match inner_type_name(
                            &entity_field.field_type,
                            &document.definitions,
                        ) {
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

fn is_derived_field(field: &Field) -> bool {
    field
        .directives
        .iter()
        .any(|dir| dir.name == Name::from("derivedFrom"))
}

// This largely duplicates graphql::schema::ast::is_entity_type_definition
// We do not use that function here to avoid this crate depending on
// graph_graphql
fn is_entity(type_name: &str, definitions: &[Definition]) -> bool {
    use self::TypeDefinition::*;

    definitions.iter().any(|defn| {
        if let Definition::TypeDefinition(type_def) = defn {
            match type_def {
                // Entity types are obvious
                Object(object_type) => {
                    object_type.name == type_name
                        && object_type
                            .directives
                            .iter()
                            .any(|directive| directive.name == "entity")
                }

                // We assume that only entities can implement interfaces;
                // thus, any interface type definition is automatically
                // an entity type
                Interface(interface_type) => interface_type.name == type_name,

                // Everything else (unions, scalars, enums) are not
                // considered entity types
                _ => false,
            }
        } else {
            false
        }
    })
}

/// Returns the value type for a GraphQL field type.
fn inner_type_name(field_type: &Type, definitions: &[Definition]) -> Result<ValueType, Error> {
    match field_type {
        Type::NamedType(ref name) => ValueType::from_str(&name).or_else(|e| {
            if is_entity(name, definitions) {
                // The field is a reference to another type and therefore of type ID
                Ok(ValueType::ID)
            } else {
                Err(e)
            }
        }),
        Type::NonNullType(inner) => inner_type_name(&inner, definitions),
        Type::ListType(inner) => inner_type_name(inner, definitions).and(Ok(ValueType::List)),
    }
}
