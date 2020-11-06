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

/// Convenient graphql queries for the metadata.
pub mod queries;

use graphql_parser::query as q;
use hex;
use lazy_static::lazy_static;
use rand::rngs::OsRng;
use rand::Rng;
use stable_hash::utils::stable_hash;
use stable_hash::{crypto::SetHasher, SequenceNumber, StableHash, StableHasher};
use std::str::FromStr;
use std::{collections::BTreeMap, fmt::Display};
use strum_macros::IntoStaticStr;
use uuid::Uuid;
use web3::types::*;

use super::SubgraphDeploymentId;
use crate::components::ethereum::EthereumBlockPointer;
use crate::components::store::{
    EntityCollection, EntityFilter, EntityKey, EntityOperation, EntityQuery, EntityRange,
    MetadataOperation,
};
use crate::data::graphql::{TryFromValue, ValueMap};
use crate::data::store::{Entity, NodeId, SubgraphEntityPair, Value};
use crate::data::subgraph::{SubgraphManifest, SubgraphName};
use crate::prelude::*;

lazy_static! {
    /// ID of the subgraph of subgraphs.
    pub static ref SUBGRAPHS_ID: SubgraphDeploymentId =
        SubgraphDeploymentId::new("subgraphs").unwrap();
}

pub const POI_TABLE: &str = "poi2$";
pub const POI_OBJECT: &str = "Poi$";

#[derive(Debug, Clone, IntoStaticStr)]
pub enum MetadataType {
    Subgraph,
    SubgraphDeployment,
    SubgraphDeploymentAssignment,
    SubgraphManifest,
    EthereumContractDataSource,
    DynamicEthereumContractDataSource,
    EthereumContractSource,
    EthereumContractMapping,
    EthereumContractAbi,
    EthereumBlockHandlerEntity,
    EthereumBlockHandlerFilterEntity,
    EthereumCallHandlerEntity,
    EthereumContractEventHandler,
    EthereumContractDataSourceTemplate,
    EthereumContractDataSourceTemplateSource,
    SubgraphError,
}

impl MetadataType {
    pub fn as_str(&self) -> &'static str {
        self.into()
    }
}

impl Display for MetadataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<MetadataType> for String {
    fn from(m: MetadataType) -> Self {
        m.to_string()
    }
}

/// Generic type for the entity types defined below.
pub trait TypedEntity {
    const TYPENAME: MetadataType;
    type IdType: ToString;

    fn query() -> EntityQuery {
        let range = EntityRange {
            first: None,
            skip: 0,
        };
        EntityQuery::new(
            SUBGRAPHS_ID.clone(),
            BLOCK_NUMBER_MAX,
            EntityCollection::All(vec![Self::TYPENAME.to_string()]),
        )
        .range(range)
    }

    fn subgraph_entity_pair() -> SubgraphEntityPair {
        (SUBGRAPHS_ID.clone(), Self::TYPENAME.to_string())
    }

    fn key(entity_id: Self::IdType) -> EntityKey {
        let (subgraph_id, entity_type) = Self::subgraph_entity_pair();
        EntityKey {
            subgraph_id,
            entity_type,
            entity_id: entity_id.to_string(),
        }
    }

    fn abort_unless(
        description: &'static str,
        filter: EntityFilter,
        entity_ids: Vec<String>,
    ) -> MetadataOperation {
        MetadataOperation::AbortUnless {
            description: description.to_owned(),
            metadata_type: Self::TYPENAME,
            filter,
            entity_ids,
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
    const TYPENAME: MetadataType = MetadataType::Subgraph;
    type IdType = String;
}

trait OperationList {
    fn add(&mut self, entity: MetadataType, id: String, data: Entity);
}

struct MetadataOperationList(Vec<MetadataOperation>);

impl OperationList for MetadataOperationList {
    fn add(&mut self, entity: MetadataType, id: String, data: Entity) {
        self.0.push(MetadataOperation::Set { entity, id, data })
    }
}

struct EntityOperationList(Vec<EntityOperation>);

impl OperationList for EntityOperationList {
    fn add(&mut self, entity: MetadataType, id: String, data: Entity) {
        self.0.push(EntityOperation::Set {
            key: EntityKey {
                subgraph_id: SUBGRAPHS_ID.clone(),
                entity_type: entity.to_string(),
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
            name,
            current_version_id,
            pending_version_id,
            created_at,
        }
    }

    pub fn write_operations(self, id: &str) -> Vec<MetadataOperation> {
        let entity = entity! {
            id: id,
            name: self.name.to_string(),
            currentVersion: self.current_version_id,
            pendingVersion: self.pending_version_id,
            createdAt: self.created_at,
        };
        vec![set_metadata_operation(Self::TYPENAME, id, entity)]
    }

    pub fn update_current_version_operations(
        id: &str,
        version_id_opt: Option<String>,
    ) -> Vec<MetadataOperation> {
        let entity = entity! {
            currentVersion: version_id_opt,
        };

        vec![update_metadata_operation(Self::TYPENAME, id, entity)]
    }

    pub fn update_pending_version_operations(
        id: &str,
        version_id_opt: Option<String>,
    ) -> Vec<MetadataOperation> {
        let entity = entity! {
            pendingVersion: version_id_opt,
        };

        vec![update_metadata_operation(Self::TYPENAME, id, entity)]
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum SubgraphHealth {
    /// Syncing without errors.
    Healthy,

    /// Syncing but has errors.
    Unhealthy,

    /// No longer syncing due to fatal error.
    Failed,
}

impl SubgraphHealth {
    pub fn as_str(&self) -> &'static str {
        match self {
            SubgraphHealth::Healthy => "healthy",
            SubgraphHealth::Unhealthy => "unhealthy",
            SubgraphHealth::Failed => "failed",
        }
    }
}

impl FromStr for SubgraphHealth {
    type Err = Error;

    fn from_str(s: &str) -> Result<SubgraphHealth, Error> {
        match s {
            "healthy" => Ok(SubgraphHealth::Healthy),
            "unhealthy" => Ok(SubgraphHealth::Unhealthy),
            "failed" => Ok(SubgraphHealth::Failed),
            _ => Err(format_err!("failed to parse `{}` as SubgraphHealth", s)),
        }
    }
}

impl From<SubgraphHealth> for String {
    fn from(health: SubgraphHealth) -> String {
        health.as_str().to_string()
    }
}

impl From<SubgraphHealth> for Value {
    fn from(health: SubgraphHealth) -> Value {
        String::from(health).into()
    }
}

impl From<SubgraphHealth> for q::Value {
    fn from(health: SubgraphHealth) -> q::Value {
        q::Value::Enum(health.into())
    }
}

impl TryFromValue for SubgraphHealth {
    fn try_from_value(value: &q::Value) -> Result<SubgraphHealth, Error> {
        match value {
            q::Value::Enum(health) => SubgraphHealth::from_str(health),
            _ => Err(format_err!(
                "cannot parse value as SubgraphHealth: `{:?}`",
                value
            )),
        }
    }
}

#[derive(Debug)]
pub struct SubgraphDeploymentEntity {
    manifest: SubgraphManifestEntity,
    failed: bool,
    health: SubgraphHealth,
    synced: bool,
    fatal_error: Option<SubgraphError>,
    non_fatal_errors: Vec<SubgraphError>,
    earliest_ethereum_block_hash: Option<H256>,
    earliest_ethereum_block_number: Option<u64>,
    latest_ethereum_block_hash: Option<H256>,
    latest_ethereum_block_number: Option<u64>,
    graft_base: Option<SubgraphDeploymentId>,
    graft_block_hash: Option<H256>,
    graft_block_number: Option<u64>,
    reorg_count: i32,
    current_reorg_depth: i32,
    max_reorg_depth: i32,
}

impl TypedEntity for SubgraphDeploymentEntity {
    const TYPENAME: MetadataType = MetadataType::SubgraphDeployment;
    type IdType = SubgraphDeploymentId;
}

impl SubgraphDeploymentEntity {
    pub fn new(
        source_manifest: &SubgraphManifest,
        synced: bool,
        earliest_ethereum_block: Option<EthereumBlockPointer>,
    ) -> Self {
        Self {
            manifest: SubgraphManifestEntity::from(source_manifest),
            failed: false,
            health: SubgraphHealth::Healthy,
            synced,
            fatal_error: None,
            non_fatal_errors: vec![],
            earliest_ethereum_block_hash: earliest_ethereum_block.map(Into::into),
            earliest_ethereum_block_number: earliest_ethereum_block.map(Into::into),
            latest_ethereum_block_hash: earliest_ethereum_block.map(Into::into),
            latest_ethereum_block_number: earliest_ethereum_block.map(Into::into),
            graft_base: None,
            graft_block_hash: None,
            graft_block_number: None,
            reorg_count: 0,
            current_reorg_depth: 0,
            max_reorg_depth: 0,
        }
    }

    pub fn graft(mut self, base: Option<(SubgraphDeploymentId, EthereumBlockPointer)>) -> Self {
        if let Some((subgraph, ptr)) = base {
            self.graft_base = Some(subgraph);
            self.graft_block_hash = Some(ptr.hash);
            self.graft_block_number = Some(ptr.number);
            self.latest_ethereum_block_hash = Some(ptr.hash);
            self.latest_ethereum_block_number = Some(ptr.number);
        }
        self
    }

    // Overwrite entity if it exists. Only in debug builds so it's not used outside tests.
    #[cfg(debug_assertions)]
    pub fn create_operations_replace(self, id: &SubgraphDeploymentId) -> Vec<MetadataOperation> {
        self.private_create_operations(id)
    }

    pub fn create_operations(self, id: &SubgraphDeploymentId) -> Vec<MetadataOperation> {
        let mut ops: Vec<MetadataOperation> = vec![];

        // Abort unless no entity exists with this ID
        ops.push(Self::abort_unless(
            "Subgraph deployment entity must not exist yet to be created",
            EntityFilter::new_equal("id", id.to_string()),
            vec![],
        ));

        ops.extend(self.private_create_operations(id));
        ops
    }

    fn private_create_operations(self, id: &SubgraphDeploymentId) -> Vec<MetadataOperation> {
        let mut ops = vec![];

        let SubgraphDeploymentEntity {
            manifest,
            failed,
            health,
            synced,
            fatal_error,
            non_fatal_errors,
            earliest_ethereum_block_hash,
            earliest_ethereum_block_number,
            latest_ethereum_block_hash,
            latest_ethereum_block_number,
            graft_base,
            graft_block_hash,
            graft_block_number,
            reorg_count: _,
            current_reorg_depth: _,
            max_reorg_depth: _,
        } = self;

        // A fresh subgraph will not have any errors.
        assert!(fatal_error.is_none());
        assert!(non_fatal_errors.is_empty());
        let non_fatal_errors = Vec::<Value>::new();

        let manifest_id = SubgraphManifestEntity::id(&id);
        ops.extend(manifest.write_operations(&manifest_id));

        let entity = entity! {
            id: id.to_string(),
            manifest: manifest_id,
            failed: failed,
            health: health,
            synced: synced,
            nonFatalErrors: non_fatal_errors,
            earliestEthereumBlockHash: earliest_ethereum_block_hash,
            earliestEthereumBlockNumber: earliest_ethereum_block_number,
            latestEthereumBlockHash: latest_ethereum_block_hash,
            latestEthereumBlockNumber: latest_ethereum_block_number,
            entityCount: 0 as u64,
            graftBase: graft_base.map(|sid| sid.to_string()),
            graftBlockHash: graft_block_hash,
            graftBlockNumber: graft_block_number,
        };

        ops.push(set_metadata_operation(
            Self::TYPENAME,
            id.to_string(),
            entity,
        ));

        ops
    }

    /// When starting the subgraph, we try to "unfail" it.
    pub fn unfail_operations(
        id: &SubgraphDeploymentId,
        new_health: SubgraphHealth,
    ) -> Vec<MetadataOperation> {
        let entity = entity! {
            failed: false,
            health: new_health,
            fatalError: Value::Null,
        };

        vec![update_metadata_operation(
            Self::TYPENAME,
            id.as_str(),
            entity,
        )]
    }

    pub fn fail_operations(
        id: &SubgraphDeploymentId,
        error: SubgraphError,
    ) -> Vec<MetadataOperation> {
        let error_id = hex::encode(&stable_hash::<SetHasher, _>(&error));

        let mut entity = Entity::new();
        entity.set("failed", true);
        entity.set("health", SubgraphHealth::Failed);
        entity.set("fatalError", error_id.clone());

        vec![
            error.create_operation(error_id),
            update_metadata_operation(Self::TYPENAME, id.as_str(), entity),
        ]
    }

    pub fn update_synced_operations(
        id: &SubgraphDeploymentId,
        synced: bool,
    ) -> Vec<MetadataOperation> {
        let entity = entity! {
            synced: synced,
        };

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
    const TYPENAME: MetadataType = MetadataType::SubgraphDeploymentAssignment;
    type IdType = SubgraphDeploymentId;
}

impl SubgraphDeploymentAssignmentEntity {
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id, cost: 1 }
    }

    pub fn write_operations(self, id: &SubgraphDeploymentId) -> Vec<MetadataOperation> {
        let entity = entity! {
            id: id.to_string(),
            nodeId: self.node_id.to_string(),
            cost: self.cost,
        };
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
    const TYPENAME: MetadataType = MetadataType::SubgraphManifest;
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

        let entity = entity! {
            id: id,
            specVersion: self.spec_version,
            description: self.description,
            repository: self.repository,
            schema: self.schema,
            dataSources: data_source_ids,
            templates: template_ids,
        };

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
    const TYPENAME: MetadataType = MetadataType::EthereumContractDataSource;
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

        let entity = entity! {
            id: id,
            kind: self.kind,
            network: self.network,
            name: self.name,
            source: source_id,
            mapping: mapping_id,
            templates: template_ids,
        };

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
                .map(EthereumContractDataSourceTemplateEntity::from)
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
    const TYPENAME: MetadataType = MetadataType::DynamicEthereumContractDataSource;
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

        let entity = entity! {
            id: id,
            kind: kind,
            network: network,
            name: name,
            source: source_id,
            mapping: mapping_id,
            templates: template_ids,
            deployment: deployment,
            ethereumBlockHash: ethereum_block_hash,
            ethereumBlockNumber: ethereum_block_number,
            context: context
                .as_ref()
                .map(|ctx| serde_json::to_string(&ctx).unwrap()),
        };

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
            ethereum_block_hash: block_ptr.hash,
            ethereum_block_number: block_ptr.number,
            name: name.clone(),
            network: network.clone(),
            source: source.clone().into(),
            mapping: EthereumContractMappingEntity::from(mapping),
            templates: templates
                .iter()
                .map(EthereumContractDataSourceTemplateEntity::from)
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
    const TYPENAME: MetadataType = MetadataType::EthereumContractSource;
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
    const TYPENAME: MetadataType = MetadataType::EthereumContractMapping;
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
    const TYPENAME: MetadataType = MetadataType::EthereumContractAbi;
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
        if let Some(filter_id) = filter_id {
            entity.set("filter", filter_id);
        }
        ops.add(Self::TYPENAME, id.to_owned(), entity);
    }
}

impl TypedEntity for EthereumBlockHandlerEntity {
    const TYPENAME: MetadataType = MetadataType::EthereumBlockHandlerEntity;
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
            filter,
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
    const TYPENAME: MetadataType = MetadataType::EthereumBlockHandlerFilterEntity;
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
    const TYPENAME: MetadataType = MetadataType::EthereumCallHandlerEntity;
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
    const TYPENAME: MetadataType = MetadataType::EthereumContractEventHandler;
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
    const TYPENAME: MetadataType = MetadataType::EthereumContractDataSourceTemplate;
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
    const TYPENAME: MetadataType = MetadataType::EthereumContractDataSourceTemplateSource;
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
    entity: MetadataType,
    entity_id: impl Into<String>,
    data: impl Into<Entity>,
) -> MetadataOperation {
    MetadataOperation::Set {
        entity,
        id: entity_id.into(),
        data: data.into(),
    }
}

fn update_metadata_operation(
    entity: MetadataType,
    entity_id: impl Into<String>,
    data: impl Into<Entity>,
) -> MetadataOperation {
    MetadataOperation::Update {
        entity,
        id: entity_id.into(),
        data: data.into(),
    }
}

#[derive(Debug)]
pub struct SubgraphError {
    pub subgraph_id: SubgraphDeploymentId,
    pub message: String,
    pub block_ptr: Option<EthereumBlockPointer>,
    pub handler: Option<String>,

    // `true` if we are certain the error is determinsitic. If in doubt, this is `false`.
    pub deterministic: bool,
}

impl StableHash for SubgraphError {
    fn stable_hash<H: StableHasher>(&self, mut sequence_number: H::Seq, state: &mut H) {
        let SubgraphError {
            subgraph_id,
            message,
            block_ptr,
            handler,
            deterministic,
        } = self;
        subgraph_id.stable_hash(sequence_number.next_child(), state);
        message.stable_hash(sequence_number.next_child(), state);
        block_ptr.stable_hash(sequence_number.next_child(), state);
        handler.stable_hash(sequence_number.next_child(), state);
        deterministic.stable_hash(sequence_number.next_child(), state);
    }
}

impl TypedEntity for SubgraphError {
    const TYPENAME: MetadataType = MetadataType::SubgraphError;
    type IdType = String;
}

impl SubgraphError {
    fn create_operation(self, id: String) -> MetadataOperation {
        let mut entity = Entity::from(self);
        entity.set("id", id.clone());
        set_metadata_operation(Self::TYPENAME, id, entity)
    }
}

impl From<SubgraphError> for Entity {
    fn from(subgraph_error: SubgraphError) -> Entity {
        let SubgraphError {
            subgraph_id,
            message,
            block_ptr,
            handler,
            deterministic,
        } = subgraph_error;

        let mut entity = Entity::new();
        entity.set("subgraphId", subgraph_id.to_string());
        entity.set("message", message);
        entity.set("blockNumber", block_ptr.map(|x| x.number));
        entity.set("blockHash", block_ptr.map(|x| x.hash));
        entity.set("handler", handler);
        entity.set("deterministic", deterministic);
        entity
    }
}

impl TryFromValue for SubgraphError {
    fn try_from_value(value: &q::Value) -> Result<SubgraphError, Error> {
        let block_number = value.get_optional("blockNumber")?;
        let block_hash = value.get_optional("blockHash")?;

        let block_ptr = match (block_number, block_hash) {
            (Some(number), Some(hash)) => Some(EthereumBlockPointer { number, hash }),
            _ => None,
        };

        Ok(SubgraphError {
            subgraph_id: value.get_required("subgraphId")?,
            message: value.get_required("message")?,
            block_ptr,
            handler: value.get_optional("handler")?,
            deterministic: value.get_optional("deterministic")?.unwrap_or(false),
        })
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
