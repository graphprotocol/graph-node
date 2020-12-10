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

use anyhow::{anyhow, Error};
use hex;
use rand::rngs::OsRng;
use rand::Rng;
use stable_hash::{SequenceNumber, StableHash, StableHasher};
use std::str::FromStr;
use std::{collections::BTreeMap, fmt, fmt::Display};
use strum_macros::{EnumString, IntoStaticStr};
use uuid::Uuid;
use web3::types::*;

use super::SubgraphDeploymentId;
use crate::components::ethereum::EthereumBlockPointer;
use crate::components::store::{EntityOperation, MetadataKey, MetadataOperation};
use crate::data::graphql::{TryFromValue, ValueMap};
use crate::data::store::{Entity, Value};
use crate::data::subgraph::SubgraphManifest;
use crate::prelude::*;

pub const POI_TABLE: &str = "poi2$";
pub const POI_OBJECT: &str = "Poi$";

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    IntoStaticStr,
    EnumString,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
pub enum MetadataType {
    SubgraphDeployment,
    // This is the only metadata type that is stored in the primary. We only
    // need this type so we can send store events for assignment changes
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

    pub fn key(&self, subgraph_id: SubgraphDeploymentId, entity_id: String) -> MetadataKey {
        MetadataKey {
            subgraph_id,
            entity_type: self.clone(),
            entity_id,
        }
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
}

// See also: ed42d219c6704a4aab57ce1ea66698e7.
// Note: The types here need to be in sync with the metadata GraphQL schema.

trait OperationList {
    fn add(&mut self, entity: MetadataType, id: String, data: Entity);
}

struct MetadataOperationList(SubgraphDeploymentId, Vec<MetadataOperation>);

impl OperationList for MetadataOperationList {
    fn add(&mut self, entity_type: MetadataType, id: String, data: Entity) {
        let key = entity_type.key(self.0.clone(), id);
        self.1.push(MetadataOperation::Set { key, data })
    }
}

struct EntityOperationList(SubgraphDeploymentId, Vec<EntityOperation>);

impl OperationList for EntityOperationList {
    fn add(&mut self, entity_type: MetadataType, id: String, data: Entity) {
        let key = entity_type.key(self.0.clone(), id).into();
        self.1.push(EntityOperation::Set { key, data })
    }
}

trait WriteOperations: Sized {
    fn generate(self, id: &str, ops: &mut dyn OperationList);

    fn write_operations(self, subgraph: &SubgraphDeploymentId, id: &str) -> Vec<MetadataOperation> {
        let mut ops = MetadataOperationList(subgraph.clone(), Vec::new());
        self.generate(id, &mut ops);
        ops.1
    }

    fn write_entity_operations(
        self,
        subgraph: &SubgraphDeploymentId,
        id: &str,
    ) -> Vec<EntityOperation> {
        let mut ops = EntityOperationList(subgraph.clone(), Vec::new());
        self.generate(id, &mut ops);
        ops.1
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

    pub fn is_failed(&self) -> bool {
        match self {
            SubgraphHealth::Healthy => false,
            SubgraphHealth::Unhealthy => false,
            SubgraphHealth::Failed => true,
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
            _ => Err(anyhow!("failed to parse `{}` as SubgraphHealth", s)),
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
            _ => Err(anyhow!(
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
    pub graft_base: Option<SubgraphDeploymentId>,
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
            // When we graft, the block pointer is only set after copying
            // from the base subgraph finished successfully
            self.latest_ethereum_block_hash = None;
            self.latest_ethereum_block_number = None;
        }
        self
    }

    pub fn create_operations(self, id: &SubgraphDeploymentId) -> Vec<MetadataOperation> {
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
        ops.extend(manifest.write_operations(id, &manifest_id));

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
            id.clone(),
            Self::TYPENAME,
            id.to_string(),
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
    features: Vec<String>,
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

    fn write_operations(self, subgraph: &SubgraphDeploymentId, id: &str) -> Vec<MetadataOperation> {
        let SubgraphManifestEntity {
            spec_version,
            description,
            repository,
            features,
            schema,
            data_sources,
            templates,
        } = self;

        let mut ops = vec![];

        let mut data_source_ids: Vec<Value> = vec![];
        for (i, data_source) in data_sources.into_iter().enumerate() {
            let data_source_id = format!("{}-data-source-{}", id, i);
            ops.extend(data_source.write_operations(subgraph, &data_source_id));
            data_source_ids.push(data_source_id.into());
        }

        let template_ids: Vec<Value> = templates
            .into_iter()
            .enumerate()
            .map(|(i, template)| {
                let template_id = format!("{}-templates-{}", id, i);
                ops.extend(template.write_operations(subgraph, &template_id));
                template_id.into()
            })
            .collect();

        let entity = entity! {
            id: id,
            specVersion: spec_version,
            description: description,
            repository: repository,
            features: features,
            schema: schema,
            dataSources: data_source_ids,
            templates: template_ids,
        };

        ops.push(set_metadata_operation(
            subgraph.clone(),
            Self::TYPENAME,
            id,
            entity,
        ));

        ops
    }
}

impl<'a> From<&'a super::SubgraphManifest> for SubgraphManifestEntity {
    fn from(manifest: &'a super::SubgraphManifest) -> Self {
        Self {
            spec_version: manifest.spec_version.clone(),
            description: manifest.description.clone(),
            repository: manifest.repository.clone(),
            features: manifest.features.iter().map(|f| f.to_string()).collect(),
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
}

impl TypedEntity for EthereumContractDataSourceEntity {
    const TYPENAME: MetadataType = MetadataType::EthereumContractDataSource;
    type IdType = String;
}

impl EthereumContractDataSourceEntity {
    pub fn write_operations(
        self,
        subgraph: &SubgraphDeploymentId,
        id: &str,
    ) -> Vec<MetadataOperation> {
        let mut ops = vec![];

        let source_id = format!("{}-source", id);
        ops.extend(self.source.write_operations(subgraph, &source_id));

        let mapping_id = format!("{}-mapping", id);
        ops.extend(self.mapping.write_operations(subgraph, &mapping_id));

        let entity = entity! {
            id: id,
            kind: self.kind,
            network: self.network,
            name: self.name,
            source: source_id,
            mapping: mapping_id,
        };

        ops.push(set_metadata_operation(
            subgraph.clone(),
            Self::TYPENAME,
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
    context: Option<DataSourceContext>,
}

impl DynamicEthereumContractDataSourceEntity {
    pub fn write_entity_operations(
        self,
        subgraph: &SubgraphDeploymentId,
        id: &str,
    ) -> Vec<EntityOperation> {
        WriteOperations::write_entity_operations(self, subgraph, id)
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
            context,
        } = self;

        let entity = entity! {
            id: id,
            kind: kind,
            network: network,
            name: name,
            source: source_id,
            mapping: mapping_id,
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
            context,
            creation_block: _,
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
            _ => Err(anyhow!(
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
            _ => Err(anyhow!(
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
            _ => Err(anyhow!("Cannot parse value into ABI entity: {:?}", value)),
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
            _ => Err(anyhow!(
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
            _ => Err(anyhow!(
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
            _ => Err(anyhow!(
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
            _ => Err(anyhow!(
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
            _ => Err(anyhow!(
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
            _ => Err(anyhow!(
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
    subgraph: SubgraphDeploymentId,
    entity: MetadataType,
    entity_id: impl Into<String>,
    data: impl Into<Entity>,
) -> MetadataOperation {
    MetadataOperation::Set {
        key: entity.key(subgraph, entity_id.into()),
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

impl Display for SubgraphError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.message)?;
        if let Some(handler) = &self.handler {
            write!(f, " in handler `{}`", handler)?;
        }
        if let Some(block_ptr) = self.block_ptr {
            write!(f, " at block {}", block_ptr)?;
        }
        Ok(())
    }
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
