use anyhow::{anyhow, Context as _, Error};
use ethabi::Contract;
use futures03::{
    future::{try_join, try_join3},
    stream::FuturesOrdered,
    TryStreamExt as _,
};
use lazy_static::lazy_static;
use semver::Version;
use serde::de;
use serde::ser;
use serde_yaml;
use slog::{debug, info, Logger};
use stable_hash::prelude::*;
use std::collections::BTreeSet;
use thiserror::Error;
use wasmparser;
use web3::types::{Address, H256};

use crate::components::link_resolver::LinkResolver;
use crate::components::store::{StoreError, SubgraphStore};
use crate::components::subgraph::DataSourceTemplateInfo;
use crate::data::graphql::TryFromValue;
use crate::data::query::QueryExecutionError;
use crate::data::schema::{Schema, SchemaImportError, SchemaValidationError};
use crate::data::store::Entity;
use crate::data::subgraph::schema::{
    EthereumBlockHandlerEntity, EthereumCallHandlerEntity, EthereumContractAbiEntity,
    EthereumContractDataSourceTemplateEntity, EthereumContractDataSourceTemplateSourceEntity,
    EthereumContractEventHandlerEntity, EthereumContractMappingEntity,
    EthereumContractSourceEntity,
};
use crate::prelude::CheapClone;

use crate::prelude::{impl_slog_value, q, BlockNumber, Deserialize, Serialize};
use crate::util::ethereum::string_to_h256;

use crate::components::ethereum::NodeCapabilities;
use std::convert::TryFrom;
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

lazy_static! {
    static ref DISABLE_GRAFTS: bool = std::env::var("GRAPH_DISABLE_GRAFTS")
        .ok()
        .map(|s| s.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    static ref MIN_SPEC_VERSION: Version = Version::new(0, 0, 2);

    // Before this check was introduced, there were already subgraphs in
    // the wild with spec version 0.0.3, due to confusion with the api
    // version. To avoid breaking those, we accept 0.0.3 though it
    // doesn't exist. In the future we should not use 0.0.3 as version
    // and skip to 0.0.4 to avoid ambiguity.
    static ref MAX_SPEC_VERSION: Version = Version::new(0, 0, 3);
}

/// Rust representation of the GraphQL schema for a `SubgraphManifest`.
pub mod schema;

pub mod status;

/// Deserialize an Address (with or without '0x' prefix).
fn deserialize_address<'de, D>(deserializer: D) -> Result<Option<Address>, D::Error>
where
    D: de::Deserializer<'de>,
{
    use serde::de::Error;

    let s: String = de::Deserialize::deserialize(deserializer)?;
    let address = s.trim_start_matches("0x");
    Address::from_str(address)
        .map_err(D::Error::custom)
        .map(Some)
}

// Note: This has a StableHash impl. Do not modify fields without a backward
// compatible change to the StableHash impl (below)
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubgraphDeploymentId(String);

impl StableHash for SubgraphDeploymentId {
    #[inline]
    fn stable_hash<H: StableHasher>(&self, mut sequence_number: H::Seq, state: &mut H) {
        self.0.stable_hash(sequence_number.next_child(), state);
    }
}

impl_slog_value!(SubgraphDeploymentId);

/// `SubgraphDeploymentId` is fixed-length so cheap to clone.
impl CheapClone for SubgraphDeploymentId {}

impl SubgraphDeploymentId {
    /// Check that `s` is a valid `SubgraphDeploymentId` and create a new one.
    /// If `s` is longer than 46 characters, or contains characters other than
    /// alphanumeric characters or `_`, return s (as a `String`) as the error
    pub fn new(s: impl Into<String>) -> Result<Self, String> {
        let s = s.into();

        // Enforce length limit
        if s.len() > 46 {
            return Err(s);
        }

        // Check that the ID contains only allowed characters.
        if !s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(s);
        }

        // Allow only deployment id's for 'real' subgraphs, not the old
        // metadata subgraph.
        if s == "subgraphs" {
            return Err(s);
        }

        Ok(SubgraphDeploymentId(s))
    }

    pub fn to_ipfs_link(&self) -> Link {
        Link {
            link: format!("/ipfs/{}", self),
        }
    }
}

impl Deref for SubgraphDeploymentId {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for SubgraphDeploymentId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl ser::Serialize for SubgraphDeploymentId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> de::Deserialize<'de> for SubgraphDeploymentId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s: String = de::Deserialize::deserialize(deserializer)?;
        SubgraphDeploymentId::new(s)
            .map_err(|s| de::Error::invalid_value(de::Unexpected::Str(&s), &"valid subgraph name"))
    }
}

impl TryFromValue for SubgraphDeploymentId {
    fn try_from_value(value: &q::Value) -> Result<Self, Error> {
        Self::new(String::try_from_value(value)?)
            .map_err(|s| anyhow!("Invalid subgraph ID `{}`", s))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubgraphName(String);

impl SubgraphName {
    pub fn new(s: impl Into<String>) -> Result<Self, ()> {
        let s = s.into();

        // Note: these validation rules must be kept consistent with the validation rules
        // implemented in any other components that rely on subgraph names.

        // Enforce length limits
        if s.is_empty() || s.len() > 255 {
            return Err(());
        }

        // Check that the name contains only allowed characters.
        if !s
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '/')
        {
            return Err(());
        }

        // Parse into components and validate each
        for part in s.split("/") {
            // Each part must be non-empty and not too long
            if part.is_empty() || part.len() > 32 {
                return Err(());
            }

            // To keep URLs unambiguous, reserve the token "graphql"
            if part == "graphql" {
                return Err(());
            }

            // Part should not start or end with a special character.
            let first_char = part.chars().next().unwrap();
            let last_char = part.chars().last().unwrap();
            if !first_char.is_ascii_alphanumeric()
                || !last_char.is_ascii_alphanumeric()
                || !part.chars().any(|c| c.is_ascii_alphabetic())
            {
                return Err(());
            }
        }

        Ok(SubgraphName(s))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for SubgraphName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl ser::Serialize for SubgraphName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> de::Deserialize<'de> for SubgraphName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s: String = de::Deserialize::deserialize(deserializer)?;
        SubgraphName::new(s.clone())
            .map_err(|()| de::Error::invalid_value(de::Unexpected::Str(&s), &"valid subgraph name"))
    }
}

#[test]
fn test_subgraph_name_validation() {
    assert!(SubgraphName::new("a").is_ok());
    assert!(SubgraphName::new("a/a").is_ok());
    assert!(SubgraphName::new("a-lOng-name_with_0ne-component").is_ok());
    assert!(SubgraphName::new("a-long-name_with_one-3omponent").is_ok());
    assert!(SubgraphName::new("a/b_c").is_ok());
    assert!(SubgraphName::new("A/Z-Z").is_ok());
    assert!(SubgraphName::new("a1/A-A").is_ok());
    assert!(SubgraphName::new("aaa/a1").is_ok());
    assert!(SubgraphName::new("1a/aaaa").is_ok());
    assert!(SubgraphName::new("aaaa/1a").is_ok());
    assert!(SubgraphName::new("2nena4test/lala").is_ok());

    assert!(SubgraphName::new("").is_err());
    assert!(SubgraphName::new("/a").is_err());
    assert!(SubgraphName::new("a/").is_err());
    assert!(SubgraphName::new("a//a").is_err());
    assert!(SubgraphName::new("a/0").is_err());
    assert!(SubgraphName::new("a/_").is_err());
    assert!(SubgraphName::new("a/a_").is_err());
    assert!(SubgraphName::new("a/_a").is_err());
    assert!(SubgraphName::new("aaaa aaaaa").is_err());
    assert!(SubgraphName::new("aaaa!aaaaa").is_err());
    assert!(SubgraphName::new("aaaa+aaaaa").is_err());
    assert!(SubgraphName::new("a/graphql").is_err());
    assert!(SubgraphName::new("graphql/a").is_err());
    assert!(SubgraphName::new("this-component-is-longer-than-the-length-limit").is_err());
}

/// Result of a creating a subgraph in the registar.
#[derive(Serialize)]
pub struct CreateSubgraphResult {
    /// The ID of the subgraph that was created.
    pub id: String,
}

#[derive(Error, Debug)]
pub enum SubgraphRegistrarError {
    #[error("subgraph resolve error: {0}")]
    ResolveError(SubgraphManifestResolveError),
    #[error("subgraph already exists: {0}")]
    NameExists(String),
    #[error("subgraph name not found: {0}")]
    NameNotFound(String),
    #[error("Ethereum network not supported by registrar: {0}")]
    NetworkNotSupported(String),
    #[error("Ethereum nodes for network {0} are missing the following capabilities: {1}")]
    SubgraphNetworkRequirementsNotSupported(String, NodeCapabilities),
    #[error("deployment not found: {0}")]
    DeploymentNotFound(String),
    #[error("deployment assignment unchanged: {0}")]
    DeploymentAssignmentUnchanged(String),
    #[error("subgraph registrar internal query error: {0}")]
    QueryExecutionError(QueryExecutionError),
    #[error("subgraph registrar error with store: {0}")]
    StoreError(StoreError),
    #[error("subgraph validation error: {0:?}")]
    ManifestValidationError(Vec<SubgraphManifestValidationError>),
    #[error("subgraph deployment error: {0}")]
    SubgraphDeploymentError(StoreError),
    #[error("subgraph registrar error: {0}")]
    Unknown(anyhow::Error),
}

impl From<QueryExecutionError> for SubgraphRegistrarError {
    fn from(e: QueryExecutionError) -> Self {
        SubgraphRegistrarError::QueryExecutionError(e)
    }
}

impl From<StoreError> for SubgraphRegistrarError {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::DeploymentNotFound(id) => SubgraphRegistrarError::DeploymentNotFound(id),
            e => SubgraphRegistrarError::StoreError(e),
        }
    }
}

impl From<Error> for SubgraphRegistrarError {
    fn from(e: Error) -> Self {
        SubgraphRegistrarError::Unknown(e)
    }
}

impl From<SubgraphManifestValidationError> for SubgraphRegistrarError {
    fn from(e: SubgraphManifestValidationError) -> Self {
        SubgraphRegistrarError::ManifestValidationError(vec![e])
    }
}

#[derive(Error, Debug)]
pub enum SubgraphAssignmentProviderError {
    #[error("Subgraph resolve error: {0}")]
    ResolveError(SubgraphManifestResolveError),
    #[error("Failed to load dynamic data sources: {0}")]
    DynamicDataSourcesError(anyhow::Error),
    /// Occurs when attempting to remove a subgraph that's not hosted.
    #[error("Subgraph with ID {0} already running")]
    AlreadyRunning(SubgraphDeploymentId),
    #[error("Subgraph with ID {0} is not running")]
    NotRunning(SubgraphDeploymentId),
    /// Occurs when a subgraph's GraphQL schema is invalid.
    #[error("GraphQL schema error: {0}")]
    SchemaValidationError(anyhow::Error),
    #[error("Error building index for subgraph {0}, entity {1} and attribute {2}")]
    BuildIndexesError(String, String, String),
    #[error("Subgraph provider error: {0}")]
    Unknown(anyhow::Error),
}

impl From<Error> for SubgraphAssignmentProviderError {
    fn from(e: Error) -> Self {
        SubgraphAssignmentProviderError::Unknown(e)
    }
}

impl From<::diesel::result::Error> for SubgraphAssignmentProviderError {
    fn from(e: ::diesel::result::Error) -> Self {
        SubgraphAssignmentProviderError::Unknown(e.into())
    }
}

/// Events emitted by [SubgraphAssignmentProvider](trait.SubgraphAssignmentProvider.html) implementations.
#[derive(Debug, PartialEq)]
pub enum SubgraphAssignmentProviderEvent {
    /// A subgraph with the given manifest should start processing.
    SubgraphStart(SubgraphManifest),
    /// The subgraph with the given ID should stop processing.
    SubgraphStop(SubgraphDeploymentId),
}

#[derive(Error, Debug)]
pub enum SubgraphManifestValidationWarning {
    #[error("schema validation produced warnings: {0:?}")]
    SchemaValidationWarning(SchemaImportError),
}

#[derive(Error, Debug)]
pub enum SubgraphManifestValidationError {
    #[error("subgraph has no data sources")]
    NoDataSources,
    #[error("subgraph source address is required")]
    SourceAddressRequired,
    #[error("subgraph cannot index data from different Ethereum networks")]
    MultipleEthereumNetworks,
    #[error("subgraph must have at least one Ethereum network data source")]
    EthereumNetworkRequired,
    #[error("subgraph data source has too many similar block handlers")]
    DataSourceBlockHandlerLimitExceeded,
    #[error("the specified block must exist on the Ethereum network")]
    BlockNotFound(String),
    #[error("imported schema(s) are invalid: {0:?}")]
    SchemaImportError(Vec<SchemaImportError>),
    #[error("schema validation failed: {0:?}")]
    SchemaValidationError(Vec<SchemaValidationError>),
    #[error("the graft base is invalid: {0}")]
    GraftBaseInvalid(String),
}

#[derive(Error, Debug)]
pub enum SubgraphManifestResolveError {
    #[error("parse error: {0}")]
    ParseError(serde_yaml::Error),
    #[error("subgraph is not UTF-8")]
    NonUtf8,
    #[error("subgraph is not valid YAML")]
    InvalidFormat,
    #[error("resolve error: {0}")]
    ResolveError(anyhow::Error),
}

impl From<serde_yaml::Error> for SubgraphManifestResolveError {
    fn from(e: serde_yaml::Error) -> Self {
        SubgraphManifestResolveError::ParseError(e)
    }
}

/// Data source contexts are conveniently represented as entities.
pub type DataSourceContext = Entity;

/// IPLD link.
#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
pub struct Link {
    #[serde(rename = "/")]
    pub link: String,
}

impl From<String> for Link {
    fn from(s: String) -> Self {
        Self { link: s }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct UnresolvedSchema {
    pub file: Link,
}

impl UnresolvedSchema {
    pub async fn resolve(
        self,
        id: SubgraphDeploymentId,
        resolver: &impl LinkResolver,
        logger: &Logger,
    ) -> Result<Schema, anyhow::Error> {
        info!(logger, "Resolve schema"; "link" => &self.file.link);

        let schema_bytes = resolver.cat(&logger, &self.file).await?;
        Schema::parse(&String::from_utf8(schema_bytes)?, id)
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct Source {
    #[serde(default, deserialize_with = "deserialize_address")]
    pub address: Option<Address>,
    pub abi: String,
    #[serde(rename = "startBlock", default)]
    pub start_block: u64,
}

impl From<EthereumContractSourceEntity> for Source {
    fn from(entity: EthereumContractSourceEntity) -> Self {
        Self {
            address: entity.address,
            abi: entity.abi,
            start_block: entity.start_block,
        }
    }
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
pub struct TemplateSource {
    pub abi: String,
}

impl From<EthereumContractDataSourceTemplateSourceEntity> for TemplateSource {
    fn from(entity: EthereumContractDataSourceTemplateSourceEntity) -> Self {
        Self { abi: entity.abi }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct UnresolvedMappingABI {
    pub name: String,
    pub file: Link,
}

impl From<EthereumContractAbiEntity> for UnresolvedMappingABI {
    fn from(entity: EthereumContractAbiEntity) -> Self {
        Self {
            name: entity.name,
            file: entity.file.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MappingABI {
    pub name: String,
    pub contract: Contract,
    pub link: Link,
}

impl UnresolvedMappingABI {
    pub async fn resolve(
        self,
        resolver: &impl LinkResolver,
        logger: &Logger,
    ) -> Result<MappingABI, anyhow::Error> {
        info!(
            logger,
            "Resolve ABI";
            "name" => &self.name,
            "link" => &self.file.link
        );

        let contract_bytes = resolver.cat(&logger, &self.file).await?;
        let contract = Contract::load(&*contract_bytes)?;
        Ok(MappingABI {
            name: self.name,
            contract,
            link: self.file,
        })
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct MappingBlockHandler {
    pub handler: String,
    pub filter: Option<BlockHandlerFilter>,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum BlockHandlerFilter {
    // Call filter will trigger on all blocks where the data source contract
    // address has been called
    Call,
}

impl From<EthereumBlockHandlerEntity> for MappingBlockHandler {
    fn from(entity: EthereumBlockHandlerEntity) -> Self {
        Self {
            handler: entity.handler,
            filter: None,
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct MappingCallHandler {
    pub function: String,
    pub handler: String,
}

impl From<EthereumCallHandlerEntity> for MappingCallHandler {
    fn from(entity: EthereumCallHandlerEntity) -> Self {
        Self {
            function: entity.function,
            handler: entity.handler,
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct MappingEventHandler {
    pub event: String,
    pub topic0: Option<H256>,
    pub handler: String,
}

impl MappingEventHandler {
    pub fn topic0(&self) -> H256 {
        self.topic0
            .unwrap_or_else(|| string_to_h256(&self.event.replace("indexed ", "")))
    }
}

impl From<EthereumContractEventHandlerEntity> for MappingEventHandler {
    fn from(entity: EthereumContractEventHandlerEntity) -> Self {
        Self {
            event: entity.event,
            topic0: entity.topic0,
            handler: entity.handler,
        }
    }
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnresolvedMapping {
    pub kind: String,
    pub api_version: String,
    pub language: String,
    pub entities: Vec<String>,
    pub abis: Vec<UnresolvedMappingABI>,
    #[serde(default)]
    pub block_handlers: Vec<MappingBlockHandler>,
    #[serde(default)]
    pub call_handlers: Vec<MappingCallHandler>,
    #[serde(default)]
    pub event_handlers: Vec<MappingEventHandler>,
    pub file: Link,
}

#[derive(Clone, Debug)]
pub struct Mapping {
    pub kind: String,
    pub api_version: String,
    pub language: String,
    pub entities: Vec<String>,
    pub abis: Vec<MappingABI>,
    pub block_handlers: Vec<MappingBlockHandler>,
    pub call_handlers: Vec<MappingCallHandler>,
    pub event_handlers: Vec<MappingEventHandler>,
    pub runtime: Arc<Vec<u8>>,
    pub link: Link,
}

impl Mapping {
    pub fn calls_host_fn(&self, host_fn: &str) -> bool {
        use wasmparser::Payload;

        let runtime = self.runtime.as_ref().as_ref();

        for payload in wasmparser::Parser::new(0).parse_all(runtime) {
            match payload.unwrap() {
                Payload::ImportSection(s) => {
                    for import in s {
                        let import = import.unwrap();
                        if import.field == Some(host_fn) {
                            return true;
                        }
                    }
                }
                _ => (),
            }
        }

        return false;
    }

    fn has_call_handler(&self) -> bool {
        !self.call_handlers.is_empty()
    }

    fn has_block_handler_with_call_filter(&self) -> bool {
        self.block_handlers
            .iter()
            .any(|handler| match handler.filter {
                Some(BlockHandlerFilter::Call) => true,
                _ => false,
            })
    }

    pub fn required_capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            traces: self.has_block_handler_with_call_filter() || self.has_call_handler(),
            archive: self.calls_host_fn("ethereum.call"),
        }
    }
}

impl UnresolvedMapping {
    pub async fn resolve(
        self,
        resolver: &impl LinkResolver,
        logger: &Logger,
    ) -> Result<Mapping, anyhow::Error> {
        let UnresolvedMapping {
            kind,
            api_version,
            language,
            entities,
            abis,
            block_handlers,
            call_handlers,
            event_handlers,
            file: link,
        } = self;

        info!(logger, "Resolve mapping"; "link" => &link.link);

        let (abis, runtime) = try_join(
            // resolve each abi
            abis.into_iter()
                .map(|unresolved_abi| unresolved_abi.resolve(resolver, logger))
                .collect::<FuturesOrdered<_>>()
                .try_collect::<Vec<_>>(),
            async {
                let module_bytes = resolver.cat(logger, &link).await?;
                Ok(Arc::new(module_bytes))
            },
        )
        .await?;

        Ok(Mapping {
            kind,
            api_version,
            language,
            entities,
            abis,
            block_handlers: block_handlers.clone(),
            call_handlers: call_handlers.clone(),
            event_handlers: event_handlers.clone(),
            runtime,
            link,
        })
    }
}

impl From<EthereumContractMappingEntity> for UnresolvedMapping {
    fn from(entity: EthereumContractMappingEntity) -> Self {
        Self {
            kind: entity.kind,
            api_version: entity.api_version,
            language: entity.language,
            entities: entity.entities,
            abis: entity.abis.into_iter().map(Into::into).collect(),
            event_handlers: entity.event_handlers.into_iter().map(Into::into).collect(),
            call_handlers: entity.call_handlers.into_iter().map(Into::into).collect(),
            block_handlers: entity.block_handlers.into_iter().map(Into::into).collect(),
            file: entity.file.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
pub struct BaseDataSource<M> {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub source: Source,
    pub mapping: M,
    pub context: Option<DataSourceContext>,
    #[serde(skip)]
    pub creation_block: Option<u64>,
}

pub type UnresolvedDataSource = BaseDataSource<UnresolvedMapping>;
pub type DataSource = BaseDataSource<Mapping>;

impl UnresolvedDataSource {
    pub async fn resolve(
        self,
        resolver: &impl LinkResolver,
        logger: &Logger,
    ) -> Result<DataSource, anyhow::Error> {
        let UnresolvedDataSource {
            kind,
            network,
            name,
            source,
            mapping,
            context,
            creation_block,
        } = self;

        info!(logger, "Resolve data source"; "name" => &name, "source" => &source.start_block);

        let mapping = mapping.resolve(&*resolver, logger).await?;

        Ok(DataSource {
            kind,
            network,
            name,
            source,
            mapping,
            context,
            creation_block,
        })
    }
}

impl TryFrom<DataSourceTemplateInfo> for DataSource {
    type Error = anyhow::Error;

    fn try_from(info: DataSourceTemplateInfo) -> Result<Self, anyhow::Error> {
        let DataSourceTemplateInfo {
            data_source: _,
            template,
            params,
            context,
            creation_block,
        } = info;

        // Obtain the address from the parameters
        let string = params
            .get(0)
            .with_context(|| {
                format!(
                    "Failed to create data source from template `{}`: address parameter is missing",
                    template.name
                )
            })?
            .trim_start_matches("0x");

        let address = Address::from_str(string).with_context(|| {
            format!(
                "Failed to create data source from template `{}`, invalid address provided",
                template.name
            )
        })?;

        Ok(DataSource {
            kind: template.kind,
            network: template.network,
            name: template.name,
            source: Source {
                address: Some(address),
                abi: template.source.abi,
                start_block: 0,
            },
            mapping: template.mapping,
            context,
            creation_block: Some(creation_block),
        })
    }
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
pub struct BaseDataSourceTemplate<M> {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub source: TemplateSource,
    pub mapping: M,
}

impl From<EthereumContractDataSourceTemplateEntity> for UnresolvedDataSourceTemplate {
    fn from(entity: EthereumContractDataSourceTemplateEntity) -> Self {
        Self {
            kind: entity.kind,
            network: entity.network,
            name: entity.name,
            source: entity.source.into(),
            mapping: entity.mapping.into(),
        }
    }
}

pub type UnresolvedDataSourceTemplate = BaseDataSourceTemplate<UnresolvedMapping>;
pub type DataSourceTemplate = BaseDataSourceTemplate<Mapping>;

impl UnresolvedDataSourceTemplate {
    pub async fn resolve(
        self,
        resolver: &impl LinkResolver,
        logger: &Logger,
    ) -> Result<DataSourceTemplate, anyhow::Error> {
        let UnresolvedDataSourceTemplate {
            kind,
            network,
            name,
            source,
            mapping,
        } = self;

        info!(logger, "Resolve data source template"; "name" => &name);

        Ok(DataSourceTemplate {
            kind,
            network,
            name,
            source,
            mapping: mapping.resolve(resolver, logger).await?,
        })
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Graft {
    pub base: SubgraphDeploymentId,
    pub block: BlockNumber,
}

impl Graft {
    fn validate<S: SubgraphStore>(&self, store: Arc<S>) -> Vec<SubgraphManifestValidationError> {
        fn gbi(msg: String) -> Vec<SubgraphManifestValidationError> {
            vec![SubgraphManifestValidationError::GraftBaseInvalid(msg)]
        }

        match store.block_ptr(&self.base) {
            Err(e) => gbi(e.to_string()),
            Ok(None) => gbi(format!(
                "failed to graft onto `{}` since it has not processed any blocks",
                self.base
            )),
            Ok(Some(ptr)) => {
                if ptr.number < self.block as u64 {
                    gbi(format!(
                        "failed to graft onto `{}` at block {} since it has only processed block {}",
                        self.base, self.block, ptr.number
                    ))
                } else {
                    vec![]
                }
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BaseSubgraphManifest<S, D, T> {
    pub id: SubgraphDeploymentId,
    pub location: String,
    pub spec_version: String,
    #[serde(default)]
    pub features: BTreeSet<SubgraphFeature>,
    pub description: Option<String>,
    pub repository: Option<String>,
    pub schema: S,
    pub data_sources: Vec<D>,
    pub graft: Option<Graft>,
    #[serde(default)]
    pub templates: Vec<T>,
}

/// Consider two subgraphs to be equal if they come from the same IPLD link.
impl<S, D, T> PartialEq for BaseSubgraphManifest<S, D, T> {
    fn eq(&self, other: &Self) -> bool {
        self.location == other.location
    }
}

/// SubgraphManifest with IPFS links unresolved
type UnresolvedSubgraphManifest =
    BaseSubgraphManifest<UnresolvedSchema, UnresolvedDataSource, UnresolvedDataSourceTemplate>;

/// SubgraphManifest validated with IPFS links resolved
pub type SubgraphManifest = BaseSubgraphManifest<Schema, DataSource, DataSourceTemplate>;

/// Unvalidated SubgraphManifest
pub struct UnvalidatedSubgraphManifest(SubgraphManifest);

impl UnvalidatedSubgraphManifest {
    /// Entry point for resolving a subgraph definition.
    /// Right now the only supported links are of the form:
    /// `/ipfs/QmUmg7BZC1YP1ca66rRtWKxpXp77WgVHrnv263JtDuvs2k`
    pub async fn resolve(
        link: Link,
        resolver: Arc<impl LinkResolver>,
        logger: &Logger,
    ) -> Result<Self, SubgraphManifestResolveError> {
        Ok(Self(
            SubgraphManifest::resolve(link, resolver.deref(), logger).await?,
        ))
    }

    pub fn validate<S: SubgraphStore>(
        self,
        store: Arc<S>,
    ) -> Result<
        (SubgraphManifest, Vec<SubgraphManifestValidationWarning>),
        Vec<SubgraphManifestValidationError>,
    > {
        let (schemas, import_errors) = self.0.schema.resolve_schema_references(store.clone());
        let validation_warnings = import_errors
            .into_iter()
            .map(SubgraphManifestValidationWarning::SchemaValidationWarning)
            .collect();

        let mut errors: Vec<SubgraphManifestValidationError> = vec![];

        // Validate that the manifest has at least one data source
        if self.0.data_sources.is_empty() {
            errors.push(SubgraphManifestValidationError::NoDataSources);
        }

        // Validate that the manifest has a `source` address in each data source
        // which has call or block handlers
        if self.0.data_sources.iter().any(|data_source| {
            let no_source_address = data_source.source.address.is_none();
            let has_call_handlers = !data_source.mapping.call_handlers.is_empty();
            let has_block_handlers = !data_source.mapping.block_handlers.is_empty();

            no_source_address && (has_call_handlers || has_block_handlers)
        }) {
            errors.push(SubgraphManifestValidationError::SourceAddressRequired)
        };

        // Validate that there are no more than one of each type of
        // block_handler in each data source.
        let has_too_many_block_handlers = self.0.data_sources.iter().any(|data_source| {
            if data_source.mapping.block_handlers.is_empty() {
                return false;
            }

            let mut non_filtered_block_handler_count = 0;
            let mut call_filtered_block_handler_count = 0;
            data_source
                .mapping
                .block_handlers
                .iter()
                .for_each(|block_handler| {
                    if block_handler.filter.is_none() {
                        non_filtered_block_handler_count += 1
                    } else {
                        call_filtered_block_handler_count += 1
                    }
                });
            non_filtered_block_handler_count > 1 || call_filtered_block_handler_count > 1
        });
        if has_too_many_block_handlers {
            errors.push(SubgraphManifestValidationError::DataSourceBlockHandlerLimitExceeded)
        }

        let mut networks = self
            .0
            .data_sources
            .iter()
            .cloned()
            .filter(|d| d.kind.eq("ethereum/contract"))
            .filter_map(|d| d.network)
            .collect::<Vec<String>>();
        networks.sort();
        networks.dedup();
        match networks.len() {
            0 => errors.push(SubgraphManifestValidationError::EthereumNetworkRequired),
            1 => (),
            _ => errors.push(SubgraphManifestValidationError::MultipleEthereumNetworks),
        }

        self.0
            .schema
            .validate(&schemas)
            .err()
            .into_iter()
            .for_each(|schema_errors| {
                errors.push(SubgraphManifestValidationError::SchemaValidationError(
                    schema_errors,
                ));
            });

        if let Some(graft) = &self.0.graft {
            if *DISABLE_GRAFTS {
                errors.push(SubgraphManifestValidationError::GraftBaseInvalid(
                    "Grafting of subgraphs is currently disabled".to_owned(),
                ));
            }
            errors.extend(graft.validate(store));
        }

        match errors.is_empty() {
            true => Ok((self.0, validation_warnings)),
            false => Err(errors),
        }
    }
}

impl SubgraphManifest {
    /// Entry point for resolving a subgraph definition.
    /// Right now the only supported links are of the form:
    /// `/ipfs/QmUmg7BZC1YP1ca66rRtWKxpXp77WgVHrnv263JtDuvs2k`
    pub async fn resolve(
        link: Link,
        resolver: &impl LinkResolver,
        logger: &Logger,
    ) -> Result<Self, SubgraphManifestResolveError> {
        info!(logger, "Resolve manifest"; "link" => &link.link);

        let file_bytes = resolver
            .cat(logger, &link)
            .await
            .map_err(SubgraphManifestResolveError::ResolveError)?;

        let file = String::from_utf8(file_bytes.to_vec())
            .map_err(|_| SubgraphManifestResolveError::NonUtf8)?;
        let mut raw: serde_yaml::Value = serde_yaml::from_str(&file)?;

        let raw_mapping = raw
            .as_mapping_mut()
            .ok_or(SubgraphManifestResolveError::InvalidFormat)?;

        // Inject the IPFS hash as the ID of the subgraph
        // into the definition.
        raw_mapping.insert(
            serde_yaml::Value::from("id"),
            serde_yaml::Value::from(link.link.trim_start_matches("/ipfs/")),
        );

        // Inject the IPFS link as the location of the data
        // source into the definition
        raw_mapping.insert(
            serde_yaml::Value::from("location"),
            serde_yaml::Value::from(link.link),
        );

        // Parse the YAML data into an UnresolvedSubgraphManifest
        let unresolved: UnresolvedSubgraphManifest = serde_yaml::from_value(raw)?;

        debug!(logger, "Features {:?}", unresolved.features);

        unresolved
            .resolve(&*resolver, logger)
            .await
            .map_err(SubgraphManifestResolveError::ResolveError)
    }

    pub fn network_name(&self) -> String {
        // Assume the manifest has been validated, ensuring network names are homogenous
        self.data_sources
            .iter()
            .cloned()
            .filter(|d| &d.kind == "ethereum/contract")
            .filter_map(|d| d.network)
            .next()
            .expect("Validated manifest does not have a network defined on any datasource")
    }

    pub fn start_blocks(&self) -> Vec<u64> {
        self.data_sources
            .iter()
            .map(|data_source| data_source.source.start_block)
            .collect()
    }

    pub fn mappings(&self) -> Vec<Mapping> {
        self.templates
            .iter()
            .map(|template| template.mapping.clone())
            .chain(
                self.data_sources
                    .iter()
                    .map(|source| source.mapping.clone()),
            )
            .collect()
    }

    pub fn requires_traces(&self) -> bool {
        self.mappings().iter().any(|mapping| {
            mapping.has_call_handler() || mapping.has_block_handler_with_call_filter()
        })
    }

    pub fn requires_archive(&self) -> bool {
        self.mappings()
            .iter()
            .any(|mapping| mapping.calls_host_fn("ethereum.call"))
    }

    pub fn required_ethereum_capabilities(&self) -> NodeCapabilities {
        let mappings = self.mappings();
        NodeCapabilities {
            archive: mappings
                .iter()
                .any(|mapping| mapping.calls_host_fn("ethereum.call")),
            traces: mappings.iter().any(|mapping| {
                mapping.has_call_handler() || mapping.has_block_handler_with_call_filter()
            }),
        }
    }
}

impl UnresolvedSubgraphManifest {
    pub async fn resolve(
        self,
        resolver: &impl LinkResolver,
        logger: &Logger,
    ) -> Result<SubgraphManifest, anyhow::Error> {
        let UnresolvedSubgraphManifest {
            id,
            location,
            spec_version,
            features,
            description,
            repository,
            schema,
            data_sources,
            graft,
            templates,
        } = self;

        match semver::Version::parse(&spec_version) {
            Ok(ver) if (*MIN_SPEC_VERSION <= ver && ver <= *MAX_SPEC_VERSION) => {}
            _ => {
                return Err(anyhow!(
                    "This Graph Node only supports manifest spec versions between {} and {},
                    but subgraph `{}` uses `{}`",
                    *MIN_SPEC_VERSION,
                    *MAX_SPEC_VERSION,
                    id,
                    spec_version
                ));
            }
        }

        let (schema, data_sources, templates) = try_join3(
            schema.resolve(id.clone(), resolver, logger),
            data_sources
                .into_iter()
                .map(|ds| ds.resolve(resolver, logger))
                .collect::<FuturesOrdered<_>>()
                .try_collect::<Vec<_>>(),
            templates
                .into_iter()
                .map(|template| template.resolve(resolver, logger))
                .collect::<FuturesOrdered<_>>()
                .try_collect::<Vec<_>>(),
        )
        .await?;

        Ok(SubgraphManifest {
            id,
            location,
            spec_version,
            features,
            description,
            repository,
            schema,
            data_sources,
            graft,
            templates,
        })
    }
}

/// Important details about the current state of a subgraph deployment
/// used while executing queries against a deployment
///
/// The `reorg_count` and `max_reorg_depth` fields are maintained (in the
/// database) by `store::metadata::forward_block_ptr` and
/// `store::metadata::revert_block_ptr` which get called as part of transacting
/// new entities into the store or reverting blocks.
#[derive(Debug, Clone)]
pub struct DeploymentState {
    pub id: SubgraphDeploymentId,
    /// The number of blocks that were ever reverted in this subgraph. This
    /// number increases monotonically every time a block is reverted
    pub reorg_count: u32,
    /// The maximum number of blocks we ever reorged without moving a block
    /// forward in between
    pub max_reorg_depth: u32,
    /// The number of the last block that the subgraph has processed
    pub latest_ethereum_block_number: BlockNumber,
}

#[derive(Debug, Deserialize, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[allow(non_camel_case_types)]
pub enum SubgraphFeature {
    nonFatalErrors,
}

impl std::fmt::Display for SubgraphFeature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubgraphFeature::nonFatalErrors => write!(f, "nonFatalErrors"),
        }
    }
}

impl FromStr for SubgraphFeature {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "nonFatalErrors" => Ok(SubgraphFeature::nonFatalErrors),
            _ => Err(anyhow::anyhow!("invalid subgraph feature {}", s)),
        }
    }
}
