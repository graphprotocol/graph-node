/// Rust representation of the GraphQL schema for a `SubgraphManifest`.
pub mod schema;

/// API version and spec version.
pub mod api_version;
pub use api_version::*;

pub mod features;
pub mod status;

pub use features::{SubgraphFeature, SubgraphFeatureValidationError};

use anyhow::ensure;
use anyhow::{anyhow, Error};
use futures03::{future::try_join3, stream::FuturesOrdered, TryStreamExt as _};
use semver::Version;
use serde::de;
use serde::ser;
use serde_yaml;
use slog::{debug, info, Logger};
use stable_hash::{FieldAddress, StableHash};
use stable_hash_legacy::SequenceNumber;
use std::{collections::BTreeSet, marker::PhantomData};
use thiserror::Error;
use wasmparser;
use web3::types::Address;

use crate::blockchain::BlockPtr;
use crate::data::store::Entity;
use crate::data::{
    schema::{Schema, SchemaImportError, SchemaValidationError},
    subgraph::features::validate_subgraph_features,
};
use crate::prelude::{r, CheapClone, ENV_VARS};
use crate::{blockchain::DataSource, data::graphql::TryFromValue};
use crate::{blockchain::DataSourceTemplate as _, data::query::QueryExecutionError};
use crate::{
    blockchain::{Blockchain, UnresolvedDataSource as _, UnresolvedDataSourceTemplate as _},
    components::{
        link_resolver::LinkResolver,
        store::{DeploymentLocator, StoreError, SubgraphStore},
    },
};

use crate::prelude::{impl_slog_value, BlockNumber, Deserialize, Serialize};

use std::fmt;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

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

/// The IPFS hash used to identifiy a deployment externally, i.e., the
/// `Qm..` string that `graph-cli` prints when deploying to a subgraph
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DeploymentHash(String);

impl stable_hash_legacy::StableHash for DeploymentHash {
    #[inline]
    fn stable_hash<H: stable_hash_legacy::StableHasher>(
        &self,
        mut sequence_number: H::Seq,
        state: &mut H,
    ) {
        let Self(inner) = self;
        stable_hash_legacy::StableHash::stable_hash(inner, sequence_number.next_child(), state);
    }
}

impl StableHash for DeploymentHash {
    fn stable_hash<H: stable_hash::StableHasher>(&self, field_address: H::Addr, state: &mut H) {
        let Self(inner) = self;
        stable_hash::StableHash::stable_hash(inner, field_address.child(0), state);
    }
}

impl_slog_value!(DeploymentHash);

/// `DeploymentHash` is fixed-length so cheap to clone.
impl CheapClone for DeploymentHash {}

impl DeploymentHash {
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

        Ok(DeploymentHash(s))
    }

    pub fn to_ipfs_link(&self) -> Link {
        Link {
            link: format!("/ipfs/{}", self),
        }
    }
}

impl Deref for DeploymentHash {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for DeploymentHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl ser::Serialize for DeploymentHash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> de::Deserialize<'de> for DeploymentHash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s: String = de::Deserialize::deserialize(deserializer)?;
        DeploymentHash::new(s)
            .map_err(|s| de::Error::invalid_value(de::Unexpected::Str(&s), &"valid subgraph name"))
    }
}

impl TryFromValue for DeploymentHash {
    fn try_from_value(value: &r::Value) -> Result<Self, Error> {
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
        for part in s.split('/') {
            // Each part must be non-empty
            if part.is_empty() {
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
    #[error("network not supported by registrar: {0}")]
    NetworkNotSupported(Error),
    #[error("deployment not found: {0}")]
    DeploymentNotFound(String),
    #[error("deployment assignment unchanged: {0}")]
    DeploymentAssignmentUnchanged(String),
    #[error("subgraph registrar internal query error: {0}")]
    QueryExecutionError(QueryExecutionError),
    #[error("subgraph registrar error with store: {0}")]
    StoreError(StoreError),
    #[error("subgraph validation error: {}", display_vector(.0))]
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
    ResolveError(Error),
    /// Occurs when attempting to remove a subgraph that's not hosted.
    #[error("Subgraph with ID {0} already running")]
    AlreadyRunning(DeploymentHash),
    #[error("Subgraph with ID {0} is not running")]
    NotRunning(DeploymentLocator),
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
    #[error("the specified block must exist on the Ethereum network")]
    BlockNotFound(String),
    #[error("imported schema(s) are invalid: {0:?}")]
    SchemaImportError(Vec<SchemaImportError>),
    #[error("schema validation failed: {0:?}")]
    SchemaValidationError(Vec<SchemaValidationError>),
    #[error("the graft base is invalid: {0}")]
    GraftBaseInvalid(String),
    #[error("subgraph must use a single apiVersion across its data sources. Found: {}", format_versions(.0))]
    DifferentApiVersions(BTreeSet<Version>),
    #[error(transparent)]
    FeatureValidationError(#[from] SubgraphFeatureValidationError),
    #[error("data source {0} is invalid: {1}")]
    DataSourceValidation(String, Error),
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
        id: DeploymentHash,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
    ) -> Result<Schema, anyhow::Error> {
        info!(logger, "Resolve schema"; "link" => &self.file.link);

        let schema_bytes = resolver.cat(logger, &self.file).await?;
        Schema::parse(&String::from_utf8(schema_bytes)?, id)
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct Source {
    /// The contract address for the data source. We allow data sources
    /// without an address for 'wildcard' triggers that catch all possible
    /// events with the given `abi`
    #[serde(default, deserialize_with = "deserialize_address")]
    pub address: Option<Address>,
    pub abi: String,
    #[serde(rename = "startBlock", default)]
    pub start_block: BlockNumber,
}

pub fn calls_host_fn(runtime: &[u8], host_fn: &str) -> anyhow::Result<bool> {
    use wasmparser::Payload;

    for payload in wasmparser::Parser::new(0).parse_all(runtime) {
        if let Payload::ImportSection(s) = payload? {
            for import in s {
                let import = import?;
                if import.field == Some(host_fn) {
                    return Ok(true);
                }
            }
        }
    }

    Ok(false)
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Graft {
    pub base: DeploymentHash,
    pub block: BlockNumber,
}

impl Graft {
    async fn validate<S: SubgraphStore>(
        &self,
        store: Arc<S>,
    ) -> Result<(), SubgraphManifestValidationError> {
        use SubgraphManifestValidationError::*;

        let last_processed_block = store
            .least_block_ptr(&self.base)
            .await
            .map_err(|e| GraftBaseInvalid(e.to_string()))?;
        let is_base_healthy = store
            .is_healthy(&self.base)
            .await
            .map_err(|e| GraftBaseInvalid(e.to_string()))?;

        // We are being defensive here: we don't know which specific
        // instance of a subgraph we will use as the base for the graft,
        // since the notion of which of these instances is active can change
        // between this check and when the graft actually happens when the
        // subgraph is started. We therefore check that any instance of the
        // base subgraph is suitable.
        match (last_processed_block, is_base_healthy) {
            (None, _) => Err(GraftBaseInvalid(format!(
                "failed to graft onto `{}` since it has not processed any blocks",
                self.base
            ))),
            (Some(ptr), true) if ptr.number < self.block => Err(GraftBaseInvalid(format!(
                "failed to graft onto `{}` at block {} since it has only processed block {}",
                self.base, self.block, ptr.number
            ))),
            // If the base deployment is failed *and* the `graft.block` is not
            // less than the `base.block`, the graft shouldn't be permitted.
            //
            // The developer should change their `graft.block` in the manifest
            // to `base.block - 1` or less.
            (Some(ptr), false) if !(self.block < ptr.number) => Err(GraftBaseInvalid(format!(
                "failed to graft onto `{}` at block {} since it's not healthy. You can graft it starting at block {} backwards",
                self.base, self.block, ptr.number - 1
            ))),
            (Some(_), _) => Ok(()),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BaseSubgraphManifest<C, S, D, T> {
    pub id: DeploymentHash,
    pub spec_version: Version,
    #[serde(default)]
    pub features: BTreeSet<SubgraphFeature>,
    pub description: Option<String>,
    pub repository: Option<String>,
    pub schema: S,
    pub data_sources: Vec<D>,
    pub graft: Option<Graft>,
    #[serde(default)]
    pub templates: Vec<T>,
    #[serde(skip_serializing, default)]
    pub chain: PhantomData<C>,
}

/// SubgraphManifest with IPFS links unresolved
type UnresolvedSubgraphManifest<C> = BaseSubgraphManifest<
    C,
    UnresolvedSchema,
    <C as Blockchain>::UnresolvedDataSource,
    <C as Blockchain>::UnresolvedDataSourceTemplate,
>;

/// SubgraphManifest validated with IPFS links resolved
pub type SubgraphManifest<C> = BaseSubgraphManifest<
    C,
    Schema,
    <C as Blockchain>::DataSource,
    <C as Blockchain>::DataSourceTemplate,
>;

/// Unvalidated SubgraphManifest
pub struct UnvalidatedSubgraphManifest<C: Blockchain>(SubgraphManifest<C>);

impl<C: Blockchain> UnvalidatedSubgraphManifest<C> {
    /// Entry point for resolving a subgraph definition.
    /// Right now the only supported links are of the form:
    /// `/ipfs/QmUmg7BZC1YP1ca66rRtWKxpXp77WgVHrnv263JtDuvs2k`
    pub async fn resolve(
        id: DeploymentHash,
        raw: serde_yaml::Mapping,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
        max_spec_version: semver::Version,
    ) -> Result<Self, SubgraphManifestResolveError> {
        Ok(Self(
            SubgraphManifest::resolve_from_raw(id, raw, resolver, logger, max_spec_version).await?,
        ))
    }

    /// Validates the subgraph manifest file.
    ///
    /// Graft base validation will be skipped if the parameter `validate_graft_base` is false.
    pub async fn validate<S: SubgraphStore>(
        self,
        store: Arc<S>,
        validate_graft_base: bool,
    ) -> Result<SubgraphManifest<C>, Vec<SubgraphManifestValidationError>> {
        let (schemas, _) = self.0.schema.resolve_schema_references(store.clone());

        let mut errors: Vec<SubgraphManifestValidationError> = vec![];

        // Validate that the manifest has at least one data source
        if self.0.data_sources.is_empty() {
            errors.push(SubgraphManifestValidationError::NoDataSources);
        }

        for ds in &self.0.data_sources {
            errors.extend(ds.validate().into_iter().map(|e| {
                SubgraphManifestValidationError::DataSourceValidation(ds.name().to_owned(), e)
            }));
        }

        // For API versions newer than 0.0.5, validate that all mappings uses the same api_version
        if let Err(different_api_versions) = self.0.unified_mapping_api_version() {
            errors.push(different_api_versions.into());
        };

        let mut networks = self
            .0
            .data_sources
            .iter()
            .filter_map(|d| d.network().map(|n| n.to_string()))
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
            if ENV_VARS.disable_grafts {
                errors.push(SubgraphManifestValidationError::GraftBaseInvalid(
                    "Grafting of subgraphs is currently disabled".to_owned(),
                ));
            }
            if validate_graft_base {
                if let Err(graft_err) = graft.validate(store).await {
                    errors.push(graft_err);
                }
            }
        }

        // Validate subgraph feature usage and declaration.
        if self.0.spec_version >= SPEC_VERSION_0_0_4 {
            if let Err(feature_validation_error) = validate_subgraph_features(&self.0) {
                errors.push(feature_validation_error.into())
            }
        }

        match errors.is_empty() {
            true => Ok(self.0),
            false => Err(errors),
        }
    }

    pub fn spec_version(&self) -> &Version {
        &self.0.spec_version
    }
}

impl<C: Blockchain> SubgraphManifest<C> {
    /// Entry point for resolving a subgraph definition.
    pub async fn resolve_from_raw(
        id: DeploymentHash,
        mut raw: serde_yaml::Mapping,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
        max_spec_version: semver::Version,
    ) -> Result<Self, SubgraphManifestResolveError> {
        // Inject the IPFS hash as the ID of the subgraph into the definition.
        raw.insert(
            serde_yaml::Value::from("id"),
            serde_yaml::Value::from(id.to_string()),
        );

        // Parse the YAML data into an UnresolvedSubgraphManifest
        let unresolved: UnresolvedSubgraphManifest<C> = serde_yaml::from_value(raw.into())?;

        debug!(logger, "Features {:?}", unresolved.features);

        unresolved
            .resolve(resolver, logger, max_spec_version)
            .await
            .map_err(SubgraphManifestResolveError::ResolveError)
    }

    pub fn network_name(&self) -> String {
        // Assume the manifest has been validated, ensuring network names are homogenous
        self.data_sources
            .iter()
            .filter_map(|d| d.network().map(|n| n.to_string()))
            .next()
            .expect("Validated manifest does not have a network defined on any datasource")
    }

    pub fn start_blocks(&self) -> Vec<BlockNumber> {
        self.data_sources
            .iter()
            .map(|data_source| data_source.start_block())
            .collect()
    }

    pub fn api_versions(&self) -> impl Iterator<Item = semver::Version> + '_ {
        self.templates
            .iter()
            .map(|template| template.api_version())
            .chain(self.data_sources.iter().map(|source| source.api_version()))
    }

    pub fn runtimes(&self) -> impl Iterator<Item = &[u8]> + '_ {
        self.templates
            .iter()
            .map(|template| template.runtime())
            .chain(self.data_sources.iter().map(|source| source.runtime()))
    }

    pub fn unified_mapping_api_version(
        &self,
    ) -> Result<UnifiedMappingApiVersion, DifferentMappingApiVersions> {
        UnifiedMappingApiVersion::try_from_versions(self.api_versions())
    }
}

impl<C: Blockchain> UnresolvedSubgraphManifest<C> {
    pub async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
        max_spec_version: semver::Version,
    ) -> Result<SubgraphManifest<C>, anyhow::Error> {
        let UnresolvedSubgraphManifest {
            id,
            spec_version,
            features,
            description,
            repository,
            schema,
            data_sources,
            graft,
            templates,
            chain,
        } = self;

        if !(MIN_SPEC_VERSION..=max_spec_version.clone()).contains(&spec_version) {
            return Err(anyhow!(
                "This Graph Node only supports manifest spec versions between {} and {}, but subgraph `{}` uses `{}`",
                MIN_SPEC_VERSION,
                max_spec_version,
                id,
                spec_version
            ));
        }

        let (schema, data_sources, templates) = try_join3(
            schema.resolve(id.clone(), &resolver, logger),
            data_sources
                .into_iter()
                .map(|ds| ds.resolve(&resolver, logger))
                .collect::<FuturesOrdered<_>>()
                .try_collect::<Vec<_>>(),
            templates
                .into_iter()
                .map(|template| template.resolve(&resolver, logger))
                .collect::<FuturesOrdered<_>>()
                .try_collect::<Vec<_>>(),
        )
        .await?;

        for ds in &data_sources {
            ensure!(
                semver::VersionReq::parse(&format!("<= {}", ENV_VARS.mappings.max_api_version))
                    .unwrap()
                    .matches(&ds.api_version()),
                "The maximum supported mapping API version of this indexer is {}, but `{}` was found",
                ENV_VARS.mappings.max_api_version,
                ds.api_version()
            );
        }

        Ok(SubgraphManifest {
            id,
            spec_version,
            features,
            description,
            repository,
            schema,
            data_sources,
            graft,
            templates,
            chain,
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
    pub id: DeploymentHash,
    /// The number of blocks that were ever reverted in this subgraph. This
    /// number increases monotonically every time a block is reverted
    pub reorg_count: u32,
    /// The maximum number of blocks we ever reorged without moving a block
    /// forward in between
    pub max_reorg_depth: u32,
    /// The last block that the subgraph has processed
    pub latest_block: BlockPtr,
    /// The earliest block that the subgraph has processed
    pub earliest_block_number: BlockNumber,
}

impl DeploymentState {
    /// Is this subgraph deployed and has it processed any blocks?
    pub fn is_deployed(&self) -> bool {
        self.latest_block.number > 0
    }

    pub fn block_queryable(&self, block: BlockNumber) -> Result<(), String> {
        if block > self.latest_block.number {
            return Err(format!(
                "subgraph {} has only indexed up to block number {} \
                        and data for block number {} is therefore not yet available",
                self.id, self.latest_block.number, block
            ));
        }
        if block < self.earliest_block_number {
            return Err(format!(
                "subgraph {} only has data starting at block number {} \
                            and data for block number {} is therefore not available",
                self.id, self.earliest_block_number, block
            ));
        }
        Ok(())
    }
}

fn display_vector(input: &[impl std::fmt::Display]) -> impl std::fmt::Display {
    let formatted_errors = input
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<String>>()
        .join("; ");
    format!("[{}]", formatted_errors)
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
    assert!(SubgraphName::new("this-component-is-very-long-but-we-dont-care").is_ok());
}

#[test]
fn test_display_vector() {
    let manifest_validation_error = SubgraphRegistrarError::ManifestValidationError(vec![
        SubgraphManifestValidationError::NoDataSources,
        SubgraphManifestValidationError::SourceAddressRequired,
    ]);

    let expected_display_message =
	"subgraph validation error: [subgraph has no data sources; subgraph source address is required]"
	.to_string();

    assert_eq!(
        expected_display_message,
        format!("{}", manifest_validation_error)
    )
}
