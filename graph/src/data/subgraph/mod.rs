use ethabi::Contract;
use failure;
use failure::{Error, SyncFailure};
use futures::stream;
use parity_wasm;
use parity_wasm::elements::Module;
use serde::de;
use serde::ser;
use serde_yaml;
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use tokio::prelude::*;
use web3::types::Address;

use crate::components::link_resolver::LinkResolver;
use crate::components::store::StoreError;
use crate::data::query::QueryExecutionError;
use crate::data::schema::Schema;

/// Rust representation of the GraphQL schema for a `SubgraphManifest`.
pub mod schema;

/// Deserialize an Address (with or without '0x' prefix).
fn deserialize_address<'de, D>(deserializer: D) -> Result<Address, D::Error>
where
    D: de::Deserializer<'de>,
{
    use serde::de::Error;

    let s: String = de::Deserialize::deserialize(deserializer)?;
    let address = s.trim_start_matches("0x");
    Address::from_str(address).map_err(D::Error::custom)
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubgraphDeploymentId(String);

impl SubgraphDeploymentId {
    pub fn new(s: impl Into<String>) -> Result<Self, ()> {
        let s = s.into();

        // Enforce length limit
        if s.len() > 46 {
            return Err(());
        }

        // Check that the ID contains only allowed characters.
        if !s.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(());
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
        SubgraphDeploymentId::new(s.clone())
            .map_err(|()| de::Error::invalid_value(de::Unexpected::Str(&s), &"valid subgraph name"))
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

            // Part should not start or end with a special character or start with a number.
            let first_char = part.chars().next().unwrap();
            let last_char = part.chars().last().unwrap();
            if !first_char.is_ascii_alphabetic() || !last_char.is_ascii_alphanumeric() {
                return Err(());
            }
        }

        Ok(SubgraphName(s))
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

    assert!(SubgraphName::new("").is_err());
    assert!(SubgraphName::new("/a").is_err());
    assert!(SubgraphName::new("a/").is_err());
    assert!(SubgraphName::new("a//a").is_err());
    assert!(SubgraphName::new("a/0").is_err());
    assert!(SubgraphName::new("a/_").is_err());
    assert!(SubgraphName::new("a/a_").is_err());
    assert!(SubgraphName::new("a/_a").is_err());
    assert!(SubgraphName::new("1a/aaaa").is_err());
    assert!(SubgraphName::new("aaaa/1a").is_err());
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

#[derive(Fail, Debug)]
pub enum SubgraphRegistrarError {
    #[fail(display = "subgraph resolve error: {}", _0)]
    ResolveError(SubgraphManifestResolveError),
    #[fail(display = "subgraph already exists: {}", _0)]
    NameExists(String),
    #[fail(display = "subgraph name not found: {}", _0)]
    NameNotFound(String),
    #[fail(display = "subgraph registrar internal query error: {}", _0)]
    QueryExecutionError(QueryExecutionError),
    #[fail(display = "subgraph registrar error with store: {}", _0)]
    StoreError(StoreError),
    #[fail(display = "subgraph registrar error: {}", _0)]
    Unknown(failure::Error),
}

impl From<QueryExecutionError> for SubgraphRegistrarError {
    fn from(e: QueryExecutionError) -> Self {
        SubgraphRegistrarError::QueryExecutionError(e)
    }
}

impl From<StoreError> for SubgraphRegistrarError {
    fn from(e: StoreError) -> Self {
        SubgraphRegistrarError::StoreError(e)
    }
}

impl From<Error> for SubgraphRegistrarError {
    fn from(e: Error) -> Self {
        SubgraphRegistrarError::Unknown(e)
    }
}

#[derive(Fail, Debug)]
pub enum SubgraphAssignmentProviderError {
    #[fail(display = "subgraph resolve error: {}", _0)]
    ResolveError(SubgraphManifestResolveError),
    /// Occurs when attempting to remove a subgraph that's not hosted.
    #[fail(display = "subgraph with ID {} already running", _0)]
    AlreadyRunning(SubgraphDeploymentId),
    #[fail(display = "subgraph with ID {} is not running", _0)]
    NotRunning(SubgraphDeploymentId),
    /// Occurs when a subgraph's GraphQL schema is invalid.
    #[fail(display = "GraphQL schema error: {}", _0)]
    SchemaValidationError(failure::Error),
    #[fail(
        display = "Error building index for subgraph {}, entity {} and attribute {}",
        _0, _1, _2
    )]
    BuildIndexesError(String, String, String),
    #[fail(display = "subgraph provider error: {}", _0)]
    Unknown(failure::Error),
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

#[derive(Fail, Debug)]
pub enum SubgraphManifestResolveError {
    #[fail(display = "parse error: {}", _0)]
    ParseError(serde_yaml::Error),
    #[fail(display = "subgraph is not UTF-8")]
    NonUtf8,
    #[fail(display = "subgraph is not valid YAML")]
    InvalidFormat,
    #[fail(display = "resolve error: {}", _0)]
    ResolveError(failure::Error),
}

impl From<serde_yaml::Error> for SubgraphManifestResolveError {
    fn from(e: serde_yaml::Error) -> Self {
        SubgraphManifestResolveError::ParseError(e)
    }
}

/// IPLD link.
#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct Link {
    #[serde(rename = "/")]
    pub link: String,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct SchemaData {
    pub file: Link,
}

impl SchemaData {
    pub fn resolve(
        self,
        id: SubgraphDeploymentId,
        resolver: &impl LinkResolver,
    ) -> impl Future<Item = Schema, Error = failure::Error> + Send {
        resolver
            .cat(&self.file)
            .and_then(|schema_bytes| Schema::parse(&String::from_utf8(schema_bytes)?, id))
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct Source {
    #[serde(deserialize_with = "deserialize_address")]
    pub address: Address,
    pub abi: String,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct UnresolvedMappingABI {
    pub name: String,
    pub file: Link,
}

#[derive(Clone, Debug)]
pub struct MappingABI {
    pub name: String,
    pub contract: Contract,
    pub link: Link,
}

impl UnresolvedMappingABI {
    pub fn resolve(
        self,
        resolver: &impl LinkResolver,
    ) -> impl Future<Item = MappingABI, Error = failure::Error> + Send {
        resolver.cat(&self.file).and_then(|contract_bytes| {
            let contract = Contract::load(&*contract_bytes).map_err(SyncFailure::new)?;
            Ok(MappingABI {
                name: self.name,
                contract,
                link: self.file,
            })
        })
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct MappingEventHandler {
    pub event: String,
    pub handler: String,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnresolvedMapping {
    pub kind: String,
    pub api_version: String,
    pub language: String,
    pub entities: Vec<String>,
    pub abis: Vec<UnresolvedMappingABI>,
    pub event_handlers: Vec<MappingEventHandler>,
    pub file: Link,
}

// Avoid deriving `Clone` because cloning a `Module` is expensive.
#[derive(Debug)]
pub struct Mapping {
    pub kind: String,
    pub api_version: String,
    pub language: String,
    pub entities: Vec<String>,
    pub abis: Vec<MappingABI>,
    pub event_handlers: Vec<MappingEventHandler>,
    pub runtime: Module,
    pub link: Link,
}

impl UnresolvedMapping {
    pub fn resolve(
        self,
        resolver: &impl LinkResolver,
    ) -> impl Future<Item = Mapping, Error = failure::Error> + Send {
        let UnresolvedMapping {
            kind,
            api_version,
            language,
            entities,
            abis,
            event_handlers,
            file: link,
        } = self;

        // resolve each abi
        stream::futures_ordered(
            abis.into_iter()
                .map(|unresolved_abi| unresolved_abi.resolve(resolver)),
        )
        .collect()
        .join(
            resolver
                .cat(&link)
                .and_then(|module_bytes| Ok(parity_wasm::deserialize_buffer(&module_bytes)?)),
        )
        .map(|(abis, runtime)| Mapping {
            kind,
            api_version,
            language,
            entities,
            abis,
            event_handlers,
            runtime,
            link,
        })
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct BaseDataSource<M> {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub source: Source,
    pub mapping: M,
}

pub type UnresolvedDataSource = BaseDataSource<UnresolvedMapping>;
pub type DataSource = BaseDataSource<Mapping>;

impl UnresolvedDataSource {
    pub fn resolve(
        self,
        resolver: &impl LinkResolver,
    ) -> impl Future<Item = DataSource, Error = failure::Error> {
        let UnresolvedDataSource {
            kind,
            network,
            name,
            source,
            mapping,
        } = self;
        mapping.resolve(resolver).map(|mapping| DataSource {
            kind,
            network,
            name,
            source,
            mapping,
        })
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BaseSubgraphManifest<S, D> {
    pub id: SubgraphDeploymentId,
    pub location: String,
    pub spec_version: String,
    pub description: Option<String>,
    pub repository: Option<String>,
    pub schema: S,
    pub data_sources: Vec<D>,
}

/// Consider two subgraphs to be equal if they come from the same IPLD link.
impl<S, D> PartialEq for BaseSubgraphManifest<S, D> {
    fn eq(&self, other: &Self) -> bool {
        self.location == other.location
    }
}

pub type UnresolvedSubgraphManifest = BaseSubgraphManifest<SchemaData, UnresolvedDataSource>;
pub type SubgraphManifest = BaseSubgraphManifest<Schema, DataSource>;

impl SubgraphManifest {
    /// Entry point for resolving a subgraph definition.
    /// Right now the only supported links are of the form:
    /// `/ipfs/QmUmg7BZC1YP1ca66rRtWKxpXp77WgVHrnv263JtDuvs2k`
    pub fn resolve(
        link: Link,
        resolver: Arc<impl LinkResolver>,
    ) -> impl Future<Item = Self, Error = SubgraphManifestResolveError> + Send {
        resolver
            .cat(&link)
            .map_err(SubgraphManifestResolveError::ResolveError)
            .and_then(move |file_bytes| {
                let file = String::from_utf8(file_bytes.to_vec())
                    .map_err(|_| SubgraphManifestResolveError::NonUtf8)?;
                let mut raw: serde_yaml::Value = serde_yaml::from_str(&file)?;
                {
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
                }
                // Parse the YAML data into an UnresolvedSubgraphManifest
                let unresolved: UnresolvedSubgraphManifest = serde_yaml::from_value(raw)?;
                Ok(unresolved)
            })
            .and_then(move |unresolved| {
                unresolved
                    .resolve(&*resolver)
                    .map_err(SubgraphManifestResolveError::ResolveError)
            })
    }
}

impl UnresolvedSubgraphManifest {
    pub fn resolve(
        self,
        resolver: &impl LinkResolver,
    ) -> impl Future<Item = SubgraphManifest, Error = failure::Error> {
        let UnresolvedSubgraphManifest {
            id,
            location,
            spec_version,
            description,
            repository,
            schema,
            data_sources,
        } = self;

        // resolve each data set
        stream::futures_ordered(
            data_sources
                .into_iter()
                .map(|data_set| data_set.resolve(resolver)),
        )
        .collect()
        .join(schema.resolve(id.clone(), resolver))
        .map(|(data_sources, schema)| SubgraphManifest {
            id,
            location,
            spec_version,
            description,
            repository,
            schema,
            data_sources,
        })
    }
}
