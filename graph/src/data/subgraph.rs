use components::link_resolver::LinkResolver;
use data::schema::Schema;
use ethabi::Contract;
use failure;
use failure::SyncFailure;
use futures::stream;
use graphql_parser;
use parity_wasm;
use parity_wasm::elements::Module;
use serde_yaml;
use tokio::prelude::*;

use std::sync::Arc;

/// The ID of a subgraph.
pub type SubgraphId = String;

#[derive(Fail, Debug)]
pub enum SubgraphProviderError {
    #[fail(display = "subgraph resolve error: {}", _0)]
    ResolveError(SubgraphManifestResolveError),
    #[fail(
        display = "name {} is invalid, only ASCII alphanumerics, `-` and `_` are allowed",
        _0
    )]
    InvalidName(String),
    /// Occurs when attempting to remove a subgraph that's not hosted.
    #[fail(display = "subgraph not found: {}", _0)]
    NotFound(String),
    /// Occurs when a subgraph's GraphQL schema is invalid.
    #[fail(display = "GraphQL schema error: {}", _0)]
    SchemaValidationError(failure::Error),
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
        id: SubgraphId,
        name: String,
        resolver: &impl LinkResolver,
    ) -> impl Future<Item = Schema, Error = failure::Error> + Send {
        resolver.cat(&self.file).and_then(|schema_bytes| {
            Ok(
                graphql_parser::parse_schema(&String::from_utf8(schema_bytes)?)
                    .map(|document| Schema { name, id, document })?,
            )
        })
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct Source {
    pub address: String,
    pub abi: String,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct BaseMappingABI<C> {
    pub name: String,
    #[serde(rename = "file")]
    pub contract: C,
}

pub type UnresolvedMappingABI = BaseMappingABI<Link>;
pub type MappingABI = BaseMappingABI<Contract>;

impl UnresolvedMappingABI {
    pub fn resolve(
        self,
        resolver: &impl LinkResolver,
    ) -> impl Future<Item = MappingABI, Error = failure::Error> + Send {
        resolver.cat(&self.contract).and_then(|contract_bytes| {
            let contract = Contract::load(&*contract_bytes).map_err(SyncFailure::new)?;
            Ok(MappingABI {
                name: self.name,
                contract,
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
pub struct BaseMapping<C, W> {
    pub kind: String,
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub language: String,
    pub entities: Vec<String>,
    pub abis: Vec<BaseMappingABI<C>>,
    #[serde(rename = "eventHandlers")]
    pub event_handlers: Vec<MappingEventHandler>,
    #[serde(rename = "file")]
    pub runtime: W,
}

pub type UnresolvedMapping = BaseMapping<Link, Link>;
pub type Mapping = BaseMapping<Contract, Module>;

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
            runtime,
        } = self;

        // resolve each abi
        stream::futures_ordered(
            abis.into_iter()
                .map(|unresolved_abi| unresolved_abi.resolve(resolver)),
        ).collect()
        .join(
            resolver
                .cat(&runtime)
                .and_then(|module_bytes| Ok(parity_wasm::deserialize_buffer(&module_bytes)?)),
        ).map(|(abis, runtime)| Mapping {
            kind,
            api_version,
            language,
            entities,
            abis,
            event_handlers,
            runtime,
        })
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct BaseDataSource<C, W> {
    pub kind: String,
    pub name: String,
    pub source: Source,
    pub mapping: BaseMapping<C, W>,
}

pub type UnresolvedDataSource = BaseDataSource<Link, Link>;
pub type DataSource = BaseDataSource<Contract, Module>;

impl UnresolvedDataSource {
    pub fn resolve(
        self,
        resolver: &impl LinkResolver,
    ) -> impl Future<Item = DataSource, Error = failure::Error> {
        let UnresolvedDataSource {
            kind,
            name,
            source,
            mapping,
        } = self;
        mapping.resolve(resolver).map(|mapping| DataSource {
            kind,
            name,
            source,
            mapping,
        })
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct BaseSubgraphManifest<S, D> {
    pub id: SubgraphId,
    pub location: String,
    #[serde(rename = "specVersion")]
    pub spec_version: String,
    pub schema: S,
    #[serde(rename = "dataSources")]
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
        name: String,
        link: Link,
        resolver: Arc<impl LinkResolver>,
    ) -> impl Future<Item = Self, Error = SubgraphManifestResolveError> + Send {
        resolver
            .cat(&link)
            .map_err(|e| SubgraphManifestResolveError::ResolveError(e))
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
                        serde_yaml::Value::from(link.link.trim_left_matches("/ipfs/")),
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
            }).and_then(move |unresolved| {
                unresolved
                    .resolve(name, &*resolver)
                    .map_err(|e| SubgraphManifestResolveError::ResolveError(e))
            })
    }
}

impl UnresolvedSubgraphManifest {
    pub fn resolve(
        self,
        name: String,
        resolver: &impl LinkResolver,
    ) -> impl Future<Item = SubgraphManifest, Error = failure::Error> {
        let UnresolvedSubgraphManifest {
            id,
            location,
            spec_version,
            schema,
            data_sources,
        } = self;

        // resolve each data set
        stream::futures_ordered(
            data_sources
                .into_iter()
                .map(|data_set| data_set.resolve(resolver)),
        ).collect()
        .join(schema.resolve(id.clone(), name, resolver))
        .map(|(data_sources, schema)| SubgraphManifest {
            id,
            location,
            spec_version,
            schema,
            data_sources,
        })
    }
}
