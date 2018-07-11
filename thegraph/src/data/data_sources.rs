use components::link_resolver::LinkResolver;
use data::schema::Schema;
use ethabi::Contract;
use failure::Fail;
use futures::prelude::*;
use futures::stream;
use graphql_parser;
use parity_wasm;
use parity_wasm::elements::Module;
use serde_yaml;
use std::error::Error;

#[derive(Debug)]
pub enum DataSourceDefinitionResolveError {
    ParseError(serde_yaml::Error),
    NonUtf8,
    InvalidFormat,
    ResolveError(Box<Error>),
}

impl From<serde_yaml::Error> for DataSourceDefinitionResolveError {
    fn from(e: serde_yaml::Error) -> Self {
        DataSourceDefinitionResolveError::ParseError(e)
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
    pub source: Link,
}

impl SchemaData {
    pub fn resolve(
        self,
        resolver: &impl LinkResolver,
    ) -> impl Future<Item = Schema, Error = Box<Error + 'static>> {
        resolver.cat(&self.source).and_then(|schema_bytes| {
            graphql_parser::parse_schema(&String::from_utf8(schema_bytes)?)
                .map(|document| Schema {
                    id: String::from("local-data-source-schema"),
                    document,
                })
                .map_err(|e| Box::new(e.compat()) as Box<Error>)
        })
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct DataStructure {
    pub abi: String,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct Data {
    pub kind: String,
    pub name: String,
    pub address: String,
    pub structure: DataStructure,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct BaseMappingABI<C> {
    pub name: String,
    #[serde(rename = "source")]
    pub contract: C,
}

pub type UnresolvedMappingABI = BaseMappingABI<Link>;
pub type MappingABI = BaseMappingABI<Contract>;

impl UnresolvedMappingABI {
    pub fn resolve(
        self,
        resolver: &impl LinkResolver,
    ) -> impl Future<Item = MappingABI, Error = Box<Error + 'static>> {
        resolver.cat(&self.contract).and_then(|contract_bytes| {
            let contract = Contract::load(&*contract_bytes)?;
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
    #[serde(rename = "source")]
    pub runtime: W,
}

pub type UnresolvedMapping = BaseMapping<Link, Link>;
pub type Mapping = BaseMapping<Contract, Module>;

impl UnresolvedMapping {
    pub fn resolve(
        self,
        resolver: &impl LinkResolver,
    ) -> impl Future<Item = Mapping, Error = Box<Error>> {
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
            .join(resolver.cat(&runtime).and_then(|module_bytes| {
                parity_wasm::deserialize_buffer(&module_bytes)
                    .map_err(|e| Box::new(e) as Box<Error>)
            }))
            .map(|(abis, runtime)| Mapping {
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
pub struct BaseDataSet<C, W> {
    pub data: Data,
    pub mapping: BaseMapping<C, W>,
}

pub type UnresolvedDataSet = BaseDataSet<Link, Link>;
pub type DataSet = BaseDataSet<Contract, Module>;

impl UnresolvedDataSet {
    pub fn resolve(
        self,
        resolver: &impl LinkResolver,
    ) -> impl Future<Item = DataSet, Error = Box<Error>> {
        let UnresolvedDataSet { data, mapping } = self;
        mapping
            .resolve(resolver)
            .map(|mapping| DataSet { data, mapping })
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct BaseDataSourceDefinition<S, D> {
    pub id: String,
    pub location: String,
    #[serde(rename = "specVersion")]
    pub spec_version: String,
    pub schema: S,
    pub datasets: Vec<D>,
}

/// Consider two data sources to be equal if they come from the same IPLD link.
impl<S, D> PartialEq for BaseDataSourceDefinition<S, D> {
    fn eq(&self, other: &Self) -> bool {
        self.location == other.location
    }
}

pub type UnresolvedDataSourceDefinition = BaseDataSourceDefinition<SchemaData, UnresolvedDataSet>;
pub type DataSourceDefinition = BaseDataSourceDefinition<Schema, DataSet>;

impl DataSourceDefinition {
    /// Entry point for resolving a data source definition.
    /// Right now the only supported links are of the form:
    /// `/ipfs/QmUmg7BZC1YP1ca66rRtWKxpXp77WgVHrnv263JtDuvs2k`
    pub fn resolve<'a, 'b>(
        link: Link,
        resolver: &'a impl LinkResolver,
    ) -> Box<Future<Item = Self, Error = DataSourceDefinitionResolveError> + 'a> {
        Box::new(
            resolver
                .cat(&link)
                .map_err(|e| DataSourceDefinitionResolveError::ResolveError(e))
                .and_then(move |file_bytes| {
                    let file = String::from_utf8(file_bytes.to_vec())
                        .map_err(|_| DataSourceDefinitionResolveError::NonUtf8)?;
                    let mut raw: serde_yaml::Value = serde_yaml::from_str(&file)?;
                    {
                        let raw_mapping = raw.as_mapping_mut()
                            .ok_or(DataSourceDefinitionResolveError::InvalidFormat)?;

                        // Inject the IPFS hash as the ID of the data source
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
                    // Parse the YAML data into an UnresolvedDataSourceDefinition
                    let unresolved: UnresolvedDataSourceDefinition = serde_yaml::from_value(raw)?;
                    Ok(unresolved)
                })
                .and_then(move |unresolved| {
                    unresolved
                        .resolve(resolver)
                        .map_err(|e| DataSourceDefinitionResolveError::ResolveError(e))
                }),
        )
    }
}

impl UnresolvedDataSourceDefinition {
    pub fn resolve(
        self,
        resolver: &impl LinkResolver,
    ) -> impl Future<Item = DataSourceDefinition, Error = Box<Error>> {
        let UnresolvedDataSourceDefinition {
            id,
            location,
            spec_version,
            schema,
            datasets,
        } = self;

        // resolve each data set
        stream::futures_ordered(
            datasets
                .into_iter()
                .map(|data_set| data_set.resolve(resolver)),
        ).collect()
            .join(schema.resolve(resolver))
            .map(|(datasets, schema)| DataSourceDefinition {
                id,
                location,
                spec_version,
                schema,
                datasets,
            })
    }
}
