use components::link_resolver::LinkResolver;
use ethabi::Contract;
use futures::prelude::*;
use data::schema::Schema;
use futures::stream;
use parity_wasm;
use parity_wasm::elements::Module;
use std::error::Error;
use graphql_parser;
use failure::Fail;

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
        ipfs_client: &impl LinkResolver,
    ) -> impl Future<Item = Schema, Error = Box<Error + 'static>> {
        ipfs_client
            .cat(&self.source.link)
            .and_then(|schema_bytes| {
                graphql_parser::parse_schema(&String::from_utf8(schema_bytes)?)
                    .map(|document| Schema {
                        id: String::from("local-data-source-schema"),
                        document,
                    }).map_err(|e| Box::new(e.compat()) as Box<Error>)
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
        ipfs_client: &impl LinkResolver,
    ) -> impl Future<Item = MappingABI, Error = Box<Error + 'static>> {
        ipfs_client
            .cat(&self.contract.link)
            .and_then(|contract_bytes| {
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
        ipfs_client: &impl LinkResolver,
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
                .map(|unresolved_abi| unresolved_abi.resolve(ipfs_client)),
        ).collect()
            .join(
                ipfs_client
                    .cat(&runtime.link)
                    .and_then(|module_bytes| {
                        parity_wasm::deserialize_buffer(&module_bytes)
                            .map_err(|e| Box::new(e) as Box<Error>)
                    }),
            )
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
        ipfs_client: &impl LinkResolver,
    ) -> impl Future<Item = DataSet, Error = Box<Error>> {
        let UnresolvedDataSet { data, mapping } = self;
        mapping
            .resolve(ipfs_client)
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

impl UnresolvedDataSourceDefinition {
    pub fn resolve(
        self,
        ipfs_client: &impl LinkResolver,
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
                .map(|data_set| data_set.resolve(ipfs_client)),
        ).collect()
            .join(schema.resolve(ipfs_client))
            .map(|(datasets, schema)| DataSourceDefinition {
                id,
                location,
                spec_version,
                schema,
                datasets,
            })
    }
}
