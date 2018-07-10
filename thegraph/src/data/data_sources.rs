use components::link_resolver::LinkResolver;
use ethabi::Contract;
use futures::prelude::*;
use futures::stream;
use parity_wasm;
use parity_wasm::elements::Module;
use std::error::Error;

/// IPLD link.
#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct Link {
    #[serde(rename = "/")]
    pub link: String,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct InnerRawSchema<S> {
    pub source: S,
}

pub type UnresolvedRawSchema = InnerRawSchema<Link>;
pub type RawSchema = InnerRawSchema<String>;

impl UnresolvedRawSchema {
    pub fn resolve(
        self,
        ipfs_client: &impl LinkResolver,
    ) -> impl Future<Item = RawSchema, Error = Box<Error + 'static>> {
        ipfs_client
            .cat(&self.source.link)
            .and_then(|schema_bytes| {
                Ok(RawSchema {
                    source: String::from_utf8(schema_bytes)?,
                })
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
pub struct InnerMappingABI<C> {
    pub name: String,
    #[serde(rename = "source")]
    pub contract: C,
}

pub type UnresolvedMappingABI = InnerMappingABI<Link>;
pub type MappingABI = InnerMappingABI<Contract>;

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
pub struct InnerMapping<C, W> {
    pub kind: String,
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub language: String,
    pub entities: Vec<String>,
    pub abis: Vec<InnerMappingABI<C>>,
    #[serde(rename = "eventHandlers")]
    pub event_handlers: Vec<MappingEventHandler>,
    #[serde(rename = "source")]
    pub runtime: W,
}

pub type UnresolvedMapping = InnerMapping<Link, Link>;
pub type Mapping = InnerMapping<Contract, Module>;

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
pub struct InnerDataSet<C, W> {
    pub data: Data,
    pub mapping: InnerMapping<C, W>,
}

pub type UnresolvedDataSet = InnerDataSet<Link, Link>;
pub type DataSet = InnerDataSet<Contract, Module>;

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
pub struct InnerDataSourceDefinition<S, D> {
    pub id: String,
    pub location: String,
    #[serde(rename = "specVersion")]
    pub spec_version: String,
    pub schema: InnerRawSchema<S>,
    pub datasets: Vec<D>,
}

/// Consider two data sources to be equal if they come from the same IPLD link.
impl<S, D> PartialEq for InnerDataSourceDefinition<S, D> {
    fn eq(&self, other: &Self) -> bool {
        self.location == other.location
    }
}

pub type UnresolvedDataSourceDefinition = InnerDataSourceDefinition<Link, UnresolvedDataSet>;
pub type DataSourceDefinition = InnerDataSourceDefinition<String, DataSet>;

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
