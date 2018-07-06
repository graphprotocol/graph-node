use components::ipfs::Ipfs;
use ethabi::Contract;
use futures::prelude::*;
use futures::stream;
use parity_wasm;
use parity_wasm::elements::Module;
use serde_yaml;
use std::error::Error;

/// IPLD link.
#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct Location {
    #[serde(rename = "/")]
    pub link: String,
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
    pub contract: C,
}

pub type UnresolvedMappingABI = InnerMappingABI<Location>;
pub type MappingABI = InnerMappingABI<Contract>;

impl UnresolvedMappingABI {
    pub fn resolve(
        self,
        ipfs_client: &impl Ipfs,
    ) -> impl Future<Item = MappingABI, Error = Box<Error + 'static>> {
        ipfs_client
            .cat_link(&self.contract.link)
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
    pub wasm_module: W,
}

pub type UnresolvedMapping = InnerMapping<Location, Location>;
pub type Mapping = InnerMapping<Contract, Module>;

impl UnresolvedMapping {
    pub fn resolve(
        self,
        ipfs_client: &impl Ipfs,
    ) -> impl Future<Item = Mapping, Error = Box<Error>> {
        let UnresolvedMapping {
            kind,
            api_version,
            language,
            entities,
            abis,
            event_handlers,
            wasm_module,
        } = self;

        // resolve each abi
        stream::futures_ordered(
            abis.into_iter()
                .map(|unresolved_abi| unresolved_abi.resolve(ipfs_client)),
        ).collect()
            .join(
                // resolve the module
                ipfs_client
                    .cat_link(&wasm_module.link)
                    .and_then(|module_bytes| {
                        parity_wasm::deserialize_buffer(&module_bytes)
                            .map_err(|e| Box::new(e) as Box<Error>)
                    }),
            )
            .map(|(abis, wasm_module)| Mapping {
                kind,
                api_version,
                language,
                entities,
                abis,
                event_handlers,
                wasm_module,
            })
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct InnerDataSet<C, W> {
    pub data: Data,
    pub mapping: InnerMapping<C, W>,
}

pub type UnresolvedDataSet = InnerDataSet<Location, Location>;
pub type DataSet = InnerDataSet<Contract, Module>;

impl UnresolvedDataSet {
    pub fn resolve(
        self,
        ipfs_client: &impl Ipfs,
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
    pub schema: S,
    pub datasets: Vec<D>,
}

/// Consider two data sources to be equal if they come from the same IPLD link.
impl<S, D> PartialEq for InnerDataSourceDefinition<S, D> {
    fn eq(&self, other: &Self) -> bool {
        self.location == other.location
    }
}

pub type UnresolvedDataSourceDefinition = InnerDataSourceDefinition<Location, Location>;
pub type DataSourceDefinition = InnerDataSourceDefinition<String, DataSet>;

impl UnresolvedDataSourceDefinition {
    pub fn resolve(
        self,
        ipfs_client: &impl Ipfs,
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
                .map(|data_set_location| {
                    ipfs_client
                        .cat_link(&data_set_location.link)
                        .and_then(move |data_set_bytes| {
                            let unresolved_data_set: UnresolvedDataSet =
                                serde_yaml::from_slice(&data_set_bytes)?;
                            Ok(unresolved_data_set.resolve(ipfs_client))
                        })
                })
                .map(Future::flatten),
        ).collect()
            .join(
                // resolve the schema
                ipfs_client
                    .cat_link(&schema.link)
                    .and_then(|schema_bytes| Ok(String::from_utf8(schema_bytes)?)),
            )
            .map(|(datasets, schema)| DataSourceDefinition {
                id,
                location,
                spec_version,
                schema,
                datasets,
            })
    }
}
