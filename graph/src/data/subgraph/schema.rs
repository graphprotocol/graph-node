//! See `path/to/blebers.schema` for corresponding graphql schema.

#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct SubgraphSchema {
    id: String,
    manifest: SubgraphManifest,
    entity_count: i32,
    created_at: u64,
    updated_at: u64,
}

impl SubgraphSchema {
    fn new(
        source_manifest: super::SubgraphManifest,
        entity_count: i32,
        created_at: u64,
        updated_at: u64,
    ) -> Self {
        Self {
            id: source_manifest.id.clone(),
            manifest: source_manifest.into(),
            entity_count,
            created_at,
            updated_at,
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct SubgraphManifest {
    spec_version: String,
    description: String,
    schema: String,
    data_sources: Vec<EthereumContractDataSource>,
    repository: String,
}

impl From<super::SubgraphManifest> for SubgraphManifest {
    fn from(manifest: super::SubgraphManifest) -> Self {
        Self {
            spec_version: manifest.spec_version,
            description: String::new(),
            schema: manifest.schema.document.to_string(),
            data_sources: manifest.data_sources.into_iter().map(Into::into).collect(),
            repository: String::new(),
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize)]
struct EthereumContractDataSource {
    kind: String,
    name: String,
    source: EthereumContractSource,
    mapping: EthereumContractMapping,
}

impl From<super::DataSource> for EthereumContractDataSource {
    fn from(data_source: super::DataSource) -> Self {
        Self {
            kind: data_source.kind,
            name: data_source.name,
            source: data_source.source.into(),
            mapping: data_source.mapping.into(),
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize)]
struct EthereumContractSource {
    address: super::Address,
    abi: String,
}

impl From<super::Source> for EthereumContractSource {
    fn from(source: super::Source) -> Self {
        Self {
            address: source.address,
            abi: source.abi,
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct EthereumContractMapping {
    kind: String,
    api_version: String,
    language: String,
    #[serde(rename = "/")]
    file: String,
    entities: Vec<String>,
    abis: Vec<EthereumContractAbi>,
    event_handlers: Vec<EthereumContractEventHandler>,
}

impl From<super::Mapping> for EthereumContractMapping {
    fn from(mapping: super::Mapping) -> Self {
        Self {
            kind: mapping.kind,
            api_version: mapping.api_version,
            language: mapping.language,
            file: mapping.link.link,
            entities: mapping.entities,
            abis: mapping.abis.into_iter().map(Into::into).collect(),
            event_handlers: mapping.event_handlers.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize)]
struct EthereumContractAbi {
    name: String,
    #[serde(rename = "/")]
    file: String,
}

impl From<super::MappingABI> for EthereumContractAbi {
    fn from(abi: super::MappingABI) -> Self {
        Self {
            name: abi.name,
            file: abi.link.link,
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize)]
struct EthereumContractEventHandler {
    event: String,
    handler: String,
}

impl From<super::MappingEventHandler> for EthereumContractEventHandler {
    fn from(event_handler: super::MappingEventHandler) -> Self {
        Self {
            event: event_handler.event,
            handler: event_handler.handler,
        }
    }
}
