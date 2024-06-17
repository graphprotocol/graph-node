use std::{collections::HashSet, sync::Arc};

use anyhow::{anyhow, bail, Context, Error};
use graph::{
    blockchain,
    components::{link_resolver::LinkResolver, subgraph::InstanceDSTemplateInfo},
    data::subgraph::DeploymentHash,
    prelude::{async_trait, ethabi::Contract, BlockNumber, CheapClone, Link},
    slog::Logger,
};

use serde::Deserialize;

use crate::{chain::Chain, Block, TriggerData};

pub const DATASET_KIND: &str = "dataset";

const DYNAMIC_DATA_SOURCE_ERROR: &str = "Datasets do not support dynamic data sources";
const TEMPLATE_ERROR: &str = "Datasets do not support templates";

const ALLOWED_MAPPING_KIND: [&str; 1] = ["dataset"];
const DATASET_HANDLER_KIND: &str = "dataset";
#[derive(Clone, Debug, PartialEq)]
/// Represents the DataSource portion of the manifest once it has been parsed
/// and the substream spkg has been downloaded + parsed.
pub struct DataSource {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub source: Source,
    pub mapping: Mapping,
    pub initial_block: Option<BlockNumber>,
    pub context: Arc<Option<graph::prelude::DataSourceContext>>,
}

impl blockchain::DataSource<Chain> for DataSource {
    fn from_template_info(
        _info: InstanceDSTemplateInfo,
        _template: &graph::data_source::DataSourceTemplate<Chain>,
    ) -> Result<Self, Error> {
        Err(anyhow!("Substreams does not support templates"))
    }

    fn address(&self) -> Option<&[u8]> {
        None
    }

    fn start_block(&self) -> BlockNumber {
        self.initial_block.unwrap_or(0)
    }

    fn end_block(&self) -> Option<BlockNumber> {
        None
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> &str {
        &self.kind
    }

    fn network(&self) -> Option<&str> {
        self.network.as_deref()
    }

    fn context(&self) -> Arc<Option<graph::prelude::DataSourceContext>> {
        self.context.cheap_clone()
    }

    fn creation_block(&self) -> Option<BlockNumber> {
        None
    }

    fn api_version(&self) -> semver::Version {
        self.mapping.api_version.clone()
    }

    fn runtime(&self) -> Option<Arc<Vec<u8>>> {
        Some(self.mapping.handler.runtime.clone())
    }

    fn handler_kinds(&self) -> HashSet<&str> {
        // This is placeholder, substreams do not have a handler kind.
        vec![DATASET_HANDLER_KIND].into_iter().collect()
    }

    // match_and_decode only seems to be used on the default trigger processor which substreams
    // bypasses so it should be fine to leave it unimplemented.
    fn match_and_decode(
        &self,
        _trigger: &TriggerData,
        _block: &Arc<Block>,
        _logger: &Logger,
    ) -> Result<Option<blockchain::TriggerWithHandler<Chain>>, Error> {
        unimplemented!()
    }

    fn is_duplicate_of(&self, _other: &Self) -> bool {
        todo!()
    }

    fn as_stored_dynamic_data_source(&self) -> graph::components::store::StoredDynamicDataSource {
        unimplemented!("{}", DYNAMIC_DATA_SOURCE_ERROR)
    }

    fn validate(&self, _version: &semver::Version) -> Vec<Error> {
        let mut errs = vec![];

        if &self.kind != DATASET_KIND {
            errs.push(anyhow!(
                "data source has invalid `kind`, expected {} but found {}",
                DATASET_KIND,
                self.kind
            ))
        }

        if self.name.is_empty() {
            errs.push(anyhow!("name cannot be empty"));
        }

        if !ALLOWED_MAPPING_KIND.contains(&self.mapping.kind.as_str()) {
            errs.push(anyhow!(
                "mapping kind has to be one of {:?}, found {}",
                ALLOWED_MAPPING_KIND,
                self.mapping.kind
            ))
        }

        errs
    }

    fn from_stored_dynamic_data_source(
        _template: &<Chain as blockchain::Blockchain>::DataSourceTemplate,
        _stored: graph::components::store::StoredDynamicDataSource,
    ) -> Result<Self, Error> {
        Err(anyhow!(DYNAMIC_DATA_SOURCE_ERROR))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Source {
    pub dataset: DeploymentHash,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Mapping {
    pub api_version: semver::Version,
    pub kind: String,
    pub handler: MappingHandler,
    pub abis: Vec<Arc<MappingABI>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MappingHandler {
    pub handler: String,
    pub runtime: Arc<Vec<u8>>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
/// Raw representation of the data source for deserialization purposes.
pub struct UnresolvedDataSource {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub source: UnresolvedSource,
    pub mapping: UnresolvedMapping,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct UnresolvedMappingABI {
    pub name: String,
    pub file: Link,
}

impl UnresolvedMappingABI {
    pub async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
    ) -> Result<MappingABI, anyhow::Error> {
        let contract_bytes = resolver.cat(logger, &self.file).await.with_context(|| {
            format!(
                "failed to resolve ABI {} from {}",
                self.name, self.file.link
            )
        })?;
        let contract = Contract::load(&*contract_bytes)?;
        Ok(MappingABI {
            name: self.name,
            contract,
        })
    }
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
/// Text api_version, before parsing and validation.
pub struct UnresolvedMapping {
    pub api_version: String,
    // pub abis: Vec<UnresolvedMappingABI>,
    pub kind: String,
    pub handler: String,
    pub file: Link,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MappingABI {
    pub name: String,
    pub contract: Contract,
}

impl From<MappingABI> for graph_chain_ethereum::MappingABI {
    fn from(val: MappingABI) -> Self {
        let MappingABI { name, contract } = val;

        graph_chain_ethereum::MappingABI { name, contract }
    }
}

#[async_trait]
impl blockchain::UnresolvedDataSource<Chain> for UnresolvedDataSource {
    async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
        _manifest_idx: u32,
    ) -> Result<DataSource, Error> {
        let runtime = resolver.cat(&logger, &self.mapping.file).await?;

        // let abis = self
        //     .mapping
        //     .abis
        //     .into_iter()
        //     .map(|unresolved_abi| async {
        //         Result::<_, Error>::Ok(Arc::new(unresolved_abi.resolve(resolver, logger).await?))
        //     })
        //     .collect::<FuturesOrdered<_>>()
        //     .try_collect::<Vec<_>>()
        //     .await?;
        struct Hack<'a> {
            name: &'a str,
            contract: &'a str,
        }

        let abis = vec![
            Hack {
                name: "NonfungiblePositionManager",
                contract: include_str!(
                    "../../../transforms/uniswap/abis/NonfungiblePositionManager.json"
                ),
            },
            Hack {
                name: "Pool",
                contract: include_str!("../../../transforms/uniswap/abis/pool.json"),
            },
            Hack {
                name: "Factory",
                contract: include_str!("../../../transforms/uniswap/abis/factory.json"),
            },
            Hack {
                name: "ERC20",
                contract: include_str!("../../../transforms/uniswap/abis/ERC20.json"),
            },
            Hack {
                name: "ERC20NameBytes",
                contract: include_str!("../../../transforms/uniswap/abis/ERC20NameBytes.json"),
            },
            Hack {
                name: "ERC20SymbolBytes",
                contract: include_str!("../../../transforms/uniswap/abis/ERC20SymbolBytes.json"),
            },
        ]
        .into_iter()
        .flat_map(|h| {
            Contract::load(h.contract.as_bytes()).map(|c| MappingABI {
                name: h.name.into(),
                contract: c,
            })
        })
        .map(Arc::new)
        .collect();

        let dataset = match DeploymentHash::new(self.source.dataset) {
            Ok(hash) => hash,
            Err(s) => bail!("not a valid deployment hash {}", s),
        };

        Ok(DataSource {
            kind: DATASET_KIND.into(),
            network: self.network,
            name: self.name,
            source: Source { dataset },
            mapping: Mapping {
                api_version: semver::Version::parse(&__self.mapping.api_version)?,
                kind: self.mapping.kind,
                handler: MappingHandler {
                    handler: self.mapping.handler,
                    runtime: Arc::new(runtime),
                },
                abis,
            },
            initial_block: self.source.start_block,
            context: Arc::new(None),
        })
    }
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
/// Source is a part of the manifest and this is needed for parsing.
pub struct UnresolvedSource {
    #[serde(rename = "startBlock", default)]
    start_block: Option<BlockNumber>,
    pub dataset: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
/// This is necessary for the Blockchain trait associated types, substreams do not support
/// data source templates so this is a noop and is not expected to be called.
pub struct NoopDataSourceTemplate {}

impl blockchain::DataSourceTemplate<Chain> for NoopDataSourceTemplate {
    fn name(&self) -> &str {
        unimplemented!("{}", TEMPLATE_ERROR);
    }

    fn api_version(&self) -> semver::Version {
        unimplemented!("{}", TEMPLATE_ERROR);
    }

    fn runtime(&self) -> Option<Arc<Vec<u8>>> {
        unimplemented!("{}", TEMPLATE_ERROR);
    }

    fn manifest_idx(&self) -> u32 {
        todo!()
    }

    fn kind(&self) -> &str {
        unimplemented!("{}", TEMPLATE_ERROR);
    }
}

#[async_trait]
impl blockchain::UnresolvedDataSourceTemplate<Chain> for NoopDataSourceTemplate {
    async fn resolve(
        self,
        _resolver: &Arc<dyn LinkResolver>,
        _logger: &Logger,
        _manifest_idx: u32,
    ) -> Result<NoopDataSourceTemplate, anyhow::Error> {
        unimplemented!("{}", TEMPLATE_ERROR)
    }
}

#[cfg(test)]
mod test {
    use std::{str::FromStr, sync::Arc};

    use anyhow::Error;
    use graph::{
        components::link_resolver::LinkResolver,
        data::subgraph::DeploymentHash,
        prelude::{async_trait, serde_yaml, JsonValueStream, Link},
        slog::Logger,
    };

    use crate::{DataSource, Mapping, UnresolvedDataSource, UnresolvedMapping, DATASET_KIND};

    #[test]
    fn parse_data_source() {
        let ds: UnresolvedDataSource = serde_yaml::from_str(TEMPLATE_DATA_SOURCE).unwrap();
        let expected = UnresolvedDataSource {
            kind: DATASET_KIND.into(),
            network: Some("mainnet".into()),
            name: "Uniswap".into(),
            source: crate::UnresolvedSource {
                start_block: None,
                dataset: "".to_string(),
            },
            mapping: UnresolvedMapping {
                api_version: "0.0.7".into(),
                kind: "substreams/graph-entities".into(),
                handler: "".to_string(),
                file: Link {
                    link: "/ipfs/QmbHnhUFZa6qqqRyubUYhXntox1TCBxqryaBM1iNGqVJzT".into(),
                },
                // abis: vec![],
            },
        };
        assert_eq!(ds, expected);
    }

    fn gen_data_source() -> DataSource {
        DataSource {
            kind: DATASET_KIND.into(),
            network: Some("mainnet".into()),
            name: "Uniswap".into(),
            source: crate::Source {
                dataset: DeploymentHash::new("QmcmBvMt1hbPTtPWaBk7HXwXx71tzAKfa2eZeyV2mpRBLQ")
                    .unwrap(),
            },
            mapping: Mapping {
                api_version: semver::Version::from_str("0.0.7").unwrap(),
                kind: "substreams/graph-entities".into(),
                handler: crate::MappingHandler {
                    handler: "".to_string(),
                    runtime: Arc::new(Vec::new()),
                },
                abis: vec![],
            },
            initial_block: None,
            context: Arc::new(None),
        }
    }

    const TEMPLATE_DATA_SOURCE: &str = r#"
    specVersion: 0.0.4
    description: Uniswap is a decentralized protocol for automated token exchange on Ethereum.
    repository: https://github.com/Uniswap/uniswap-v3-subgraph
    schema:
      file: ./schema.graphql
    dataSources:
      - kind: dataset
        name: blocks
        network: mainnet
        source:
          dataset: QmSB1Vw3ZmNX7wwkbPoybK944fDKzLZ3KWLhjbeD9DwyVL
        mapping:
          kind: substreams/graph-entities
          apiVersion: 0.0.7
          file: ./src/mappings/fast.ts
          handler: handleBlock
    "#;

    #[derive(Debug)]
    struct NoopLinkResolver {}

    #[async_trait]
    impl LinkResolver for NoopLinkResolver {
        fn with_timeout(&self, _timeout: std::time::Duration) -> Box<dyn LinkResolver> {
            unimplemented!()
        }

        fn with_retries(&self) -> Box<dyn LinkResolver> {
            unimplemented!()
        }

        async fn cat(&self, _logger: &Logger, _link: &Link) -> Result<Vec<u8>, Error> {
            todo!()
        }

        async fn get_block(&self, _logger: &Logger, _link: &Link) -> Result<Vec<u8>, Error> {
            unimplemented!()
        }

        async fn json_stream(
            &self,
            _logger: &Logger,
            _link: &Link,
        ) -> Result<JsonValueStream, Error> {
            unimplemented!()
        }
    }
}
