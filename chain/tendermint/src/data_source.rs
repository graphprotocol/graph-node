use std::collections::BTreeMap;
use std::{convert::TryFrom, sync::Arc};

use anyhow::{Error, Result};
use graph::{
    blockchain::{self, Block, Blockchain, TriggerWithHandler},
    components::store::StoredDynamicDataSource,
    data::subgraph::{DataSourceContext, Source},
    prelude::{
        anyhow, async_trait, info, BlockNumber, CheapClone, DataSourceTemplateInfo, Deserialize,
        Link, LinkResolver, Logger,
    },
};

use crate::chain::Chain;
use crate::trigger::TendermintTrigger;

pub const TENDERMINT_KIND: &str = "tendermint/data";

/// Runtime representation of a data source.
// Note: Not great for memory usage that this needs to be `Clone`, considering how there may be tens
// of thousands of data sources in memory at once.
#[derive(Clone, Debug)]
pub struct DataSource {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub source: Source,
    pub mapping: Mapping,
    pub context: Arc<Option<DataSourceContext>>,
    pub creation_block: Option<BlockNumber>,
}

impl blockchain::DataSource<Chain> for DataSource {
    fn address(&self) -> Option<&[u8]> {
        self.source.address.as_ref().map(|x| x.as_bytes())
    }

    fn start_block(&self) -> BlockNumber {
        self.source.start_block
    }

    fn match_and_decode(
        &self,
        trigger: &<Chain as Blockchain>::TriggerData,
        block: Arc<<Chain as Blockchain>::Block>,
        _logger: &Logger,
    ) -> Result<Option<TriggerWithHandler<Chain>>> {
        if self.source.start_block > block.number() {
            return Ok(None);
        }

        match trigger {
            TendermintTrigger::Block(_) => {
                let handler = match self.handler_for_block() {
                    Some(handler) => handler,
                    None => return Ok(None),
                };

                Ok(Some(TriggerWithHandler::new(
                    trigger.cheap_clone(),
                    handler.handler,
                )))
            }
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> &str {
        &self.kind
    }

    fn network(&self) -> Option<&str> {
        self.network.as_ref().map(|s| s.as_str())
    }

    fn context(&self) -> Arc<Option<DataSourceContext>> {
        self.context.cheap_clone()
    }

    fn creation_block(&self) -> Option<BlockNumber> {
        self.creation_block
    }

    fn is_duplicate_of(&self, other: &Self) -> bool {
        let DataSource {
            kind,
            network,
            name,
            source,
            mapping,
            context,

            // The creation block is ignored for detection duplicate data sources.
            // Contract ABI equality is implicit in `source` and `mapping.abis` equality.
            creation_block: _,
        } = self;

        // mapping_request_sender, host_metrics, and (most of) host_exports are operational structs
        // used at runtime but not needed to define uniqueness; each runtime host should be for a
        // unique data source.
        kind == &other.kind
            && network == &other.network
            && name == &other.name
            && source == &other.source
            && mapping.block_handlers == other.mapping.block_handlers
            && context == &other.context
    }

    fn as_stored_dynamic_data_source(&self) -> StoredDynamicDataSource {
        // FIXME (NEAR): Implement me!
        todo!()
    }

    fn from_stored_dynamic_data_source(
        _templates: &BTreeMap<&str, &DataSourceTemplate>,
        _stored: StoredDynamicDataSource,
    ) -> Result<Self> {
        // FIXME (NEAR): Implement me correctly
        todo!()
    }

    fn validate(&self) -> Vec<Error> {
        let mut errors = Vec::new();

        if self.kind != TENDERMINT_KIND {
            errors.push(anyhow!(
                "data source has invalid `kind`, expected {} but found {}",
                TENDERMINT_KIND,
                self.kind
            ))
        }

        // Validate that there is a `source` address if there are receipt handlers
        let no_source_address = self.address().is_none();
        // let has_receipt_handlers = !self.mapping.receipt_handlers.is_empty();
        // if no_source_address && has_receipt_handlers {
        //    errors.push(SubgraphManifestValidationError::SourceAddressRequired.into());
        // };

        // Validate that there are no more than one of both block handlers and receipt handlers
        /*
        if self.mapping.block_handlers.len() > 1 {
            errors.push(anyhow!("data source has duplicated block handlers"));
        }
        if self.mapping.receipt_handlers.len() > 1 {
            errors.push(anyhow!("data source has duplicated receipt handlers"));
        }
        */
        errors
    }

    fn api_version(&self) -> semver::Version {
        self.mapping.api_version.clone()
    }

    fn runtime(&self) -> &[u8] {
        self.mapping.runtime.as_ref()
    }
}

impl DataSource {
    fn from_manifest(
        kind: String,
        network: Option<String>,
        name: String,
        source: Source,
        mapping: Mapping,
        context: Option<DataSourceContext>,
    ) -> Result<Self> {
        // Data sources in the manifest are created "before genesis" so they have no creation block.
        let creation_block = None;

        Ok(DataSource {
            kind,
            network,
            name,
            source,
            mapping,
            context: Arc::new(context),
            creation_block,
        })
    }

    fn handler_for_block(&self) -> Option<MappingBlockHandler> {
        // FIXME (NEAR): We need to decide how to deal with multi block handlers, allow only 1?
        self.mapping.block_handlers.first().map(|v| v.clone())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
pub struct UnresolvedDataSource {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub source: Source,
    pub mapping: UnresolvedMapping,
    pub context: Option<DataSourceContext>,
}

#[async_trait]
impl blockchain::UnresolvedDataSource<Chain> for UnresolvedDataSource {
    async fn resolve(self, resolver: &impl LinkResolver, logger: &Logger) -> Result<DataSource> {
        let UnresolvedDataSource {
            kind,
            network,
            name,
            source,
            mapping,
            context,
        } = self;

        info!(logger, "Resolve data source"; "name" => &name, "source" => &source.start_block);

        let mapping = mapping.resolve(&*resolver, logger).await?;

        DataSource::from_manifest(kind, network, name, source, mapping, context)
    }
}

impl TryFrom<DataSourceTemplateInfo<Chain>> for DataSource {
    type Error = Error;

    fn try_from(info: DataSourceTemplateInfo<Chain>) -> Result<Self> {
        let DataSourceTemplateInfo {
            template,
            params: _,
            context,
            creation_block,
        } = info;

        Ok(DataSource {
            kind: template.kind,
            network: template.network,
            name: template.name,
            source: Source {
                // FIXME (NEAR): Made those element dummy elements
                address: None,
                abi: "".to_string(),
                start_block: 0,
            },
            mapping: template.mapping,
            context: Arc::new(context),
            creation_block: Some(creation_block),
        })
    }
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
pub struct BaseDataSourceTemplate<M> {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub mapping: M,
}

pub type UnresolvedDataSourceTemplate = BaseDataSourceTemplate<UnresolvedMapping>;
pub type DataSourceTemplate = BaseDataSourceTemplate<Mapping>;

#[async_trait]
impl blockchain::UnresolvedDataSourceTemplate<Chain> for UnresolvedDataSourceTemplate {
    async fn resolve(
        self,
        resolver: &impl LinkResolver,
        logger: &Logger,
    ) -> Result<DataSourceTemplate> {
        let UnresolvedDataSourceTemplate {
            kind,
            network,
            name,
            mapping,
        } = self;

        info!(logger, "Resolve data source template"; "name" => &name);

        Ok(DataSourceTemplate {
            kind,
            network,
            name,
            mapping: mapping.resolve(resolver, logger).await?,
        })
    }
}

impl blockchain::DataSourceTemplate<Chain> for DataSourceTemplate {
    fn name(&self) -> &str {
        &self.name
    }

    fn api_version(&self) -> semver::Version {
        self.mapping.api_version.clone()
    }

    fn runtime(&self) -> &[u8] {
        self.mapping.runtime.as_ref()
    }
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnresolvedMapping {
    pub kind: String,
    pub api_version: String,
    pub language: String,
    pub entities: Vec<String>,
    #[serde(default)]
    pub block_handlers: Vec<MappingBlockHandler>,
    pub file: Link,
}

impl UnresolvedMapping {
    pub async fn resolve(self, resolver: &impl LinkResolver, logger: &Logger) -> Result<Mapping> {
        let UnresolvedMapping {
            kind,
            api_version,
            language,
            entities,
            block_handlers,
            file: link,
        } = self;

        let api_version = semver::Version::parse(&api_version)?;

        info!(logger, "Resolve mapping"; "link" => &link.link);
        let module_bytes = resolver.cat(logger, &link).await?;

        Ok(Mapping {
            kind,
            api_version,
            language,
            entities,
            block_handlers: block_handlers.clone(),
            runtime: Arc::new(module_bytes),
            link,
        })
    }
}

#[derive(Clone, Debug)]
pub struct Mapping {
    pub kind: String,
    pub api_version: semver::Version,
    pub language: String,
    pub entities: Vec<String>,
    pub block_handlers: Vec<MappingBlockHandler>,
    pub runtime: Arc<Vec<u8>>,
    pub link: Link,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct MappingBlockHandler {
    pub handler: String,
}
