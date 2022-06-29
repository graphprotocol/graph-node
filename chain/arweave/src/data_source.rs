use graph::blockchain::{Block, TriggerWithHandler};
use graph::components::store::StoredDynamicDataSource;
use graph::data::subgraph::DataSourceContext;
use graph::prelude::SubgraphManifestValidationError;
use graph::{
    anyhow::{anyhow, Error},
    blockchain::{self, Blockchain},
    prelude::{
        async_trait, info, BlockNumber, CheapClone, DataSourceTemplateInfo, Deserialize, Link,
        LinkResolver, Logger,
    },
    semver,
};
use std::{convert::TryFrom, sync::Arc};

use crate::chain::Chain;
use crate::trigger::ArweaveTrigger;

pub const ARWEAVE_KIND: &str = "arweave";

/// Runtime representation of a data source.
#[derive(Clone, Debug)]
pub struct DataSource {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub(crate) source: Source,
    pub mapping: Mapping,
    pub context: Arc<Option<DataSourceContext>>,
    pub creation_block: Option<BlockNumber>,
}

impl blockchain::DataSource<Chain> for DataSource {
    // FIXME
    //
    // need to decode the base64url encoding?
    fn address(&self) -> Option<&[u8]> {
        self.source.owner.as_ref().map(String::as_bytes)
    }

    fn start_block(&self) -> BlockNumber {
        self.source.start_block
    }

    fn match_and_decode(
        &self,
        trigger: &<Chain as Blockchain>::TriggerData,
        block: &Arc<<Chain as Blockchain>::Block>,
        _logger: &Logger,
    ) -> Result<Option<TriggerWithHandler<Chain>>, Error> {
        if self.source.start_block > block.number() {
            return Ok(None);
        }

        let handler = match trigger {
            // A block trigger matches if a block handler is present.
            ArweaveTrigger::Block(_) => match self.handler_for_block() {
                Some(handler) => &handler.handler,
                None => return Ok(None),
            },
            // A transaction trigger matches if a transaction handler is present.
            ArweaveTrigger::Transaction(_) => match self.handler_for_transaction() {
                Some(handler) => &handler.handler,
                None => return Ok(None),
            },
        };

        Ok(Some(TriggerWithHandler::new(
            trigger.cheap_clone(),
            handler.to_owned(),
        )))
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
        // FIXME (Arweave): Implement me!
        todo!()
    }

    fn from_stored_dynamic_data_source(
        _template: &DataSourceTemplate,
        _stored: StoredDynamicDataSource,
    ) -> Result<Self, Error> {
        // FIXME (Arweave): Implement me correctly
        todo!()
    }

    fn validate(&self) -> Vec<Error> {
        let mut errors = Vec::new();

        if self.kind != ARWEAVE_KIND {
            errors.push(anyhow!(
                "data source has invalid `kind`, expected {} but found {}",
                ARWEAVE_KIND,
                self.kind
            ))
        }

        // Validate that there is a `source` address if there are transaction handlers
        let no_source_address = self.address().is_none();
        let has_transaction_handlers = !self.mapping.transaction_handlers.is_empty();
        if no_source_address && has_transaction_handlers {
            errors.push(SubgraphManifestValidationError::SourceAddressRequired.into());
        };

        // Validate that there are no more than one of both block handlers and transaction handlers
        if self.mapping.block_handlers.len() > 1 {
            errors.push(anyhow!("data source has duplicated block handlers"));
        }
        if self.mapping.transaction_handlers.len() > 1 {
            errors.push(anyhow!("data source has duplicated transaction handlers"));
        }

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
    ) -> Result<Self, Error> {
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

    fn handler_for_block(&self) -> Option<&MappingBlockHandler> {
        self.mapping.block_handlers.first()
    }

    fn handler_for_transaction(&self) -> Option<&TransactionHandler> {
        self.mapping.transaction_handlers.first()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
pub struct UnresolvedDataSource {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub(crate) source: Source,
    pub mapping: UnresolvedMapping,
    pub context: Option<DataSourceContext>,
}

#[async_trait]
impl blockchain::UnresolvedDataSource<Chain> for UnresolvedDataSource {
    async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
    ) -> Result<DataSource, Error> {
        let UnresolvedDataSource {
            kind,
            network,
            name,
            source,
            mapping,
            context,
        } = self;

        info!(logger, "Resolve data source"; "name" => &name, "source_address" => format_args!("{:?}", base64_url::encode(&source.owner.clone().unwrap_or_default())), "source_start_block" => source.start_block);

        let mapping = mapping.resolve(resolver, logger).await?;

        DataSource::from_manifest(kind, network, name, source, mapping, context)
    }
}

/// # TODO
///
/// add templates for arweave subgraphs
impl TryFrom<DataSourceTemplateInfo<Chain>> for DataSource {
    type Error = Error;

    fn try_from(_info: DataSourceTemplateInfo<Chain>) -> Result<Self, Error> {
        Err(anyhow!("Arweave subgraphs do not support templates"))
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
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
    ) -> Result<DataSourceTemplate, Error> {
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
    pub api_version: String,
    pub language: String,
    pub entities: Vec<String>,
    #[serde(default)]
    pub block_handlers: Vec<MappingBlockHandler>,
    #[serde(default)]
    pub transaction_handlers: Vec<TransactionHandler>,
    pub file: Link,
}

impl UnresolvedMapping {
    pub async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
    ) -> Result<Mapping, Error> {
        let UnresolvedMapping {
            api_version,
            language,
            entities,
            block_handlers,
            transaction_handlers,
            file: link,
        } = self;

        let api_version = semver::Version::parse(&api_version)?;

        info!(logger, "Resolve mapping"; "link" => &link.link);
        let module_bytes = resolver.cat(logger, &link).await?;

        Ok(Mapping {
            api_version,
            language,
            entities,
            block_handlers,
            transaction_handlers,
            runtime: Arc::new(module_bytes),
            link,
        })
    }
}

#[derive(Clone, Debug)]
pub struct Mapping {
    pub api_version: semver::Version,
    pub language: String,
    pub entities: Vec<String>,
    pub block_handlers: Vec<MappingBlockHandler>,
    pub transaction_handlers: Vec<TransactionHandler>,
    pub runtime: Arc<Vec<u8>>,
    pub link: Link,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct MappingBlockHandler {
    pub handler: String,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct TransactionHandler {
    pub handler: String,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub(crate) struct Source {
    // A data source that does not have an owner can only have block handlers.
    pub(crate) owner: Option<String>,
    #[serde(rename = "startBlock", default)]
    pub(crate) start_block: BlockNumber,
}
