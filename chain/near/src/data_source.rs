use anyhow::Error;
use graph::components::near::NearBlockExt;
use graph::components::store::StoredDynamicDataSource;
use std::collections::BTreeMap;
use std::{convert::TryFrom, sync::Arc};

use graph::{
    blockchain::{self, Blockchain, DataSource as _},
    prelude::{
        async_trait, info, BlockNumber, CheapClone, DataSourceTemplateInfo, Deserialize,
        LinkResolver, Logger,
    },
};

use graph::data::subgraph::{
    BlockHandlerFilter, DataSourceContext, Mapping, MappingBlockHandler, Source, TemplateSource,
    UnresolvedMapping,
};

use crate::chain::Chain;
use crate::trigger::{NearBlockTriggerType, NearTrigger};
use crate::MappingTrigger;

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
        logger: &Logger,
    ) -> Result<Option<<Chain as Blockchain>::MappingTrigger>, Error> {
        if self.source.start_block > block.number() {
            return Ok(None);
        }

        match trigger {
            NearTrigger::Block(_, trigger_type) => {
                let handler = match self.handler_for_block(&trigger_type) {
                    Some(handler) => handler,
                    None => return Ok(None),
                };

                Ok(Some(MappingTrigger::Block { block, handler }))
            }
        }
    }

    fn mapping(&self) -> &Mapping {
        &self.mapping
    }

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
            && mapping.abis == other.mapping.abis
            && mapping.event_handlers == other.mapping.event_handlers
            && mapping.call_handlers == other.mapping.call_handlers
            && mapping.block_handlers == other.mapping.block_handlers
            && context == &other.context
    }

    fn as_stored_dynamic_data_source(&self) -> StoredDynamicDataSource {
        todo!()
    }

    fn from_stored_dynamic_data_source(
        templates: &BTreeMap<&str, &DataSourceTemplate>,
        stored: StoredDynamicDataSource,
    ) -> Result<Self, Error> {
        todo!()
    }

    // FIXME (NEAR): Commented out for now, re-added `todo!()`
    // fn as_stored_dynamic_data_source(&self) -> StoredDynamicDataSource {
    //     StoredDynamicDataSource {
    //         name: self.name.to_owned(),
    //         source: self.source.clone(),
    //         context: self
    //             .context
    //             .as_ref()
    //             .as_ref()
    //             .map(|ctx| serde_json::to_string(&ctx).unwrap()),
    //         creation_block: self.creation_block,
    //     }
    // }

    // fn from_stored_dynamic_data_source(
    //     templates: &BTreeMap<&str, &DataSourceTemplate>,
    //     stored: StoredDynamicDataSource,
    // ) -> Result<Self, Error> {
    //     let StoredDynamicDataSource {
    //         name,
    //         source,
    //         context,
    //         creation_block,
    //     } = stored;
    //     let template = templates
    //         .get(name.as_str())
    //         .ok_or_else(|| anyhow!("no template named `{}` was found", name))?;
    //     let context = context
    //         .map(|ctx| serde_json::from_str::<Entity>(&ctx))
    //         .transpose()?;

    //     let contract_abi = template.mapping.find_abi(&template.source.abi)?;

    //     Ok(DataSource {
    //         kind: template.kind.to_string(),
    //         network: template.network.as_ref().map(|s| s.to_string()),
    //         name,
    //         source,
    //         mapping: template.mapping.clone(),
    //         context: Arc::new(context),
    //         creation_block,
    //         contract_abi,
    //     })
    // }
}

impl DataSource {
    fn handler_for_block(
        &self,
        trigger_type: &NearBlockTriggerType,
    ) -> Option<MappingBlockHandler> {
        match trigger_type {
            NearBlockTriggerType::Every => self
                .mapping
                .block_handlers
                .iter()
                .find(move |handler| handler.filter == None)
                .cloned(),
            NearBlockTriggerType::WithCallTo(_address) => self
                .mapping
                .block_handlers
                .iter()
                .find(move |handler| handler.filter == Some(BlockHandlerFilter::Call))
                .cloned(),
        }
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
    async fn resolve(
        self,
        resolver: &impl LinkResolver,
        logger: &Logger,
    ) -> Result<DataSource, anyhow::Error> {
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
    type Error = anyhow::Error;

    fn try_from(info: DataSourceTemplateInfo<Chain>) -> Result<Self, anyhow::Error> {
        let DataSourceTemplateInfo {
            template,
            params,
            context,
            creation_block,
        } = info;

        // FIXME (NEAR): Commented code
        // Obtain the address from the parameters
        // let string = params
        //     .get(0)
        //     .with_context(|| {
        //         format!(
        //             "Failed to create data source from template `{}`: address parameter is missing",
        //             template.name
        //         )
        //     })?
        //     .trim_start_matches("0x");

        // let address = Address::from_str(string).with_context(|| {
        //     format!(
        //         "Failed to create data source from template `{}`, invalid address provided",
        //         template.name
        //     )
        // })?;

        // let contract_abi = template
        //     .mapping
        //     .find_abi(&template.source.abi)
        //     .with_context(|| format!("template `{}`", template.name))?;

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
    pub source: TemplateSource,
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
    ) -> Result<DataSourceTemplate, anyhow::Error> {
        let UnresolvedDataSourceTemplate {
            kind,
            network,
            name,
            source,
            mapping,
        } = self;

        info!(logger, "Resolve data source template"; "name" => &name);

        Ok(DataSourceTemplate {
            kind,
            network,
            name,
            source,
            mapping: mapping.resolve(resolver, logger).await?,
        })
    }
}

impl blockchain::DataSourceTemplate<Chain> for DataSourceTemplate {
    fn mapping(&self) -> &Mapping {
        &self.mapping
    }

    fn name(&self) -> &str {
        &self.name
    }
}
