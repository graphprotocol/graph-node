use crate::{
    blockchain::{BlockPtr, BlockTime, Blockchain},
    components::{link_resolver::LinkResolver, store::BlockNumber},
    data::{subgraph::SPEC_VERSION_1_3_0, value::Word},
    data_source,
    prelude::{DataSourceContext, DeploymentHash, Link},
};
use anyhow::{Context, Error};
use serde::Deserialize;
use slog::{info, Logger};
use std::{fmt, sync::Arc};

use super::{DataSourceTemplateInfo, TriggerWithHandler};

pub const SUBGRAPH_DS_KIND: &str = "subgraph";

const ENTITY_HANDLER_KINDS: &str = "entity";

#[derive(Debug, Clone)]
pub struct DataSource {
    pub kind: String,
    pub name: String,
    pub network: String,
    pub manifest_idx: u32,
    pub source: Source,
    pub mapping: Mapping,
    pub context: Arc<Option<DataSourceContext>>,
    pub creation_block: Option<BlockNumber>,
}

impl DataSource {
    pub fn new(
        kind: String,
        name: String,
        network: String,
        manifest_idx: u32,
        source: Source,
        mapping: Mapping,
        context: Arc<Option<DataSourceContext>>,
        creation_block: Option<BlockNumber>,
    ) -> Self {
        Self {
            kind,
            name,
            network,
            manifest_idx,
            source,
            mapping,
            context,
            creation_block,
        }
    }

    pub fn min_spec_version(&self) -> semver::Version {
        SPEC_VERSION_1_3_0
    }

    pub fn handler_kind(&self) -> &str {
        ENTITY_HANDLER_KINDS
    }

    pub fn network(&self) -> Option<&str> {
        Some(&self.network)
    }
}

impl DataSource {
    pub fn match_and_decode<C: Blockchain>(
        &self,
        trigger: &TriggerData,
    ) -> Option<TriggerWithHandler<super::MappingTrigger<C>>> {
        if self.source != trigger.source {
            return None;
        }

        let trigger_ref = self.mapping.handlers.iter().find_map(|handler| {
            if handler.entity == trigger.entity {
                Some(TriggerWithHandler::new(
                    data_source::MappingTrigger::Subgraph(trigger.clone()),
                    handler.handler.clone(),
                    BlockPtr::new(Default::default(), self.creation_block.unwrap_or(0)),
                    BlockTime::NONE,
                ))
            } else {
                None
            }
        });

        return trigger_ref;
    }

    pub fn address(&self) -> Option<Vec<u8>> {
        Some(self.source.address().to_string().into_bytes())
    }
}

pub type Base64 = Word;

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
pub struct Source {
    pub address: DeploymentHash,
    #[serde(default)]
    pub start_block: BlockNumber,
}

impl Source {
    /// The concept of an address may or not make sense for an subgraph data source, but graph node
    /// will use this in a few places where some sort of not necessarily unique id is useful:
    /// 1. This is used as the value to be returned to mappings from the `dataSource.address()` host
    ///    function, so changing this is a breaking change.
    /// 2. This is used to match with triggers with hosts in `fn hosts_for_trigger`, so make sure
    ///    the `source` of the data source is equal the `source` of the `TriggerData`.
    pub fn address(&self) -> DeploymentHash {
        self.address.clone()
    }
}

#[derive(Clone, Debug)]
pub struct Mapping {
    pub language: String,
    pub api_version: semver::Version,
    pub entities: Vec<String>,
    pub handlers: Vec<EntityHandler>,
    pub runtime: Arc<Vec<u8>>,
    pub link: Link,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct EntityHandler {
    pub handler: String,
    pub entity: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize)]
pub struct UnresolvedDataSource {
    pub kind: String,
    pub name: String,
    pub network: String,
    pub source: UnresolvedSource,
    pub mapping: UnresolvedMapping,
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
pub struct UnresolvedSource {
    address: DeploymentHash,
    #[serde(default)]
    start_block: BlockNumber,
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnresolvedMapping {
    pub api_version: String,
    pub language: String,
    pub file: Link,
    pub handlers: Vec<EntityHandler>,
    pub entities: Vec<String>,
}

impl UnresolvedDataSource {
    #[allow(dead_code)]
    pub(super) async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
        manifest_idx: u32,
    ) -> Result<DataSource, Error> {
        info!(logger, "Resolve subgraph data source";
            "name" => &self.name,
            "kind" => &self.kind,
            "source" => format_args!("{:?}", &self.source),
        );

        let kind = self.kind;
        let source = Source {
            address: self.source.address,
            start_block: self.source.start_block,
        };

        Ok(DataSource {
            manifest_idx,
            kind,
            name: self.name,
            network: self.network,
            source,
            mapping: self.mapping.resolve(resolver, logger).await?,
            context: Arc::new(None),
            creation_block: None,
        })
    }
}

impl UnresolvedMapping {
    pub async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
    ) -> Result<Mapping, Error> {
        info!(logger, "Resolve subgraph ds mapping"; "link" => &self.file.link);

        Ok(Mapping {
            language: self.language,
            api_version: semver::Version::parse(&self.api_version)?,
            entities: self.entities,
            handlers: self.handlers,
            runtime: Arc::new(resolver.cat(logger, &self.file).await?),
            link: self.file,
        })
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct UnresolvedDataSourceTemplate {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub mapping: UnresolvedMapping,
}

#[derive(Clone, Debug)]
pub struct DataSourceTemplate {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub manifest_idx: u32,
    pub mapping: Mapping,
}

impl Into<DataSourceTemplateInfo> for DataSourceTemplate {
    fn into(self) -> DataSourceTemplateInfo {
        let DataSourceTemplate {
            kind,
            network: _,
            name,
            manifest_idx,
            mapping,
        } = self;

        DataSourceTemplateInfo {
            api_version: mapping.api_version.clone(),
            runtime: Some(mapping.runtime),
            name,
            manifest_idx: Some(manifest_idx),
            kind: kind.to_string(),
        }
    }
}

impl UnresolvedDataSourceTemplate {
    pub async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
        manifest_idx: u32,
    ) -> Result<DataSourceTemplate, Error> {
        let kind = self.kind;

        let mapping = self
            .mapping
            .resolve(resolver, logger)
            .await
            .with_context(|| format!("failed to resolve data source template {}", self.name))?;

        Ok(DataSourceTemplate {
            kind,
            network: self.network,
            name: self.name,
            manifest_idx,
            mapping,
        })
    }
}

#[derive(Clone)]
pub struct TriggerData {
    pub source: Source,
    pub entity: String,
    pub data: Arc<bytes::Bytes>,
}

impl fmt::Debug for TriggerData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        #[derive(Debug)]
        struct TriggerDataWithoutData<'a> {
            _source: &'a Source,
        }
        write!(
            f,
            "{:?}",
            TriggerDataWithoutData {
                _source: &self.source
            }
        )
    }
}
