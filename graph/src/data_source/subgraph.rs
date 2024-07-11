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
        manifest_idx: u32,
        source: Source,
        mapping: Mapping,
        context: Arc<Option<DataSourceContext>>,
        creation_block: Option<BlockNumber>,
    ) -> Self {
        Self {
            kind,
            name,
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
}

impl DataSource {
    pub fn match_and_decode<C: Blockchain>(
        &self,
        trigger: &TriggerData,
    ) -> Option<TriggerWithHandler<super::MappingTrigger<C>>> {
        if self.source != trigger.source {
            return None;
        }
        Some(TriggerWithHandler::new(
            data_source::MappingTrigger::Subgraph(trigger.clone()),
            self.mapping.handler.clone(),
            BlockPtr::new(Default::default(), self.creation_block.unwrap_or(0)),
            BlockTime::NONE,
        ))
    }

    pub fn address(&self) -> Option<Vec<u8>> {
        Some(self.source.address().to_string().into_bytes())
    }
}

pub type Base64 = Word;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Source(DeploymentHash);

impl Source {
    /// The concept of an address may or not make sense for an subgraph data source, but graph node
    /// will use this in a few places where some sort of not necessarily unique id is useful:
    /// 1. This is used as the value to be returned to mappings from the `dataSource.address()` host
    ///    function, so changing this is a breaking change.
    /// 2. This is used to match with triggers with hosts in `fn hosts_for_trigger`, so make sure
    ///    the `source` of the data source is equal the `source` of the `TriggerData`.
    pub fn address(&self) -> DeploymentHash {
        self.0.clone()
    }
}

#[derive(Clone, Debug)]
pub struct Mapping {
    pub language: String,
    pub api_version: semver::Version,
    pub entities: Vec<String>,
    pub handler: String,
    pub runtime: Arc<Vec<u8>>,
    pub link: Link,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize)]
pub struct UnresolvedDataSource {
    pub kind: String,
    pub name: String,
    pub source: UnresolvedSource,
    pub mapping: UnresolvedMapping,
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
pub struct UnresolvedSource(DeploymentHash);

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnresolvedMapping {
    pub api_version: String,
    pub language: String,
    pub file: Link,
    pub handler: String,
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
        let source = Source(self.source.0.clone());

        Ok(DataSource {
            manifest_idx,
            kind,
            name: self.name,
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
            handler: self.handler,
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
