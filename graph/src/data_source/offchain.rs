use crate::{
    bail,
    blockchain::{BlockPtr, Blockchain},
    components::{
        link_resolver::LinkResolver,
        store::{BlockNumber, EntityType, StoredDynamicDataSource},
        subgraph::DataSourceTemplateInfo,
    },
    data::store::scalar::Bytes,
    data_source,
    ipfs_client::CidFile,
    prelude::{DataSourceContext, Link},
};
use anyhow::{self, Context, Error};
use serde::Deserialize;
use slog::{info, Logger};
use std::{
    fmt,
    sync::{atomic::AtomicI32, Arc},
};

use super::{CausalityRegion, DataSourceCreationError, TriggerWithHandler};

pub const OFFCHAIN_KINDS: &[&str] = &["file/ipfs"];
const NOT_DONE_VALUE: i32 = -1;

#[derive(Debug, Clone)]
pub struct DataSource {
    pub kind: String,
    pub name: String,
    pub manifest_idx: u32,
    pub source: Source,
    pub mapping: Mapping,
    pub context: Arc<Option<DataSourceContext>>,
    pub creation_block: Option<BlockNumber>,
    done_at: Arc<AtomicI32>,
    pub causality_region: CausalityRegion,
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
        causality_region: CausalityRegion,
    ) -> Self {
        Self {
            kind,
            name,
            manifest_idx,
            source,
            mapping,
            context,
            creation_block,
            done_at: Arc::new(AtomicI32::new(NOT_DONE_VALUE)),
            causality_region,
        }
    }

    // mark this data source as processed.
    pub fn mark_processed_at(&self, block_no: i32) {
        assert!(block_no != NOT_DONE_VALUE);
        self.done_at
            .store(block_no, std::sync::atomic::Ordering::SeqCst);
    }

    // returns `true` if the data source is processed.
    pub fn is_processed(&self) -> bool {
        self.done_at.load(std::sync::atomic::Ordering::SeqCst) != NOT_DONE_VALUE
    }

    pub fn done_at(&self) -> Option<i32> {
        match self.done_at.load(std::sync::atomic::Ordering::SeqCst) {
            NOT_DONE_VALUE => None,
            n => Some(n),
        }
    }

    pub fn set_done_at(&self, block: Option<i32>) {
        let value = block.unwrap_or(NOT_DONE_VALUE);

        self.done_at
            .store(value, std::sync::atomic::Ordering::SeqCst);
    }
}

impl DataSource {
    pub fn from_template_info(
        info: DataSourceTemplateInfo<impl Blockchain>,
        causality_region: CausalityRegion,
    ) -> Result<Self, DataSourceCreationError> {
        let template = match info.template {
            data_source::DataSourceTemplate::Offchain(template) => template,
            data_source::DataSourceTemplate::Onchain(_) => {
                bail!("Cannot create offchain data source from onchain template")
            }
        };
        let source = info.params.into_iter().next().ok_or(anyhow::anyhow!(
            "Failed to create data source from template `{}`: source parameter is missing",
            template.name
        ))?;

        let source = match source.parse() {
            Ok(source) => Source::Ipfs(source),

            // Ignore data sources created with an invalid CID.
            Err(e) => return Err(DataSourceCreationError::Ignore(source, e)),
        };

        Ok(Self {
            kind: template.kind.clone(),
            name: template.name.clone(),
            manifest_idx: template.manifest_idx,
            source,
            mapping: template.mapping,
            context: Arc::new(info.context),
            creation_block: Some(info.creation_block),
            done_at: Arc::new(AtomicI32::new(NOT_DONE_VALUE)),
            causality_region,
        })
    }

    pub fn match_and_decode<C: Blockchain>(
        &self,
        trigger: &TriggerData,
    ) -> Option<TriggerWithHandler<super::MappingTrigger<C>>> {
        if self.source != trigger.source || self.is_processed() {
            return None;
        }
        Some(TriggerWithHandler::new(
            data_source::MappingTrigger::Offchain(trigger.clone()),
            self.mapping.handler.clone(),
            BlockPtr::new(Default::default(), self.creation_block.unwrap_or(0)),
        ))
    }

    pub fn as_stored_dynamic_data_source(&self) -> StoredDynamicDataSource {
        let param = match self.source {
            Source::Ipfs(ref link) => Bytes::from(link.to_bytes()),
        };

        let done_at = self.done_at.load(std::sync::atomic::Ordering::SeqCst);
        let done_at = if done_at == NOT_DONE_VALUE {
            None
        } else {
            Some(done_at)
        };

        let context = self
            .context
            .as_ref()
            .as_ref()
            .map(|ctx| serde_json::to_value(ctx).unwrap());

        StoredDynamicDataSource {
            manifest_idx: self.manifest_idx,
            param: Some(param),
            context,
            creation_block: self.creation_block,
            done_at,
            causality_region: self.causality_region,
        }
    }

    pub fn from_stored_dynamic_data_source(
        template: &DataSourceTemplate,
        stored: StoredDynamicDataSource,
    ) -> Result<Self, Error> {
        let StoredDynamicDataSource {
            manifest_idx,
            param,
            context,
            creation_block,
            done_at,
            causality_region,
        } = stored;

        let param = param.context("no param on stored data source")?;
        let cid_file = CidFile::try_from(param)?;

        let source = Source::Ipfs(cid_file);
        let context = Arc::new(context.map(serde_json::from_value).transpose()?);

        Ok(Self {
            kind: template.kind.clone(),
            name: template.name.clone(),
            manifest_idx,
            source,
            mapping: template.mapping.clone(),
            context,
            creation_block,
            done_at: Arc::new(AtomicI32::new(done_at.unwrap_or(NOT_DONE_VALUE))),
            causality_region,
        })
    }

    /// The concept of an address may or not make sense for an offchain data source, but this is
    /// used as the value to be returned to mappings from the `dataSource.address()` host function.
    pub fn address(&self) -> Option<Vec<u8>> {
        match self.source {
            Source::Ipfs(ref cid) => Some(cid.to_bytes()),
        }
    }

    pub(super) fn is_duplicate_of(&self, b: &DataSource) -> bool {
        let DataSource {
            // Inferred from the manifest_idx
            kind: _,
            name: _,
            mapping: _,

            manifest_idx,
            source,
            context,

            // We want to deduplicate across done status or creation block.
            done_at: _,
            creation_block: _,

            // The causality region is also ignored, to be able to detect duplicated file data
            // sources.
            //
            // Note to future: This will become more complicated if we allow for example file data
            // sources to create other file data sources, because which one is created first (the
            // original) and which is created later (the duplicate) is no longer deterministic. One
            // fix would be to check the equality of the parent causality region.
            causality_region: _,
        } = self;

        // See also: data-source-is-duplicate-of
        manifest_idx == &b.manifest_idx && source == &b.source && context == &b.context
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Source {
    Ipfs(CidFile),
}

#[derive(Clone, Debug)]
pub struct Mapping {
    pub language: String,
    pub api_version: semver::Version,
    pub entities: Vec<EntityType>,
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
pub struct UnresolvedSource {
    file: Link,
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnresolvedMapping {
    pub api_version: String,
    pub language: String,
    pub file: Link,
    pub handler: String,
    pub entities: Vec<EntityType>,
}

impl UnresolvedDataSource {
    #[allow(dead_code)]
    pub(super) async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
        manifest_idx: u32,
        causality_region: CausalityRegion,
    ) -> Result<DataSource, Error> {
        info!(logger, "Resolve offchain data source";
            "name" => &self.name,
            "kind" => &self.kind,
            "source" => format_args!("{:?}", &self.source),
        );
        let source = match self.kind.as_str() {
            "file/ipfs" => Source::Ipfs(self.source.file.link.parse()?),
            _ => {
                anyhow::bail!(
                    "offchain data source has invalid `kind`, expected `file/ipfs` but found {}",
                    self.kind
                );
            }
        };
        Ok(DataSource {
            manifest_idx,
            kind: self.kind,
            name: self.name,
            source,
            mapping: self.mapping.resolve(resolver, logger).await?,
            context: Arc::new(None),
            creation_block: None,
            done_at: Arc::new(AtomicI32::new(NOT_DONE_VALUE)),
            causality_region,
        })
    }
}

impl UnresolvedMapping {
    pub async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
    ) -> Result<Mapping, Error> {
        info!(logger, "Resolve offchain mapping"; "link" => &self.file.link);
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

impl UnresolvedDataSourceTemplate {
    pub async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
        manifest_idx: u32,
    ) -> Result<DataSourceTemplate, Error> {
        let mapping = self
            .mapping
            .resolve(resolver, logger)
            .await
            .with_context(|| format!("failed to resolve data source template {}", self.name))?;

        Ok(DataSourceTemplate {
            kind: self.kind,
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
