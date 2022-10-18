use crate::{
    blockchain::{BlockPtr, Blockchain},
    components::{
        link_resolver::LinkResolver,
        store::{BlockNumber, StoredDynamicDataSource},
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
    sync::{Arc, Mutex},
};

use super::TriggerWithHandler;

pub const OFFCHAIN_KINDS: &'static [&'static str] = &["file/ipfs"];

#[derive(Debug)]
pub struct DataSource {
    pub kind: String,
    pub name: String,
    pub manifest_idx: u32,
    pub source: Source,
    pub mapping: Mapping,
    pub context: Arc<Option<DataSourceContext>>,
    pub creation_block: Option<BlockNumber>,
    pub done_at: Mutex<Option<i32>>,
}

impl Clone for DataSource {
    fn clone(&self) -> Self {
        Self {
            kind: self.kind.clone(),
            name: self.name.clone(),
            manifest_idx: self.manifest_idx.clone(),
            source: self.source.clone(),
            mapping: self.mapping.clone(),
            context: self.context.clone(),
            creation_block: self.creation_block.clone(),
            done_at: Mutex::new(*self.done_at.lock().unwrap()),
        }
    }
}

impl DataSource {
    // mark this data source as processed.
    pub fn mark_processed_at(&self, block_no: i32) {
        *self.done_at.lock().unwrap() = Some(block_no);
    }

    // returns `true` if the data source is processed.
    pub fn is_processed(&self) -> bool {
        self.done_at.lock().unwrap().is_some()
    }
}

impl<C: Blockchain> TryFrom<DataSourceTemplateInfo<C>> for DataSource {
    type Error = Error;

    fn try_from(info: DataSourceTemplateInfo<C>) -> Result<Self, Self::Error> {
        let template = match info.template {
            data_source::DataSourceTemplate::Offchain(template) => template,
            data_source::DataSourceTemplate::Onchain(_) => {
                anyhow::bail!("Cannot create offchain data source from onchain template")
            }
        };
        let source = info.params.get(0).ok_or(anyhow::anyhow!(
            "Failed to create data source from template `{}`: source parameter is missing",
            template.name
        ))?;
        Ok(Self {
            kind: template.kind.clone(),
            name: template.name.clone(),
            manifest_idx: template.manifest_idx,
            source: Source::Ipfs(source.parse()?),
            mapping: template.mapping.clone(),
            context: Arc::new(info.context),
            creation_block: Some(info.creation_block),
            done_at: Mutex::new(None),
        })
    }
}

impl DataSource {
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
        let context = self
            .context
            .as_ref()
            .as_ref()
            .map(|ctx| serde_json::to_value(&ctx).unwrap());
        StoredDynamicDataSource {
            manifest_idx: self.manifest_idx,
            param: Some(param),
            context,
            creation_block: self.creation_block,
            is_offchain: true,
            done_at: *self.done_at.lock().unwrap(),
        }
    }

    pub fn from_stored_dynamic_data_source(
        template: &DataSourceTemplate,
        stored: StoredDynamicDataSource,
    ) -> Result<Self, Error> {
        let param = stored.param.context("no param on stored data source")?;
        let cid_file = CidFile::try_from(param)?;

        let source = Source::Ipfs(cid_file);
        let context = Arc::new(stored.context.map(serde_json::from_value).transpose()?);
        Ok(Self {
            kind: template.kind.clone(),
            name: template.name.clone(),
            manifest_idx: stored.manifest_idx,
            source,
            mapping: template.mapping.clone(),
            context,
            creation_block: stored.creation_block,
            done_at: Mutex::new(stored.done_at),
        })
    }

    /// The concept of an address may or not make sense for an offchain data source, but this is
    /// used as the value to be returned to mappings from the `dataSource.address()` host function.
    pub fn address(&self) -> Option<Vec<u8>> {
        match self.source {
            Source::Ipfs(ref cid) => Some(cid.to_bytes()),
        }
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
    pub entities: Vec<String>,
}

impl UnresolvedDataSource {
    #[allow(unreachable_code)]
    #[allow(unused_variables)]
    pub async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
        manifest_idx: u32,
    ) -> Result<DataSource, Error> {
        anyhow::bail!(
            "static file data sources are not yet supported, \\
             for details see https://github.com/graphprotocol/graph-node/issues/3864"
        );

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
            mapping: self.mapping.resolve(&*resolver, logger).await?,
            context: Arc::new(None),
            creation_block: None,
            done_at: Mutex::new(None),
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
        info!(logger, "Resolve data source template"; "name" => &self.name);

        Ok(DataSourceTemplate {
            kind: self.kind,
            network: self.network,
            name: self.name,
            manifest_idx,
            mapping: self.mapping.resolve(resolver, logger).await?,
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
