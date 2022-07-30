use crate::{
    blockchain::Blockchain,
    components::{
        link_resolver::LinkResolver,
        store::{BlockNumber, StoredDynamicDataSource},
        subgraph::DataSourceTemplateInfo,
    },
    data::store::scalar::Bytes,
    data_source,
    prelude::{DataSourceContext, Link},
};
use anyhow::{self, Error};
use cid::Cid;
use serde::Deserialize;
use slog::{info, Logger};
use std::{fmt, sync::Arc};

pub const OFFCHAIN_KINDS: &'static [&'static str] = &["file/ipfs"];

#[derive(Clone, Debug)]
pub struct DataSource {
    pub kind: String,
    pub name: String,
    pub source: Option<Source>,
    pub mapping: Mapping,
    pub context: Arc<Option<DataSourceContext>>,
    pub creation_block: Option<BlockNumber>,
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
            source: Some(Source::Ipfs(source.parse()?)),
            mapping: template.mapping.clone(),
            context: Arc::new(info.context),
            creation_block: Some(info.creation_block),
        })
    }
}

impl DataSource {
    pub fn as_stored_dynamic_data_source(&self) -> StoredDynamicDataSource {
        let param = self.source.as_ref().map(|source| match source {
            Source::Ipfs(link) => Bytes::from(link.to_bytes()),
        });
        let context = self
            .context
            .as_ref()
            .as_ref()
            .map(|ctx| serde_json::to_value(&ctx).unwrap());
        StoredDynamicDataSource {
            name: self.name.clone(),
            param,
            context,
            creation_block: self.creation_block,
        }
    }

    pub fn from_stored_dynamic_data_source(
        template: &DataSourceTemplate,
        stored: StoredDynamicDataSource,
    ) -> Result<Self, Error> {
        let source = stored.param.and_then(|bytes| {
            Cid::try_from(bytes.as_slice().to_vec())
                .ok()
                .map(Source::Ipfs)
        });
        let context = Arc::new(stored.context.map(serde_json::from_value).transpose()?);
        Ok(Self {
            kind: template.kind.clone(),
            name: template.name.clone(),
            source,
            mapping: template.mapping.clone(),
            context,
            creation_block: stored.creation_block,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Source {
    Ipfs(Cid),
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
    pub source: Option<UnresolvedSource>,
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
    pub async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
    ) -> Result<DataSource, Error> {
        info!(logger, "Resolve offchain data source";
            "name" => &self.name,
            "kind" => &self.kind,
            "source" => format_args!("{:?}", &self.source),
        );
        let source = match self.kind.as_str() {
            "file/ipfs" => self
                .source
                .map(|src| src.file.link.parse().map(Source::Ipfs))
                .transpose()?,
            _ => {
                anyhow::bail!(
                    "offchain data source has invalid `kind`, expected `file/ipfs` but found {}",
                    self.kind
                );
            }
        };
        Ok(DataSource {
            kind: self.kind,
            name: self.name,
            source,
            mapping: self.mapping.resolve(&*resolver, logger).await?,
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

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct BaseDataSourceTemplate<M> {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub mapping: M,
}

pub type UnresolvedDataSourceTemplate = BaseDataSourceTemplate<UnresolvedMapping>;
pub type DataSourceTemplate = BaseDataSourceTemplate<Mapping>;

impl UnresolvedDataSourceTemplate {
    pub async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
    ) -> Result<DataSourceTemplate, Error> {
        info!(logger, "Resolve data source template"; "name" => &self.name);

        Ok(DataSourceTemplate {
            kind: self.kind,
            network: self.network,
            name: self.name,
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
