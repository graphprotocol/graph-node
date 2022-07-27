use crate::{components::link_resolver::LinkResolver, prelude::Link};

use anyhow::anyhow;
use serde::Deserialize;
use slog::{info, Logger};
use std::sync::Arc;

#[derive(Debug)]
pub struct DataSource {
    pub name: String,
    pub source: Option<Source>,
    pub mapping: Mapping,
}

#[derive(Debug)]
pub enum Source {
    Ipfs(Link),
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
    ) -> Result<DataSource, anyhow::Error> {
        info!(logger, "Resolve offchain data source";
            "name" => &self.name,
            "kind" => &self.kind,
            "source" => format_args!("{:?}", &self.source),
        );
        let source = match self.kind.as_str() {
            "file/ipfs" => self.source.map(|src| Source::Ipfs(src.file)),
            _ => {
                return Err(anyhow!(
                    "offchain data source has invalid `kind`, expected `file/ipfs` but found {}",
                    self.kind
                ))
            }
        };
        Ok(DataSource {
            name: self.name,
            source,
            mapping: self.mapping.resolve(&*resolver, logger).await?,
        })
    }
}

impl UnresolvedMapping {
    pub async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
    ) -> Result<Mapping, anyhow::Error> {
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
