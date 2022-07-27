use std::sync::Arc;

use anyhow::{anyhow, Error};
use graph::{
    blockchain,
    cheap_clone::CheapClone,
    components::link_resolver::LinkResolver,
    prelude::{async_trait, BlockNumber, DataSourceTemplateInfo, Link},
    slog::Logger,
};
use serde::Deserialize;

use crate::{
    chain::{Block, Chain},
    TriggerData,
};

pub const SUBSTREAMS_KIND: &str = "substreams";

const DYNAMIC_DATA_SOURCE_ERROR: &str = "Substreams do not support dynamic data sources";
const TEMPLATE_ERROR: &str = "Substreams do not support templates";

// TODO(filipe): Remove once we implement the TriggerProcessor for substreams.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct DataSource {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub(crate) source: Source,
    pub mapping: Mapping,
    pub context: Arc<Option<graph::prelude::DataSourceContext>>,
}

impl TryFrom<DataSourceTemplateInfo<Chain>> for DataSource {
    type Error = anyhow::Error;

    fn try_from(_value: DataSourceTemplateInfo<Chain>) -> Result<Self, Self::Error> {
        Err(anyhow!("Substreams does not support templates"))
    }
}

impl blockchain::DataSource<Chain> for DataSource {
    fn address(&self) -> Option<&[u8]> {
        None
    }

    fn start_block(&self) -> BlockNumber {
        // TODO(filipe): Figure out how to handle this
        0
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

    fn context(&self) -> Arc<Option<graph::prelude::DataSourceContext>> {
        self.context.cheap_clone()
    }

    fn creation_block(&self) -> Option<BlockNumber> {
        None
    }

    fn api_version(&self) -> semver::Version {
        self.mapping.api_version.clone()
    }

    // runtime is not needed for substreams, it will cause the host creation to be skipped.
    fn runtime(&self) -> Option<Arc<Vec<u8>>> {
        None
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

    fn validate(&self) -> Vec<Error> {
        todo!()
    }

    fn from_stored_dynamic_data_source(
        _template: &<Chain as blockchain::Blockchain>::DataSourceTemplate,
        _stored: graph::components::store::StoredDynamicDataSource,
    ) -> Result<Self, Error> {
        Err(anyhow!(DYNAMIC_DATA_SOURCE_ERROR))
    }
}

#[derive(Clone, Debug)]
pub struct Mapping {
    pub api_version: semver::Version,
    pub kind: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct UnresolvedDataSource {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub(crate) source: Source,
    pub mapping: UnresolvedMapping,
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
pub struct UnresolvedMapping {
    pub api_version: String,
    pub kind: String,
}

#[async_trait]
impl blockchain::UnresolvedDataSource<Chain> for UnresolvedDataSource {
    async fn resolve(
        self,
        _resolver: &Arc<dyn LinkResolver>,
        _logger: &Logger,
    ) -> Result<DataSource, Error> {
        Ok(DataSource {
            kind: SUBSTREAMS_KIND.into(),
            network: self.network,
            name: self.name,
            source: self.source,
            mapping: Mapping {
                api_version: semver::Version::parse(&self.mapping.api_version)?,
                kind: self.mapping.kind,
            },
            context: Arc::new(None),
        })
    }
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Source {
    package: Package,
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Package {
    pub module_name: String,
    pub file: Link,
}

#[derive(Debug, Clone, Default, Deserialize)]
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
}

#[async_trait]
impl blockchain::UnresolvedDataSourceTemplate<Chain> for NoopDataSourceTemplate {
    async fn resolve(
        self,
        _resolver: &Arc<dyn LinkResolver>,
        _logger: &Logger,
    ) -> Result<NoopDataSourceTemplate, anyhow::Error> {
        unimplemented!("{}", TEMPLATE_ERROR)
    }
}
