use std::sync::Arc;

use anyhow::Error;
use graph::{
    blockchain,
    components::link_resolver::LinkResolver,
    prelude::{async_trait, BlockNumber, DataSourceTemplateInfo, Link},
    slog::Logger,
};
use serde::Deserialize;

use crate::{
    chain::{Block, Chain},
    TriggerData,
};

#[derive(Clone, Debug)]
pub struct DataSource {}

impl TryFrom<DataSourceTemplateInfo<Chain>> for DataSource {
    type Error = anyhow::Error;

    fn try_from(_value: DataSourceTemplateInfo<Chain>) -> Result<Self, Self::Error> {
        todo!()
    }
}

impl blockchain::DataSource<Chain> for DataSource {
    fn address(&self) -> Option<&[u8]> {
        todo!()
    }

    fn start_block(&self) -> BlockNumber {
        todo!()
    }

    fn name(&self) -> &str {
        todo!()
    }

    fn kind(&self) -> &str {
        todo!()
    }

    fn network(&self) -> Option<&str> {
        todo!()
    }

    fn context(&self) -> Arc<Option<graph::prelude::DataSourceContext>> {
        todo!()
    }

    fn creation_block(&self) -> Option<BlockNumber> {
        todo!()
    }

    fn api_version(&self) -> semver::Version {
        todo!()
    }

    fn runtime(&self) -> Option<Arc<Vec<u8>>> {
        todo!()
    }

    fn match_and_decode(
        &self,
        _trigger: &TriggerData,
        _block: &Arc<Block>,
        _logger: &Logger,
    ) -> Result<Option<blockchain::TriggerWithHandler<Chain>>, Error> {
        todo!()
    }

    fn is_duplicate_of(&self, _other: &Self) -> bool {
        todo!()
    }

    fn as_stored_dynamic_data_source(&self) -> graph::components::store::StoredDynamicDataSource {
        todo!()
    }

    fn validate(&self) -> Vec<Error> {
        todo!()
    }

    fn from_stored_dynamic_data_source(
        _template: &<Chain as blockchain::Blockchain>::DataSourceTemplate,
        _stored: graph::components::store::StoredDynamicDataSource,
    ) -> Result<Self, Error> {
        todo!()
    }
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
pub struct BaseDataSourceTemplate<M> {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub mapping: M,
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnresolvedMapping {
    pub api_version: String,
    pub language: String,
    pub entities: Vec<String>,
    pub file: Link,
}

#[derive(Clone, Debug)]
pub struct Mapping {
    pub api_version: semver::Version,
    pub language: String,
    pub entities: Vec<String>,
    pub runtime: Arc<Vec<u8>>,
    pub link: Link,
}

pub type UnresolvedDataSourceTemplate = BaseDataSourceTemplate<UnresolvedMapping>;
pub type DataSourceTemplate = BaseDataSourceTemplate<Mapping>;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
pub struct UnresolvedDataSource {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub(crate) source: (),
    pub mapping: UnresolvedMapping,
    pub context: Option<()>,
}

#[async_trait]
impl blockchain::UnresolvedDataSource<Chain> for UnresolvedDataSource {
    async fn resolve(
        self,
        _resolver: &Arc<dyn LinkResolver>,
        _logger: &Logger,
    ) -> Result<DataSource, Error> {
        unimplemented!()
    }
}
impl blockchain::DataSourceTemplate<Chain> for DataSourceTemplate {
    fn api_version(&self) -> semver::Version {
        todo!()
    }

    fn runtime(&self) -> Option<Arc<Vec<u8>>> {
        todo!()
    }

    fn name(&self) -> &str {
        todo!()
    }
}

#[async_trait]
impl blockchain::UnresolvedDataSourceTemplate<Chain> for UnresolvedDataSourceTemplate {
    async fn resolve(
        self,
        _resolver: &Arc<dyn LinkResolver>,
        _logger: &Logger,
    ) -> Result<DataSourceTemplate, anyhow::Error> {
        unimplemented!()
    }
}
