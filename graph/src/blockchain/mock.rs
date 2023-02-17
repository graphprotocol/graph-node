use crate::{
    components::{link_resolver::LinkResolver, store::BlockNumber},
    prelude::DataSourceTemplateInfo,
};
use anyhow::Error;
use async_trait::async_trait;
use core::fmt;
use serde::Deserialize;
use std::{convert::TryFrom, sync::Arc};

use super::{
    block_stream::{self, FirehoseCursor},
    client::ChainClient,
    HostFn, IngestorError, TriggerWithHandler,
};

use super::{
    block_stream::BlockWithTriggers, Block, BlockPtr, Blockchain, BlockchainKind, DataSource,
    DataSourceTemplate, NodeCapabilities, RuntimeAdapter, TriggerData, TriggerFilter,
    TriggersAdapter, UnresolvedDataSource, UnresolvedDataSourceTemplate,
};

#[derive(Debug)]
pub struct MockBlockchain;

#[derive(Clone, Hash, Eq, PartialEq, Debug, Default)]
pub struct MockBlock {
    pub number: u64,
}

impl Block for MockBlock {
    fn ptr(&self) -> BlockPtr {
        todo!()
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        todo!()
    }
}

#[derive(Clone)]
pub struct MockDataSource;

impl<C: Blockchain> TryFrom<DataSourceTemplateInfo<C>> for MockDataSource {
    type Error = Error;

    fn try_from(_value: DataSourceTemplateInfo<C>) -> Result<Self, Self::Error> {
        todo!()
    }
}

impl<C: Blockchain> DataSource<C> for MockDataSource {
    fn from_template_info(_template_info: DataSourceTemplateInfo<C>) -> Result<Self, Error> {
        todo!()
    }

    fn address(&self) -> Option<&[u8]> {
        todo!()
    }

    fn start_block(&self) -> crate::components::store::BlockNumber {
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

    fn context(&self) -> std::sync::Arc<Option<crate::prelude::DataSourceContext>> {
        todo!()
    }

    fn creation_block(&self) -> Option<crate::components::store::BlockNumber> {
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
        _trigger: &C::TriggerData,
        _block: &std::sync::Arc<C::Block>,
        _logger: &slog::Logger,
    ) -> Result<Option<TriggerWithHandler<C>>, anyhow::Error> {
        todo!()
    }

    fn is_duplicate_of(&self, _other: &Self) -> bool {
        todo!()
    }

    fn as_stored_dynamic_data_source(&self) -> crate::components::store::StoredDynamicDataSource {
        todo!()
    }

    fn from_stored_dynamic_data_source(
        _template: &C::DataSourceTemplate,
        _stored: crate::components::store::StoredDynamicDataSource,
    ) -> Result<Self, anyhow::Error> {
        todo!()
    }

    fn validate(&self) -> Vec<anyhow::Error> {
        todo!()
    }
}

#[derive(Clone, Default, Deserialize)]
pub struct MockUnresolvedDataSource;

#[async_trait]
impl<C: Blockchain> UnresolvedDataSource<C> for MockUnresolvedDataSource {
    async fn resolve(
        self,
        _resolver: &Arc<dyn LinkResolver>,
        _logger: &slog::Logger,
        _manifest_idx: u32,
    ) -> Result<C::DataSource, anyhow::Error> {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct MockDataSourceTemplate;

impl<C: Blockchain> DataSourceTemplate<C> for MockDataSourceTemplate {
    fn api_version(&self) -> semver::Version {
        todo!()
    }

    fn runtime(&self) -> Option<Arc<Vec<u8>>> {
        todo!()
    }

    fn name(&self) -> &str {
        todo!()
    }

    fn manifest_idx(&self) -> u32 {
        todo!()
    }
}

#[derive(Clone, Default, Deserialize)]
pub struct MockUnresolvedDataSourceTemplate;

#[async_trait]
impl<C: Blockchain> UnresolvedDataSourceTemplate<C> for MockUnresolvedDataSourceTemplate {
    async fn resolve(
        self,
        _resolver: &Arc<dyn LinkResolver>,
        _logger: &slog::Logger,
        _manifest_idx: u32,
    ) -> Result<C::DataSourceTemplate, anyhow::Error> {
        todo!()
    }
}

pub struct MockTriggersAdapter;

#[async_trait]
impl<C: Blockchain> TriggersAdapter<C> for MockTriggersAdapter {
    async fn ancestor_block(
        &self,
        _ptr: BlockPtr,
        _offset: BlockNumber,
    ) -> Result<Option<C::Block>, Error> {
        todo!()
    }

    async fn scan_triggers(
        &self,
        _from: crate::components::store::BlockNumber,
        _to: crate::components::store::BlockNumber,
        _filter: &C::TriggerFilter,
    ) -> Result<Vec<block_stream::BlockWithTriggers<C>>, Error> {
        todo!()
    }

    async fn triggers_in_block(
        &self,
        _logger: &slog::Logger,
        _block: C::Block,
        _filter: &C::TriggerFilter,
    ) -> Result<BlockWithTriggers<C>, Error> {
        todo!()
    }

    async fn is_on_main_chain(&self, _ptr: BlockPtr) -> Result<bool, Error> {
        todo!()
    }

    async fn parent_ptr(&self, _block: &BlockPtr) -> Result<Option<BlockPtr>, Error> {
        todo!()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct MockTriggerData;

impl TriggerData for MockTriggerData {
    fn error_context(&self) -> String {
        todo!()
    }
}

#[derive(Debug)]
pub struct MockMappingTrigger {}

#[derive(Clone, Default)]
pub struct MockTriggerFilter;

impl<C: Blockchain> TriggerFilter<C> for MockTriggerFilter {
    fn extend<'a>(&mut self, _data_sources: impl Iterator<Item = &'a C::DataSource> + Clone) {
        todo!()
    }

    fn node_capabilities(&self) -> C::NodeCapabilities {
        todo!()
    }

    fn extend_with_template(
        &mut self,
        _data_source: impl Iterator<Item = <C as Blockchain>::DataSourceTemplate>,
    ) {
        todo!()
    }

    fn to_firehose_filter(self) -> Vec<prost_types::Any> {
        todo!()
    }
}

#[derive(Debug)]
pub struct MockNodeCapabilities;

impl fmt::Display for MockNodeCapabilities {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl<C: Blockchain> NodeCapabilities<C> for MockNodeCapabilities {
    fn from_data_sources(_data_sources: &[C::DataSource]) -> Self {
        todo!()
    }
}

pub struct MockRuntimeAdapter;

impl<C: Blockchain> RuntimeAdapter<C> for MockRuntimeAdapter {
    fn host_fns(&self, _ds: &C::DataSource) -> Result<Vec<HostFn>, Error> {
        todo!()
    }
}

#[async_trait]
impl Blockchain for MockBlockchain {
    const KIND: BlockchainKind = BlockchainKind::Ethereum;

    type Client = ();
    type Block = MockBlock;

    type DataSource = MockDataSource;

    type UnresolvedDataSource = MockUnresolvedDataSource;

    type DataSourceTemplate = MockDataSourceTemplate;

    type UnresolvedDataSourceTemplate = MockUnresolvedDataSourceTemplate;

    type TriggerData = MockTriggerData;

    type MappingTrigger = MockMappingTrigger;

    type TriggerFilter = MockTriggerFilter;

    type NodeCapabilities = MockNodeCapabilities;

    fn triggers_adapter(
        &self,
        _loc: &crate::components::store::DeploymentLocator,
        _capabilities: &Self::NodeCapabilities,
        _unified_api_version: crate::data::subgraph::UnifiedMappingApiVersion,
    ) -> Result<std::sync::Arc<dyn TriggersAdapter<Self>>, anyhow::Error> {
        todo!()
    }

    async fn new_firehose_block_stream(
        &self,
        _deployment: crate::components::store::DeploymentLocator,
        _block_cursor: FirehoseCursor,
        _start_blocks: Vec<crate::components::store::BlockNumber>,
        _subgraph_current_block: Option<BlockPtr>,
        _filter: std::sync::Arc<Self::TriggerFilter>,
        _unified_api_version: crate::data::subgraph::UnifiedMappingApiVersion,
    ) -> Result<Box<dyn block_stream::BlockStream<Self>>, anyhow::Error> {
        todo!()
    }

    async fn new_polling_block_stream(
        &self,
        _deployment: crate::components::store::DeploymentLocator,
        _start_blocks: Vec<crate::components::store::BlockNumber>,
        _subgraph_current_block: Option<BlockPtr>,
        _filter: std::sync::Arc<Self::TriggerFilter>,
        _unified_api_version: crate::data::subgraph::UnifiedMappingApiVersion,
    ) -> Result<Box<dyn block_stream::BlockStream<Self>>, anyhow::Error> {
        todo!()
    }

    fn is_refetch_block_required(&self) -> bool {
        false
    }

    async fn refetch_firehose_block(
        &self,
        _logger: &slog::Logger,
        _cursor: FirehoseCursor,
    ) -> Result<MockBlock, Error> {
        todo!()
    }

    fn chain_store(&self) -> std::sync::Arc<dyn crate::components::store::ChainStore> {
        todo!()
    }

    async fn block_pointer_from_number(
        &self,
        _logger: &slog::Logger,
        _number: crate::components::store::BlockNumber,
    ) -> Result<BlockPtr, IngestorError> {
        todo!()
    }

    fn runtime_adapter(&self) -> std::sync::Arc<dyn RuntimeAdapter<Self>> {
        todo!()
    }

    fn chain_client(&self) -> Arc<ChainClient<MockBlockchain>> {
        todo!()
    }
}
