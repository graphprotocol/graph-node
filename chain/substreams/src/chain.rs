use crate::{data_source::*, EntitiesChanges, TriggerData, TriggerFilter, TriggersAdapter};
use anyhow::Error;
use core::fmt;
use graph::firehose::FirehoseEndpoints;
use graph::prelude::{BlockHash, LoggerFactory, MetricsRegistry};
use graph::{
    blockchain::{
        self,
        block_stream::{BlockStream, BlockStreamBuilder, FirehoseCursor},
        BlockPtr, Blockchain, BlockchainKind, IngestorError, RuntimeAdapter,
    },
    components::store::DeploymentLocator,
    data::subgraph::UnifiedMappingApiVersion,
    impl_slog_value,
    prelude::{async_trait, BlockNumber, ChainStore},
    slog::Logger,
};
use std::{str::FromStr, sync::Arc};

#[derive(Clone, Debug, Default)]
pub struct Block {
    pub block_num: BlockNumber,
    pub block_hash: BlockHash,
    pub parent_block_num: BlockNumber,
    pub parent_block_hash: BlockHash,
    pub entities_changes: EntitiesChanges,
}

impl blockchain::Block for Block {
    fn ptr(&self) -> BlockPtr {
        return BlockPtr {
            hash: self.block_hash.clone(),
            number: self.block_num as i32,
        };
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        Some(BlockPtr {
            hash: BlockHash(Box::from(self.parent_block_hash.as_slice())),
            number: self.parent_block_num as i32,
        })
    }

    fn number(&self) -> i32 {
        self.ptr().number
    }

    fn hash(&self) -> BlockHash {
        self.ptr().hash
    }

    fn parent_hash(&self) -> Option<BlockHash> {
        self.parent_ptr().map(|ptr| ptr.hash)
    }

    fn data(&self) -> Result<jsonrpc_core::serde_json::Value, jsonrpc_core::serde_json::Error> {
        Ok(jsonrpc_core::serde_json::Value::Null)
    }
}

pub struct Chain {
    chain_store: Arc<dyn ChainStore>,
    block_stream_builder: Arc<dyn BlockStreamBuilder<Self>>,

    pub(crate) logger_factory: LoggerFactory,
    pub(crate) endpoints: Arc<FirehoseEndpoints>,
    pub(crate) metrics_registry: Arc<dyn MetricsRegistry>,
}

impl Chain {
    pub fn new(
        logger_factory: LoggerFactory,
        endpoints: Arc<FirehoseEndpoints>,
        metrics_registry: Arc<dyn MetricsRegistry>,
        chain_store: Arc<dyn ChainStore>,
        block_stream_builder: Arc<dyn BlockStreamBuilder<Self>>,
    ) -> Self {
        Self {
            logger_factory,
            endpoints,
            metrics_registry,
            chain_store,
            block_stream_builder,
        }
    }
}

impl std::fmt::Debug for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "chain: substreams")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq)]
pub struct NodeCapabilities {}

impl FromStr for NodeCapabilities {
    type Err = Error;

    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        Ok(NodeCapabilities {})
    }
}

impl fmt::Display for NodeCapabilities {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("substream")
    }
}

impl_slog_value!(NodeCapabilities, "{}");

impl graph::blockchain::NodeCapabilities<Chain> for NodeCapabilities {
    fn from_data_sources(_data_sources: &[DataSource]) -> Self {
        NodeCapabilities {}
    }
}

#[async_trait]
impl Blockchain for Chain {
    const KIND: BlockchainKind = BlockchainKind::Substream;

    type Block = Block;
    type DataSource = DataSource;
    type UnresolvedDataSource = UnresolvedDataSource;

    type DataSourceTemplate = NoopDataSourceTemplate;
    type UnresolvedDataSourceTemplate = NoopDataSourceTemplate;

    /// Trigger data as parsed from the triggers adapter.
    type TriggerData = TriggerData;

    /// Decoded trigger ready to be processed by the mapping.
    /// New implementations should have this be the same as `TriggerData`.
    type MappingTrigger = TriggerData;

    /// Trigger filter used as input to the triggers adapter.
    type TriggerFilter = TriggerFilter;

    type NodeCapabilities = NodeCapabilities;

    fn triggers_adapter(
        &self,
        _log: &DeploymentLocator,
        _capabilities: &Self::NodeCapabilities,
        _unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Arc<dyn blockchain::TriggersAdapter<Self>>, Error> {
        Ok(Arc::new(TriggersAdapter {}))
    }

    async fn new_firehose_block_stream(
        &self,
        deployment: DeploymentLocator,
        block_cursor: FirehoseCursor,
        start_blocks: Vec<BlockNumber>,
        subgraph_current_block: Option<BlockPtr>,
        filter: Arc<Self::TriggerFilter>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Self>>, Error> {
        self.block_stream_builder.build_firehose(
            self,
            deployment,
            block_cursor,
            start_blocks,
            subgraph_current_block,
            filter,
            unified_api_version,
        )
    }

    async fn new_polling_block_stream(
        &self,
        _deployment: DeploymentLocator,
        _start_blocks: Vec<BlockNumber>,
        _subgraph_current_block: Option<BlockPtr>,
        _filter: Arc<Self::TriggerFilter>,
        _unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Self>>, Error> {
        unimplemented!("this should never be called for substreams")
    }

    fn chain_store(&self) -> Arc<dyn ChainStore> {
        self.chain_store.clone()
    }

    async fn block_pointer_from_number(
        &self,
        _logger: &Logger,
        _number: BlockNumber,
    ) -> Result<BlockPtr, IngestorError> {
        unimplemented!()
    }
    fn runtime_adapter(&self) -> Arc<dyn RuntimeAdapter<Self>> {
        unimplemented!()
    }

    fn is_firehose_supported(&self) -> bool {
        unimplemented!()
    }
}
