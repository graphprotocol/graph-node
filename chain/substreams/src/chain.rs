use crate::{data_source::*, EntityChanges, TriggerData, TriggerFilter, TriggersAdapter};
use anyhow::Error;
use graph::blockchain::client::ChainClient;
use graph::blockchain::{BlockIngestor, EmptyNodeCapabilities, NoopRuntimeAdapter};
use graph::components::store::DeploymentCursorTracker;
use graph::firehose::FirehoseEndpoints;
use graph::prelude::{BlockHash, LoggerFactory, MetricsRegistry};
use graph::{
    blockchain::{
        self,
        block_stream::{BlockStream, BlockStreamBuilder, FirehoseCursor},
        BlockPtr, Blockchain, BlockchainKind, IngestorError, RuntimeAdapter as RuntimeAdapterTrait,
    },
    components::store::DeploymentLocator,
    data::subgraph::UnifiedMappingApiVersion,
    prelude::{async_trait, BlockNumber, ChainStore},
    slog::Logger,
};
use std::sync::Arc;

#[derive(Default, Debug, Clone)]
pub struct Block {
    pub hash: BlockHash,
    pub number: BlockNumber,
    pub changes: EntityChanges,
}

impl blockchain::Block for Block {
    fn ptr(&self) -> BlockPtr {
        BlockPtr {
            hash: self.hash.clone(),
            number: self.number,
        }
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        None
    }
}

pub struct Chain {
    chain_store: Arc<dyn ChainStore>,
    block_stream_builder: Arc<dyn BlockStreamBuilder<Self>>,

    pub(crate) logger_factory: LoggerFactory,
    pub(crate) client: Arc<ChainClient<Self>>,
    pub(crate) metrics_registry: Arc<MetricsRegistry>,
}

impl Chain {
    pub fn new(
        logger_factory: LoggerFactory,
        firehose_endpoints: FirehoseEndpoints,
        metrics_registry: Arc<MetricsRegistry>,
        chain_store: Arc<dyn ChainStore>,
        block_stream_builder: Arc<dyn BlockStreamBuilder<Self>>,
    ) -> Self {
        Self {
            logger_factory,
            client: Arc::new(ChainClient::new_firehose(firehose_endpoints)),
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

#[async_trait]
impl Blockchain for Chain {
    const KIND: BlockchainKind = BlockchainKind::Substreams;

    type Client = ();
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

    type NodeCapabilities = EmptyNodeCapabilities<Self>;

    fn triggers_adapter(
        &self,
        _log: &DeploymentLocator,
        _capabilities: &Self::NodeCapabilities,
        _unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Arc<dyn blockchain::TriggersAdapter<Self>>, Error> {
        Ok(Arc::new(TriggersAdapter {}))
    }

    async fn new_block_stream(
        &self,
        deployment: DeploymentLocator,
        store: impl DeploymentCursorTracker,
        start_blocks: Vec<BlockNumber>,
        filter: Arc<Self::TriggerFilter>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Self>>, Error> {
        self.block_stream_builder
            .build_firehose(
                self,
                deployment,
                store.firehose_cursor(),
                start_blocks,
                store.block_ptr(),
                filter,
                unified_api_version,
            )
            .await
    }

    fn is_refetch_block_required(&self) -> bool {
        false
    }
    async fn refetch_firehose_block(
        &self,
        _logger: &Logger,
        _cursor: FirehoseCursor,
    ) -> Result<Block, Error> {
        unimplemented!("This chain does not support Dynamic Data Sources. is_refetch_block_required always returns false, this shouldn't be called.")
    }

    fn chain_store(&self) -> Arc<dyn ChainStore> {
        self.chain_store.clone()
    }

    async fn block_pointer_from_number(
        &self,
        _logger: &Logger,
        number: BlockNumber,
    ) -> Result<BlockPtr, IngestorError> {
        // This is the same thing TriggersAdapter does, not sure if it's going to work but
        // we also don't yet have a good way of getting this value until we sort out the
        // chain store.
        // TODO(filipe): Fix this once the chain_store is correctly setup for substreams.
        Ok(BlockPtr {
            hash: BlockHash::from(vec![0xff; 32]),
            number,
        })
    }
    fn runtime_adapter(&self) -> Arc<dyn RuntimeAdapterTrait<Self>> {
        Arc::new(NoopRuntimeAdapter::default())
    }

    fn chain_client(&self) -> Arc<ChainClient<Self>> {
        self.client.clone()
    }

    fn block_ingestor(&self) -> anyhow::Result<Box<dyn BlockIngestor>> {
        unreachable!("Substreams rely on the block ingestor from the network they are processing")
    }
}
