use std::{pin::Pin, sync::Arc, task::Context};

use anyhow::Error;
use graph::{
    blockchain::{
        self as bc,
        block_stream::{
            BlockStream, BlockStreamEvent, BlockWithTriggers, ScanTriggersError,
            TriggersAdapter as TriggersAdapterTrait,
        },
        Block, BlockHash, Blockchain, IngestorAdapter as IngestorAdapterTrait, IngestorError,
        Manifest,
    },
    cheap_clone::CheapClone,
    components::{ethereum::NodeCapabilities, store::DeploymentLocator},
    log::factory::{ComponentLoggerConfig, ElasticComponentLoggerConfig},
    prelude::{
        async_trait, error, o, serde_yaml, web3::types::H256, BlockNumber, BlockPtr, ChainStore,
        DataSource, DeploymentHash, Future01CompatExt, LinkResolver, Logger, LoggerFactory,
        MetricsRegistry,
    },
    runtime::{AscType, DeterministicHostError},
    tokio_stream::Stream,
};

use crate::{adapter::EthereumAdapter as _, SubgraphEthRpcMetrics, TriggerFilter};
use crate::{network::EthereumNetworkAdapters, EthereumAdapter};

pub struct Chain {
    logger_factory: LoggerFactory,
    registry: Arc<dyn MetricsRegistry>,
    eth_adapters: EthereumNetworkAdapters,
    ancestor_count: BlockNumber,
    chain_store: Arc<dyn ChainStore>,
    pub is_ingestible: bool,
}

impl Chain {
    pub fn new(
        logger_factory: LoggerFactory,
        registry: Arc<dyn MetricsRegistry>,
        chain_store: Arc<dyn ChainStore>,
        eth_adapters: EthereumNetworkAdapters,
        ancestor_count: BlockNumber,
        is_ingestible: bool,
    ) -> Self {
        Chain {
            logger_factory,
            registry,
            eth_adapters,
            ancestor_count,
            chain_store,
            is_ingestible,
        }
    }
}

impl Blockchain for Chain {
    type Block = DummyBlock;

    type DataSource = graph::data::subgraph::DataSource;

    type DataSourceTemplate = DummyDataSourceTemplate;

    type Manifest = DummyManifest;

    type TriggersAdapter = TriggersAdapter;

    type BlockStream = DummyBlockStream;

    type TriggerData = DummyTriggerData;

    type MappingTrigger = DummyMappingTrigger;

    type TriggerFilter = crate::adapter::TriggerFilter;

    type NodeCapabilities = NodeCapabilities;

    type IngestorAdapter = IngestorAdapter;

    fn reorg_threshold() -> u32 {
        todo!()
    }

    fn triggers_adapter(
        &self,
        loc: &DeploymentLocator,
        capabilities: &Self::NodeCapabilities,
    ) -> Result<Arc<Self::TriggersAdapter>, Error> {
        let eth_adapter = self.eth_adapters.cheapest_with(capabilities)?.clone();
        let logger = self
            .logger_factory
            .subgraph_logger(&loc)
            .new(o!("component" => "BlockStream"));
        let ethrpc_metrics = Arc::new(SubgraphEthRpcMetrics::new(self.registry.clone(), &loc.hash));

        let adapter = TriggersAdapter {
            logger,
            _ethrpc_metrics: ethrpc_metrics,
            eth_adapter,
            chain_store: self.chain_store.cheap_clone(),
        };
        Ok(Arc::new(adapter))
    }

    fn new_block_stream(
        &self,
        _current_head: BlockPtr,
        _filter: Self::TriggerFilter,
    ) -> Result<Self::BlockStream, Error> {
        todo!()
    }

    fn ingestor_adapter(&self) -> Arc<Self::IngestorAdapter> {
        let eth_adapter = self.eth_adapters.cheapest().unwrap().clone();
        let logger = self
            .logger_factory
            .component_logger(
                "BlockIngestor",
                Some(ComponentLoggerConfig {
                    elastic: Some(ElasticComponentLoggerConfig {
                        index: String::from("block-ingestor-logs"),
                    }),
                }),
            )
            .new(o!("provider" => eth_adapter.provider().to_string()));

        let adapter = IngestorAdapter {
            eth_adapter,
            logger,
            ancestor_count: self.ancestor_count,
            chain_store: self.chain_store.clone(),
        };
        Arc::new(adapter)
    }

    fn node_capabilities(&self, archive: bool, traces: bool) -> Self::NodeCapabilities {
        NodeCapabilities { archive, traces }
    }

    fn chain_store(&self) -> Arc<dyn ChainStore> {
        self.chain_store.clone()
    }
}

pub struct DummyBlock;

impl Block for DummyBlock {
    fn ptr(&self) -> BlockPtr {
        todo!()
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        todo!()
    }
}

pub struct WrappedDataSource(DataSource);

impl bc::DataSource<Chain> for WrappedDataSource {
    fn match_and_decode(
        &self,
        _trigger: &DummyTriggerData,
        _block: &DummyBlock,
        _logger: &Logger,
    ) -> Result<Option<DummyMappingTrigger>, Error> {
        todo!()
    }
}

impl std::ops::Deref for WrappedDataSource {
    type Target = graph::prelude::DataSource;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<DataSource> for WrappedDataSource {
    fn from(ds: DataSource) -> Self {
        WrappedDataSource(ds)
    }
}

pub struct DummyDataSourceTemplate;

pub struct DummyManifest;

#[async_trait]
impl Manifest<Chain> for DummyManifest {
    async fn resolve_from_raw(
        _id: DeploymentHash,
        _raw: serde_yaml::Mapping,
        _resolver: &impl LinkResolver,
        _logger: &Logger,
    ) -> Result<Self, Error> {
        todo!()
    }

    fn data_sources(&self) -> &[DataSource] {
        todo!()
    }

    fn templates(&self) -> &[DummyDataSourceTemplate] {
        todo!()
    }
}

pub struct TriggersAdapter {
    logger: Logger,
    _ethrpc_metrics: Arc<SubgraphEthRpcMetrics>,
    chain_store: Arc<dyn ChainStore>,
    eth_adapter: Arc<EthereumAdapter>,
}

#[async_trait]
impl TriggersAdapterTrait<Chain> for TriggersAdapter {
    async fn scan_triggers(
        &self,
        _chain_base: BlockPtr,
        _step_size: u32,
        _filter: TriggerFilter,
    ) -> Result<Vec<BlockWithTriggers<Chain>>, ScanTriggersError> {
        todo!()
    }

    async fn triggers_in_block(
        &self,
        _block: DummyBlock,
        _filter: TriggerFilter,
    ) -> Result<BlockWithTriggers<Chain>, Error> {
        todo!()
    }

    async fn is_on_main_chain(&self, ptr: BlockPtr) -> Result<bool, Error> {
        self.eth_adapter
            .is_on_main_chain(&self.logger, self.chain_store.clone(), ptr.clone())
            .await
    }
}

pub struct DummyBlockStream;

impl Stream for DummyBlockStream {
    type Item = Result<BlockStreamEvent<Chain>, Error>;

    fn poll_next(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        todo!()
    }
}

impl BlockStream<Chain> for DummyBlockStream {}

pub struct DummyTriggerData;

pub struct DummyMappingTrigger;

impl AscType for DummyMappingTrigger {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        todo!()
    }

    fn from_asc_bytes(_asc_obj: &[u8]) -> Result<Self, DeterministicHostError> {
        todo!()
    }
}

pub struct IngestorAdapter {
    logger: Logger,
    ancestor_count: i32,
    eth_adapter: Arc<EthereumAdapter>,
    chain_store: Arc<dyn ChainStore>,
}

#[async_trait]
impl IngestorAdapterTrait<Chain> for IngestorAdapter {
    fn logger(&self) -> &Logger {
        &self.logger
    }

    fn ancestor_count(&self) -> BlockNumber {
        self.ancestor_count
    }

    async fn latest_block(&self) -> Result<BlockPtr, IngestorError> {
        self.eth_adapter
            .latest_block_header(&self.logger)
            .compat()
            .await
            .map(|block| block.into())
    }

    async fn ingest_block(
        &self,
        block_hash: &BlockHash,
    ) -> Result<Option<BlockHash>, IngestorError> {
        // TODO: H256::from_slice can panic
        let block_hash = H256::from_slice(block_hash.as_slice());

        // Get the fully populated block
        let block = self
            .eth_adapter
            .block_by_hash(&self.logger, block_hash)
            .compat()
            .await?
            .ok_or_else(|| IngestorError::BlockUnavailable(block_hash))?;
        let block = self
            .eth_adapter
            .load_full_block(&self.logger, block)
            .compat()
            .await?;

        // Store it in the database and try to advance the chain head pointer
        self.chain_store.upsert_block(block).await?;

        self.chain_store
            .cheap_clone()
            .attempt_chain_head_update(self.ancestor_count)
            .await
            .map(|missing| missing.map(|h256| h256.into()))
            .map_err(|e| {
                error!(self.logger, "failed to update chain head");
                IngestorError::Unknown(e)
            })
    }

    fn chain_head_ptr(&self) -> Result<Option<BlockPtr>, Error> {
        self.chain_store.chain_head_ptr()
    }

    fn cleanup_cached_blocks(&self) -> Result<Option<(i32, usize)>, Error> {
        self.chain_store.cleanup_cached_blocks(self.ancestor_count)
    }
}
