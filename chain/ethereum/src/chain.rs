use std::collections::HashSet;
use std::iter::FromIterator;
use std::{pin::Pin, sync::Arc, task::Context};

use anyhow::Error;
use graph::{
    blockchain::{
        block_stream::{
            BlockStream, BlockStreamEvent, BlockStreamMetrics, BlockWithTriggers,
            TriggersAdapter as TriggersAdapterTrait,
        },
        Block, BlockHash, BlockPtr, Blockchain, ChainHeadUpdateListener,
        IngestorAdapter as IngestorAdapterTrait, IngestorError, Manifest, TriggerFilter as _,
    },
    cheap_clone::CheapClone,
    components::{ethereum::NodeCapabilities, store::DeploymentLocator},
    log::factory::{ComponentLoggerConfig, ElasticComponentLoggerConfig},
    prelude::{
        async_trait, error, lazy_static, o, serde_yaml, web3::types::H256, BlockFinality,
        BlockNumber, ChainStore, DeploymentHash, EthereumBlockWithCalls, EthereumTrigger,
        Future01CompatExt, LinkResolver, Logger, LoggerFactory, MetricsRegistry, NodeId,
        SubgraphStore,
    },
    runtime::{AscType, DeterministicHostError},
    tokio_stream::Stream,
};

use crate::{
    adapter::EthereumAdapter as _,
    data_source::DataSource,
    ethereum_adapter::{
        blocks_with_triggers, get_calls, parse_block_triggers, parse_call_triggers,
        parse_log_triggers,
    },
    SubgraphEthRpcMetrics, TriggerFilter,
};
use crate::{network::EthereumNetworkAdapters, EthereumAdapter};

lazy_static! {
    /// Maximum number of blocks to request in each chunk.
    static ref MAX_BLOCK_RANGE_SIZE: BlockNumber = std::env::var("GRAPH_ETHEREUM_MAX_BLOCK_RANGE_SIZE")
        .unwrap_or("2000".into())
        .parse::<BlockNumber>()
        .expect("invalid GRAPH_ETHEREUM_MAX_BLOCK_RANGE_SIZE");

    /// Ideal number of triggers in a range. The range size will adapt to try to meet this.
    static ref TARGET_TRIGGERS_PER_BLOCK_RANGE: u64 = std::env::var("GRAPH_ETHEREUM_TARGET_TRIGGERS_PER_BLOCK_RANGE")
        .unwrap_or("100".into())
        .parse::<u64>()
        .expect("invalid GRAPH_ETHEREUM_TARGET_TRIGGERS_PER_BLOCK_RANGE");
}

pub struct Chain {
    logger_factory: LoggerFactory,
    name: String,
    node_id: NodeId,
    registry: Arc<dyn MetricsRegistry>,
    eth_adapters: EthereumNetworkAdapters,
    ancestor_count: BlockNumber,
    chain_store: Arc<dyn ChainStore>,
    subgraph_store: Arc<dyn SubgraphStore>,
    chain_head_update_listener: Arc<dyn ChainHeadUpdateListener>,
    reorg_threshold: BlockNumber,
    pub is_ingestible: bool,
}

impl Chain {
    pub fn new(
        logger_factory: LoggerFactory,
        name: String,
        node_id: NodeId,
        registry: Arc<dyn MetricsRegistry>,
        chain_store: Arc<dyn ChainStore>,
        subgraph_store: Arc<dyn SubgraphStore>,
        eth_adapters: EthereumNetworkAdapters,
        chain_head_update_listener: Arc<dyn ChainHeadUpdateListener>,
        ancestor_count: BlockNumber,
        reorg_threshold: BlockNumber,
        is_ingestible: bool,
    ) -> Self {
        Chain {
            logger_factory,
            name,
            node_id,
            registry,
            eth_adapters,
            ancestor_count,
            chain_store,
            subgraph_store,
            chain_head_update_listener,
            reorg_threshold,
            is_ingestible,
        }
    }
}

impl Blockchain for Chain {
    type Block = WrappedBlockFinality;

    type DataSource = DataSource;

    type DataSourceTemplate = DummyDataSourceTemplate;

    type Manifest = DummyManifest;

    type TriggersAdapter = TriggersAdapter;

    type TriggerData = EthereumTrigger;

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
            ethrpc_metrics,
            eth_adapter,
            chain_store: self.chain_store.cheap_clone(),
        };
        Ok(Arc::new(adapter))
    }

    fn new_block_stream(
        &self,
        deployment: DeploymentLocator,
        start_blocks: Vec<BlockNumber>,
        filter: TriggerFilter,
        metrics: Arc<BlockStreamMetrics>,
    ) -> Result<BlockStream<Self>, Error> {
        let logger = self
            .logger_factory
            .subgraph_logger(&deployment)
            .new(o!("component" => "BlockStream"));
        let chain_store = self.chain_store().clone();
        let writable = self
            .subgraph_store
            .writable(&deployment)
            .expect(&format!("no store for deployment `{}`", deployment.hash));
        let chain_head_update_stream = self.chain_head_update_listener.subscribe(self.name.clone());

        let requirements = filter.node_capabilities();

        let triggers_adapter = self
            .triggers_adapter(&deployment, &requirements)
            .expect(&format!(
                "no adapter for network {} with capabilities {}",
                self.name, requirements
            ));

        Ok(BlockStream::new(
            writable,
            chain_store,
            chain_head_update_stream,
            triggers_adapter,
            self.node_id.clone(),
            deployment.hash,
            filter,
            start_blocks,
            self.reorg_threshold,
            logger,
            metrics,
            *MAX_BLOCK_RANGE_SIZE,
            *TARGET_TRIGGERS_PER_BLOCK_RANGE,
        ))
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

    fn chain_store(&self) -> Arc<dyn ChainStore> {
        self.chain_store.clone()
    }
}

// ETHDEP: Wrapper until we can move BlockFinality into this crate
#[derive(Clone)]
pub struct WrappedBlockFinality(pub BlockFinality);

impl Block for WrappedBlockFinality {
    fn ptr(&self) -> BlockPtr {
        self.0.ptr()
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        self.0.parent_ptr()
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
    ethrpc_metrics: Arc<SubgraphEthRpcMetrics>,
    chain_store: Arc<dyn ChainStore>,
    eth_adapter: Arc<EthereumAdapter>,
}

#[async_trait]
impl TriggersAdapterTrait<Chain> for TriggersAdapter {
    async fn scan_triggers(
        &self,
        from: BlockNumber,
        to: BlockNumber,
        filter: &TriggerFilter,
    ) -> Result<Vec<BlockWithTriggers<Chain>>, Error> {
        blocks_with_triggers(
            self.eth_adapter.clone(),
            self.logger.clone(),
            self.chain_store.clone(),
            self.ethrpc_metrics.clone(),
            from,
            to,
            filter,
        )
        .await
    }

    async fn triggers_in_block(
        &self,
        logger: &Logger,
        block: WrappedBlockFinality,
        filter: &TriggerFilter,
    ) -> Result<BlockWithTriggers<Chain>, Error> {
        let block = get_calls(
            self.eth_adapter.as_ref(),
            logger.clone(),
            self.ethrpc_metrics.clone(),
            filter.clone(),
            block.0,
        )
        .await?;

        match &block {
            BlockFinality::Final(_) => {
                let block_number = block.number() as BlockNumber;
                let blocks = blocks_with_triggers(
                    self.eth_adapter.clone(),
                    logger.clone(),
                    self.chain_store.clone(),
                    self.ethrpc_metrics.clone(),
                    block_number,
                    block_number,
                    filter,
                )
                .await?;
                assert!(blocks.len() == 1);
                Ok(blocks.into_iter().next().unwrap())
            }
            BlockFinality::NonFinal(full_block) => {
                let mut triggers = Vec::new();
                triggers.append(&mut parse_log_triggers(
                    &filter.log,
                    &full_block.ethereum_block,
                ));
                triggers.append(&mut parse_call_triggers(&filter.call, &full_block));
                triggers.append(&mut parse_block_triggers(filter.block.clone(), &full_block));
                Ok(BlockWithTriggers::new(
                    WrappedBlockFinality(block),
                    triggers,
                ))
            }
        }
    }

    async fn is_on_main_chain(&self, ptr: BlockPtr) -> Result<bool, Error> {
        self.eth_adapter
            .is_on_main_chain(&self.logger, self.chain_store.clone(), ptr.clone())
            .await
    }

    fn ancestor_block(
        &self,
        ptr: BlockPtr,
        offset: BlockNumber,
    ) -> Result<Option<WrappedBlockFinality>, Error> {
        let block = self.chain_store.ancestor_block(ptr, offset)?;
        Ok(block.map(|block| {
            WrappedBlockFinality(BlockFinality::NonFinal(EthereumBlockWithCalls {
                ethereum_block: block,
                calls: None,
            }))
        }))
    }

    /// Panics if `block` is genesis.
    /// But that's ok since this is only called when reverting `block`.
    async fn parent_ptr(&self, block: &BlockPtr) -> Result<BlockPtr, Error> {
        use futures::stream::Stream;
        use graph::prelude::LightEthereumBlockExt;

        let blocks = self
            .eth_adapter
            .load_blocks(
                self.logger.cheap_clone(),
                self.chain_store.cheap_clone(),
                HashSet::from_iter(Some(block.hash_as_h256())),
            )
            .collect()
            .compat()
            .await?;
        assert_eq!(blocks.len(), 1);

        // Expect: This is only called when reverting and therefore never for genesis.
        Ok(blocks[0]
            .parent_ptr()
            .expect("genesis block cannot be reverted"))
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
