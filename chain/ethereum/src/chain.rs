use anyhow::{anyhow, bail, Result};
use anyhow::{Context, Error};
use graph::blockchain::client::ChainClient;
use graph::blockchain::firehose_block_ingestor::{FirehoseBlockIngestor, Transforms};
use graph::blockchain::{
    BlockIngestor, BlockTime, BlockchainKind, ChainIdentifier, ExtendedBlockPtr,
    TriggerFilterWrapper, TriggersAdapterSelector,
};
use graph::components::network_provider::ChainName;
use graph::components::store::{DeploymentCursorTracker, SourceableStore};
use graph::data::subgraph::UnifiedMappingApiVersion;
use graph::firehose::{FirehoseEndpoint, ForkStep};
use graph::futures03::compat::Future01CompatExt;
use graph::futures03::TryStreamExt;
use graph::prelude::{
    retry, BlockHash, ComponentLoggerConfig, ElasticComponentLoggerConfig, EthereumBlock,
    EthereumCallCache, LightEthereumBlock, LightEthereumBlockExt, MetricsRegistry,
};
use graph::schema::InputSchema;
use graph::slog::{debug, error, trace, warn};
use graph::substreams::Clock;
use graph::{
    blockchain::{
        block_stream::{
            BlockRefetcher, BlockStreamEvent, BlockWithTriggers, FirehoseError,
            FirehoseMapper as FirehoseMapperTrait, TriggersAdapter as TriggersAdapterTrait,
        },
        firehose_block_stream::FirehoseBlockStream,
        polling_block_stream::PollingBlockStream,
        Block, BlockPtr, Blockchain, ChainHeadUpdateListener, IngestorError,
        RuntimeAdapter as RuntimeAdapterTrait, TriggerFilter as _,
    },
    cheap_clone::CheapClone,
    components::store::DeploymentLocator,
    firehose,
    prelude::{
        async_trait, o, serde_json as json, BlockNumber, ChainStore, EthereumBlockWithCalls,
        Logger, LoggerFactory, NodeId,
    },
};
use prost::Message;
use std::collections::{BTreeSet, HashSet};
use std::future::Future;
use std::iter::FromIterator;
use std::sync::Arc;
use std::time::Duration;

use crate::codec::HeaderOnlyBlock;
use crate::data_source::DataSourceTemplate;
use crate::data_source::UnresolvedDataSourceTemplate;
use crate::ingestor::PollingBlockIngestor;
use crate::network::EthereumNetworkAdapters;
use crate::runtime::runtime_adapter::eth_call_gas;
use crate::{
    adapter::EthereumAdapter as _,
    codec,
    data_source::{DataSource, UnresolvedDataSource},
    ethereum_adapter::{
        blocks_with_triggers, get_calls, parse_block_triggers, parse_call_triggers,
        parse_log_triggers,
    },
    SubgraphEthRpcMetrics, TriggerFilter, ENV_VARS,
};
use crate::{BufferedCallCache, NodeCapabilities};
use crate::{EthereumAdapter, RuntimeAdapter};
use graph::blockchain::block_stream::{
    BlockStream, BlockStreamBuilder, BlockStreamError, BlockStreamMapper, FirehoseCursor,
    TriggersAdapterWrapper,
};

/// Celo Mainnet: 42220, Testnet Alfajores: 44787, Testnet Baklava: 62320
const CELO_CHAIN_IDS: [u64; 3] = [42220, 44787, 62320];

pub struct EthereumStreamBuilder {}

#[async_trait]
impl BlockStreamBuilder<Chain> for EthereumStreamBuilder {
    async fn build_firehose(
        &self,
        chain: &Chain,
        deployment: DeploymentLocator,
        block_cursor: FirehoseCursor,
        start_blocks: Vec<BlockNumber>,
        subgraph_current_block: Option<BlockPtr>,
        filter: Arc<<Chain as Blockchain>::TriggerFilter>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Chain>>> {
        let requirements = filter.node_capabilities();
        let adapter = chain
            .triggers_adapter(&deployment, &requirements, unified_api_version)
            .unwrap_or_else(|_| {
                panic!(
                    "no adapter for network {} with capabilities {}",
                    chain.name, requirements
                )
            });

        let logger = chain
            .logger_factory
            .subgraph_logger(&deployment)
            .new(o!("component" => "FirehoseBlockStream"));

        let firehose_mapper = Arc::new(FirehoseMapper { adapter, filter });

        Ok(Box::new(FirehoseBlockStream::new(
            deployment.hash,
            chain.chain_client(),
            subgraph_current_block,
            block_cursor,
            firehose_mapper,
            start_blocks,
            logger,
            chain.registry.clone(),
        )))
    }

    async fn build_substreams(
        &self,
        _chain: &Chain,
        _schema: InputSchema,
        _deployment: DeploymentLocator,
        _block_cursor: FirehoseCursor,
        _subgraph_current_block: Option<BlockPtr>,
        _filter: Arc<<Chain as Blockchain>::TriggerFilter>,
    ) -> Result<Box<dyn BlockStream<Chain>>> {
        unimplemented!()
    }

    async fn build_subgraph_block_stream(
        &self,
        chain: &Chain,
        deployment: DeploymentLocator,
        start_blocks: Vec<BlockNumber>,
        source_subgraph_stores: Vec<Arc<dyn SourceableStore>>,
        subgraph_current_block: Option<BlockPtr>,
        filter: Arc<TriggerFilterWrapper<Chain>>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Chain>>> {
        self.build_polling(
            chain,
            deployment,
            start_blocks,
            source_subgraph_stores,
            subgraph_current_block,
            filter,
            unified_api_version,
        )
        .await
    }

    async fn build_polling(
        &self,
        chain: &Chain,
        deployment: DeploymentLocator,
        start_blocks: Vec<BlockNumber>,
        source_subgraph_stores: Vec<Arc<dyn SourceableStore>>,
        subgraph_current_block: Option<BlockPtr>,
        filter: Arc<TriggerFilterWrapper<Chain>>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Chain>>> {
        let requirements = filter.chain_filter.node_capabilities();
        let is_using_subgraph_composition = !source_subgraph_stores.is_empty();
        let adapter = TriggersAdapterWrapper::new(
            chain
                .triggers_adapter(&deployment, &requirements, unified_api_version.clone())
                .unwrap_or_else(|_| {
                    panic!(
                        "no adapter for network {} with capabilities {}",
                        chain.name, requirements
                    )
                }),
            source_subgraph_stores,
        );

        let logger = chain
            .logger_factory
            .subgraph_logger(&deployment)
            .new(o!("component" => "BlockStream"));
        let chain_store = chain.chain_store();
        let chain_head_update_stream = chain
            .chain_head_update_listener
            .subscribe(chain.name.to_string(), logger.clone());

        // Special case: Detect Celo and set the threshold to 0, so that eth_getLogs is always used.
        // This is ok because Celo blocks are always final. And we _need_ to do this because
        // some events appear only in eth_getLogs but not in transaction receipts.
        // See also ca0edc58-0ec5-4c89-a7dd-2241797f5e50.
        let reorg_threshold = match chain.chain_client().as_ref() {
            ChainClient::Rpc(adapter) => {
                let chain_id = adapter
                    .cheapest()
                    .await
                    .ok_or(anyhow!("unable to get eth adapter for chan_id call"))?
                    .chain_id()
                    .await?;

                if CELO_CHAIN_IDS.contains(&chain_id) {
                    0
                } else {
                    chain.reorg_threshold
                }
            }
            _ if is_using_subgraph_composition => chain.reorg_threshold,
            _ => panic!(
                "expected rpc when using polling blockstream : {}",
                is_using_subgraph_composition
            ),
        };

        let max_block_range_size = if is_using_subgraph_composition {
            ENV_VARS.max_block_range_size * 10
        } else {
            ENV_VARS.max_block_range_size
        };

        Ok(Box::new(PollingBlockStream::new(
            chain_store,
            chain_head_update_stream,
            Arc::new(adapter),
            chain.node_id.clone(),
            deployment.hash,
            filter,
            start_blocks,
            reorg_threshold,
            logger,
            max_block_range_size,
            ENV_VARS.target_triggers_per_block_range,
            unified_api_version,
            subgraph_current_block,
        )))
    }
}

pub struct EthereumBlockRefetcher {}

#[async_trait]
impl BlockRefetcher<Chain> for EthereumBlockRefetcher {
    fn required(&self, chain: &Chain) -> bool {
        chain.chain_client().is_firehose()
    }

    async fn get_block(
        &self,
        chain: &Chain,
        logger: &Logger,
        cursor: FirehoseCursor,
    ) -> Result<BlockFinality, Error> {
        let endpoint: Arc<FirehoseEndpoint> = chain.chain_client().firehose_endpoint().await?;
        let block = endpoint.get_block::<codec::Block>(cursor, logger).await?;
        let ethereum_block: EthereumBlockWithCalls = (&block).try_into()?;
        Ok(BlockFinality::NonFinal(ethereum_block))
    }
}

pub struct EthereumAdapterSelector {
    logger_factory: LoggerFactory,
    client: Arc<ChainClient<Chain>>,
    registry: Arc<MetricsRegistry>,
    chain_store: Arc<dyn ChainStore>,
    eth_adapters: Arc<EthereumNetworkAdapters>,
}

impl EthereumAdapterSelector {
    pub fn new(
        logger_factory: LoggerFactory,
        client: Arc<ChainClient<Chain>>,
        registry: Arc<MetricsRegistry>,
        chain_store: Arc<dyn ChainStore>,
        eth_adapters: Arc<EthereumNetworkAdapters>,
    ) -> Self {
        Self {
            logger_factory,
            client,
            registry,
            chain_store,
            eth_adapters,
        }
    }
}

impl TriggersAdapterSelector<Chain> for EthereumAdapterSelector {
    fn triggers_adapter(
        &self,
        loc: &DeploymentLocator,
        capabilities: &<Chain as Blockchain>::NodeCapabilities,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Arc<dyn TriggersAdapterTrait<Chain>>, Error> {
        let logger = self
            .logger_factory
            .subgraph_logger(loc)
            .new(o!("component" => "BlockStream"));

        let ethrpc_metrics = Arc::new(SubgraphEthRpcMetrics::new(self.registry.clone(), &loc.hash));

        let adapter = TriggersAdapter {
            logger: logger.clone(),
            ethrpc_metrics,
            chain_client: self.client.cheap_clone(),
            chain_store: self.chain_store.cheap_clone(),
            unified_api_version,
            capabilities: *capabilities,
            eth_adapters: self.eth_adapters.cheap_clone(),
        };
        Ok(Arc::new(adapter))
    }
}

/// We need this so that the runner tests can use a `NoopRuntimeAdapter`
/// instead of the `RuntimeAdapter` from this crate to avoid needing
/// ethereum adapters
pub trait RuntimeAdapterBuilder: Send + Sync + 'static {
    fn build(
        &self,
        eth_adapters: Arc<EthereumNetworkAdapters>,
        call_cache: Arc<dyn EthereumCallCache>,
        chain_identifier: Arc<ChainIdentifier>,
    ) -> Arc<dyn RuntimeAdapterTrait<Chain>>;
}

pub struct EthereumRuntimeAdapterBuilder {}

impl RuntimeAdapterBuilder for EthereumRuntimeAdapterBuilder {
    fn build(
        &self,
        eth_adapters: Arc<EthereumNetworkAdapters>,
        call_cache: Arc<dyn EthereumCallCache>,
        chain_identifier: Arc<ChainIdentifier>,
    ) -> Arc<dyn RuntimeAdapterTrait<Chain>> {
        Arc::new(RuntimeAdapter {
            eth_adapters,
            call_cache,
            chain_identifier,
        })
    }
}

pub struct Chain {
    logger_factory: LoggerFactory,
    pub name: ChainName,
    node_id: NodeId,
    registry: Arc<MetricsRegistry>,
    client: Arc<ChainClient<Self>>,
    chain_store: Arc<dyn ChainStore>,
    call_cache: Arc<dyn EthereumCallCache>,
    chain_head_update_listener: Arc<dyn ChainHeadUpdateListener>,
    reorg_threshold: BlockNumber,
    polling_ingestor_interval: Duration,
    pub is_ingestible: bool,
    block_stream_builder: Arc<dyn BlockStreamBuilder<Self>>,
    block_refetcher: Arc<dyn BlockRefetcher<Self>>,
    adapter_selector: Arc<dyn TriggersAdapterSelector<Self>>,
    runtime_adapter_builder: Arc<dyn RuntimeAdapterBuilder>,
    eth_adapters: Arc<EthereumNetworkAdapters>,
}

impl std::fmt::Debug for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "chain: ethereum")
    }
}

impl Chain {
    /// Creates a new Ethereum [`Chain`].
    pub fn new(
        logger_factory: LoggerFactory,
        name: ChainName,
        node_id: NodeId,
        registry: Arc<MetricsRegistry>,
        chain_store: Arc<dyn ChainStore>,
        call_cache: Arc<dyn EthereumCallCache>,
        client: Arc<ChainClient<Self>>,
        chain_head_update_listener: Arc<dyn ChainHeadUpdateListener>,
        block_stream_builder: Arc<dyn BlockStreamBuilder<Self>>,
        block_refetcher: Arc<dyn BlockRefetcher<Self>>,
        adapter_selector: Arc<dyn TriggersAdapterSelector<Self>>,
        runtime_adapter_builder: Arc<dyn RuntimeAdapterBuilder>,
        eth_adapters: Arc<EthereumNetworkAdapters>,
        reorg_threshold: BlockNumber,
        polling_ingestor_interval: Duration,
        is_ingestible: bool,
    ) -> Self {
        Chain {
            logger_factory,
            name,
            node_id,
            registry,
            client,
            chain_store,
            call_cache,
            chain_head_update_listener,
            block_stream_builder,
            block_refetcher,
            adapter_selector,
            runtime_adapter_builder,
            eth_adapters,
            reorg_threshold,
            is_ingestible,
            polling_ingestor_interval,
        }
    }

    /// Returns a handler to this chain's [`EthereumCallCache`].
    pub fn call_cache(&self) -> Arc<dyn EthereumCallCache> {
        self.call_cache.clone()
    }

    // TODO: This is only used to build the block stream which could prolly
    // be moved to the chain itself and return a block stream future that the
    // caller can spawn.
    pub async fn cheapest_adapter(&self) -> Arc<EthereumAdapter> {
        let adapters = match self.client.as_ref() {
            ChainClient::Firehose(_) => panic!("no adapter with firehose"),
            ChainClient::Rpc(adapter) => adapter,
        };
        adapters.cheapest().await.unwrap()
    }
}

#[async_trait]
impl Blockchain for Chain {
    const KIND: BlockchainKind = BlockchainKind::Ethereum;
    const ALIASES: &'static [&'static str] = &["ethereum/contract"];

    type Client = EthereumNetworkAdapters;
    type Block = BlockFinality;

    type DataSource = DataSource;

    type UnresolvedDataSource = UnresolvedDataSource;

    type DataSourceTemplate = DataSourceTemplate;

    type UnresolvedDataSourceTemplate = UnresolvedDataSourceTemplate;

    type TriggerData = crate::trigger::EthereumTrigger;

    type MappingTrigger = crate::trigger::MappingTrigger;

    type TriggerFilter = crate::adapter::TriggerFilter;

    type NodeCapabilities = crate::capabilities::NodeCapabilities;

    type DecoderHook = crate::data_source::DecoderHook;

    fn triggers_adapter(
        &self,
        loc: &DeploymentLocator,
        capabilities: &Self::NodeCapabilities,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Arc<dyn TriggersAdapterTrait<Self>>, Error> {
        self.adapter_selector
            .triggers_adapter(loc, capabilities, unified_api_version)
    }

    async fn new_block_stream(
        &self,
        deployment: DeploymentLocator,
        store: impl DeploymentCursorTracker,
        start_blocks: Vec<BlockNumber>,
        source_subgraph_stores: Vec<Arc<dyn SourceableStore>>,
        filter: Arc<TriggerFilterWrapper<Self>>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Self>>, Error> {
        let current_ptr = store.block_ptr();

        if !filter.subgraph_filter.is_empty() {
            return self
                .block_stream_builder
                .build_subgraph_block_stream(
                    self,
                    deployment,
                    start_blocks,
                    source_subgraph_stores,
                    current_ptr,
                    filter,
                    unified_api_version,
                )
                .await;
        }

        match self.chain_client().as_ref() {
            ChainClient::Rpc(_) => {
                self.block_stream_builder
                    .build_polling(
                        self,
                        deployment,
                        start_blocks,
                        source_subgraph_stores,
                        current_ptr,
                        filter,
                        unified_api_version,
                    )
                    .await
            }
            ChainClient::Firehose(_) => {
                self.block_stream_builder
                    .build_firehose(
                        self,
                        deployment,
                        store.firehose_cursor(),
                        start_blocks,
                        current_ptr,
                        filter.chain_filter.clone(),
                        unified_api_version,
                    )
                    .await
            }
        }
    }

    fn chain_store(&self) -> Arc<dyn ChainStore> {
        self.chain_store.clone()
    }

    async fn block_pointer_from_number(
        &self,
        logger: &Logger,
        number: BlockNumber,
    ) -> Result<BlockPtr, IngestorError> {
        match self.client.as_ref() {
            ChainClient::Firehose(endpoints) => endpoints
                .endpoint()
                .await?
                .block_ptr_for_number::<HeaderOnlyBlock>(logger, number)
                .await
                .map_err(IngestorError::Unknown),
            ChainClient::Rpc(adapters) => {
                let adapter = adapters
                    .cheapest()
                    .await
                    .with_context(|| format!("no adapter for chain {}", self.name))?
                    .clone();

                adapter
                    .next_existing_ptr_to_number(logger, number)
                    .await
                    .map_err(From::from)
            }
        }
    }

    fn is_refetch_block_required(&self) -> bool {
        self.block_refetcher.required(self)
    }

    async fn refetch_firehose_block(
        &self,
        logger: &Logger,
        cursor: FirehoseCursor,
    ) -> Result<BlockFinality, Error> {
        self.block_refetcher.get_block(self, logger, cursor).await
    }

    fn runtime(&self) -> anyhow::Result<(Arc<dyn RuntimeAdapterTrait<Self>>, Self::DecoderHook)> {
        let call_cache = Arc::new(BufferedCallCache::new(self.call_cache.cheap_clone()));
        let chain_ident = self.chain_store.chain_identifier()?;

        let builder = self.runtime_adapter_builder.build(
            self.eth_adapters.cheap_clone(),
            call_cache.cheap_clone(),
            Arc::new(chain_ident.clone()),
        );
        let eth_call_gas = eth_call_gas(&chain_ident);

        let decoder_hook = crate::data_source::DecoderHook::new(
            self.eth_adapters.cheap_clone(),
            call_cache,
            eth_call_gas,
        );

        Ok((builder, decoder_hook))
    }

    fn chain_client(&self) -> Arc<ChainClient<Self>> {
        self.client.clone()
    }

    async fn block_ingestor(&self) -> anyhow::Result<Box<dyn BlockIngestor>> {
        let ingestor: Box<dyn BlockIngestor> = match self.chain_client().as_ref() {
            ChainClient::Firehose(_) => {
                let ingestor = FirehoseBlockIngestor::<HeaderOnlyBlock, Self>::new(
                    self.chain_store.cheap_clone(),
                    self.chain_client(),
                    self.logger_factory
                        .component_logger("EthereumFirehoseBlockIngestor", None),
                    self.name.clone(),
                );
                let ingestor = ingestor.with_transforms(vec![Transforms::EthereumHeaderOnly]);

                Box::new(ingestor)
            }
            ChainClient::Rpc(_) => {
                let logger = self
                    .logger_factory
                    .component_logger(
                        "EthereumPollingBlockIngestor",
                        Some(ComponentLoggerConfig {
                            elastic: Some(ElasticComponentLoggerConfig {
                                index: String::from("block-ingestor-logs"),
                            }),
                        }),
                    )
                    .new(o!());

                if !self.is_ingestible {
                    bail!(
                        "Not starting block ingestor (chain is defective), network_name {}",
                        &self.name
                    );
                }

                // The block ingestor must be configured to keep at least REORG_THRESHOLD ancestors,
                // because the json-rpc BlockStream expects blocks after the reorg threshold to be
                // present in the DB.
                Box::new(PollingBlockIngestor::new(
                    logger,
                    graph::env::ENV_VARS.reorg_threshold,
                    self.chain_client(),
                    self.chain_store().cheap_clone(),
                    self.polling_ingestor_interval,
                    self.name.clone(),
                )?)
            }
        };

        Ok(ingestor)
    }
}

/// This is used in `EthereumAdapter::triggers_in_block`, called when re-processing a block for
/// newly created data sources. This allows the re-processing to be reorg safe without having to
/// always fetch the full block data.
#[derive(Clone, Debug)]
pub enum BlockFinality {
    /// If a block is final, we only need the header and the triggers.
    Final(Arc<LightEthereumBlock>),

    // If a block may still be reorged, we need to work with more local data.
    NonFinal(EthereumBlockWithCalls),

    Ptr(Arc<ExtendedBlockPtr>),
}

impl Default for BlockFinality {
    fn default() -> Self {
        Self::Final(Arc::default())
    }
}

impl BlockFinality {
    pub(crate) fn light_block(&self) -> &Arc<LightEthereumBlock> {
        match self {
            BlockFinality::Final(block) => block,
            BlockFinality::NonFinal(block) => &block.ethereum_block.block,
            BlockFinality::Ptr(_) => unreachable!("light_block called on HeaderOnly"),
        }
    }
}

impl<'a> From<&'a BlockFinality> for BlockPtr {
    fn from(block: &'a BlockFinality) -> BlockPtr {
        match block {
            BlockFinality::Final(b) => BlockPtr::from(&**b),
            BlockFinality::NonFinal(b) => BlockPtr::from(&b.ethereum_block),
            BlockFinality::Ptr(b) => BlockPtr::new(b.hash.clone(), b.number),
        }
    }
}

impl Block for BlockFinality {
    fn ptr(&self) -> BlockPtr {
        match self {
            BlockFinality::Final(block) => block.block_ptr(),
            BlockFinality::NonFinal(block) => block.ethereum_block.block.block_ptr(),
            BlockFinality::Ptr(block) => BlockPtr::new(block.hash.clone(), block.number),
        }
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        match self {
            BlockFinality::Final(block) => block.parent_ptr(),
            BlockFinality::NonFinal(block) => block.ethereum_block.block.parent_ptr(),
            BlockFinality::Ptr(block) => {
                Some(BlockPtr::new(block.parent_hash.clone(), block.number - 1))
            }
        }
    }

    fn data(&self) -> Result<json::Value, json::Error> {
        // The serialization here very delicately depends on how the
        // `ChainStore`'s `blocks` and `ancestor_block` return the data we
        // store here. This should be fixed in a better way to ensure we
        // serialize/deserialize appropriately.
        //
        // Commit #d62e9846 inadvertently introduced a variation in how
        // chain stores store ethereum blocks in that they now sometimes
        // store an `EthereumBlock` that has a `block` field with a
        // `LightEthereumBlock`, and sometimes they just store the
        // `LightEthereumBlock` directly. That causes issues because the
        // code reading from the chain store always expects the JSON data to
        // have the form of an `EthereumBlock`.
        //
        // Even though this bug is fixed now and we always use the
        // serialization of an `EthereumBlock`, there are still chain stores
        // in existence that used the old serialization form, and we need to
        // deal with that when deserializing
        //
        // see also 7736e440-4c6b-11ec-8c4d-b42e99f52061
        match self {
            BlockFinality::Final(block) => {
                let eth_block = EthereumBlock {
                    block: block.clone(),
                    transaction_receipts: vec![],
                };
                json::to_value(eth_block)
            }
            BlockFinality::NonFinal(block) => json::to_value(&block.ethereum_block),
            BlockFinality::Ptr(_) => Ok(json::Value::Null),
        }
    }

    fn timestamp(&self) -> BlockTime {
        match self {
            BlockFinality::Final(block) => {
                let ts = i64::try_from(block.timestamp.as_u64()).unwrap();
                BlockTime::since_epoch(ts, 0)
            }
            BlockFinality::NonFinal(block) => {
                let ts = i64::try_from(block.ethereum_block.block.timestamp.as_u64()).unwrap();
                BlockTime::since_epoch(ts, 0)
            }
            BlockFinality::Ptr(block) => block.timestamp,
        }
    }
}

pub struct DummyDataSourceTemplate;

pub struct TriggersAdapter {
    logger: Logger,
    ethrpc_metrics: Arc<SubgraphEthRpcMetrics>,
    chain_store: Arc<dyn ChainStore>,
    chain_client: Arc<ChainClient<Chain>>,
    capabilities: NodeCapabilities,
    unified_api_version: UnifiedMappingApiVersion,
    eth_adapters: Arc<EthereumNetworkAdapters>,
}

/// Fetches blocks from the cache based on block numbers, excluding duplicates
/// (i.e., multiple blocks for the same number), and identifying missing blocks that
/// need to be fetched via RPC/Firehose. Returns a tuple of the found blocks and the missing block numbers.
async fn fetch_unique_blocks_from_cache(
    logger: &Logger,
    chain_store: Arc<dyn ChainStore>,
    block_numbers: BTreeSet<BlockNumber>,
) -> (Vec<Arc<ExtendedBlockPtr>>, Vec<i32>) {
    // Load blocks from the cache
    let blocks_map = chain_store
        .cheap_clone()
        .block_ptrs_by_numbers(block_numbers.iter().map(|&b| b.into()).collect::<Vec<_>>())
        .await
        .map_err(|e| {
            error!(logger, "Error accessing block cache {}", e);
            e
        })
        .unwrap_or_default();

    // Collect blocks and filter out ones with multiple entries
    let blocks: Vec<Arc<ExtendedBlockPtr>> = blocks_map
        .into_iter()
        .filter_map(|(_, values)| {
            if values.len() == 1 {
                Some(Arc::new(values[0].clone()))
            } else {
                None
            }
        })
        .collect();

    // Identify missing blocks
    let missing_blocks: Vec<i32> = block_numbers
        .into_iter()
        .filter(|&number| !blocks.iter().any(|block| block.block_number() == number))
        .collect();

    if !missing_blocks.is_empty() {
        debug!(
            logger,
            "Loading {} block(s) not in the block cache",
            missing_blocks.len()
        );
        trace!(logger, "Missing blocks {:?}", missing_blocks.len());
    }

    (blocks, missing_blocks)
}

// This is used to load blocks from the RPC.
async fn load_blocks_with_rpc(
    logger: &Logger,
    adapter: Arc<EthereumAdapter>,
    chain_store: Arc<dyn ChainStore>,
    block_numbers: BTreeSet<BlockNumber>,
) -> Result<Vec<BlockFinality>> {
    let logger_clone = logger.clone();
    load_blocks(
        logger,
        chain_store,
        block_numbers,
        |missing_numbers| async move {
            adapter
                .load_block_ptrs_by_numbers_rpc(logger_clone, missing_numbers)
                .try_collect()
                .await
        },
    )
    .await
}

/// Fetches blocks by their numbers, first attempting to load from cache.
/// Missing blocks are retrieved from an external source, with all blocks sorted and converted to `BlockFinality` format.
async fn load_blocks<F, Fut>(
    logger: &Logger,
    chain_store: Arc<dyn ChainStore>,
    block_numbers: BTreeSet<BlockNumber>,
    fetch_missing: F,
) -> Result<Vec<BlockFinality>>
where
    F: FnOnce(Vec<BlockNumber>) -> Fut,
    Fut: Future<Output = Result<Vec<Arc<ExtendedBlockPtr>>>>,
{
    // Fetch cached blocks and identify missing ones
    let (mut cached_blocks, missing_block_numbers) =
        fetch_unique_blocks_from_cache(logger, chain_store, block_numbers).await;

    // Fetch missing blocks if any
    if !missing_block_numbers.is_empty() {
        let missing_blocks = fetch_missing(missing_block_numbers).await?;
        cached_blocks.extend(missing_blocks);
        cached_blocks.sort_by_key(|block| block.number);
    }

    Ok(cached_blocks.into_iter().map(BlockFinality::Ptr).collect())
}

#[async_trait]
impl TriggersAdapterTrait<Chain> for TriggersAdapter {
    async fn scan_triggers(
        &self,
        from: BlockNumber,
        to: BlockNumber,
        filter: &TriggerFilter,
    ) -> Result<(Vec<BlockWithTriggers<Chain>>, BlockNumber), Error> {
        blocks_with_triggers(
            self.chain_client
                .rpc()?
                .cheapest_with(&self.capabilities)
                .await?,
            self.logger.clone(),
            self.chain_store.clone(),
            self.ethrpc_metrics.clone(),
            from,
            to,
            filter,
            self.unified_api_version.clone(),
        )
        .await
    }

    async fn load_block_ptrs_by_numbers(
        &self,
        logger: Logger,
        block_numbers: BTreeSet<BlockNumber>,
    ) -> Result<Vec<BlockFinality>> {
        match &*self.chain_client {
            ChainClient::Firehose(endpoints) => {
                // If the force_rpc_for_block_ptrs flag is set, we will use the RPC to load the blocks
                // even if the firehose is available. If no adapter is available, we will log an error.
                // And then fallback to the firehose.
                if ENV_VARS.force_rpc_for_block_ptrs {
                    trace!(
                        logger,
                        "Loading blocks from RPC (force_rpc_for_block_ptrs is set)";
                        "block_numbers" => format!("{:?}", block_numbers)
                    );
                    match self.eth_adapters.cheapest_with(&self.capabilities).await {
                        Ok(adapter) => {
                            match load_blocks_with_rpc(
                                &logger,
                                adapter,
                                self.chain_store.clone(),
                                block_numbers.clone(),
                            )
                            .await
                            {
                                Ok(blocks) => return Ok(blocks),
                                Err(e) => {
                                    warn!(logger, "Error loading blocks from RPC: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            warn!(logger, "Error getting cheapest adapter: {}", e);
                        }
                    }
                }

                trace!(
                    logger,
                    "Loading blocks from firehose";
                    "block_numbers" => format!("{:?}", block_numbers)
                );

                let endpoint = endpoints.endpoint().await?;
                let chain_store = self.chain_store.clone();
                let logger_clone = logger.clone();

                load_blocks(
                    &logger,
                    chain_store,
                    block_numbers,
                    |missing_numbers| async move {
                        let blocks = endpoint
                            .load_blocks_by_numbers::<codec::Block>(
                                missing_numbers.iter().map(|&n| n as u64).collect(),
                                &logger_clone,
                            )
                            .await?
                            .into_iter()
                            .map(|block| {
                                Arc::new(ExtendedBlockPtr {
                                    hash: block.hash(),
                                    number: block.number(),
                                    parent_hash: block.parent_hash().unwrap_or_default(),
                                    timestamp: block.timestamp(),
                                })
                            })
                            .collect::<Vec<_>>();
                        Ok(blocks)
                    },
                )
                .await
            }

            ChainClient::Rpc(eth_adapters) => {
                trace!(
                    logger,
                    "Loading blocks from RPC";
                    "block_numbers" => format!("{:?}", block_numbers)
                );

                let adapter = eth_adapters.cheapest_with(&self.capabilities).await?;
                load_blocks_with_rpc(&logger, adapter, self.chain_store.clone(), block_numbers)
                    .await
            }
        }
    }

    async fn chain_head_ptr(&self) -> Result<Option<BlockPtr>, Error> {
        let chain_store = self.chain_store.clone();
        chain_store.chain_head_ptr().await
    }

    async fn triggers_in_block(
        &self,
        logger: &Logger,
        block: BlockFinality,
        filter: &TriggerFilter,
    ) -> Result<BlockWithTriggers<Chain>, Error> {
        let block = get_calls(
            &self.chain_client,
            logger.clone(),
            self.ethrpc_metrics.clone(),
            &self.capabilities,
            filter.requires_traces(),
            block,
        )
        .await?;

        match &block {
            BlockFinality::Final(_) => {
                let adapter = self
                    .chain_client
                    .rpc()?
                    .cheapest_with(&self.capabilities)
                    .await?;
                let block_number = block.number() as BlockNumber;
                let (blocks, _) = blocks_with_triggers(
                    adapter,
                    logger.clone(),
                    self.chain_store.clone(),
                    self.ethrpc_metrics.clone(),
                    block_number,
                    block_number,
                    filter,
                    self.unified_api_version.clone(),
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
                triggers.append(&mut parse_call_triggers(&filter.call, full_block)?);
                triggers.append(&mut parse_block_triggers(&filter.block, full_block));
                Ok(BlockWithTriggers::new(block, triggers, logger))
            }
            BlockFinality::Ptr(_) => unreachable!("triggers_in_block called on HeaderOnly"),
        }
    }

    async fn is_on_main_chain(&self, ptr: BlockPtr) -> Result<bool, Error> {
        match &*self.chain_client {
            ChainClient::Firehose(endpoints) => {
                let endpoint = endpoints.endpoint().await?;
                let block = endpoint
                    .get_block_by_number_with_retry::<codec::Block>(ptr.number as u64, &self.logger)
                    .await
                    .context(format!(
                        "Failed to fetch block {} from firehose",
                        ptr.number
                    ))?;
                Ok(block.hash() == ptr.hash)
            }
            ChainClient::Rpc(adapter) => {
                let adapter = adapter
                    .cheapest()
                    .await
                    .ok_or_else(|| anyhow!("unable to get adapter for is_on_main_chain"))?;

                adapter.is_on_main_chain(&self.logger, ptr).await
            }
        }
    }

    async fn ancestor_block(
        &self,
        ptr: BlockPtr,
        offset: BlockNumber,
        root: Option<BlockHash>,
    ) -> Result<Option<BlockFinality>, Error> {
        let block: Option<EthereumBlock> = self
            .chain_store
            .cheap_clone()
            .ancestor_block(ptr, offset, root)
            .await?
            .map(|x| x.0)
            .map(json::from_value)
            .transpose()?;
        Ok(block.map(|block| {
            BlockFinality::NonFinal(EthereumBlockWithCalls {
                ethereum_block: block,
                calls: None,
            })
        }))
    }

    async fn parent_ptr(&self, block: &BlockPtr) -> Result<Option<BlockPtr>, Error> {
        use graph::futures01::stream::Stream;
        use graph::prelude::LightEthereumBlockExt;

        let block = match self.chain_client.as_ref() {
            ChainClient::Firehose(endpoints) => {
                let chain_store = self.chain_store.cheap_clone();
                // First try to get the block from the store
                if let Ok(blocks) = chain_store.blocks(vec![block.hash.clone()]).await {
                    if let Some(block) = blocks.first() {
                        if let Ok(block) = json::from_value::<LightEthereumBlock>(block.clone()) {
                            return Ok(block.parent_ptr());
                        }
                    }
                }

                // If not in store, fetch from Firehose
                let endpoint = endpoints.endpoint().await?;
                let logger = self.logger.clone();
                let retry_log_message =
                    format!("get_block_by_ptr for block {} with firehose", block);
                let block = block.clone();

                retry(retry_log_message, &logger)
                    .limit(ENV_VARS.request_retries)
                    .timeout_secs(ENV_VARS.json_rpc_timeout.as_secs())
                    .run(move || {
                        let endpoint = endpoint.cheap_clone();
                        let logger = logger.cheap_clone();
                        let block = block.clone();
                        async move {
                            endpoint
                                .get_block_by_ptr::<codec::Block>(&block, &logger)
                                .await
                                .context(format!(
                                    "Failed to fetch block by ptr {} from firehose",
                                    block
                                ))
                        }
                    })
                    .await?
                    .parent_ptr()
            }
            ChainClient::Rpc(adapters) => {
                let blocks = adapters
                    .cheapest_with(&self.capabilities)
                    .await?
                    .load_blocks(
                        self.logger.cheap_clone(),
                        self.chain_store.cheap_clone(),
                        HashSet::from_iter(Some(block.hash_as_h256())),
                    )
                    .await
                    .collect()
                    .compat()
                    .await?;
                assert_eq!(blocks.len(), 1);

                blocks[0].parent_ptr()
            }
        };

        Ok(block)
    }
}

pub struct FirehoseMapper {
    adapter: Arc<dyn TriggersAdapterTrait<Chain>>,
    filter: Arc<TriggerFilter>,
}

#[async_trait]
impl BlockStreamMapper<Chain> for FirehoseMapper {
    fn decode_block(
        &self,
        output: Option<&[u8]>,
    ) -> Result<Option<BlockFinality>, BlockStreamError> {
        let block = match output {
            Some(block) => codec::Block::decode(block)?,
            None => Err(anyhow::anyhow!(
                "ethereum mapper is expected to always have a block"
            ))?,
        };

        // See comment(437a9f17-67cc-478f-80a3-804fe554b227) ethereum_block.calls is always Some even if calls
        // is empty
        let ethereum_block: EthereumBlockWithCalls = (&block).try_into()?;

        Ok(Some(BlockFinality::NonFinal(ethereum_block)))
    }

    async fn block_with_triggers(
        &self,
        logger: &Logger,
        block: BlockFinality,
    ) -> Result<BlockWithTriggers<Chain>, BlockStreamError> {
        self.adapter
            .triggers_in_block(logger, block, &self.filter)
            .await
            .map_err(BlockStreamError::from)
    }

    async fn handle_substreams_block(
        &self,
        _logger: &Logger,
        _clock: Clock,
        _cursor: FirehoseCursor,
        _block: Vec<u8>,
    ) -> Result<BlockStreamEvent<Chain>, BlockStreamError> {
        unimplemented!()
    }
}

#[async_trait]
impl FirehoseMapperTrait<Chain> for FirehoseMapper {
    fn trigger_filter(&self) -> &TriggerFilter {
        self.filter.as_ref()
    }

    async fn to_block_stream_event(
        &self,
        logger: &Logger,
        response: &firehose::Response,
    ) -> Result<BlockStreamEvent<Chain>, FirehoseError> {
        let step = ForkStep::try_from(response.step).unwrap_or_else(|_| {
            panic!(
                "unknown step i32 value {}, maybe you forgot update & re-regenerate the protobuf definitions?",
                response.step
            )
        });
        let any_block = response
            .block
            .as_ref()
            .expect("block payload information should always be present");

        // Right now, this is done in all cases but in reality, with how the BlockStreamEvent::Revert
        // is defined right now, only block hash and block number is necessary. However, this information
        // is not part of the actual firehose::Response payload. As such, we need to decode the full
        // block which is useless.
        //
        // Check about adding basic information about the block in the firehose::Response or maybe
        // define a slimmed down stuct that would decode only a few fields and ignore all the rest.
        let block = codec::Block::decode(any_block.value.as_ref())?;

        use firehose::ForkStep::*;
        match step {
            StepNew => {
                // unwrap: Input cannot be None so output will be error or block.
                let block = self.decode_block(Some(any_block.value.as_ref()))?.unwrap();
                let block_with_triggers = self.block_with_triggers(logger, block).await?;

                Ok(BlockStreamEvent::ProcessBlock(
                    block_with_triggers,
                    FirehoseCursor::from(response.cursor.clone()),
                ))
            }

            StepUndo => {
                let parent_ptr = block
                    .parent_ptr()
                    .expect("Genesis block should never be reverted");

                Ok(BlockStreamEvent::Revert(
                    parent_ptr,
                    FirehoseCursor::from(response.cursor.clone()),
                ))
            }

            StepFinal => {
                unreachable!("irreversible step is not handled and should not be requested in the Firehose request")
            }

            StepUnset => {
                unreachable!("unknown step should not happen in the Firehose response")
            }
        }
    }

    async fn block_ptr_for_number(
        &self,
        logger: &Logger,
        endpoint: &Arc<FirehoseEndpoint>,
        number: BlockNumber,
    ) -> Result<BlockPtr, Error> {
        endpoint
            .block_ptr_for_number::<codec::HeaderOnlyBlock>(logger, number)
            .await
    }

    async fn final_block_ptr_for(
        &self,
        logger: &Logger,
        endpoint: &Arc<FirehoseEndpoint>,
        block: &BlockFinality,
    ) -> Result<BlockPtr, Error> {
        // Firehose for Ethereum has an hard-coded confirmations for finality sets to 200 block
        // behind the current block. The magic value 200 here comes from this hard-coded Firehose
        // value.
        let final_block_number = match block.number() {
            x if x >= 200 => x - 200,
            _ => 0,
        };

        self.block_ptr_for_number(logger, endpoint, final_block_number)
            .await
    }
}

#[cfg(test)]
mod tests {
    use graph::blockchain::mock::MockChainStore;
    use graph::{slog, tokio};

    use super::*;
    use std::sync::Arc;

    // Helper function to create test blocks
    fn create_test_block(number: BlockNumber, hash: &str) -> ExtendedBlockPtr {
        let hash = BlockHash(hash.as_bytes().to_vec().into_boxed_slice());
        let ptr = BlockPtr::new(hash.clone(), number);
        ExtendedBlockPtr {
            hash,
            number,
            parent_hash: BlockHash(vec![0; 32].into_boxed_slice()),
            timestamp: BlockTime::for_test(&ptr),
        }
    }

    #[tokio::test]
    async fn test_fetch_unique_blocks_single_block() {
        let logger = Logger::root(slog::Discard, o!());
        let mut chain_store = MockChainStore::default();

        // Add a single block
        let block = create_test_block(1, "block1");
        chain_store.blocks.insert(1, vec![block.clone()]);

        let block_numbers: BTreeSet<_> = vec![1].into_iter().collect();

        let (blocks, missing) =
            fetch_unique_blocks_from_cache(&logger, Arc::new(chain_store), block_numbers).await;

        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].number, 1);
        assert!(missing.is_empty());
    }

    #[tokio::test]
    async fn test_fetch_unique_blocks_duplicate_blocks() {
        let logger = Logger::root(slog::Discard, o!());
        let mut chain_store = MockChainStore::default();

        // Add multiple blocks for the same number
        let block1 = create_test_block(1, "block1a");
        let block2 = create_test_block(1, "block1b");
        chain_store
            .blocks
            .insert(1, vec![block1.clone(), block2.clone()]);

        let block_numbers: BTreeSet<_> = vec![1].into_iter().collect();

        let (blocks, missing) =
            fetch_unique_blocks_from_cache(&logger, Arc::new(chain_store), block_numbers).await;

        // Should filter out the duplicate block
        assert!(blocks.is_empty());
        assert_eq!(missing, vec![1]);
        assert_eq!(missing[0], 1);
    }

    #[tokio::test]
    async fn test_fetch_unique_blocks_missing_blocks() {
        let logger = Logger::root(slog::Discard, o!());
        let mut chain_store = MockChainStore::default();

        // Add block number 1 but not 2
        let block = create_test_block(1, "block1");
        chain_store.blocks.insert(1, vec![block.clone()]);

        let block_numbers: BTreeSet<_> = vec![1, 2].into_iter().collect();

        let (blocks, missing) =
            fetch_unique_blocks_from_cache(&logger, Arc::new(chain_store), block_numbers).await;

        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].number, 1);
        assert_eq!(missing, vec![2]);
    }

    #[tokio::test]
    async fn test_fetch_unique_blocks_multiple_valid_blocks() {
        let logger = Logger::root(slog::Discard, o!());
        let mut chain_store = MockChainStore::default();

        // Add multiple valid blocks
        let block1 = create_test_block(1, "block1");
        let block2 = create_test_block(2, "block2");
        chain_store.blocks.insert(1, vec![block1.clone()]);
        chain_store.blocks.insert(2, vec![block2.clone()]);

        let block_numbers: BTreeSet<_> = vec![1, 2].into_iter().collect();

        let (blocks, missing) =
            fetch_unique_blocks_from_cache(&logger, Arc::new(chain_store), block_numbers).await;

        assert_eq!(blocks.len(), 2);
        assert!(blocks.iter().any(|b| b.number == 1));
        assert!(blocks.iter().any(|b| b.number == 2));
        assert!(missing.is_empty());
    }

    #[tokio::test]
    async fn test_fetch_unique_blocks_mixed_scenario() {
        let logger = Logger::root(slog::Discard, o!());
        let mut chain_store = MockChainStore::default();

        // Add a mix of scenarios:
        // - Block 1: Single valid block
        // - Block 2: Multiple blocks (duplicate)
        // - Block 3: Missing
        let block1 = create_test_block(1, "block1");
        let block2a = create_test_block(2, "block2a");
        let block2b = create_test_block(2, "block2b");

        chain_store.blocks.insert(1, vec![block1.clone()]);
        chain_store
            .blocks
            .insert(2, vec![block2a.clone(), block2b.clone()]);

        let block_numbers: BTreeSet<_> = vec![1, 2, 3].into_iter().collect();

        let (blocks, missing) =
            fetch_unique_blocks_from_cache(&logger, Arc::new(chain_store), block_numbers).await;

        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].number, 1);
        assert_eq!(missing.len(), 2);
        assert!(missing.contains(&2));
        assert!(missing.contains(&3));
    }
}
