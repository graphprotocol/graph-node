use anyhow::{Context, Error};
use graph::blockchain::BlockchainKind;
use graph::data::subgraph::UnifiedMappingApiVersion;
use graph::firehose::endpoints::FirehoseNetworkEndpoints;
use graph::prelude::{
    EthereumBlock, EthereumCallCache, LightEthereumBlock, LightEthereumBlockExt, StopwatchMetrics,
};
use graph::slog::debug;
use graph::{
    blockchain::{
        block_stream::{
            BlockStreamEvent, BlockStreamMetrics, BlockWithTriggers, FirehoseError,
            FirehoseMapper as FirehoseMapperTrait, TriggersAdapter as TriggersAdapterTrait,
        },
        firehose_block_stream::FirehoseBlockStream,
        polling_block_stream::PollingBlockStream,
        Block, BlockHash, BlockPtr, Blockchain, ChainHeadUpdateListener,
        IngestorAdapter as IngestorAdapterTrait, IngestorError, TriggerFilter as _,
    },
    cheap_clone::CheapClone,
    components::store::DeploymentLocator,
    firehose::bstream,
    log::factory::{ComponentLoggerConfig, ElasticComponentLoggerConfig},
    prelude::{
        async_trait, error, lazy_static, o, serde_json as json, web3::types::H256, BlockNumber,
        ChainStore, EthereumBlockWithCalls, Future01CompatExt, Logger, LoggerFactory,
        MetricsRegistry, NodeId, SubgraphStore,
    },
};
use prost::Message;
use std::collections::HashSet;
use std::iter::FromIterator;
use std::sync::Arc;

use crate::data_source::DataSourceTemplate;
use crate::data_source::UnresolvedDataSourceTemplate;
use crate::RuntimeAdapter;
use crate::{
    adapter::EthereumAdapter as _,
    codec,
    data_source::{DataSource, UnresolvedDataSource},
    ethereum_adapter::{
        blocks_with_triggers, get_calls, parse_block_triggers, parse_call_triggers,
        parse_log_triggers,
    },
    SubgraphEthRpcMetrics, TriggerFilter,
};
use crate::{network::EthereumNetworkAdapters, EthereumAdapter};
use graph::blockchain::block_stream::{BlockStream, FirehoseCursor};

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

/// Celo Mainnet: 42220, Testnet Alfajores: 44787, Testnet Baklava: 62320
const CELO_CHAIN_IDS: [u64; 3] = [42220, 44787, 62320];

pub struct Chain {
    logger_factory: LoggerFactory,
    name: String,
    node_id: NodeId,
    registry: Arc<dyn MetricsRegistry>,
    firehose_endpoints: Arc<FirehoseNetworkEndpoints>,
    eth_adapters: Arc<EthereumNetworkAdapters>,
    ancestor_count: BlockNumber,
    chain_store: Arc<dyn ChainStore>,
    call_cache: Arc<dyn EthereumCallCache>,
    subgraph_store: Arc<dyn SubgraphStore>,
    chain_head_update_listener: Arc<dyn ChainHeadUpdateListener>,
    reorg_threshold: BlockNumber,
    pub is_ingestible: bool,
}

impl std::fmt::Debug for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "chain: ethereum")
    }
}

impl Chain {
    pub fn new(
        logger_factory: LoggerFactory,
        name: String,
        node_id: NodeId,
        registry: Arc<dyn MetricsRegistry>,
        chain_store: Arc<dyn ChainStore>,
        call_cache: Arc<dyn EthereumCallCache>,
        subgraph_store: Arc<dyn SubgraphStore>,
        firehose_endpoints: FirehoseNetworkEndpoints,
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
            firehose_endpoints: Arc::new(firehose_endpoints),
            eth_adapters: Arc::new(eth_adapters),
            ancestor_count,
            chain_store,
            call_cache,
            subgraph_store,
            chain_head_update_listener,
            reorg_threshold,
            is_ingestible,
        }
    }

    async fn new_polling_block_stream(
        &self,
        deployment: DeploymentLocator,
        start_blocks: Vec<BlockNumber>,
        adapter: Arc<TriggersAdapter>,
        filter: Arc<TriggerFilter>,
        metrics: Arc<BlockStreamMetrics>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Self>>, Error> {
        let logger = self
            .logger_factory
            .subgraph_logger(&deployment)
            .new(o!("component" => "BlockStream"));
        let chain_store = self.chain_store().clone();
        let writable = self
            .subgraph_store
            .cheap_clone()
            .writable(logger.clone(), deployment.id)
            .await
            .with_context(|| format!("no store for deployment `{}`", deployment.hash))?;
        let chain_head_update_stream = self
            .chain_head_update_listener
            .subscribe(self.name.clone(), logger.clone());

        // Special case: Detect Celo and set the threshold to 0, so that eth_getLogs is always used.
        // This is ok because Celo blocks are always final. And we _need_ to do this because
        // some events appear only in eth_getLogs but not in transaction receipts.
        // See also ca0edc58-0ec5-4c89-a7dd-2241797f5e50.
        let chain_id = self.eth_adapters.cheapest().unwrap().chain_id().await?;
        let reorg_threshold = match CELO_CHAIN_IDS.contains(&chain_id) {
            false => self.reorg_threshold,
            true => 0,
        };

        let start_block = writable.block_ptr()?;

        Ok(Box::new(PollingBlockStream::new(
            writable,
            chain_store,
            chain_head_update_stream,
            adapter,
            self.node_id.clone(),
            deployment.hash,
            filter,
            start_blocks,
            reorg_threshold,
            logger,
            metrics,
            *MAX_BLOCK_RANGE_SIZE,
            *TARGET_TRIGGERS_PER_BLOCK_RANGE,
            unified_api_version,
            start_block,
        )))
    }

    async fn new_firehose_block_stream(
        &self,
        deployment: DeploymentLocator,
        start_blocks: Vec<BlockNumber>,
        adapter: Arc<TriggersAdapter>,
        filter: Arc<TriggerFilter>,
    ) -> Result<Box<dyn BlockStream<Self>>, Error> {
        let firehose_endpoint = match self.firehose_endpoints.random() {
            Some(e) => e.clone(),
            None => return Err(anyhow::format_err!("no firehose endpoint available",)),
        };

        let logger = self
            .logger_factory
            .subgraph_logger(&deployment)
            .new(o!("component" => "FirehoseBlockStream"));

        let firehose_mapper = Arc::new(FirehoseMapper {});
        let firehose_cursor = self
            .subgraph_store
            .cheap_clone()
            .writable(logger.clone(), deployment.id)
            .await?
            .block_cursor()?;

        Ok(Box::new(FirehoseBlockStream::new(
            firehose_endpoint,
            firehose_cursor,
            firehose_mapper,
            adapter,
            filter,
            start_blocks,
            logger,
        )))
    }
}

#[async_trait]
impl Blockchain for Chain {
    const KIND: BlockchainKind = BlockchainKind::Ethereum;

    type Block = BlockFinality;

    type DataSource = DataSource;

    type UnresolvedDataSource = UnresolvedDataSource;

    type DataSourceTemplate = DataSourceTemplate;

    type UnresolvedDataSourceTemplate = UnresolvedDataSourceTemplate;

    type TriggersAdapter = TriggersAdapter;

    type TriggerData = crate::trigger::EthereumTrigger;

    type MappingTrigger = crate::trigger::MappingTrigger;

    type TriggerFilter = crate::adapter::TriggerFilter;

    type NodeCapabilities = crate::capabilities::NodeCapabilities;

    type IngestorAdapter = IngestorAdapter;

    type RuntimeAdapter = RuntimeAdapter;

    fn triggers_adapter(
        &self,
        loc: &DeploymentLocator,
        capabilities: &Self::NodeCapabilities,
        unified_api_version: UnifiedMappingApiVersion,
        stopwatch_metrics: StopwatchMetrics,
    ) -> Result<Arc<Self::TriggersAdapter>, Error> {
        let logger = self
            .logger_factory
            .subgraph_logger(&loc)
            .new(o!("component" => "BlockStream"));

        let eth_adapter = if capabilities.traces && self.firehose_endpoints.len() > 0 {
            debug!(logger, "Removing 'traces' capability requirement for adapter as FirehoseBlockStream will provide the traces");
            let adjusted_capabilities = crate::capabilities::NodeCapabilities {
                archive: capabilities.archive,
                traces: false,
            };

            self.eth_adapters
                .cheapest_with(&adjusted_capabilities)?
                .clone()
        } else {
            self.eth_adapters.cheapest_with(capabilities)?.clone()
        };

        let ethrpc_metrics = Arc::new(SubgraphEthRpcMetrics::new(self.registry.clone(), &loc.hash));

        let adapter = TriggersAdapter {
            logger,
            ethrpc_metrics,
            eth_adapter,
            stopwatch_metrics,
            chain_store: self.chain_store.cheap_clone(),
            unified_api_version,
        };
        Ok(Arc::new(adapter))
    }

    async fn new_block_stream(
        &self,
        deployment: DeploymentLocator,
        start_blocks: Vec<BlockNumber>,
        filter: Arc<TriggerFilter>,
        metrics: Arc<BlockStreamMetrics>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Self>>, Error> {
        let requirements = filter.node_capabilities();
        let adapter = self
            .triggers_adapter(
                &deployment,
                &requirements,
                unified_api_version.clone(),
                metrics.stopwatch.clone(),
            )
            .expect(&format!(
                "no adapter for network {} with capabilities {}",
                self.name, requirements
            ));

        if self.firehose_endpoints.len() > 0 {
            self.new_firehose_block_stream(deployment, start_blocks, adapter, filter)
                .await
        } else {
            self.new_polling_block_stream(
                deployment,
                start_blocks,
                adapter,
                filter,
                metrics,
                unified_api_version,
            )
            .await
        }
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

    async fn block_pointer_from_number(
        &self,
        logger: &Logger,
        number: BlockNumber,
    ) -> Result<BlockPtr, IngestorError> {
        let eth_adapter = self
            .eth_adapters
            .cheapest()
            .with_context(|| format!("no adapter for chain {}", self.name))?
            .clone();
        eth_adapter
            .block_pointer_from_number(logger, number)
            .compat()
            .await
    }

    fn runtime_adapter(&self) -> Arc<Self::RuntimeAdapter> {
        Arc::new(RuntimeAdapter {
            eth_adapters: self.eth_adapters.cheap_clone(),
            call_cache: self.call_cache.cheap_clone(),
        })
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
}

impl BlockFinality {
    pub(crate) fn light_block(&self) -> Arc<LightEthereumBlock> {
        match self {
            BlockFinality::Final(block) => block.cheap_clone(),
            BlockFinality::NonFinal(block) => block.ethereum_block.block.cheap_clone(),
        }
    }
}

impl<'a> From<&'a BlockFinality> for BlockPtr {
    fn from(block: &'a BlockFinality) -> BlockPtr {
        match block {
            BlockFinality::Final(b) => BlockPtr::from(&**b),
            BlockFinality::NonFinal(b) => BlockPtr::from(&b.ethereum_block),
        }
    }
}

impl Block for BlockFinality {
    fn ptr(&self) -> BlockPtr {
        match self {
            BlockFinality::Final(block) => block.block_ptr(),
            BlockFinality::NonFinal(block) => block.ethereum_block.block.block_ptr(),
        }
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        match self {
            BlockFinality::Final(block) => block.parent_ptr(),
            BlockFinality::NonFinal(block) => block.ethereum_block.block.parent_ptr(),
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
        }
    }
}

pub struct DummyDataSourceTemplate;

pub struct TriggersAdapter {
    logger: Logger,
    ethrpc_metrics: Arc<SubgraphEthRpcMetrics>,
    stopwatch_metrics: StopwatchMetrics,
    chain_store: Arc<dyn ChainStore>,
    eth_adapter: Arc<EthereumAdapter>,
    unified_api_version: UnifiedMappingApiVersion,
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
            self.stopwatch_metrics.clone(),
            from,
            to,
            filter,
            self.unified_api_version.clone(),
        )
        .await
    }

    async fn triggers_in_block(
        &self,
        logger: &Logger,
        block: BlockFinality,
        filter: &TriggerFilter,
    ) -> Result<BlockWithTriggers<Chain>, Error> {
        let block = get_calls(
            self.eth_adapter.as_ref(),
            logger.clone(),
            self.ethrpc_metrics.clone(),
            filter.requires_traces(),
            block,
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
                    self.stopwatch_metrics.clone(),
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
                triggers.append(&mut parse_call_triggers(&filter.call, &full_block)?);
                triggers.append(&mut parse_block_triggers(filter.block.clone(), &full_block));
                Ok(BlockWithTriggers::new(block, triggers))
            }
        }
    }

    async fn is_on_main_chain(&self, ptr: BlockPtr) -> Result<bool, Error> {
        self.eth_adapter
            .is_on_main_chain(&self.logger, ptr.clone())
            .await
    }

    fn ancestor_block(
        &self,
        ptr: BlockPtr,
        offset: BlockNumber,
    ) -> Result<Option<BlockFinality>, Error> {
        let block: Option<EthereumBlock> = self
            .chain_store
            .ancestor_block(ptr, offset)?
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

        Ok(blocks[0].parent_ptr())
    }
}

pub struct FirehoseMapper {}

#[async_trait]
impl FirehoseMapperTrait<Chain> for FirehoseMapper {
    async fn to_block_stream_event(
        &self,
        logger: &Logger,
        response: &bstream::BlockResponseV2,
        adapter: &TriggersAdapter,
        filter: &TriggerFilter,
    ) -> Result<BlockStreamEvent<Chain>, FirehoseError> {
        let step = bstream::ForkStep::from_i32(response.step).unwrap_or_else(|| {
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
        // is not part of the actual bstream::BlockResponseV2 payload. As such, we need to decode the full
        // block which is useless.
        //
        // Check about adding basic information about the block in the bstream::BlockResponseV2 or maybe
        // define a slimmed down stuct that would decode only a few fields and ignore all the rest.
        let block = codec::Block::decode(any_block.value.as_ref())?;

        match step {
            bstream::ForkStep::StepNew => {
                let ethereum_block: EthereumBlockWithCalls = (&block).into();
                let block_with_triggers = adapter
                    .triggers_in_block(logger, BlockFinality::NonFinal(ethereum_block), filter)
                    .await?;

                Ok(BlockStreamEvent::ProcessBlock(
                    block_with_triggers,
                    FirehoseCursor::Some(response.cursor.clone()),
                ))
            }

            bstream::ForkStep::StepUndo => Ok(BlockStreamEvent::Revert(
                BlockPtr {
                    hash: BlockHash::from(block.hash),
                    number: block.number as i32,
                },
                FirehoseCursor::Some(response.cursor.clone()),
                Some(BlockPtr {
                    hash: BlockHash::from(block.header.unwrap().parent_hash),
                    number: (block.number.checked_sub(1).unwrap() as i32), // Will never receive undo on blocknum 0
                }),
            )),

            bstream::ForkStep::StepIrreversible => {
                unreachable!("irreversible step is not handled and should not be requested in the Firehose request")
            }

            bstream::ForkStep::StepUnknown => {
                unreachable!("unknown step should not happen in the Firehose response")
            }
        }
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
        let ethereum_block = self
            .eth_adapter
            .load_full_block(&self.logger, block)
            .await?;

        // We need something that implements `Block` to store the block; the
        // store does not care whether the block is final or not
        let ethereum_block = BlockFinality::NonFinal(EthereumBlockWithCalls {
            ethereum_block,
            calls: None,
        });

        // Store it in the database and try to advance the chain head pointer
        self.chain_store
            .upsert_block(Arc::new(ethereum_block))
            .await?;

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
