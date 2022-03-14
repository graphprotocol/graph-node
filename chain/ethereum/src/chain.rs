use anyhow::{Context, Error};
use graph::blockchain::BlockchainKind;
use graph::data::subgraph::UnifiedMappingApiVersion;
use graph::env::env_var;
use graph::firehose::{FirehoseEndpoint, FirehoseEndpoints, ForkStep};
use graph::prelude::{EthereumBlock, EthereumCallCache, LightEthereumBlock, LightEthereumBlockExt};
use graph::slog::debug;
use graph::{
    blockchain::{
        block_stream::{
            BlockStreamEvent, BlockWithTriggers, FirehoseError,
            FirehoseMapper as FirehoseMapperTrait, TriggersAdapter as TriggersAdapterTrait,
        },
        firehose_block_stream::FirehoseBlockStream,
        polling_block_stream::PollingBlockStream,
        Block, BlockPtr, Blockchain, ChainHeadUpdateListener, IngestorError, TriggerFilter as _,
    },
    cheap_clone::CheapClone,
    components::store::DeploymentLocator,
    firehose,
    prelude::{
        async_trait, lazy_static, o, serde_json as json, BlockNumber, ChainStore,
        EthereumBlockWithCalls, Future01CompatExt, Logger, LoggerFactory, MetricsRegistry, NodeId,
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
    static ref MAX_BLOCK_RANGE_SIZE: BlockNumber = env_var("GRAPH_ETHEREUM_MAX_BLOCK_RANGE_SIZE", 2000);

    /// Ideal number of triggers in a range. The range size will adapt to try to meet this.
    static ref TARGET_TRIGGERS_PER_BLOCK_RANGE: u64 = env_var("GRAPH_ETHEREUM_TARGET_TRIGGERS_PER_BLOCK_RANGE", 100);

    /// Controls if firehose should be preferred over RPC if Firehose endpoints are present, if not set, the default behavior is
    /// is kept which is to automatically favor Firehose.
    static ref IS_FIREHOSE_PREFERRED: bool = env_var("GRAPH_ETHEREUM_IS_FIREHOSE_PREFERRED", true);
}

/// Celo Mainnet: 42220, Testnet Alfajores: 44787, Testnet Baklava: 62320
const CELO_CHAIN_IDS: [u64; 3] = [42220, 44787, 62320];

pub struct Chain {
    logger_factory: LoggerFactory,
    name: String,
    node_id: NodeId,
    registry: Arc<dyn MetricsRegistry>,
    firehose_endpoints: Arc<FirehoseEndpoints>,
    eth_adapters: Arc<EthereumNetworkAdapters>,
    chain_store: Arc<dyn ChainStore>,
    call_cache: Arc<dyn EthereumCallCache>,
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
        firehose_endpoints: FirehoseEndpoints,
        eth_adapters: EthereumNetworkAdapters,
        chain_head_update_listener: Arc<dyn ChainHeadUpdateListener>,
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
            chain_store,
            call_cache,
            chain_head_update_listener,
            reorg_threshold,
            is_ingestible,
        }
    }
}

impl Chain {
    pub fn cheapest_adapter(&self) -> Arc<EthereumAdapter> {
        self.eth_adapters.cheapest().unwrap().clone()
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

    type RuntimeAdapter = RuntimeAdapter;

    fn triggers_adapter(
        &self,
        loc: &DeploymentLocator,
        capabilities: &Self::NodeCapabilities,
        unified_api_version: UnifiedMappingApiVersion,
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
            chain_store: self.chain_store.cheap_clone(),
            unified_api_version,
        };
        Ok(Arc::new(adapter))
    }

    async fn new_firehose_block_stream(
        &self,
        deployment: DeploymentLocator,
        block_cursor: Option<String>,
        start_blocks: Vec<BlockNumber>,
        subgraph_current_block: Option<BlockPtr>,
        filter: Arc<Self::TriggerFilter>,
        unified_api_version: UnifiedMappingApiVersion,
        grpc_filters: bool,
    ) -> Result<Box<dyn BlockStream<Self>>, Error> {
        let requirements = filter.node_capabilities();
        let adapter = self
            .triggers_adapter(&deployment, &requirements, unified_api_version)
            .expect(&format!(
                "no adapter for network {} with capabilities {}",
                self.name, requirements
            ));

        let firehose_endpoint = match self.firehose_endpoints.random() {
            Some(e) => e.clone(),
            None => return Err(anyhow::format_err!("no firehose endpoint available",)),
        };

        let logger = self
            .logger_factory
            .subgraph_logger(&deployment)
            .new(o!("component" => "FirehoseBlockStream"));

        let firehose_mapper = Arc::new(FirehoseMapper {
            endpoint: firehose_endpoint.cheap_clone(),
        });

        Ok(Box::new(FirehoseBlockStream::new(
            firehose_endpoint,
            subgraph_current_block,
            block_cursor,
            firehose_mapper,
            adapter,
            filter,
            start_blocks,
            logger,
            grpc_filters,
        )))
    }

    async fn new_polling_block_stream(
        &self,
        deployment: DeploymentLocator,
        start_blocks: Vec<BlockNumber>,
        subgraph_current_block: Option<BlockPtr>,
        filter: Arc<Self::TriggerFilter>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Self>>, Error> {
        let requirements = filter.node_capabilities();
        let adapter = self
            .triggers_adapter(&deployment, &requirements, unified_api_version.clone())
            .expect(&format!(
                "no adapter for network {} with capabilities {}",
                self.name, requirements
            ));

        let logger = self
            .logger_factory
            .subgraph_logger(&deployment)
            .new(o!("component" => "BlockStream"));
        let chain_store = self.chain_store().clone();
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

        Ok(Box::new(PollingBlockStream::new(
            chain_store,
            chain_head_update_stream,
            adapter,
            self.node_id.clone(),
            deployment.hash,
            filter,
            start_blocks,
            reorg_threshold,
            logger,
            *MAX_BLOCK_RANGE_SIZE,
            *TARGET_TRIGGERS_PER_BLOCK_RANGE,
            unified_api_version,
            subgraph_current_block,
        )))
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

    fn is_firehose_supported(&self) -> bool {
        *IS_FIREHOSE_PREFERRED && self.firehose_endpoints.len() > 0
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
    pub(crate) fn light_block(&self) -> &Arc<LightEthereumBlock> {
        match self {
            BlockFinality::Final(block) => block,
            BlockFinality::NonFinal(block) => &block.ethereum_block.block,
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
                triggers.append(&mut parse_block_triggers(&filter.block, &full_block));
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

pub struct FirehoseMapper {
    endpoint: Arc<FirehoseEndpoint>,
}

#[async_trait]
impl FirehoseMapperTrait<Chain> for FirehoseMapper {
    async fn to_block_stream_event(
        &self,
        logger: &Logger,
        response: &firehose::Response,
        adapter: &TriggersAdapter,
        filter: &TriggerFilter,
    ) -> Result<BlockStreamEvent<Chain>, FirehoseError> {
        let step = ForkStep::from_i32(response.step).unwrap_or_else(|| {
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
                // See comment(437a9f17-67cc-478f-80a3-804fe554b227) ethereum_block.calls is always Some even if calls
                // is empty
                let ethereum_block: EthereumBlockWithCalls = (&block).into();

                // triggers in block never actually calls the ethereum traces api.
                // TODO: Split the trigger parsing from call retrieving.
                let block_with_triggers = adapter
                    .triggers_in_block(logger, BlockFinality::NonFinal(ethereum_block), filter)
                    .await?;

                Ok(BlockStreamEvent::ProcessBlock(
                    block_with_triggers,
                    FirehoseCursor::Some(response.cursor.clone()),
                ))
            }

            StepUndo => {
                let parent_ptr = block
                    .parent_ptr()
                    .expect("Genesis block should never be reverted");

                Ok(BlockStreamEvent::Revert(
                    parent_ptr,
                    FirehoseCursor::Some(response.cursor.clone()),
                ))
            }

            StepIrreversible => {
                unreachable!("irreversible step is not handled and should not be requested in the Firehose request")
            }

            StepUnknown => {
                unreachable!("unknown step should not happen in the Firehose response")
            }
        }
    }

    async fn block_ptr_for_number(
        &self,
        logger: &Logger,
        number: BlockNumber,
    ) -> Result<BlockPtr, Error> {
        self.endpoint
            .block_ptr_for_number::<codec::HeaderOnlyBlock>(logger, number)
            .await
    }

    async fn final_block_ptr_for(
        &self,
        logger: &Logger,
        block: &BlockFinality,
    ) -> Result<BlockPtr, Error> {
        // Firehose for Ethereum has an hard-coded confirmations for finality sets to 200 block
        // behind the current block. The magic value 200 here comes from this hard-coded Firehose
        // value.
        let final_block_number = match block.number() {
            x if x >= 200 => x - 200,
            _ => 0,
        };

        self.endpoint
            .block_ptr_for_number::<codec::HeaderOnlyBlock>(logger, final_block_number)
            .await
    }
}
