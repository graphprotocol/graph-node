use std::sync::Arc;

use graph::cheap_clone::CheapClone;
use graph::components::store::WritableStore;
use graph::data::subgraph::UnifiedMappingApiVersion;
use graph::{
    anyhow,
    blockchain::{
        block_stream::{
            BlockStream, BlockStreamEvent, BlockStreamMetrics, BlockWithTriggers, FirehoseError,
            FirehoseMapper as FirehoseMapperTrait, TriggersAdapter as TriggersAdapterTrait,
        },
        firehose_block_stream::FirehoseBlockStream,
        BlockHash, BlockPtr, Blockchain, BlockchainKind, IngestorError,
    },
    components::store::DeploymentLocator,
    firehose::{self, FirehoseEndpoints, ForkStep},
    prelude::{
        async_trait, o, BlockNumber, ChainStore, Error, Logger, LoggerFactory, StopwatchMetrics,
    },
};
use prost::Message;

use crate::capabilities::NodeCapabilities;
use crate::data_source::{
    DataSource, DataSourceTemplate, UnresolvedDataSource, UnresolvedDataSourceTemplate,
};
use crate::trigger::TendermintTrigger;
use crate::RuntimeAdapter;
use crate::{codec, TriggerFilter};

pub struct Chain {
    logger_factory: LoggerFactory,
    name: String,
    firehose_endpoints: Arc<FirehoseEndpoints>,
    chain_store: Arc<dyn ChainStore>,
}

impl std::fmt::Debug for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "chain: tendermint")
    }
}

impl Chain {
    pub fn new(
        logger_factory: LoggerFactory,
        name: String,
        chain_store: Arc<dyn ChainStore>,
        firehose_endpoints: FirehoseEndpoints,
    ) -> Self {
        Chain {
            logger_factory,
            name,
            firehose_endpoints: Arc::new(firehose_endpoints),
            chain_store,
        }
    }
}

#[async_trait]
impl Blockchain for Chain {
    const KIND: BlockchainKind = BlockchainKind::Tendermint;

    type Block = codec::EventList;

    type DataSource = DataSource;

    type UnresolvedDataSource = UnresolvedDataSource;

    type DataSourceTemplate = DataSourceTemplate;

    type UnresolvedDataSourceTemplate = UnresolvedDataSourceTemplate;

    type TriggersAdapter = TriggersAdapter;

    type TriggerData = TendermintTrigger;

    type MappingTrigger = TendermintTrigger;

    type TriggerFilter = TriggerFilter;

    type NodeCapabilities = NodeCapabilities;

    type RuntimeAdapter = RuntimeAdapter;

    fn triggers_adapter(
        &self,
        _loc: &DeploymentLocator,
        _capabilities: &Self::NodeCapabilities,
        _unified_api_version: UnifiedMappingApiVersion,
        _stopwatch_metrics: StopwatchMetrics,
    ) -> Result<Arc<Self::TriggersAdapter>, Error> {
        let adapter = TriggersAdapter {};
        Ok(Arc::new(adapter))
    }

    async fn new_firehose_block_stream(
        &self,
        deployment: DeploymentLocator,
        store: Arc<dyn WritableStore>,
        start_blocks: Vec<BlockNumber>,
        filter: Arc<TriggerFilter>,
        metrics: Arc<BlockStreamMetrics>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Self>>, Error> {
        let adapter = self
            .triggers_adapter(
                &deployment,
                &NodeCapabilities {},
                unified_api_version,
                metrics.stopwatch.clone(),
            )
            .unwrap_or_else(|_| panic!("no adapter for network {}", self.name));

        let firehose_endpoint = match self.firehose_endpoints.random() {
            Some(e) => e.clone(),
            None => return Err(anyhow::format_err!("no firehose endpoint available",)),
        };

        let logger = self
            .logger_factory
            .subgraph_logger(&deployment)
            .new(o!("component" => "FirehoseBlockStream"));

        let firehose_mapper = Arc::new(FirehoseMapper {});
        let firehose_cursor = store.block_cursor();

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

    async fn new_polling_block_stream(
        &self,
        _deployment: DeploymentLocator,
        _start_blocks: Vec<BlockNumber>,
        _subgraph_start_block: Option<BlockPtr>,
        _filter: Arc<Self::TriggerFilter>,
        _metrics: Arc<BlockStreamMetrics>,
        _unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Self>>, Error> {
        panic!("Tendermint does not support polling block stream")
    }

    fn chain_store(&self) -> Arc<dyn ChainStore> {
        self.chain_store.cheap_clone()
    }

    async fn block_pointer_from_number(
        &self,
        _logger: &Logger,
        _number: BlockNumber,
    ) -> Result<BlockPtr, IngestorError> {
        // FIXME (Tendermint): Hmmm, what to do with this?
        Ok(BlockPtr {
            hash: BlockHash::from(vec![0xff; 32]),
            number: 0,
        })
    }

    fn runtime_adapter(&self) -> Arc<Self::RuntimeAdapter> {
        Arc::new(RuntimeAdapter {})
    }

    fn is_firehose_supported(&self) -> bool {
        true
    }
}

pub struct TriggersAdapter {}

#[async_trait]
impl TriggersAdapterTrait<Chain> for TriggersAdapter {
    async fn scan_triggers(
        &self,
        _from: BlockNumber,
        _to: BlockNumber,
        _filter: &TriggerFilter,
    ) -> Result<Vec<BlockWithTriggers<Chain>>, Error> {
        panic!("Should never be called since not used by FirehoseBlockStream")
    }

    async fn triggers_in_block(
        &self,
        _logger: &Logger,
        block: codec::EventList,
        _filter: &TriggerFilter,
    ) -> Result<BlockWithTriggers<Chain>, Error> {
        let shared_block = Arc::new(block.clone());

        let mut triggers: Vec<_> = shared_block
            .events()
            .cloned()
            .map(|event| {
                TendermintTrigger::Event(Arc::new(codec::EventData {
                    event: Some(event),
                    // FIXME (Tendermint): Optimize. Should use an Arc instead of cloning the
                    // block. This is not currently possible because EventData is automatically
                    // generated.
                    block: Some(block.block().clone()),
                }))
            })
            .collect();

        triggers.push(TendermintTrigger::Block(shared_block.cheap_clone()));

        Ok(BlockWithTriggers::new(block, triggers))
    }

    async fn is_on_main_chain(&self, _ptr: BlockPtr) -> Result<bool, Error> {
        panic!("Should never be called since not used by FirehoseBlockStream")
    }

    fn ancestor_block(
        &self,
        _ptr: BlockPtr,
        _offset: BlockNumber,
    ) -> Result<Option<codec::EventList>, Error> {
        panic!("Should never be called since not used by FirehoseBlockStream")
    }

    /// Panics if `block` is genesis.
    /// But that's ok since this is only called when reverting `block`.
    async fn parent_ptr(&self, block: &BlockPtr) -> Result<Option<BlockPtr>, Error> {
        Ok(Some(BlockPtr {
            hash: BlockHash::from(vec![0xff; 32]),
            number: block.number.saturating_sub(1),
        }))
    }
}

pub struct FirehoseMapper {}

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
        // is not part of the actual bstream::BlockResponseV2 payload. As such, we need to decode the full
        // block which is useless.
        //
        // Check about adding basic information about the block in the bstream::BlockResponseV2 or maybe
        // define a slimmed down struct that would decode only a few fields and ignore all the rest.
        let sp = codec::EventList::decode(any_block.value.as_ref())?;

        match step {
            ForkStep::StepNew => Ok(BlockStreamEvent::ProcessBlock(
                adapter.triggers_in_block(logger, sp, filter).await?,
                Some(response.cursor.clone()),
            )),

            ForkStep::StepUndo => {
                let parent_ptr = sp
                    .parent_ptr()
                    .expect("Genesis block should never be reverted");

                Ok(BlockStreamEvent::Revert(
                    sp.into(),
                    parent_ptr,
                    Some(response.cursor.clone()),
                ))
            }

            ForkStep::StepIrreversible => {
                panic!("irreversible step is not handled and should not be requested in the Firehose request")
            }

            ForkStep::StepUnknown => {
                panic!("unknown step should not happen in the Firehose response")
            }
        }
    }
}
