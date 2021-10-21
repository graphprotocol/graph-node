use graph::blockchain::BlockchainKind;
use graph::cheap_clone::CheapClone;
use graph::data::subgraph::UnifiedMappingApiVersion;
use graph::firehose::endpoints::FirehoseNetworkEndpoints;
use graph::prelude::StopwatchMetrics;
use graph::{
    anyhow,
    blockchain::{
        block_stream::{
            BlockStreamEvent, BlockStreamMetrics, BlockWithTriggers, FirehoseError,
            FirehoseMapper as FirehoseMapperTrait, TriggersAdapter as TriggersAdapterTrait,
        },
        firehose_block_stream::FirehoseBlockStream,
        BlockHash, BlockPtr, Blockchain, IngestorAdapter as IngestorAdapterTrait, IngestorError,
    },
    components::store::DeploymentLocator,
    firehose::bstream,
    log::factory::{ComponentLoggerConfig, ElasticComponentLoggerConfig},
    prelude::{
        async_trait, o, BlockNumber, ChainStore, Error, Logger, LoggerFactory, SubgraphStore,
    },
};
use prost::Message;
use std::sync::Arc;

use crate::adapter::TriggerFilter;
use crate::capabilities::NodeCapabilities;
use crate::data_source::{DataSourceTemplate, UnresolvedDataSourceTemplate};
use crate::runtime::RuntimeAdapter;
use crate::trigger::{self, NearTrigger};
use crate::{
    codec,
    data_source::{DataSource, UnresolvedDataSource},
};
use graph::blockchain::block_stream::BlockStream;

pub struct Chain {
    logger_factory: LoggerFactory,
    name: String,
    firehose_endpoints: Arc<FirehoseNetworkEndpoints>,
    chain_store: Arc<dyn ChainStore>,
    subgraph_store: Arc<dyn SubgraphStore>,
}

impl std::fmt::Debug for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "chain: near")
    }
}

impl Chain {
    pub fn new(
        logger_factory: LoggerFactory,
        name: String,
        chain_store: Arc<dyn ChainStore>,
        subgraph_store: Arc<dyn SubgraphStore>,
        firehose_endpoints: FirehoseNetworkEndpoints,
    ) -> Self {
        Chain {
            logger_factory,
            name,
            firehose_endpoints: Arc::new(firehose_endpoints),
            chain_store,
            subgraph_store,
        }
    }
}

#[async_trait]
impl Blockchain for Chain {
    const KIND: BlockchainKind = BlockchainKind::Near;

    type Block = codec::BlockWrapper;

    type DataSource = DataSource;

    type UnresolvedDataSource = UnresolvedDataSource;

    type DataSourceTemplate = DataSourceTemplate;

    type UnresolvedDataSourceTemplate = UnresolvedDataSourceTemplate;

    type TriggersAdapter = TriggersAdapter;

    type TriggerData = crate::trigger::NearTrigger;

    type MappingTrigger = crate::trigger::NearTrigger;

    type TriggerFilter = crate::adapter::TriggerFilter;

    type NodeCapabilities = crate::capabilities::NodeCapabilities;

    type IngestorAdapter = IngestorAdapter;

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

    async fn new_block_stream(
        &self,
        deployment: DeploymentLocator,
        start_blocks: Vec<BlockNumber>,
        filter: Arc<TriggerFilter>,
        metrics: Arc<BlockStreamMetrics>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Self>>, Error> {
        let adapter = self
            .triggers_adapter(
                &deployment,
                &NodeCapabilities {},
                unified_api_version.clone(),
                metrics.stopwatch.clone(),
            )
            .expect(&format!("no adapter for network {}", self.name,));

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
            deployment.hash,
            adapter,
            filter,
            start_blocks,
            logger,
        )))
    }

    fn ingestor_adapter(&self) -> Arc<Self::IngestorAdapter> {
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
            .new(o!());

        let adapter = IngestorAdapter { logger };
        Arc::new(adapter)
    }

    fn chain_store(&self) -> Arc<dyn ChainStore> {
        self.chain_store.clone()
    }

    async fn block_pointer_from_number(
        &self,
        _logger: &Logger,
        _number: BlockNumber,
    ) -> Result<BlockPtr, IngestorError> {
        // FIXME (NEAR): Hmmm, what to do with this?
        Ok(BlockPtr {
            hash: BlockHash::from(vec![0xff; 32]),
            number: 0,
        })
    }

    fn runtime_adapter(&self) -> Arc<Self::RuntimeAdapter> {
        Arc::new(RuntimeAdapter {})
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
        // FIXME (NEAR): Scanning triggers makes little sense in Firehose approach, let's see
        Ok(vec![])
    }

    async fn triggers_in_block(
        &self,
        _logger: &Logger,
        _block: codec::BlockWrapper,
        _filter: &TriggerFilter,
    ) -> Result<BlockWithTriggers<Chain>, Error> {
        // FIXME (NEAR): Share implementation with FirehoseMapper::firehose_triggers_in_block version.
        // This is currently unreachable since Near does not yet support dynamic data sources.
        todo!()
    }

    async fn is_on_main_chain(&self, _ptr: BlockPtr) -> Result<bool, Error> {
        // FIXME (NEAR): Might not be necessary for NEAR support for now
        Ok(true)
    }

    fn ancestor_block(
        &self,
        _ptr: BlockPtr,
        _offset: BlockNumber,
    ) -> Result<Option<codec::BlockWrapper>, Error> {
        // FIXME (NEAR):  Might not be necessary for NEAR support for now
        Ok(None)
    }

    /// Panics if `block` is genesis.
    /// But that's ok since this is only called when reverting `block`.
    async fn parent_ptr(&self, block: &BlockPtr) -> Result<Option<BlockPtr>, Error> {
        // FIXME (NEAR):  Might not be necessary for NEAR support for now
        Ok(Some(BlockPtr {
            hash: BlockHash::from(vec![0xff; 32]),
            number: block.number.saturating_sub(1),
        }))
    }
}

pub struct FirehoseMapper {}

impl FirehoseMapperTrait<Chain> for FirehoseMapper {
    fn to_block_stream_event(
        &self,
        _logger: &Logger,
        response: &bstream::BlockResponseV2,
        _adapter: &TriggersAdapter,
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
        let block = codec::BlockWrapper::decode(any_block.value.as_ref())?;

        match step {
            bstream::ForkStep::StepNew => Ok(BlockStreamEvent::ProcessBlock(
                self.firehose_triggers_in_block(&block, filter)?,
                Some(response.cursor.clone()),
            )),

            bstream::ForkStep::StepUndo => {
                let block = block.block.as_ref().unwrap();
                let header = block.header.as_ref().unwrap();

                Ok(BlockStreamEvent::Revert(
                    BlockPtr {
                        hash: BlockHash::from(header.hash.as_ref().unwrap().bytes.clone()),
                        number: header.height as i32,
                    },
                    Some(response.cursor.clone()),
                ))
            }

            bstream::ForkStep::StepIrreversible => {
                panic!("irreversible step is not handled and should not be requested in the Firehose request")
            }

            bstream::ForkStep::StepUnknown => {
                panic!("unknown step should not happen in the Firehose response")
            }
        }
    }
}

impl FirehoseMapper {
    // FIXME: This should be replaced by using the `TriggersAdapter` struct directly. However, the TriggersAdapter trait
    //        is async. It's actual async usage is done inside a manual `poll` implementation in `firehose_block_stream#poll_next`
    //        value. An upcoming improvement will be to remove this `poll_next`. Once the refactor occurs, this should be
    //        removed and TriggersAdapter::triggers_in_block should be use straight.
    fn firehose_triggers_in_block(
        &self,
        block: &codec::BlockWrapper,
        _filter: &TriggerFilter,
    ) -> Result<BlockWithTriggers<Chain>, FirehoseError> {
        // TODO: Find the best place to introduce an `Arc` and avoid this clone.
        let block = Arc::new(block.clone());

        // Filter non-successful or non-action receipts.
        let receipts = block.shards.iter().flat_map(|shard| {
            shard
                .receipt_execution_outcomes
                .clone()
                .into_iter()
                .filter_map(|outcome| {
                    if !outcome
                        .execution_outcome
                        .as_ref()?
                        .outcome
                        .as_ref()?
                        .status
                        .as_ref()?
                        .is_success()
                    {
                        return None;
                    }
                    if !matches!(
                        outcome.receipt.as_ref()?.receipt,
                        Some(codec::receipt::Receipt::Action(_))
                    ) {
                        return None;
                    }
                    Some(trigger::ReceiptWithOutcome {
                        outcome: outcome.execution_outcome?,
                        receipt: outcome.receipt?,
                        block: block.cheap_clone(),
                    })
                })
        });

        let mut trigger_data: Vec<_> = receipts
            .map(|r| NearTrigger::Receipt(Arc::new(r)))
            .collect();

        trigger_data.push(NearTrigger::Block(block.cheap_clone()));

        // TODO: `block` should probably be an `Arc` in `BlockWithTriggers` to avoid this clone.
        Ok(BlockWithTriggers::new(block.as_ref().clone(), trigger_data))
    }
}

pub struct IngestorAdapter {
    logger: Logger,
}

#[async_trait]
impl IngestorAdapterTrait<Chain> for IngestorAdapter {
    fn logger(&self) -> &Logger {
        &self.logger
    }

    fn ancestor_count(&self) -> BlockNumber {
        0
    }

    async fn latest_block(&self) -> Result<BlockPtr, IngestorError> {
        Ok(BlockPtr {
            hash: BlockHash::from(vec![0xff; 32]),
            number: 0,
        })
    }

    async fn ingest_block(
        &self,
        _block_hash: &BlockHash,
    ) -> Result<Option<BlockHash>, IngestorError> {
        // FIXME (NEAR):  Might not be necessary for NEAR support for now
        Ok(None)
    }

    fn chain_head_ptr(&self) -> Result<Option<BlockPtr>, Error> {
        // FIXME (NEAR):  Might not be necessary for NEAR support for now
        Ok(None)
    }

    fn cleanup_cached_blocks(&self) -> Result<Option<(i32, usize)>, Error> {
        // FIXME (NEAR):  Might not be necessary for NEAR support for now
        Ok(None)
    }
}
