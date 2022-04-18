use graph::blockchain::BlockchainKind;
use graph::cheap_clone::CheapClone;
use graph::data::subgraph::UnifiedMappingApiVersion;
use graph::firehose::{FirehoseEndpoint, FirehoseEndpoints};
use graph::prelude::{MetricsRegistry, TryFutureExt};
use graph::{
    anyhow,
    anyhow::Result,
    blockchain::{
        block_stream::{
            BlockStreamEvent, BlockWithTriggers, FirehoseError,
            FirehoseMapper as FirehoseMapperTrait, TriggersAdapter as TriggersAdapterTrait,
        },
        firehose_block_stream::FirehoseBlockStream,
        BlockHash, BlockPtr, Blockchain, IngestorError, RuntimeAdapter as RuntimeAdapterTrait,
    },
    components::store::DeploymentLocator,
    firehose::{self as firehose, ForkStep},
    prelude::{async_trait, o, BlockNumber, ChainStore, Error, Logger, LoggerFactory},
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
use graph::blockchain::block_stream::{BlockStream, BlockStreamBuilder};

pub struct NearStreamBuilder {}

#[async_trait]
impl BlockStreamBuilder<Chain> for NearStreamBuilder {
    fn build_firehose(
        &self,
        chain: &Chain,
        deployment: DeploymentLocator,
        block_cursor: Option<String>,
        start_blocks: Vec<BlockNumber>,
        subgraph_current_block: Option<BlockPtr>,
        filter: Arc<<Chain as Blockchain>::TriggerFilter>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Chain>>> {
        let adapter = chain
            .triggers_adapter(&deployment, &NodeCapabilities {}, unified_api_version)
            .expect(&format!("no adapter for network {}", chain.name,));

        let firehose_endpoint = match chain.firehose_endpoints.random() {
            Some(e) => e.clone(),
            None => return Err(anyhow::format_err!("no firehose endpoint available")),
        };

        let logger = chain
            .logger_factory
            .subgraph_logger(&deployment)
            .new(o!("component" => "FirehoseBlockStream"));

        let firehose_mapper = Arc::new(FirehoseMapper {
            endpoint: firehose_endpoint.cheap_clone(),
        });

        Ok(Box::new(FirehoseBlockStream::new(
            deployment.hash,
            firehose_endpoint,
            subgraph_current_block,
            block_cursor,
            firehose_mapper,
            adapter,
            filter,
            start_blocks,
            logger,
            chain.metrics_registry.clone(),
        )))
    }

    async fn build_polling(
        &self,
        _chain: Arc<Chain>,
        _deployment: DeploymentLocator,
        _start_blocks: Vec<BlockNumber>,
        _subgraph_current_block: Option<BlockPtr>,
        _filter: Arc<<Chain as Blockchain>::TriggerFilter>,
        _unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Chain>>> {
        todo!()
    }
}

pub struct Chain {
    logger_factory: LoggerFactory,
    name: String,
    firehose_endpoints: Arc<FirehoseEndpoints>,
    chain_store: Arc<dyn ChainStore>,
    metrics_registry: Arc<dyn MetricsRegistry>,
    block_stream_builder: Arc<dyn BlockStreamBuilder<Self>>,
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
        firehose_endpoints: FirehoseEndpoints,
        metrics_registry: Arc<dyn MetricsRegistry>,
        block_stream_builder: Arc<dyn BlockStreamBuilder<Self>>,
    ) -> Self {
        Chain {
            logger_factory,
            name,
            firehose_endpoints: Arc::new(firehose_endpoints),
            chain_store,
            metrics_registry,
            block_stream_builder,
        }
    }
}

#[async_trait]
impl Blockchain for Chain {
    const KIND: BlockchainKind = BlockchainKind::Near;

    type Block = codec::Block;

    type DataSource = DataSource;

    type UnresolvedDataSource = UnresolvedDataSource;

    type DataSourceTemplate = DataSourceTemplate;

    type UnresolvedDataSourceTemplate = UnresolvedDataSourceTemplate;

    type TriggerData = crate::trigger::NearTrigger;

    type MappingTrigger = crate::trigger::NearTrigger;

    type TriggerFilter = crate::adapter::TriggerFilter;

    type NodeCapabilities = crate::capabilities::NodeCapabilities;

    fn triggers_adapter(
        &self,
        _loc: &DeploymentLocator,
        _capabilities: &Self::NodeCapabilities,
        _unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Arc<dyn TriggersAdapterTrait<Self>>, Error> {
        let adapter = TriggersAdapter {};
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
        panic!("NEAR does not support polling block stream")
    }

    fn chain_store(&self) -> Arc<dyn ChainStore> {
        self.chain_store.clone()
    }

    async fn block_pointer_from_number(
        &self,
        logger: &Logger,
        number: BlockNumber,
    ) -> Result<BlockPtr, IngestorError> {
        let firehose_endpoint = match self.firehose_endpoints.random() {
            Some(e) => e.clone(),
            None => return Err(anyhow::format_err!("no firehose endpoint available").into()),
        };

        firehose_endpoint
            .block_ptr_for_number::<codec::HeaderOnlyBlock>(logger, number)
            .map_err(Into::into)
            .await
    }

    fn runtime_adapter(&self) -> Arc<dyn RuntimeAdapterTrait<Self>> {
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
        block: codec::Block,
        filter: &TriggerFilter,
    ) -> Result<BlockWithTriggers<Chain>, Error> {
        // TODO: Find the best place to introduce an `Arc` and avoid this clone.
        let shared_block = Arc::new(block.clone());

        let TriggerFilter {
            block_filter,
            receipt_filter,
        } = filter;

        // Filter non-successful or non-action receipts.
        let receipts = block.shards.iter().flat_map(|shard| {
            shard
                .receipt_execution_outcomes
                .iter()
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

                    let receipt = outcome.receipt.as_ref()?.clone();
                    if !receipt_filter.matches(&receipt.receiver_id) {
                        return None;
                    }

                    Some(trigger::ReceiptWithOutcome {
                        outcome: outcome.execution_outcome.as_ref()?.clone(),
                        receipt,
                        block: shared_block.cheap_clone(),
                    })
                })
        });

        let mut trigger_data: Vec<_> = receipts
            .map(|r| NearTrigger::Receipt(Arc::new(r)))
            .collect();

        if block_filter.trigger_every_block {
            trigger_data.push(NearTrigger::Block(shared_block.cheap_clone()));
        }

        Ok(BlockWithTriggers::new(block, trigger_data))
    }

    async fn is_on_main_chain(&self, _ptr: BlockPtr) -> Result<bool, Error> {
        panic!("Should never be called since not used by FirehoseBlockStream")
    }

    async fn ancestor_block(
        &self,
        _ptr: BlockPtr,
        _offset: BlockNumber,
    ) -> Result<Option<codec::Block>, Error> {
        panic!("Should never be called since FirehoseBlockStream cannot resolve it")
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

pub struct FirehoseMapper {
    endpoint: Arc<FirehoseEndpoint>,
}

#[async_trait]
impl FirehoseMapperTrait<Chain> for FirehoseMapper {
    async fn to_block_stream_event(
        &self,
        logger: &Logger,
        response: &firehose::Response,
        adapter: &Arc<dyn TriggersAdapterTrait<Chain>>,
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
        // define a slimmed down stuct that would decode only a few fields and ignore all the rest.
        let block = codec::Block::decode(any_block.value.as_ref())?;

        use ForkStep::*;
        match step {
            StepNew => Ok(BlockStreamEvent::ProcessBlock(
                adapter.triggers_in_block(logger, block, filter).await?,
                Some(response.cursor.clone()),
            )),

            StepUndo => {
                let parent_ptr = block
                    .header()
                    .parent_ptr()
                    .expect("Genesis block should never be reverted");

                Ok(BlockStreamEvent::Revert(
                    parent_ptr,
                    Some(response.cursor.clone()),
                ))
            }

            StepIrreversible => {
                panic!("irreversible step is not handled and should not be requested in the Firehose request")
            }

            StepUnknown => {
                panic!("unknown step should not happen in the Firehose response")
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
        block: &codec::Block,
    ) -> Result<BlockPtr, Error> {
        let final_block_number = block.header().last_final_block_height as BlockNumber;

        self.endpoint
            .block_ptr_for_number::<codec::HeaderOnlyBlock>(logger, final_block_number)
            .await
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, vec};

    use graph::{
        blockchain::{block_stream::BlockWithTriggers, TriggersAdapter as _},
        prelude::tokio,
        slog::{self, o, Logger},
    };

    use crate::{
        adapter::{NearReceiptFilter, TriggerFilter},
        codec::{
            self, execution_outcome,
            receipt::{self},
            BlockHeader, DataReceiver, ExecutionOutcome, ExecutionOutcomeWithId,
            IndexerExecutionOutcomeWithReceipt, IndexerShard, ReceiptAction,
            SuccessValueExecutionStatus,
        },
        Chain,
    };

    use super::TriggersAdapter;

    #[tokio::test]
    async fn test_trigger_filter_empty() {
        let account1: String = "account1".into();

        let adapter = TriggersAdapter {};

        let logger = Logger::root(slog::Discard, o!());
        let block1 = new_success_block(1, &account1);

        let filter = TriggerFilter::default();

        let block_with_triggers: BlockWithTriggers<Chain> = adapter
            .triggers_in_block(&logger, block1, &filter)
            .await
            .expect("failed to execute triggers_in_block");
        assert_eq!(block_with_triggers.trigger_count(), 0);
    }

    #[tokio::test]
    async fn test_trigger_filter_every_block() {
        let account1: String = "account1".into();

        let adapter = TriggersAdapter {};

        let logger = Logger::root(slog::Discard, o!());
        let block1 = new_success_block(1, &account1);

        let filter = TriggerFilter {
            block_filter: crate::adapter::NearBlockFilter {
                trigger_every_block: true,
            },
            ..Default::default()
        };

        let block_with_triggers: BlockWithTriggers<Chain> = adapter
            .triggers_in_block(&logger, block1, &filter)
            .await
            .expect("failed to execute triggers_in_block");
        assert_eq!(block_with_triggers.trigger_count(), 1);

        let height: Vec<u64> = heights_from_triggers(&block_with_triggers);
        assert_eq!(height, vec![1]);
    }

    #[tokio::test]
    async fn test_trigger_filter_every_receipt() {
        let account1: String = "account1".into();

        let adapter = TriggersAdapter {};

        let logger = Logger::root(slog::Discard, o!());
        let block1 = new_success_block(1, &account1);

        let filter = TriggerFilter {
            receipt_filter: NearReceiptFilter {
                accounts: HashSet::from_iter(vec![account1]),
            },
            ..Default::default()
        };

        let block_with_triggers: BlockWithTriggers<Chain> = adapter
            .triggers_in_block(&logger, block1, &filter)
            .await
            .expect("failed to execute triggers_in_block");
        assert_eq!(block_with_triggers.trigger_count(), 1);

        let height: Vec<u64> = heights_from_triggers(&block_with_triggers);
        assert_eq!(height.len(), 0);
    }

    fn heights_from_triggers(block: &BlockWithTriggers<Chain>) -> Vec<u64> {
        block
            .trigger_data
            .clone()
            .into_iter()
            .filter_map(|x| match x {
                crate::trigger::NearTrigger::Block(b) => b.header.clone().map(|x| x.height),
                _ => None,
            })
            .collect()
    }

    fn new_success_block(height: u64, receiver_id: &String) -> codec::Block {
        codec::Block {
            header: Some(BlockHeader {
                height,
                ..Default::default()
            }),
            shards: vec![IndexerShard {
                receipt_execution_outcomes: vec![IndexerExecutionOutcomeWithReceipt {
                    receipt: Some(crate::codec::Receipt {
                        receipt: Some(receipt::Receipt::Action(ReceiptAction {
                            output_data_receivers: vec![DataReceiver {
                                receiver_id: receiver_id.clone(),
                                ..Default::default()
                            }],
                            ..Default::default()
                        })),
                        receiver_id: receiver_id.clone(),
                        ..Default::default()
                    }),
                    execution_outcome: Some(ExecutionOutcomeWithId {
                        outcome: Some(ExecutionOutcome {
                            status: Some(execution_outcome::Status::SuccessValue(
                                SuccessValueExecutionStatus::default(),
                            )),

                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }
}
