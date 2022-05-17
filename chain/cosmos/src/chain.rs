use std::sync::Arc;

use graph::cheap_clone::CheapClone;
use graph::data::subgraph::UnifiedMappingApiVersion;
use graph::prelude::MetricsRegistry;
use graph::{
    anyhow::anyhow,
    blockchain::{
        block_stream::{
            BlockStream, BlockStreamEvent, BlockWithTriggers, FirehoseError,
            FirehoseMapper as FirehoseMapperTrait, TriggersAdapter as TriggersAdapterTrait,
        },
        firehose_block_stream::FirehoseBlockStream,
        Block as _, BlockHash, BlockPtr, Blockchain, BlockchainKind, IngestorError,
        RuntimeAdapter as RuntimeAdapterTrait,
    },
    components::store::DeploymentLocator,
    firehose::{self, FirehoseEndpoint, FirehoseEndpoints, ForkStep},
    prelude::{async_trait, o, BlockNumber, ChainStore, Error, Logger, LoggerFactory},
};
use prost::Message;

use crate::capabilities::NodeCapabilities;
use crate::data_source::{
    DataSource, DataSourceTemplate, EventOrigin, UnresolvedDataSource, UnresolvedDataSourceTemplate,
};
use crate::trigger::CosmosTrigger;
use crate::RuntimeAdapter;
use crate::{codec, TriggerFilter};

pub struct Chain {
    logger_factory: LoggerFactory,
    name: String,
    firehose_endpoints: Arc<FirehoseEndpoints>,
    chain_store: Arc<dyn ChainStore>,
    metrics_registry: Arc<dyn MetricsRegistry>,
}

impl std::fmt::Debug for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "chain: cosmos")
    }
}

impl Chain {
    pub fn new(
        logger_factory: LoggerFactory,
        name: String,
        chain_store: Arc<dyn ChainStore>,
        firehose_endpoints: FirehoseEndpoints,
        metrics_registry: Arc<dyn MetricsRegistry>,
    ) -> Self {
        Chain {
            logger_factory,
            name,
            firehose_endpoints: Arc::new(firehose_endpoints),
            chain_store,
            metrics_registry,
        }
    }
}

#[async_trait]
impl Blockchain for Chain {
    const KIND: BlockchainKind = BlockchainKind::Cosmos;

    type Block = codec::Block;

    type DataSource = DataSource;

    type UnresolvedDataSource = UnresolvedDataSource;

    type DataSourceTemplate = DataSourceTemplate;

    type UnresolvedDataSourceTemplate = UnresolvedDataSourceTemplate;

    type TriggerData = CosmosTrigger;

    type MappingTrigger = CosmosTrigger;

    type TriggerFilter = TriggerFilter;

    type NodeCapabilities = NodeCapabilities;

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
        let adapter = self
            .triggers_adapter(&deployment, &NodeCapabilities {}, unified_api_version)
            .unwrap_or_else(|_| panic!("no adapter for network {}", self.name));

        let firehose_endpoint = match self.firehose_endpoints.random() {
            Some(e) => e.clone(),
            None => return Err(anyhow!("no firehose endpoint available",)),
        };

        let logger = self
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
            self.metrics_registry.clone(),
        )))
    }

    async fn new_polling_block_stream(
        &self,
        _deployment: DeploymentLocator,
        _start_blocks: Vec<BlockNumber>,
        _subgraph_start_block: Option<BlockPtr>,
        _filter: Arc<Self::TriggerFilter>,
        _unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Self>>, Error> {
        panic!("Cosmos does not support polling block stream")
    }

    fn chain_store(&self) -> Arc<dyn ChainStore> {
        self.chain_store.cheap_clone()
    }

    async fn block_pointer_from_number(
        &self,
        logger: &Logger,
        number: BlockNumber,
    ) -> Result<BlockPtr, IngestorError> {
        let firehose_endpoint = match self.firehose_endpoints.random() {
            Some(e) => e.clone(),
            None => return Err(anyhow!("no firehose endpoint available").into()),
        };

        firehose_endpoint
            .block_ptr_for_number::<codec::HeaderOnlyBlock>(logger, number)
            .await
            .map_err(Into::into)
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
        let shared_block = Arc::new(block.clone());

        let header_only_block = codec::HeaderOnlyBlock::from(&block);

        let mut triggers: Vec<_> = shared_block
            .begin_block_events()
            .cloned()
            // FIXME (Cosmos): Optimize. Should use an Arc instead of cloning the
            // block. This is not currently possible because EventData is automatically
            // generated.
            .filter_map(|event| {
                filter_event_trigger(filter, event, &header_only_block, EventOrigin::BeginBlock)
            })
            .chain(shared_block.tx_events().cloned().filter_map(|event| {
                filter_event_trigger(filter, event, &header_only_block, EventOrigin::DeliverTx)
            }))
            .chain(
                shared_block
                    .end_block_events()
                    .cloned()
                    .filter_map(|event| {
                        filter_event_trigger(
                            filter,
                            event,
                            &header_only_block,
                            EventOrigin::EndBlock,
                        )
                    }),
            )
            .collect();

        triggers.extend(
            shared_block
                .transactions()
                .cloned()
                .map(|tx| CosmosTrigger::with_transaction(tx, header_only_block.clone())),
        );

        if filter.block_filter.trigger_every_block {
            triggers.push(CosmosTrigger::Block(shared_block.cheap_clone()));
        }

        Ok(BlockWithTriggers::new(block, triggers))
    }

    async fn is_on_main_chain(&self, _ptr: BlockPtr) -> Result<bool, Error> {
        panic!("Should never be called since not used by FirehoseBlockStream")
    }

    async fn ancestor_block(
        &self,
        _ptr: BlockPtr,
        _offset: BlockNumber,
    ) -> Result<Option<codec::Block>, Error> {
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

/// Returns a new event trigger only if the given event matches the event filter.
fn filter_event_trigger(
    filter: &TriggerFilter,
    event: codec::Event,
    block: &codec::HeaderOnlyBlock,
    origin: EventOrigin,
) -> Option<CosmosTrigger> {
    if filter.event_type_filter.matches(&event.event_type) {
        Some(CosmosTrigger::with_event(event, block.clone(), origin))
    } else {
        None
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
        // define a slimmed down struct that would decode only a few fields and ignore all the rest.
        let sp = codec::Block::decode(any_block.value.as_ref())?;

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
        // Cosmos provides instant block finality.
        self.endpoint
            .block_ptr_for_number::<codec::HeaderOnlyBlock>(logger, block.number())
            .await
    }
}

#[cfg(test)]
mod test {
    use graph::prelude::{
        slog::{o, Discard, Logger},
        tokio,
    };

    use super::*;

    use codec::{
        Block, Event, Header, HeaderOnlyBlock, ResponseBeginBlock, ResponseDeliverTx,
        ResponseEndBlock, TxResult,
    };

    #[tokio::test]
    async fn test_trigger_filters() {
        let adapter = TriggersAdapter {};
        let logger = Logger::root(Discard, o!());

        let block_with_events = Block::test_with_event_types(
            vec!["begin_event_1", "begin_event_2", "begin_event_3"],
            vec!["tx_event_1", "tx_event_2", "tx_event_3"],
            vec!["end_event_1", "end_event_2", "end_event_3"],
        );

        let header_only_block = HeaderOnlyBlock::from(&block_with_events);

        let cases = [
            (
                Block::test_new(),
                TriggerFilter::test_new(false, &[]),
                vec![],
            ),
            (
                Block::test_new(),
                TriggerFilter::test_new(true, &[]),
                vec![CosmosTrigger::Block(Arc::new(Block::test_new()))],
            ),
            (
                Block::test_new(),
                TriggerFilter::test_new(false, &["event_1", "event_2", "event_3"]),
                vec![],
            ),
            (
                block_with_events.clone(),
                TriggerFilter::test_new(false, &["begin_event_3", "tx_event_3", "end_event_3"]),
                vec![
                    CosmosTrigger::with_event(
                        Event::test_with_type("begin_event_3"),
                        header_only_block.clone(),
                        EventOrigin::BeginBlock,
                    ),
                    CosmosTrigger::with_event(
                        Event::test_with_type("tx_event_3"),
                        header_only_block.clone(),
                        EventOrigin::DeliverTx,
                    ),
                    CosmosTrigger::with_event(
                        Event::test_with_type("end_event_3"),
                        header_only_block.clone(),
                        EventOrigin::EndBlock,
                    ),
                    CosmosTrigger::with_transaction(
                        TxResult::test_with_event_type("tx_event_1"),
                        header_only_block.clone(),
                    ),
                    CosmosTrigger::with_transaction(
                        TxResult::test_with_event_type("tx_event_2"),
                        header_only_block.clone(),
                    ),
                    CosmosTrigger::with_transaction(
                        TxResult::test_with_event_type("tx_event_3"),
                        header_only_block.clone(),
                    ),
                ],
            ),
            (
                block_with_events.clone(),
                TriggerFilter::test_new(true, &["begin_event_3", "tx_event_2", "end_event_1"]),
                vec![
                    CosmosTrigger::Block(Arc::new(block_with_events.clone())),
                    CosmosTrigger::with_event(
                        Event::test_with_type("begin_event_3"),
                        header_only_block.clone(),
                        EventOrigin::BeginBlock,
                    ),
                    CosmosTrigger::with_event(
                        Event::test_with_type("tx_event_2"),
                        header_only_block.clone(),
                        EventOrigin::DeliverTx,
                    ),
                    CosmosTrigger::with_event(
                        Event::test_with_type("end_event_1"),
                        header_only_block.clone(),
                        EventOrigin::EndBlock,
                    ),
                    CosmosTrigger::with_transaction(
                        TxResult::test_with_event_type("tx_event_1"),
                        header_only_block.clone(),
                    ),
                    CosmosTrigger::with_transaction(
                        TxResult::test_with_event_type("tx_event_2"),
                        header_only_block.clone(),
                    ),
                    CosmosTrigger::with_transaction(
                        TxResult::test_with_event_type("tx_event_3"),
                        header_only_block.clone(),
                    ),
                ],
            ),
        ];

        for (block, trigger_filter, expected_triggers) in cases {
            let triggers = adapter
                .triggers_in_block(&logger, block, &trigger_filter)
                .await
                .expect("failed to get triggers in block");

            assert_eq!(
                triggers.trigger_data.len(),
                expected_triggers.len(),
                "Expected trigger list to contain exactly {:?}, but it didn't: {:?}",
                expected_triggers,
                triggers.trigger_data
            );

            // they may not be in the same order
            for trigger in expected_triggers {
                assert!(
                    triggers.trigger_data.contains(&trigger),
                    "Expected trigger list to contain {:?}, but it only contains: {:?}",
                    trigger,
                    triggers.trigger_data
                );
            }
        }
    }

    impl Block {
        fn test_new() -> Block {
            Block::test_with_event_types(vec![], vec![], vec![])
        }

        fn test_with_event_types(
            begin_event_types: Vec<&str>,
            tx_event_types: Vec<&str>,
            end_event_types: Vec<&str>,
        ) -> Block {
            Block {
                header: Some(Header {
                    version: None,
                    chain_id: "test".to_string(),
                    height: 1,
                    time: None,
                    last_block_id: None,
                    last_commit_hash: vec![],
                    data_hash: vec![],
                    validators_hash: vec![],
                    next_validators_hash: vec![],
                    consensus_hash: vec![],
                    app_hash: vec![],
                    last_results_hash: vec![],
                    evidence_hash: vec![],
                    proposer_address: vec![],
                    hash: vec![],
                }),
                evidence: None,
                last_commit: None,
                result_begin_block: Some(ResponseBeginBlock {
                    events: begin_event_types
                        .into_iter()
                        .map(Event::test_with_type)
                        .collect(),
                }),
                result_end_block: Some(ResponseEndBlock {
                    validator_updates: vec![],
                    consensus_param_updates: None,
                    events: end_event_types
                        .into_iter()
                        .map(Event::test_with_type)
                        .collect(),
                }),
                transactions: tx_event_types
                    .into_iter()
                    .map(TxResult::test_with_event_type)
                    .collect(),
                validator_updates: vec![],
            }
        }
    }

    impl Event {
        fn test_with_type(event_type: &str) -> Event {
            Event {
                event_type: event_type.to_string(),
                attributes: vec![],
            }
        }
    }

    impl TxResult {
        fn test_with_event_type(event_type: &str) -> TxResult {
            TxResult {
                height: 1,
                index: 1,
                tx: None,
                result: Some(ResponseDeliverTx {
                    code: 1,
                    data: vec![],
                    log: "".to_string(),
                    info: "".to_string(),
                    gas_wanted: 1,
                    gas_used: 1,
                    codespace: "".to_string(),
                    events: vec![Event::test_with_type(event_type)],
                }),
                hash: vec![],
            }
        }
    }
}
