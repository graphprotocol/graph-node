use std::sync::Arc;

use graph::cheap_clone::CheapClone;
use graph::data::subgraph::UnifiedMappingApiVersion;
use graph::{
    anyhow::anyhow,
    blockchain::{
        block_stream::{
            BlockStream, BlockStreamEvent, BlockWithTriggers, FirehoseError,
            FirehoseMapper as FirehoseMapperTrait, TriggersAdapter as TriggersAdapterTrait,
        },
        firehose_block_stream::FirehoseBlockStream,
        Block as _, BlockHash, BlockPtr, Blockchain, BlockchainKind, IngestorError,
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
    ) -> Result<Arc<Self::TriggersAdapter>, Error> {
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
        grpc_filters: bool,
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
        _deployment: DeploymentLocator,
        _start_blocks: Vec<BlockNumber>,
        _subgraph_start_block: Option<BlockPtr>,
        _filter: Arc<Self::TriggerFilter>,
        _unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Self>>, Error> {
        panic!("Tendermint does not support polling block stream")
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
            .block_ptr_for_number::<codec::EventList>(logger, number)
            .await
            .map_err(Into::into)
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
            .begin_block_events()
            .cloned()
            // FIXME (Tendermint): Optimize. Should use an Arc instead of cloning the
            // block. This is not currently possible because EventData is automatically
            // generated.
            .map(|event| {
                TendermintTrigger::with_event(event, block.block().clone(), EventOrigin::BeginBlock)
            })
            .chain(shared_block.tx_events().cloned().map(|event| {
                TendermintTrigger::with_event(event, block.block().clone(), EventOrigin::DeliverTx)
            }))
            .chain(shared_block.end_block_events().cloned().map(|event| {
                TendermintTrigger::with_event(event, block.block().clone(), EventOrigin::EndBlock)
            }))
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
            .block_ptr_for_number::<codec::EventList>(logger, number)
            .await
    }

    async fn final_block_ptr_for(
        &self,
        logger: &Logger,
        block: &codec::EventList,
    ) -> Result<BlockPtr, Error> {
        // Tendermint provides instant block finality.
        self.endpoint
            .block_ptr_for_number::<codec::EventList>(logger, block.number())
            .await
    }
}
