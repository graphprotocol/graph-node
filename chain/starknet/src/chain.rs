use graph::{
    anyhow::Result,
    blockchain::{
        block_stream::{
            BlockStream, BlockStreamBuilder, BlockStreamEvent, BlockWithTriggers, FirehoseCursor,
            FirehoseError, FirehoseMapper as FirehoseMapperTrait,
            TriggersAdapter as TriggersAdapterTrait,
        },
        client::ChainClient,
        firehose_block_ingestor::FirehoseBlockIngestor,
        firehose_block_stream::FirehoseBlockStream,
        BasicBlockchainBuilder, Block, BlockIngestor, BlockPtr, Blockchain, BlockchainBuilder,
        BlockchainKind, EmptyNodeCapabilities, IngestorError, NoopDecoderHook, NoopRuntimeAdapter,
        RuntimeAdapter as RuntimeAdapterTrait,
    },
    cheap_clone::CheapClone,
    components::{
        adapter::ChainId,
        store::{DeploymentCursorTracker, DeploymentLocator},
    },
    data::subgraph::UnifiedMappingApiVersion,
    env::EnvVars,
    firehose::{self, FirehoseEndpoint, ForkStep},
    futures03::future::TryFutureExt,
    prelude::{
        async_trait, BlockHash, BlockNumber, ChainStore, Error, Logger, LoggerFactory,
        MetricsRegistry,
    },
    schema::InputSchema,
    slog::o,
};
use prost::Message;
use std::sync::Arc;

use crate::{
    adapter::TriggerFilter,
    codec,
    data_source::{
        DataSource, DataSourceTemplate, UnresolvedDataSource, UnresolvedDataSourceTemplate,
    },
    trigger::{StarknetBlockTrigger, StarknetEventTrigger, StarknetTrigger},
};

pub struct Chain {
    logger_factory: LoggerFactory,
    name: ChainId,
    client: Arc<ChainClient<Self>>,
    chain_store: Arc<dyn ChainStore>,
    metrics_registry: Arc<MetricsRegistry>,
    block_stream_builder: Arc<dyn BlockStreamBuilder<Self>>,
}

pub struct StarknetStreamBuilder;

pub struct FirehoseMapper {
    adapter: Arc<dyn TriggersAdapterTrait<Chain>>,
    filter: Arc<TriggerFilter>,
}

pub struct TriggersAdapter;

#[async_trait]
impl BlockchainBuilder<Chain> for BasicBlockchainBuilder {
    async fn build(self, _config: &Arc<EnvVars>) -> Chain {
        Chain {
            logger_factory: self.logger_factory,
            name: self.name,
            chain_store: self.chain_store,
            client: Arc::new(ChainClient::new_firehose(self.firehose_endpoints)),
            metrics_registry: self.metrics_registry,
            block_stream_builder: Arc::new(StarknetStreamBuilder {}),
        }
    }
}

impl std::fmt::Debug for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "chain: starknet")
    }
}

#[async_trait]
impl Blockchain for Chain {
    const KIND: BlockchainKind = BlockchainKind::Starknet;

    type Client = ();
    type Block = codec::Block;
    type DataSource = DataSource;
    type UnresolvedDataSource = UnresolvedDataSource;

    type DataSourceTemplate = DataSourceTemplate;
    type UnresolvedDataSourceTemplate = UnresolvedDataSourceTemplate;

    type TriggerData = crate::trigger::StarknetTrigger;

    type MappingTrigger = crate::trigger::StarknetTrigger;

    type TriggerFilter = crate::adapter::TriggerFilter;

    type NodeCapabilities = EmptyNodeCapabilities<Self>;

    type DecoderHook = NoopDecoderHook;

    fn triggers_adapter(
        &self,
        _log: &DeploymentLocator,
        _capabilities: &Self::NodeCapabilities,
        _unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Arc<dyn TriggersAdapterTrait<Self>>, Error> {
        Ok(Arc::new(TriggersAdapter))
    }

    async fn new_block_stream(
        &self,
        deployment: DeploymentLocator,
        store: impl DeploymentCursorTracker,
        start_blocks: Vec<BlockNumber>,
        filter: Arc<Self::TriggerFilter>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Self>>, Error> {
        self.block_stream_builder
            .build_firehose(
                self,
                deployment,
                store.firehose_cursor(),
                start_blocks,
                store.block_ptr(),
                filter,
                unified_api_version,
            )
            .await
    }

    fn is_refetch_block_required(&self) -> bool {
        false
    }

    async fn refetch_firehose_block(
        &self,
        _logger: &Logger,
        _cursor: FirehoseCursor,
    ) -> Result<codec::Block, Error> {
        unimplemented!("This chain does not support Dynamic Data Sources. is_refetch_block_required always returns false, this shouldn't be called.")
    }

    fn chain_store(&self) -> Arc<dyn ChainStore> {
        self.chain_store.clone()
    }

    async fn block_pointer_from_number(
        &self,
        logger: &Logger,
        number: BlockNumber,
    ) -> Result<BlockPtr, IngestorError> {
        let firehose_endpoint = self.client.firehose_endpoint().await?;

        firehose_endpoint
            .block_ptr_for_number::<codec::Block>(logger, number)
            .map_err(Into::into)
            .await
    }

    fn runtime(
        &self,
    ) -> graph::anyhow::Result<(Arc<dyn RuntimeAdapterTrait<Self>>, Self::DecoderHook)> {
        Ok((Arc::new(NoopRuntimeAdapter::default()), NoopDecoderHook))
    }

    fn chain_client(&self) -> Arc<ChainClient<Self>> {
        self.client.clone()
    }

    async fn block_ingestor(&self) -> Result<Box<dyn BlockIngestor>> {
        let ingestor = FirehoseBlockIngestor::<crate::Block, Self>::new(
            self.chain_store.cheap_clone(),
            self.chain_client(),
            self.logger_factory
                .component_logger("StarknetFirehoseBlockIngestor", None),
            self.name.clone(),
        );
        Ok(Box::new(ingestor))
    }
}

#[async_trait]
impl BlockStreamBuilder<Chain> for StarknetStreamBuilder {
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

    async fn build_firehose(
        &self,
        chain: &Chain,
        deployment: DeploymentLocator,
        block_cursor: FirehoseCursor,
        start_blocks: Vec<BlockNumber>,
        subgraph_current_block: Option<BlockPtr>,
        filter: Arc<TriggerFilter>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Chain>>> {
        let adapter = chain
            .triggers_adapter(
                &deployment,
                &EmptyNodeCapabilities::default(),
                unified_api_version,
            )
            .unwrap_or_else(|_| panic!("no adapter for network {}", chain.name));

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
            chain.metrics_registry.clone(),
        )))
    }

    async fn build_polling(
        &self,
        _chain: &Chain,
        _deployment: DeploymentLocator,
        _start_blocks: Vec<BlockNumber>,
        _subgraph_current_block: Option<BlockPtr>,
        _filter: Arc<TriggerFilter>,
        _unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Chain>>> {
        panic!("StarkNet does not support polling block stream")
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
        // is not part of the actual bstream::BlockResponseV2 payload. As such, we need to decode the full
        // block which is useless.
        //
        // Check about adding basic information about the block in the bstream::BlockResponseV2 or maybe
        // define a slimmed down stuct that would decode only a few fields and ignore all the rest.
        let block = codec::Block::decode(any_block.value.as_ref())?;

        use ForkStep::*;
        match step {
            StepNew => Ok(BlockStreamEvent::ProcessBlock(
                self.adapter
                    .triggers_in_block(logger, block, &self.filter)
                    .await?,
                FirehoseCursor::from(response.cursor.clone()),
            )),

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
                panic!("irreversible step is not handled and should not be requested in the Firehose request")
            }

            StepUnset => {
                panic!("unknown step should not happen in the Firehose response")
            }
        }
    }

    /// Returns the [BlockPtr] value for this given block number. This is the block pointer
    /// of the longuest according to Firehose view of the blockchain state.
    ///
    /// This is a thin wrapper around [FirehoseEndpoint#block_ptr_for_number] to make
    /// it chain agnostic and callable from chain agnostic [FirehoseBlockStream].
    async fn block_ptr_for_number(
        &self,
        logger: &Logger,
        endpoint: &Arc<FirehoseEndpoint>,
        number: BlockNumber,
    ) -> Result<BlockPtr, Error> {
        endpoint
            .block_ptr_for_number::<codec::Block>(logger, number)
            .await
    }

    /// Returns the closest final block ptr to the block ptr received.
    /// On probablitics chain like Ethereum, final is determined by
    /// the confirmations threshold configured for the Firehose stack (currently
    /// hard-coded to 200).
    ///
    /// On some other chain like NEAR, the actual final block number is determined
    /// from the block itself since it contains information about which block number
    /// is final against the current block.
    ///
    /// To take an example, assuming we are on Ethereum, the final block pointer
    /// for block #10212 would be the determined final block #10012 (10212 - 200 = 10012).
    async fn final_block_ptr_for(
        &self,
        logger: &Logger,
        endpoint: &Arc<FirehoseEndpoint>,
        block: &codec::Block,
    ) -> Result<BlockPtr, Error> {
        // Firehose for Starknet has an hard-coded confirmations for finality sets to 100 block
        // behind the current block. The magic value 100 here comes from this hard-coded Firehose
        // value.
        let final_block_number = match block.number() {
            x if x >= 100 => x - 100,
            _ => 0,
        };

        self.block_ptr_for_number(logger, endpoint, final_block_number)
            .await
    }
}

#[async_trait]
impl TriggersAdapterTrait<Chain> for TriggersAdapter {
    // Return the block that is `offset` blocks before the block pointed to
    // by `ptr` from the local cache. An offset of 0 means the block itself,
    // an offset of 1 means the block's parent etc. If the block is not in
    // the local cache, return `None`
    async fn ancestor_block(
        &self,
        _ptr: BlockPtr,
        _offset: BlockNumber,
        _root: Option<BlockHash>,
    ) -> Result<Option<codec::Block>, Error> {
        panic!("Should never be called since FirehoseBlockStream cannot resolve it")
    }

    // Returns a sequence of blocks in increasing order of block number.
    // Each block will include all of its triggers that match the given `filter`.
    // The sequence may omit blocks that contain no triggers,
    // but all returned blocks must part of a same chain starting at `chain_base`.
    // At least one block will be returned, even if it contains no triggers.
    // `step_size` is the suggested number blocks to be scanned.
    async fn scan_triggers(
        &self,
        _from: BlockNumber,
        _to: BlockNumber,
        _filter: &crate::adapter::TriggerFilter,
    ) -> Result<(Vec<BlockWithTriggers<Chain>>, BlockNumber), Error> {
        panic!("Should never be called since not used by FirehoseBlockStream")
    }

    #[allow(unused)]
    async fn triggers_in_block(
        &self,
        logger: &Logger,
        block: codec::Block,
        filter: &crate::adapter::TriggerFilter,
    ) -> Result<BlockWithTriggers<Chain>, Error> {
        let shared_block = Arc::new(block.clone());

        let mut triggers: Vec<_> = shared_block
            .transactions
            .iter()
            .flat_map(|transaction| -> Vec<StarknetTrigger> {
                let transaction = Arc::new(transaction.clone());
                transaction
                    .events
                    .iter()
                    .map(|event| {
                        StarknetTrigger::Event(StarknetEventTrigger {
                            event: Arc::new(event.clone()),
                            block: shared_block.clone(),
                            transaction: transaction.clone(),
                        })
                    })
                    .collect()
            })
            .collect();

        triggers.push(StarknetTrigger::Block(StarknetBlockTrigger {
            block: shared_block,
        }));

        Ok(BlockWithTriggers::new(block, triggers, logger))
    }

    /// Return `true` if the block with the given hash and number is on the
    /// main chain, i.e., the chain going back from the current chain head.
    async fn is_on_main_chain(&self, _ptr: BlockPtr) -> Result<bool, Error> {
        panic!("Should never be called since not used by FirehoseBlockStream")
    }

    /// Get pointer to parent of `block`. This is called when reverting `block`.
    async fn parent_ptr(&self, block: &BlockPtr) -> Result<Option<BlockPtr>, Error> {
        // Panics if `block` is genesis.
        // But that's ok since this is only called when reverting `block`.
        Ok(Some(BlockPtr {
            hash: BlockHash::from(vec![0xff; 32]),
            number: block.number.saturating_sub(1),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use graph::{blockchain::DataSource as _, data::subgraph::LATEST_VERSION};

    use crate::{
        data_source::{
            DataSource, Mapping, MappingBlockHandler, MappingEventHandler, STARKNET_KIND,
        },
        felt::Felt,
    };

    #[test]
    fn validate_no_handler() {
        let ds = new_data_source(None);

        let errs = ds.validate(LATEST_VERSION);
        assert_eq!(errs.len(), 1, "{:?}", ds);
        assert_eq!(
            errs[0].to_string(),
            "data source does not define any handler"
        );
    }

    #[test]
    fn validate_address_without_event_handler() {
        let mut ds = new_data_source(Some([1u8; 32].into()));
        ds.mapping.block_handler = Some(MappingBlockHandler {
            handler: "asdf".into(),
        });

        let errs = ds.validate(LATEST_VERSION);
        assert_eq!(errs.len(), 1, "{:?}", ds);
        assert_eq!(
            errs[0].to_string(),
            "data source cannot have source address without event handlers"
        );
    }

    #[test]
    fn validate_no_address_with_event_handler() {
        let mut ds = new_data_source(None);
        ds.mapping.event_handlers.push(MappingEventHandler {
            handler: "asdf".into(),
            event_selector: [2u8; 32].into(),
        });

        let errs = ds.validate(LATEST_VERSION);
        assert_eq!(errs.len(), 1, "{:?}", ds);
        assert_eq!(errs[0].to_string(), "subgraph source address is required");
    }

    fn new_data_source(address: Option<Felt>) -> DataSource {
        DataSource {
            kind: STARKNET_KIND.to_string(),
            network: "starknet-mainnet".into(),
            name: "asd".to_string(),
            source: crate::data_source::Source {
                start_block: 10,
                end_block: None,
                address,
            },
            mapping: Mapping {
                block_handler: None,
                event_handlers: vec![],
                runtime: Arc::new(vec![]),
            },
        }
    }
}
