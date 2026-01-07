use crate::blockchain::SubgraphFilter;
use crate::data_source::{subgraph, CausalityRegion};
use anyhow::Error;
use async_stream::stream;
use async_trait::async_trait;
use futures03::Stream;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc::{self, Receiver, Sender};

use super::{Block, BlockPtr, BlockTime, Blockchain, Trigger, TriggerFilterWrapper};
use crate::anyhow::Result;
use crate::components::store::{BlockNumber, DeploymentLocator, SourceableStore};
use crate::data::subgraph::UnifiedMappingApiVersion;
use crate::firehose::{self, FirehoseEndpoint};
use crate::futures03::stream::StreamExt as _;
use crate::schema::{EntityType, InputSchema};
use crate::{prelude::*, prometheus::labels};

pub const BUFFERED_BLOCK_STREAM_SIZE: usize = 100;
pub const FIREHOSE_BUFFER_STREAM_SIZE: usize = 1;
pub const SUBSTREAMS_BUFFER_STREAM_SIZE: usize = 100;

pub struct BufferedBlockStream<C: Blockchain> {
    inner: Pin<Box<dyn Stream<Item = Result<BlockStreamEvent<C>, BlockStreamError>> + Send>>,
}

impl<C: Blockchain + 'static> BufferedBlockStream<C> {
    pub fn spawn_from_stream(
        size_hint: usize,
        stream: Box<dyn BlockStream<C>>,
    ) -> Box<dyn BlockStream<C>> {
        let (sender, receiver) =
            mpsc::channel::<Result<BlockStreamEvent<C>, BlockStreamError>>(size_hint);
        crate::spawn(async move { BufferedBlockStream::stream_blocks(stream, sender).await });

        Box::new(BufferedBlockStream::new(receiver))
    }

    pub fn new(mut receiver: Receiver<Result<BlockStreamEvent<C>, BlockStreamError>>) -> Self {
        let inner = stream! {
            loop {
                let event = match receiver.recv().await {
                    Some(evt) => evt,
                    None => return,
                };

                yield event
            }
        };

        Self {
            inner: Box::pin(inner),
        }
    }

    pub async fn stream_blocks(
        mut stream: Box<dyn BlockStream<C>>,
        sender: Sender<Result<BlockStreamEvent<C>, BlockStreamError>>,
    ) -> Result<(), Error> {
        while let Some(event) = stream.next().await {
            match sender.send(event).await {
                Ok(_) => continue,
                Err(err) => {
                    return Err(anyhow!(
                        "buffered blockstream channel is closed, stopping. Err: {}",
                        err
                    ))
                }
            }
        }

        Ok(())
    }
}

impl<C: Blockchain> BlockStream<C> for BufferedBlockStream<C> {
    fn buffer_size_hint(&self) -> usize {
        unreachable!()
    }
}

impl<C: Blockchain> Stream for BufferedBlockStream<C> {
    type Item = Result<BlockStreamEvent<C>, BlockStreamError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

pub trait BlockStream<C: Blockchain>:
    Stream<Item = Result<BlockStreamEvent<C>, BlockStreamError>> + Unpin + Send
{
    fn buffer_size_hint(&self) -> usize;
}

/// BlockRefetcher abstraction allows a chain to decide if a block must be refetched after a dynamic data source was added
#[async_trait]
pub trait BlockRefetcher<C: Blockchain>: Send + Sync {
    fn required(&self, chain: &C) -> bool;

    async fn get_block(
        &self,
        chain: &C,
        logger: &Logger,
        cursor: FirehoseCursor,
    ) -> Result<C::Block, Error>;
}

/// BlockStreamBuilder is an abstraction that would separate the logic for building streams from the blockchain trait
#[async_trait]
pub trait BlockStreamBuilder<C: Blockchain>: Send + Sync {
    async fn build_firehose(
        &self,
        chain: &C,
        deployment: DeploymentLocator,
        block_cursor: FirehoseCursor,
        start_blocks: Vec<BlockNumber>,
        subgraph_current_block: Option<BlockPtr>,
        filter: Arc<C::TriggerFilter>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<C>>>;

    async fn build_polling(
        &self,
        chain: &C,
        deployment: DeploymentLocator,
        start_blocks: Vec<BlockNumber>,
        source_subgraph_stores: Vec<Arc<dyn SourceableStore>>,
        subgraph_current_block: Option<BlockPtr>,
        filter: Arc<TriggerFilterWrapper<C>>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<C>>>;

    async fn build_subgraph_block_stream(
        &self,
        chain: &C,
        deployment: DeploymentLocator,
        start_blocks: Vec<BlockNumber>,
        source_subgraph_stores: Vec<Arc<dyn SourceableStore>>,
        subgraph_current_block: Option<BlockPtr>,
        filter: Arc<TriggerFilterWrapper<C>>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<C>>> {
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
}

#[derive(Debug, Clone)]
pub struct FirehoseCursor(Option<String>);

impl FirehoseCursor {
    #[allow(non_upper_case_globals)]
    pub const None: Self = FirehoseCursor(None);

    pub fn is_none(&self) -> bool {
        self.0.is_none()
    }
}

impl fmt::Display for FirehoseCursor {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_str(self.0.as_deref().unwrap_or(""))
    }
}

impl From<String> for FirehoseCursor {
    fn from(cursor: String) -> Self {
        // Treat a cursor of "" as None, not absolutely necessary for correctness since the firehose
        // treats both as the same, but makes it a little clearer.
        if cursor.is_empty() {
            FirehoseCursor::None
        } else {
            FirehoseCursor(Some(cursor))
        }
    }
}

impl From<Option<String>> for FirehoseCursor {
    fn from(cursor: Option<String>) -> Self {
        match cursor {
            None => FirehoseCursor::None,
            Some(s) => FirehoseCursor::from(s),
        }
    }
}

impl AsRef<Option<String>> for FirehoseCursor {
    fn as_ref(&self) -> &Option<String> {
        &self.0
    }
}

#[derive(Debug)]
pub struct BlockWithTriggers<C: Blockchain> {
    pub block: C::Block,
    pub trigger_data: Vec<Trigger<C>>,
}

impl<C: Blockchain> Clone for BlockWithTriggers<C>
where
    C::TriggerData: Clone,
{
    fn clone(&self) -> Self {
        Self {
            block: self.block.clone(),
            trigger_data: self.trigger_data.clone(),
        }
    }
}

impl<C: Blockchain> BlockWithTriggers<C> {
    /// Creates a BlockWithTriggers structure, which holds
    /// the trigger data ordered and without any duplicates.
    pub fn new(block: C::Block, trigger_data: Vec<C::TriggerData>, logger: &Logger) -> Self {
        Self::new_with_triggers(
            block,
            trigger_data.into_iter().map(Trigger::Chain).collect(),
            logger,
        )
    }

    pub fn new_with_subgraph_triggers(
        block: C::Block,
        trigger_data: Vec<subgraph::TriggerData>,
        logger: &Logger,
    ) -> Self {
        Self::new_with_triggers(
            block,
            trigger_data.into_iter().map(Trigger::Subgraph).collect(),
            logger,
        )
    }

    fn new_with_triggers(
        block: C::Block,
        mut trigger_data: Vec<Trigger<C>>,
        logger: &Logger,
    ) -> Self {
        // This is where triggers get sorted.
        trigger_data.sort();

        let old_len = trigger_data.len();

        // This is removing the duplicate triggers in the case of multiple
        // data sources fetching the same event/call/etc.
        trigger_data.dedup();

        let new_len = trigger_data.len();

        if new_len != old_len {
            debug!(
                logger,
                "Trigger data had duplicate triggers";
                "block_number" => block.number(),
                "block_hash" => block.hash().hash_hex(),
                "old_length" => old_len,
                "new_length" => new_len,
            );
        }

        Self {
            block,
            trigger_data,
        }
    }

    pub fn trigger_count(&self) -> usize {
        self.trigger_data.len()
    }

    pub fn ptr(&self) -> BlockPtr {
        self.block.ptr()
    }

    pub fn parent_ptr(&self) -> Option<BlockPtr> {
        self.block.parent_ptr()
    }

    pub fn extend_triggers(&mut self, triggers: Vec<Trigger<C>>) {
        self.trigger_data.extend(triggers);
        self.trigger_data.sort();
    }
}

/// The `TriggersAdapterWrapper` wraps the chain-specific `TriggersAdapter`, enabling chain-agnostic
/// handling of subgraph datasource triggers. Without this wrapper, we would have to duplicate the same
/// logic for each chain, increasing code repetition.
pub struct TriggersAdapterWrapper<C: Blockchain> {
    pub adapter: Arc<dyn TriggersAdapter<C>>,
    pub source_subgraph_stores: HashMap<DeploymentHash, Arc<dyn SourceableStore>>,
}

impl<C: Blockchain> TriggersAdapterWrapper<C> {
    pub fn new(
        adapter: Arc<dyn TriggersAdapter<C>>,
        source_subgraph_stores: Vec<Arc<dyn SourceableStore>>,
    ) -> Self {
        let stores_map: HashMap<_, _> = source_subgraph_stores
            .iter()
            .map(|store| (store.input_schema().id().clone(), store.clone()))
            .collect();
        Self {
            adapter,
            source_subgraph_stores: stores_map,
        }
    }

    pub async fn blocks_with_subgraph_triggers(
        &self,
        logger: &Logger,
        filters: &[SubgraphFilter],
        range: SubgraphTriggerScanRange<C>,
    ) -> Result<Vec<BlockWithTriggers<C>>, Error> {
        if filters.is_empty() {
            return Err(anyhow!("No subgraph filters provided"));
        }

        let (blocks, hash_to_entities) = match range {
            SubgraphTriggerScanRange::Single(block) => {
                let hash_to_entities = self
                    .fetch_entities_for_filters(filters, block.number(), block.number())
                    .await?;

                (vec![block], hash_to_entities)
            }
            SubgraphTriggerScanRange::Range(from, to) => {
                let hash_to_entities = self.fetch_entities_for_filters(filters, from, to).await?;

                // Get block numbers that have entities
                let mut block_numbers: BTreeSet<_> = hash_to_entities
                    .iter()
                    .flat_map(|(_, entities, _)| entities.keys().copied())
                    .collect();

                // Always include the last block in the range
                block_numbers.insert(to);

                let blocks = self
                    .adapter
                    .load_block_ptrs_by_numbers(logger.clone(), block_numbers)
                    .await?;

                (blocks, hash_to_entities)
            }
        };

        create_subgraph_triggers::<C>(logger.clone(), blocks, hash_to_entities).await
    }

    async fn fetch_entities_for_filters(
        &self,
        filters: &[SubgraphFilter],
        from: BlockNumber,
        to: BlockNumber,
    ) -> Result<
        Vec<(
            DeploymentHash,
            BTreeMap<BlockNumber, Vec<EntitySourceOperation>>,
            u32,
        )>,
        Error,
    > {
        let futures = filters
            .iter()
            .filter_map(|filter| {
                self.source_subgraph_stores
                    .get(&filter.subgraph)
                    .map(|store| {
                        let store = store.clone();
                        let schema = store.input_schema();

                        async move {
                            let entities =
                                get_entities_for_range(&store, filter, &schema, from, to).await?;
                            Ok::<_, Error>((filter.subgraph.clone(), entities, filter.manifest_idx))
                        }
                    })
            })
            .collect::<Vec<_>>();

        if futures.is_empty() {
            return Ok(Vec::new());
        }

        futures03::future::try_join_all(futures).await
    }
}

fn create_subgraph_trigger_from_entities(
    subgraph: &DeploymentHash,
    entities: Vec<EntitySourceOperation>,
    manifest_idx: u32,
) -> Vec<subgraph::TriggerData> {
    entities
        .into_iter()
        .map(|entity| subgraph::TriggerData {
            source: subgraph.clone(),
            entity,
            source_idx: manifest_idx,
        })
        .collect()
}

async fn create_subgraph_triggers<C: Blockchain>(
    logger: Logger,
    blocks: Vec<C::Block>,
    subgraph_data: Vec<(
        DeploymentHash,
        BTreeMap<BlockNumber, Vec<EntitySourceOperation>>,
        u32,
    )>,
) -> Result<Vec<BlockWithTriggers<C>>, Error> {
    let logger_clone = logger.cheap_clone();
    let blocks: Vec<BlockWithTriggers<C>> = blocks
        .into_iter()
        .map(|block| {
            let block_number = block.number();
            let mut all_trigger_data = Vec::new();

            for (hash, entities, manifest_idx) in subgraph_data.iter() {
                if let Some(block_entities) = entities.get(&block_number) {
                    let trigger_data = create_subgraph_trigger_from_entities(
                        hash,
                        block_entities.clone(),
                        *manifest_idx,
                    );
                    all_trigger_data.extend(trigger_data);
                }
            }

            BlockWithTriggers::new_with_subgraph_triggers(block, all_trigger_data, &logger_clone)
        })
        .collect();

    Ok(blocks)
}

pub enum SubgraphTriggerScanRange<C: Blockchain> {
    Single(C::Block),
    Range(BlockNumber, BlockNumber),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum EntityOperationKind {
    Create,
    Modify,
    Delete,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EntitySourceOperation {
    pub entity_op: EntityOperationKind,
    pub entity_type: EntityType,
    pub entity: Entity,
    pub vid: i64,
}

async fn get_entities_for_range(
    store: &Arc<dyn SourceableStore>,
    filter: &SubgraphFilter,
    schema: &InputSchema,
    from: BlockNumber,
    to: BlockNumber,
) -> Result<BTreeMap<BlockNumber, Vec<EntitySourceOperation>>, Error> {
    let entity_types: Result<Vec<EntityType>> = filter
        .entities
        .iter()
        .map(|name| schema.entity_type(name))
        .collect();
    Ok(store
        .get_range(entity_types?, CausalityRegion::ONCHAIN, from..to)
        .await?)
}

impl<C: Blockchain> TriggersAdapterWrapper<C> {
    pub async fn ancestor_block(
        &self,
        ptr: BlockPtr,
        offset: BlockNumber,
        root: Option<BlockHash>,
    ) -> Result<Option<C::Block>, Error> {
        self.adapter.ancestor_block(ptr, offset, root).await
    }

    pub async fn scan_triggers(
        &self,
        logger: &Logger,
        from: BlockNumber,
        to: BlockNumber,
        filter: &Arc<TriggerFilterWrapper<C>>,
    ) -> Result<(Vec<BlockWithTriggers<C>>, BlockNumber), Error> {
        if !filter.subgraph_filter.is_empty() {
            let blocks_with_triggers = self
                .blocks_with_subgraph_triggers(
                    logger,
                    &filter.subgraph_filter,
                    SubgraphTriggerScanRange::Range(from, to),
                )
                .await?;

            return Ok((blocks_with_triggers, to));
        }

        self.adapter
            .scan_triggers(from, to, &filter.chain_filter)
            .await
    }

    pub async fn triggers_in_block(
        &self,
        logger: &Logger,
        block: C::Block,
        filter: &Arc<TriggerFilterWrapper<C>>,
    ) -> Result<BlockWithTriggers<C>, Error> {
        trace!(
            logger,
            "triggers_in_block";
            "block_number" => block.number(),
            "block_hash" => block.hash().hash_hex(),
        );

        if !filter.subgraph_filter.is_empty() {
            let blocks_with_triggers = self
                .blocks_with_subgraph_triggers(
                    logger,
                    &filter.subgraph_filter,
                    SubgraphTriggerScanRange::Single(block),
                )
                .await?;

            return Ok(blocks_with_triggers.into_iter().next().unwrap());
        }

        self.adapter
            .triggers_in_block(logger, block, &filter.chain_filter)
            .await
    }

    pub async fn is_on_main_chain(&self, ptr: BlockPtr) -> Result<bool, Error> {
        self.adapter.is_on_main_chain(ptr).await
    }

    pub async fn parent_ptr(&self, block: &BlockPtr) -> Result<Option<BlockPtr>, Error> {
        self.adapter.parent_ptr(block).await
    }

    pub async fn chain_head_ptr(&self) -> Result<Option<BlockPtr>, Error> {
        if self.source_subgraph_stores.is_empty() {
            return self.adapter.chain_head_ptr().await;
        }

        let ptrs = futures03::future::try_join_all(
            self.source_subgraph_stores
                .iter()
                .map(|(_, store)| store.block_ptr()),
        )
        .await?;

        let min_ptr = ptrs.into_iter().flatten().min_by_key(|ptr| ptr.number);

        Ok(min_ptr)
    }
}

#[async_trait]
pub trait TriggersAdapter<C: Blockchain>: Send + Sync {
    // Return the block that is `offset` blocks before the block pointed to by `ptr` from the local
    // cache. An offset of 0 means the block itself, an offset of 1 means the block's parent etc. If
    // `root` is passed, short-circuit upon finding a child of `root`. If the block is not in the
    // local cache, return `None`.
    async fn ancestor_block(
        &self,
        ptr: BlockPtr,
        offset: BlockNumber,
        root: Option<BlockHash>,
    ) -> Result<Option<C::Block>, Error>;

    // Returns a sequence of blocks in increasing order of block number.
    // Each block will include all of its triggers that match the given `filter`.
    // The sequence may omit blocks that contain no triggers,
    // but all returned blocks must part of a same chain starting at `chain_base`.
    // At least one block will be returned, even if it contains no triggers.
    // `step_size` is the suggested number blocks to be scanned.
    async fn scan_triggers(
        &self,
        from: BlockNumber,
        to: BlockNumber,
        filter: &C::TriggerFilter,
    ) -> Result<(Vec<BlockWithTriggers<C>>, BlockNumber), Error>;

    // Used for reprocessing blocks when creating a data source.
    async fn triggers_in_block(
        &self,
        logger: &Logger,
        block: C::Block,
        filter: &C::TriggerFilter,
    ) -> Result<BlockWithTriggers<C>, Error>;

    /// Return `true` if the block with the given hash and number is on the
    /// main chain, i.e., the chain going back from the current chain head.
    async fn is_on_main_chain(&self, ptr: BlockPtr) -> Result<bool, Error>;

    /// Get pointer to parent of `block`. This is called when reverting `block`.
    async fn parent_ptr(&self, block: &BlockPtr) -> Result<Option<BlockPtr>, Error>;

    /// Get pointer to parent of `block`. This is called when reverting `block`.
    async fn chain_head_ptr(&self) -> Result<Option<BlockPtr>, Error>;

    async fn load_block_ptrs_by_numbers(
        &self,
        logger: Logger,
        block_numbers: BTreeSet<BlockNumber>,
    ) -> Result<Vec<C::Block>>;
}

#[async_trait]
pub trait FirehoseMapper<C: Blockchain>: Send + Sync {
    fn trigger_filter(&self) -> &C::TriggerFilter;

    async fn to_block_stream_event(
        &self,
        logger: &Logger,
        response: &firehose::Response,
    ) -> Result<BlockStreamEvent<C>, FirehoseError>;

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
    ) -> Result<BlockPtr, Error>;

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
        block: &C::Block,
    ) -> Result<BlockPtr, Error>;
}

#[async_trait]
pub trait BlockStreamMapper<C: Blockchain>: Send + Sync {
    fn decode_block(&self, output: Option<&[u8]>) -> Result<Option<C::Block>, BlockStreamError>;

    async fn block_with_triggers(
        &self,
        logger: &Logger,
        block: C::Block,
    ) -> Result<BlockWithTriggers<C>, BlockStreamError>;
}

#[derive(Error, Debug)]
pub enum FirehoseError {
    /// We were unable to decode the received block payload into the chain specific Block struct (e.g. chain_ethereum::pb::Block)
    #[error("received gRPC block payload cannot be decoded: {0}")]
    DecodingError(#[from] prost::DecodeError),

    /// Some unknown error occurred
    #[error("unknown error")]
    UnknownError(#[from] anyhow::Error),
}

impl From<BlockStreamError> for FirehoseError {
    fn from(value: BlockStreamError) -> Self {
        match value {
            BlockStreamError::ProtobufDecodingError(e) => FirehoseError::DecodingError(e),
            e => FirehoseError::UnknownError(anyhow!(e.to_string())),
        }
    }
}

#[derive(Error, Debug)]
pub enum SubstreamsError {
    #[error("response is missing the clock information")]
    MissingClockError,

    #[error("invalid undo message")]
    InvalidUndoError,

    #[error("entity validation failed {0}")]
    EntityValidationError(#[from] crate::data::store::EntityValidationError),

    /// We were unable to decode the received block payload into the chain specific Block struct (e.g. chain_ethereum::pb::Block)
    #[error("received gRPC block payload cannot be decoded: {0}")]
    DecodingError(#[from] prost::DecodeError),

    /// Some unknown error occurred
    #[error("unknown error {0}")]
    UnknownError(#[from] anyhow::Error),

    #[error("multiple module output error")]
    MultipleModuleOutputError,

    #[error("module output was not available (none) or wrong data provided")]
    ModuleOutputNotPresentOrUnexpected,

    #[error("unexpected store delta output")]
    UnexpectedStoreDeltaOutput,
}

impl SubstreamsError {
    pub fn is_deterministic(&self) -> bool {
        use SubstreamsError::*;

        match self {
            EntityValidationError(_) => true,
            MissingClockError
            | InvalidUndoError
            | DecodingError(_)
            | UnknownError(_)
            | MultipleModuleOutputError
            | ModuleOutputNotPresentOrUnexpected
            | UnexpectedStoreDeltaOutput => false,
        }
    }
}

#[derive(Debug, Error)]
pub enum BlockStreamError {
    #[error("Failed to decode protobuf {0}")]
    ProtobufDecodingError(#[from] prost::DecodeError),
    #[error("substreams error: {0}")]
    SubstreamsError(#[from] SubstreamsError),
    #[error("block stream error {0}")]
    Unknown(#[from] anyhow::Error),
    #[error("block stream fatal error {0}")]
    Fatal(String),
}

impl BlockStreamError {
    pub fn is_deterministic(&self) -> bool {
        matches!(self, Self::Fatal(_))
    }
}

#[derive(Debug)]
pub enum BlockStreamEvent<C: Blockchain> {
    // The payload is the block the subgraph should revert to, so it becomes the new subgraph head.
    Revert(BlockPtr, FirehoseCursor),

    ProcessBlock(BlockWithTriggers<C>, FirehoseCursor),
    ProcessWasmBlock(BlockPtr, BlockTime, Box<[u8]>, String, FirehoseCursor),
}

#[derive(Clone)]
pub struct BlockStreamMetrics {
    pub deployment_head: Box<Gauge>,
    pub reverted_blocks: Gauge,
    pub stopwatch: StopwatchMetrics,
}

impl BlockStreamMetrics {
    pub fn new(
        registry: Arc<MetricsRegistry>,
        deployment_id: &DeploymentHash,
        network: String,
        shard: String,
        stopwatch: StopwatchMetrics,
    ) -> Self {
        let reverted_blocks = registry
            .new_deployment_gauge(
                "deployment_reverted_blocks",
                "Track the last reverted block for a subgraph deployment",
                deployment_id.as_str(),
            )
            .expect("Failed to create `deployment_reverted_blocks` gauge");
        let labels = labels! {
            String::from("deployment") => deployment_id.to_string(),
            String::from("network") => network,
            String::from("shard") => shard
        };
        let deployment_head = registry
            .new_gauge(
                "deployment_head",
                "Tracks the most recent block number processed by a deployment",
                labels.clone(),
            )
            .expect("failed to create `deployment_head` gauge");
        Self {
            deployment_head,
            reverted_blocks,
            stopwatch,
        }
    }
}

/// Notifications about the chain head advancing. The block ingestor sends
/// an update on this stream whenever the head of the underlying chain
/// changes. The updates have no payload, receivers should call
/// `Store::chain_head_ptr` to check what the latest block is.
pub type ChainHeadUpdateStream = Box<dyn Stream<Item = ()> + Send + Unpin>;

pub trait ChainHeadUpdateListener: Send + Sync + 'static {
    /// Subscribe to chain head updates for the given network.
    fn subscribe(&self, network: String, logger: Logger) -> ChainHeadUpdateStream;
}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, task::Poll};

    use futures03::{Stream, StreamExt, TryStreamExt};

    use crate::{
        blockchain::mock::{MockBlock, MockBlockchain},
        ext::futures::{CancelableError, SharedCancelGuard, StreamExtension},
    };

    use super::{
        BlockStream, BlockStreamError, BlockStreamEvent, BlockWithTriggers, BufferedBlockStream,
        FirehoseCursor,
    };

    #[derive(Debug)]
    struct TestStream {
        number: u64,
    }

    impl BlockStream<MockBlockchain> for TestStream {
        fn buffer_size_hint(&self) -> usize {
            1
        }
    }

    impl Stream for TestStream {
        type Item = Result<BlockStreamEvent<MockBlockchain>, BlockStreamError>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            self.number += 1;
            Poll::Ready(Some(Ok(BlockStreamEvent::ProcessBlock(
                BlockWithTriggers::<MockBlockchain> {
                    block: MockBlock {
                        number: self.number - 1,
                    },
                    trigger_data: vec![],
                },
                FirehoseCursor::None,
            ))))
        }
    }

    #[crate::test]
    async fn consume_stream() {
        let initial_block = 100;
        let buffer_size = 5;

        let stream = Box::new(TestStream {
            number: initial_block,
        });
        let guard = SharedCancelGuard::new();

        let mut stream = BufferedBlockStream::spawn_from_stream(buffer_size, stream)
            .map_err(CancelableError::Error)
            .cancelable(&guard);

        let mut blocks = HashSet::<MockBlock>::new();
        let mut count = 0;
        loop {
            match stream.next().await {
                None if blocks.is_empty() => panic!("None before blocks"),
                Some(Err(CancelableError::Cancel)) => {
                    assert!(guard.is_canceled(), "Guard shouldn't be called yet");

                    break;
                }
                Some(Ok(BlockStreamEvent::ProcessBlock(block_triggers, _))) => {
                    let block = block_triggers.block;
                    blocks.insert(block.clone());
                    count += 1;

                    if block.number > initial_block + buffer_size as u64 {
                        guard.cancel();
                    }
                }
                _ => panic!("Should not happen"),
            };
        }
        assert!(
            blocks.len() > buffer_size,
            "should consume at least a full buffer, consumed {}",
            count
        );
        assert_eq!(count, blocks.len(), "should not have duplicated blocks");
    }
}
