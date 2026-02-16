//! Mock block stream infrastructure for feeding pre-defined test blocks.
//!
//! These types implement graph-node's `BlockStreamBuilder`/`BlockStream` traits
//! to feed pre-defined test blocks instead of connecting to a real RPC endpoint.
//! This is the core mock: everything else (store, WASM runtime, trigger processing)
//! is real graph-node code.

use async_trait::async_trait;
use graph::blockchain::block_stream::{
    BlockStream, BlockStreamBuilder, BlockStreamError, BlockStreamEvent, BlockWithTriggers,
    FirehoseCursor,
};
use graph::blockchain::{BlockPtr, Blockchain, TriggerFilterWrapper};
use graph::components::store::{DeploymentLocator, SourceableStore};
use graph::futures03::Stream;
use graph::prelude::BlockNumber;
use graph_chain_ethereum::Chain;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};

/// Builds block streams that yield pre-defined blocks from test data.
///
/// Implements `BlockStreamBuilder<Chain>` so it can be plugged into graph-node's
/// `Chain` constructor. Both `build_firehose` and `build_polling` return the
/// same static stream since we don't care about the transport mechanism.
///
/// If `current_block` is provided (e.g., after a restart), the stream skips
/// blocks up to and including that pointer to avoid reprocessing.
pub(super) struct StaticStreamBuilder {
    pub chain: Vec<BlockWithTriggers<Chain>>,
}

#[async_trait]
impl BlockStreamBuilder<Chain> for StaticStreamBuilder {
    async fn build_firehose(
        &self,
        _chain: &Chain,
        _deployment: DeploymentLocator,
        _block_cursor: FirehoseCursor,
        _start_blocks: Vec<BlockNumber>,
        current_block: Option<BlockPtr>,
        _filter: Arc<<Chain as Blockchain>::TriggerFilter>,
        _unified_api_version: graph::data::subgraph::UnifiedMappingApiVersion,
    ) -> anyhow::Result<Box<dyn BlockStream<Chain>>> {
        let current_idx = current_block.map(|current_block| {
            self.chain
                .iter()
                .enumerate()
                .find(|(_, b)| b.ptr() == current_block)
                .map(|(i, _)| i)
                .unwrap_or(0)
        });
        Ok(Box::new(StaticStream::new(self.chain.clone(), current_idx)))
    }

    async fn build_polling(
        &self,
        _chain: &Chain,
        _deployment: DeploymentLocator,
        _start_blocks: Vec<BlockNumber>,
        _source_subgraph_stores: Vec<Arc<dyn SourceableStore>>,
        current_block: Option<BlockPtr>,
        _filter: Arc<TriggerFilterWrapper<Chain>>,
        _unified_api_version: graph::data::subgraph::UnifiedMappingApiVersion,
    ) -> anyhow::Result<Box<dyn BlockStream<Chain>>> {
        let current_idx = current_block.map(|current_block| {
            self.chain
                .iter()
                .enumerate()
                .find(|(_, b)| b.ptr() == current_block)
                .map(|(i, _)| i)
                .unwrap_or(0)
        });
        Ok(Box::new(StaticStream::new(self.chain.clone(), current_idx)))
    }
}

/// A `Stream` that synchronously yields pre-defined blocks one at a time.
///
/// Each `poll_next` call returns the next block immediately (no async waiting).
/// When all blocks have been emitted, returns `None` to signal stream completion,
/// which tells the indexer that sync is done.
struct StaticStream {
    blocks: Vec<BlockWithTriggers<Chain>>,
    current_idx: usize,
}

impl StaticStream {
    /// Create a new stream, optionally skipping past already-processed blocks.
    ///
    /// `skip_to`: If `Some(i)`, start from block `i+1` (block `i` was already processed).
    /// If `None`, start from the beginning.
    fn new(blocks: Vec<BlockWithTriggers<Chain>>, skip_to: Option<usize>) -> Self {
        Self {
            blocks,
            current_idx: skip_to.map(|i| i + 1).unwrap_or(0),
        }
    }
}

impl BlockStream<Chain> for StaticStream {
    fn buffer_size_hint(&self) -> usize {
        1
    }
}

impl Unpin for StaticStream {}

impl Stream for StaticStream {
    type Item = Result<BlockStreamEvent<Chain>, BlockStreamError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        if self.current_idx >= self.blocks.len() {
            return Poll::Ready(None);
        }

        let block = self.blocks[self.current_idx].clone();
        let cursor = FirehoseCursor::from(format!("test-cursor-{}", self.current_idx));
        self.current_idx += 1;

        Poll::Ready(Some(Ok(BlockStreamEvent::ProcessBlock(block, cursor))))
    }
}

/// Thread-safe wrapper around a `BlockStreamBuilder` to allow dynamic replacement.
///
/// Graph-node's `Chain` takes an `Arc<dyn BlockStreamBuilder>` at construction time.
/// This wrapper uses a `Mutex` so we could theoretically swap the inner builder
/// (e.g., for re-running with different blocks), though currently only used once.
pub(super) struct MutexBlockStreamBuilder(pub Mutex<Arc<dyn BlockStreamBuilder<Chain>>>);

#[async_trait]
impl BlockStreamBuilder<Chain> for MutexBlockStreamBuilder {
    async fn build_firehose(
        &self,
        chain: &Chain,
        deployment: DeploymentLocator,
        block_cursor: FirehoseCursor,
        start_blocks: Vec<BlockNumber>,
        subgraph_current_block: Option<BlockPtr>,
        filter: Arc<<Chain as Blockchain>::TriggerFilter>,
        unified_api_version: graph::data::subgraph::UnifiedMappingApiVersion,
    ) -> anyhow::Result<Box<dyn BlockStream<Chain>>> {
        let builder = self
            .0
            .lock()
            .expect("block stream builder lock poisoned")
            .clone();
        builder
            .build_firehose(
                chain,
                deployment,
                block_cursor,
                start_blocks,
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
        unified_api_version: graph::data::subgraph::UnifiedMappingApiVersion,
    ) -> anyhow::Result<Box<dyn BlockStream<Chain>>> {
        let builder = self
            .0
            .lock()
            .expect("block stream builder lock poisoned")
            .clone();
        builder
            .build_polling(
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
