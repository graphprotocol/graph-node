//! Mock `BlockStreamBuilder` that feeds pre-defined test blocks.

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
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

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
