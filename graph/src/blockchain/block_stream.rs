use anyhow::Error;
use async_stream::stream;
use futures03::Stream;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc::{self, Receiver, Sender};

use super::{Block, BlockPtr, Blockchain};
use crate::components::store::BlockNumber;
use crate::firehose;
use crate::{prelude::*, prometheus::labels};

pub struct BufferedBlockStream<C: Blockchain> {
    inner: Pin<Box<dyn Stream<Item = Result<BlockStreamEvent<C>, Error>> + Send>>,
}

impl<C: Blockchain + 'static> BufferedBlockStream<C> {
    pub fn spawn_from_stream(
        stream: Box<dyn BlockStream<C>>,
        size_hint: usize,
    ) -> Box<dyn BlockStream<C>> {
        let (sender, receiver) = mpsc::channel::<Result<BlockStreamEvent<C>, Error>>(size_hint);
        crate::spawn(async move { BufferedBlockStream::stream_blocks(stream, sender).await });

        Box::new(BufferedBlockStream::new(receiver))
    }

    pub fn new(mut receiver: Receiver<Result<BlockStreamEvent<C>, Error>>) -> Self {
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
        sender: Sender<Result<BlockStreamEvent<C>, Error>>,
    ) -> Result<(), Error> {
        loop {
            let event = match stream.next().await {
                Some(evt) => evt,
                None => {
                    break;
                }
            };

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

impl<C: Blockchain> BlockStream<C> for BufferedBlockStream<C> {}

impl<C: Blockchain> Stream for BufferedBlockStream<C> {
    type Item = Result<BlockStreamEvent<C>, Error>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

pub trait BlockStream<C: Blockchain>:
    Stream<Item = Result<BlockStreamEvent<C>, Error>> + Unpin + Send
{
}

pub type FirehoseCursor = Option<String>;

pub struct BlockWithTriggers<C: Blockchain> {
    pub block: C::Block,
    pub trigger_data: Vec<C::TriggerData>,
}

impl<C: Blockchain> BlockWithTriggers<C> {
    pub fn new(block: C::Block, mut trigger_data: Vec<C::TriggerData>) -> Self {
        // This is where triggers get sorted.
        trigger_data.sort();
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
}

#[async_trait]
pub trait TriggersAdapter<C: Blockchain>: Send + Sync {
    // Return the block that is `offset` blocks before the block pointed to
    // by `ptr` from the local cache. An offset of 0 means the block itself,
    // an offset of 1 means the block's parent etc. If the block is not in
    // the local cache, return `None`
    fn ancestor_block(&self, ptr: BlockPtr, offset: BlockNumber)
        -> Result<Option<C::Block>, Error>;

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
    ) -> Result<Vec<BlockWithTriggers<C>>, Error>;

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
}

#[async_trait]
pub trait FirehoseMapper<C: Blockchain>: Send + Sync {
    async fn to_block_stream_event(
        &self,
        logger: &Logger,
        response: &firehose::Response,
        adapter: &C::TriggersAdapter,
        filter: &C::TriggerFilter,
    ) -> Result<BlockStreamEvent<C>, FirehoseError>;
}

#[derive(Error, Debug)]
pub enum FirehoseError {
    /// We were unable to decode the received block payload into the chain specific Block struct (e.g. chain_ethereum::pb::Block)
    #[error("received gRPC block payload cannot be decoded")]
    DecodingError(#[from] prost::DecodeError),

    /// Some unknown error occured
    #[error("unknown error")]
    UnknownError(#[from] anyhow::Error),
}

pub enum BlockStreamEvent<C: Blockchain> {
    // The payload is the current subgraph head pointer, which should be reverted, such that the
    // parent of the current subgraph head becomes the new subgraph head.
    // An optional pointer to the parent block will save a round trip operation when reverting.
    Revert(BlockPtr, FirehoseCursor, Option<BlockPtr>),

    ProcessBlock(BlockWithTriggers<C>, FirehoseCursor),
}

#[derive(Clone)]
pub struct BlockStreamMetrics {
    pub deployment_head: Box<Gauge>,
    pub deployment_failed: Box<Gauge>,
    pub reverted_blocks: Box<Gauge>,
    pub stopwatch: StopwatchMetrics,
}

impl BlockStreamMetrics {
    pub fn new(
        registry: Arc<impl MetricsRegistry>,
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
                "Track the head block number for a deployment",
                labels.clone(),
            )
            .expect("failed to create `deployment_head` gauge");
        let deployment_failed = registry
            .new_gauge(
                "deployment_failed",
                "Boolean gauge to indicate whether the deployment has failed (1 == failed)",
                labels,
            )
            .expect("failed to create `deployment_failed` gauge");
        Self {
            deployment_head,
            deployment_failed,
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

    use anyhow::Error;
    use futures03::{Stream, StreamExt, TryStreamExt};

    use crate::{
        blockchain::mock::{MockBlock, MockBlockchain},
        ext::futures::{CancelableError, SharedCancelGuard, StreamExtension},
    };

    use super::{BlockStream, BlockStreamEvent, BlockWithTriggers, BufferedBlockStream};

    #[derive(Debug)]
    struct TestStream {
        number: u64,
    }

    impl BlockStream<MockBlockchain> for TestStream {}

    impl Stream for TestStream {
        type Item = Result<BlockStreamEvent<MockBlockchain>, Error>;

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
                None,
            ))))
        }
    }

    #[tokio::test]
    async fn consume_stream() {
        let initial_block = 100;
        let buffer_size = 5;

        let stream = Box::new(TestStream {
            number: initial_block,
        });
        let guard = SharedCancelGuard::new();

        let mut stream = BufferedBlockStream::spawn_from_stream(stream, buffer_size)
            .map_err(CancelableError::Error)
            .cancelable(&guard, || Err(CancelableError::Cancel));

        let mut blocks = HashSet::<MockBlock>::new();
        let mut count = 0;
        loop {
            match stream.next().await {
                None if blocks.len() == 0 => panic!("None before blocks"),
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
