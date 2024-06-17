use std::time::Duration;
use std::{collections::HashMap, pin::Pin, sync::Arc};

use crate::blockchain::block_stream::BlockStreamError;
use crate::blockchain::BlockPtr;
use crate::components::metrics::stopwatch::StopwatchMetrics;
use crate::components::store::{
    DeploymentId, SegmentDetails, SubgraphSegment, SubgraphSegmentId, WritableStore,
};
use crate::util::backoff::ExponentialBackoff;
use crate::util::futures::retry;
use crate::{
    blockchain::{
        block_stream::{BlockStreamEvent, FirehoseCursor},
        Blockchain,
    },
    components::store::{DeploymentCursorTracker, DeploymentLocator},
    data::subgraph::UnifiedMappingApiVersion,
    itertools::Itertools,
    prelude::{BlockNumber, CheapClone, ENV_VARS},
    schema::InputSchema,
};
use anyhow::{anyhow, Error, Result};
use async_trait::async_trait;
use borsh::{BorshDeserialize, BorshSerialize};
use futures03::{Stream, StreamExt};
use slog::Logger;
use tokio::{sync::mpsc, time::Instant};

pub mod block_stream;
pub mod store;

pub type Item = Box<[u8]>;

#[derive(Clone, BorshSerialize, BorshDeserialize)]
pub struct StateDelta {
    delta: Vec<StateOperation>,
}

#[derive(BorshSerialize, BorshDeserialize, Clone)]
enum StateOperation {
    Set(Key, Item),
    Unset(Key, Item),
}

// TODO: Maybe this should be a type defined by the store so it can have more efficient representation
// for each store implementation.
#[derive(Default, BorshSerialize, BorshDeserialize)]
pub struct State {
    items: HashMap<Box<[u8]>, Item>,
    tags: HashMap<String, Vec<Box<[u8]>>>,
    deltas: Vec<StateOperation>,
}

impl State {
    pub fn delta(&self) -> StateDelta {
        StateDelta {
            delta: self.deltas.clone(),
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Hash, Clone)]
pub struct Key {
    pub id: Box<[u8]>,
    pub tag: Option<String>,
}

impl State {
    pub fn set_encode<B: BorshSerialize>(&mut self, key: Key, item: B) -> Result<()> {
        self.set(key, borsh::to_vec(&item)?)
    }

    pub fn set(&mut self, _key: Key, _item: impl AsRef<[u8]>) -> Result<()> {
        unimplemented!();
    }
    pub fn get(&mut self, _key: Key) -> Result<Option<Item>> {
        unimplemented!()
    }
    pub fn get_keys(&mut self, tag: &'static str) -> Result<Vec<Key>> {
        let keys = self
            .tags
            .get(tag)
            .unwrap_or(&vec![])
            .into_iter()
            .map(|k| Key {
                id: k.clone(),
                // This is not ideal but the derive macro only works for String, will look into this later
                tag: Some(tag.to_string()),
            })
            .collect_vec();

        Ok(keys)
    }

    pub fn apply(&mut self, _delta: StateDelta) {
        todo!()
    }
}

pub struct EncodedBlock(pub Box<[u8]>);
pub struct EncodedTriggers(pub Box<[u8]>);
pub type BlockSender = mpsc::Sender<(BlockPtr, EncodedTriggers)>;

pub struct TriggerMap(HashMap<BlockNumber, EncodedTriggers>);

#[async_trait]
/// Indexer store is the store where the triggers will be kept to be processed by subgraphs
/// later. The indexer store will be used to populate several logical segments of a dataset,
/// therefore it can not assume to know the full state of the underlying storage at any time.
pub trait IndexerStore: Sync + Send {
    /// Last Stable Block (LSB) is the last block the rest of the system can use
    /// for streaming, copying, whatever else.
    async fn get_last_stable_block(&self) -> Result<Option<BlockNumber>>;
    /// Stream from will send all relevant blocks starting with bn inclusively up to
    /// LSB, forever.
    async fn stream_from(&self, bn: BlockNumber, bs: BlockSender) -> Result<()>;
    /// Get the triggers for a specific block.
    async fn get(&self, bn: BlockNumber, s: SubgraphSegment) -> Result<Option<EncodedTriggers>>;
    /// Set the triggers for a specific block. Set can be called in parallel for different
    /// segments of the Indexer store, therefore, set can assume it will be forward-only within
    /// a segment but not on the entirety of the data.
    async fn set(
        &self,
        bn: BlockPtr,
        s: &SubgraphSegment,
        state: &State,
        triggers: EncodedTriggers,
    ) -> Result<()>;
    /// Get state is currently not implemented and will prolly be removed.
    async fn get_state(&self, bn: BlockNumber) -> Result<State>;
    /// Sets the latest block up to witch data can be consumed.
    async fn set_last_stable_block(&self, segment: SubgraphSegment, bn: BlockNumber) -> Result<()>;
    /// Get segments if set
    async fn get_segments(&self) -> Result<Vec<SubgraphSegment>>;
    /// Create segments if none exist otherwise should be a noop.
    async fn set_segments(
        &self,
        segments: Vec<(BlockNumber, BlockNumber)>,
    ) -> Result<Vec<SubgraphSegment>>;
}

/// BlockTransform the specific transformation to apply to every block, one of the implemtnations
/// will be the WASM mapping.
pub trait BlockTransform: Clone + Sync + Send {
    fn transform(&self, block: EncodedBlock, state: State) -> (State, EncodedTriggers);
}

/// IndexerContext will provide all inputs necessary for the processing
pub struct IndexerContext<B: Blockchain, T: BlockTransform, S: IndexerStore> {
    pub chain: Arc<B>,
    pub transform: Arc<T>,
    pub store: Arc<S>,
    pub deployment: DeploymentLocator,
    pub logger: Logger,
}

impl<B: Blockchain, T: BlockTransform, S: IndexerStore> IndexerContext<B, T, S> {}

#[derive(Clone, Debug)]
struct IndexerCursorTracker {
    schema: InputSchema,
    start_block: BlockNumber,
    stop_block: Option<BlockNumber>,
    firehose_cursor: FirehoseCursor,
}

impl DeploymentCursorTracker for IndexerCursorTracker {
    fn input_schema(&self) -> crate::schema::InputSchema {
        self.schema.cheap_clone()
    }

    fn block_ptr(&self) -> Option<crate::prelude::BlockPtr> {
        None
    }

    fn firehose_cursor(&self) -> FirehoseCursor {
        FirehoseCursor::None
    }
}

/// Split the work in similar sized ranges, the end_block is non inclusive.
fn calculate_segments(
    start_block: BlockNumber,
    stop_block: BlockNumber,
    workers: i16,
) -> Vec<(BlockNumber, BlockNumber)> {
    let total = stop_block - start_block;
    let chunk_size = (total / workers as i32) + 1;
    let mut res = vec![];

    let mut start = start_block;
    loop {
        if start >= stop_block {
            break;
        }
        let end = (start + chunk_size).min(stop_block);
        res.push((start, end));
        start = end;
    }

    res
}

/// The IndexWorker glues all of the other types together and will manage the lifecycle
/// of the pre-indexing.
#[derive(Clone, Debug)]
pub struct IndexWorker {}

impl IndexWorker {
    async fn get_or_create_segments<S>(
        &self,
        store: &Arc<S>,
        start_block: BlockNumber,
        stop_block: BlockNumber,
        workers: i16,
    ) -> Result<Vec<SubgraphSegment>>
    where
        S: IndexerStore + 'static,
    {
        let segments = store.get_segments().await?;
        if !segments.is_empty() {
            return Ok(segments);
        }

        let segments = calculate_segments(start_block, stop_block, workers);

        store
            .set_segments(segments)
            .await
            .map_err(anyhow::Error::from)
    }

    /// Breaks the finite range into segments. Schedules each chunk to a worker tokio task,
    /// this work is IO bound for the most so this should be fine. The upper limit of the
    /// range is going to be chain_head - REORG_THRESHOLD, this range will be processed in parallel
    /// until it catches up to the head - reorg threshold. Afterwards it needs to switch to linear
    /// processing so it can correctly handle reverts. If the start_block is within
    /// 2*REORG_THRESHOLD of head already then linear  should be used to avoid constantly swapping
    /// between the two if the chain is fast enough.
    pub async fn run_many<B, T, S>(
        &self,
        ctx: Arc<IndexerContext<B, T, S>>,
        cursor_tracker: impl DeploymentCursorTracker,
        start_block: BlockNumber,
        stop_block: Option<BlockNumber>,
        filter: Arc<B::TriggerFilter>,
        api_version: UnifiedMappingApiVersion,
        workers: i16,
        stopwatch: StopwatchMetrics,
    ) -> Result<()>
    where
        B: Blockchain + 'static,
        T: BlockTransform + 'static,
        S: IndexerStore + 'static,
    {
        let chain_store = ctx.chain.chain_store();
        let chain_head = forever_async(&ctx.logger, "get chain head", || {
            let chain_store = chain_store.cheap_clone();
            async move {
                chain_store
                    .chain_head_ptr()
                    .await
                    .and_then(|r| r.ok_or(anyhow!("Expected chain head to exist")))
            }
        })
        .await?;

        let chain_head = chain_head.block_number() - ENV_VARS.reorg_threshold;
        let stop_block = match stop_block {
            Some(stop_block) => stop_block.min(chain_head),
            None => chain_head,
        };

        let segments = self
            .get_or_create_segments(&ctx.store, start_block, stop_block, workers)
            .await?;

        let mut handles = vec![];
        for (i, segment) in segments.iter().enumerate() {
            if segment.is_complete() {
                continue;
            }
            // Handle this more gracefully, if AllBlocks is here then we should just stop run_many
            // and switch to linear processing.
            let details = segment.details().unwrap();
            let cursor_tracker = IndexerCursorTracker {
                schema: cursor_tracker.input_schema(),
                start_block: details.start_block,
                stop_block: Some(details.stop_block),
                firehose_cursor: FirehoseCursor::None,
            };

            let filter = filter.cheap_clone();
            let api_version = api_version.clone();
            let ctx = ctx.cheap_clone();
            let segment = segment.clone();
            let stopwatch = stopwatch.cheap_clone();
            handles.push(crate::spawn(async move {
                let now = Instant::now();
                let r = Self::run(
                    ctx.cheap_clone(),
                    &segment,
                    cursor_tracker,
                    filter,
                    api_version,
                    stopwatch,
                )
                .await;
                if r.is_ok() {
                    // uwnrap: all segments at this point should be finite.
                    ctx.store
                        .set_last_stable_block(segment.clone(), segment.stop_block().unwrap() - 1)
                        .await?;
                }
                let end = Instant::now().duration_since(now).as_secs();
                println!("### task{} finished (took {}s)", i, end);
                r
            }));
        }

        futures03::future::try_join_all(handles)
            .await
            .unwrap()
            .into_iter()
            .collect::<Result<Vec<IndexerCursorTracker>, Error>>()
            .unwrap();

        Ok(())
    }

    /// Performs the indexing work forever, or until the stop_block is reached. Run will
    /// start a new block_stream for the chain.
    async fn run<B, T, S>(
        ctx: Arc<IndexerContext<B, T, S>>,
        segment: &SubgraphSegment,
        mut cursor_tracker: IndexerCursorTracker,
        filter: Arc<B::TriggerFilter>,
        api_version: UnifiedMappingApiVersion,
        stopwatch: StopwatchMetrics,
    ) -> Result<IndexerCursorTracker>
    where
        B: Blockchain,
        T: BlockTransform,
        S: IndexerStore,
    {
        let block_stream = ctx
            .chain
            .new_block_stream(
                ctx.deployment.clone(),
                cursor_tracker.clone(),
                vec![cursor_tracker.start_block],
                filter,
                api_version,
                stopwatch,
            )
            .await?;

        let cursor =
            Self::process_stream(ctx, State::default(), Box::pin(block_stream), segment).await?;
        cursor_tracker.firehose_cursor = cursor;

        Ok(cursor_tracker)
    }

    /// Processes the stream until it ends or stop_block is reached. The stop_block is not
    /// processed, once it's reached the previous cursor should be returned.
    async fn process_stream<B, T, S>(
        ctx: Arc<IndexerContext<B, T, S>>,
        initial_state: State,
        mut stream: Pin<Box<impl Stream<Item = Result<BlockStreamEvent<B>, BlockStreamError>>>>,
        segment: &SubgraphSegment,
    ) -> Result<FirehoseCursor>
    where
        B: Blockchain,
        T: BlockTransform,
        S: IndexerStore,
    {
        let mut firehose_cursor = FirehoseCursor::None;
        let mut previous_state = initial_state;
        let stop_block = match segment {
            SubgraphSegment::AllBlocks => None,
            SubgraphSegment::Range(details) => Some(details.stop_block),
        };

        loop {
            let evt = stream.next().await;

            let cursor = match evt {
                Some(Ok(BlockStreamEvent::ProcessWasmBlock(
                    block_ptr,
                    _block_time,
                    data,
                    _handler,
                    cursor,
                ))) => {
                    if let Some(stop_block) = stop_block {
                        if block_ptr.number >= stop_block {
                            break;
                        }
                    }

                    let (state, triggers) = ctx
                        .transform
                        .transform(EncodedBlock(data), std::mem::take(&mut previous_state));
                    previous_state = state;
                    ctx.store
                        .set(block_ptr, &segment, &previous_state, triggers)
                        .await?;

                    cursor
                }

                Some(Ok(BlockStreamEvent::ProcessBlock(_block, _cursor))) => {
                    unreachable!("Process block not implemented yet")
                }
                Some(Ok(BlockStreamEvent::Revert(revert_to_ptr, cursor))) => {
                    println!("Revert detected to block {}", revert_to_ptr);

                    cursor
                }
                Some(Err(e)) => return Err(e.into()),

                None => {
                    println!("### done!");
                    break;
                }
            };

            firehose_cursor = cursor;
        }

        Ok(firehose_cursor)
    }
}

#[cfg(test)]
mod tests {
    use crate::components::store::BlockNumber;

    #[test]
    fn calculate_segments() {
        #[derive(Debug, Clone)]
        struct Case {
            start: BlockNumber,
            end: BlockNumber,
            workers: i16,
            result: Vec<(i32, i32)>,
        }

        let cases = vec![
            Case {
                start: 1,
                end: 1000,
                workers: 1,
                result: vec![(1, 1000)],
            },
            Case {
                start: 1,
                end: 1000,
                workers: 2,
                result: vec![(1, 501), (501, 1000)],
            },
        ];

        for case in cases.into_iter() {
            let Case {
                start,
                end,
                workers,
                result,
            } = case.clone();

            let res = super::calculate_segments(start, end, workers);
            assert_eq!(result, res, "{:?}", case);
        }
    }
}

// TODO: Re-use something
const BACKOFF_BASE: Duration = Duration::from_millis(100);
const BACKOFF_CEIL: Duration = Duration::from_secs(10);

async fn forever_async<T, F, Fut>(logger: &Logger, op: &str, f: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut backoff = ExponentialBackoff::new(BACKOFF_BASE, BACKOFF_CEIL);
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                slog::error!(&logger, "Failed to {}, retrying...\nerror: {:?}", op, e)
            }
        }
        backoff.sleep_async().await;
    }
}
