use crate::blockchain::block_stream::{
    BlockStream, BlockStreamError, BlockStreamEvent, FirehoseCursor,
};
use crate::blockchain::{BlockTime, Blockchain};
use crate::components::store::{SegmentDetails, SubgraphSegment};
use crate::prelude::*;
use crate::util::backoff::ExponentialBackoff;
use async_stream::try_stream;
use futures03::{Stream, StreamExt};
use std::ops::Range;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use super::IndexerStore;

pub const INDEXER_STREAM_BUFFER_STREAM_SIZE: usize = 10;

pub struct IndexerBlockStream<C: Blockchain> {
    //fixme: not sure if this is ok to be set as public, maybe
    // we do not want to expose the stream to the caller
    stream: Pin<Box<dyn Stream<Item = Result<BlockStreamEvent<C>, BlockStreamError>> + Send>>,
}

impl<C> IndexerBlockStream<C>
where
    C: Blockchain,
{
    pub fn new(
        from: DeploymentHash,
        store: Arc<dyn IndexerStore>,
        subgraph_current_block: Option<BlockPtr>,
        start_blocks: Vec<BlockNumber>,
        end_blocks: Vec<BlockNumber>,
        logger: Logger,
        handler: String,
        registry: Arc<MetricsRegistry>,
    ) -> Self {
        let metrics = IndexerStreamMetrics::new(registry, from.clone());

        let start_block = start_blocks.iter().min();
        let start_block = match (subgraph_current_block, start_block) {
            (None, None) => 0,
            (None, Some(i)) => *i,
            (Some(ptr), _) => ptr.number,
        };

        IndexerBlockStream {
            stream: Box::pin(stream_blocks(
                from,
                store,
                logger,
                handler,
                metrics,
                start_block,
                end_blocks.iter().min().map(|n| *n),
            )),
        }
    }
}

fn stream_blocks<C: Blockchain>(
    from: DeploymentHash,
    store: Arc<dyn IndexerStore>,
    logger: Logger,
    handler: String,
    _metrics: IndexerStreamMetrics,
    start_block: BlockNumber,
    stop_block: Option<BlockNumber>,
) -> impl Stream<Item = Result<BlockStreamEvent<C>, BlockStreamError>> {
    // Back off exponentially whenever we encounter a connection error or a stream with bad data
    let _backoff = ExponentialBackoff::new(Duration::from_millis(500), Duration::from_secs(45));
    let stop_block = stop_block.unwrap_or(i32::MAX);

    // This attribute is needed because `try_stream!` seems to break detection of `skip_backoff` assignments
    #[allow(unused_assignments)]
    let mut skip_backoff = false;

    try_stream! {
            let logger = logger.new(o!("from_deployment" => from.clone()));

        loop {
            info!(
                &logger,
                "IndexerStream starting";
                "start_block" => start_block,
                "stop_block" => stop_block,
            );

            // We just reconnected, assume that we want to back off on errors
            skip_backoff = false;

            let (tx, mut rx) = mpsc::channel(100);
            let store = store.cheap_clone();
            let handle = crate::spawn(async move {
               store.stream_from(start_block, tx).await
            });


            info!(&logger, "IndexerStream started");

            // Track the time it takes to set up the block stream
            // metrics.observe_successful_connection(&mut connect_start, &endpoint.provider);

            let mut last = 0;
            loop {
                let response = rx.recv().await;
                match response {
                    None => {
                        debug!(&logger, "channel has been closed");
                        break;
                    },
                    Some((block_ptr, triggers)) => {
                        if block_ptr.number>last+1000 {
                            debug!(&logger,"block_ptr: {}", block_ptr);
                            last = block_ptr.number;
                        }
                        yield BlockStreamEvent::ProcessWasmBlock(block_ptr, BlockTime::NONE, triggers.0, handler.clone(), FirehoseCursor::None);
                    }
                  }

            }
            match handle.await {
                Ok(_) => {debug!(&logger, "restart...")},
                Err(err) => debug!(&logger, "IndexerStream error, restarting. error: {:?}", err),
            };
        }
    }
}

impl<C: Blockchain> Stream for IndexerBlockStream<C> {
    type Item = Result<BlockStreamEvent<C>, BlockStreamError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

impl<C: Blockchain> BlockStream<C> for IndexerBlockStream<C> {
    fn buffer_size_hint(&self) -> usize {
        INDEXER_STREAM_BUFFER_STREAM_SIZE
    }
}

struct IndexerStreamMetrics {
    deployment: DeploymentHash,
    restarts: CounterVec,
    connect_duration: GaugeVec,
    time_between_responses: HistogramVec,
    responses: CounterVec,
}

impl IndexerStreamMetrics {
    pub fn new(registry: Arc<MetricsRegistry>, deployment: DeploymentHash) -> Self {
        Self {
            deployment,
            restarts: registry
                .global_counter_vec(
                    "deployment_substreams_blockstream_restarts",
                    "Counts the number of times a Substreams block stream is (re)started",
                    vec!["deployment", "provider", "success"].as_slice(),
                )
                .unwrap(),

            connect_duration: registry
                .global_gauge_vec(
                    "deployment_substreams_blockstream_connect_duration",
                    "Measures the time it takes to connect a Substreams block stream",
                    vec!["deployment", "provider"].as_slice(),
                )
                .unwrap(),

            time_between_responses: registry
                .global_histogram_vec(
                    "deployment_substreams_blockstream_time_between_responses",
                    "Measures the time between receiving and processing Substreams stream responses",
                    vec!["deployment", "provider"].as_slice(),
                )
                .unwrap(),

            responses: registry
                .global_counter_vec(
                    "deployment_substreams_blockstream_responses",
                    "Counts the number of responses received from a Substreams block stream",
                    vec!["deployment", "provider", "kind"].as_slice(),
                )
                .unwrap(),
        }
    }

    fn observe_successful_connection(&self, time: &mut Instant, provider: &str) {
        self.restarts
            .with_label_values(&[&self.deployment, &provider, "true"])
            .inc();
        self.connect_duration
            .with_label_values(&[&self.deployment, &provider])
            .set(time.elapsed().as_secs_f64());

        // Reset last connection timestamp
        *time = Instant::now();
    }

    fn observe_failed_connection(&self, time: &mut Instant, provider: &str) {
        self.restarts
            .with_label_values(&[&self.deployment, &provider, "false"])
            .inc();
        self.connect_duration
            .with_label_values(&[&self.deployment, &provider])
            .set(time.elapsed().as_secs_f64());

        // Reset last connection timestamp
        *time = Instant::now();
    }

    fn observe_response(&self, kind: &str, time: &mut Instant, provider: &str) {
        self.time_between_responses
            .with_label_values(&[&self.deployment, &provider])
            .observe(time.elapsed().as_secs_f64());
        self.responses
            .with_label_values(&[&self.deployment, &provider, kind])
            .inc();

        // Reset last response timestamp
        *time = Instant::now();
    }
}

/// Gets the next range of blocks that is ready to stream
/// Returns the range that includes the start block if one is provided.
/// If start block is provided it will return the range [start_block,`current_block`[  
/// wthin the relevant segment.
/// If start block is None, the same range of the lowest segment is returned.
/// When the `current_block` of the segment is lower or eql the provided start_block then (start_block, start_block)
/// is returned indicating there are no new blocks for processing.
async fn next_segment_block_range(
    segments: &Vec<SegmentDetails>,
    start_block: Option<BlockNumber>,
    range_size: i32,
) -> Result<Option<Range<BlockNumber>>, StoreError> {
    // Start block will be included in the range
    fn take_n_blocks(
        segments: &Vec<SegmentDetails>,
        start_block: BlockNumber,
        n: i32,
    ) -> Option<Range<BlockNumber>> {
        let mut stop_block = start_block;

        for segment in segments {
            let is_complete = segment.is_complete();
            let starts_after_this_segment = start_block >= segment.stop_block;

            match (is_complete, starts_after_this_segment) {
                (true, true) => continue,
                // [start_block, stop_block] + next segment
                (true, false) => {
                    stop_block = segment.stop_block;

                    let size = stop_block - start_block;
                    if size >= n {
                        return Some(start_block..start_block + n);
                    }

                    continue;
                }
                (false, true) => return None,
                // last segment we can process
                (false, false) => {
                    stop_block = match segment.current_block {
                        // at this point either this is the first segment and stop_block == start_block
                        // or a previous segment has been included.
                        Some(curr) if curr > stop_block => curr,
                        _ => stop_block,
                    };

                    if start_block == stop_block {
                        return None;
                    }

                    break;
                }
            }
        }

        let size = stop_block - start_block;
        if size >= n {
            return Some(start_block..start_block + n);
        }

        Some(start_block..stop_block)
    }

    if segments.is_empty() {
        return Ok(None);
    }

    let start_block = match start_block {
        Some(sb) => sb,
        None => {
            return Ok(take_n_blocks(
                &segments,
                segments.first().unwrap().start_block,
                range_size,
            ))
        }
    };

    if segments.last().unwrap().stop_block >= start_block {
        return Ok(Some(start_block..i32::MAX));
    }

    Ok(take_n_blocks(&segments, start_block, range_size))
}

#[cfg(test)]
mod test {
    use std::ops::Range;

    use crate::components::store::{BlockNumber, DeploymentId, SegmentDetails};

    #[ignore]
    #[tokio::test]
    async fn next_segment_block_range() {
        struct Case<'a> {
            name: &'a str,
            segments: Vec<(BlockNumber, BlockNumber, Option<BlockNumber>)>,
            start_block: Option<BlockNumber>,
            range_size: i32,
            result: Option<Range<BlockNumber>>,
        }

        let cases = vec![
            Case {
                name: "no segments",
                segments: vec![],
                start_block: todo!(),
                range_size: todo!(),
                result: todo!(),
            },
            Case {
                name: "none start block",
                segments: vec![],
                start_block: todo!(),
                range_size: todo!(),
                result: todo!(),
            },
            Case {
                name: "available blocks in segments shorter than range",
                segments: vec![],
                start_block: todo!(),
                range_size: todo!(),
                result: todo!(),
            },
            Case {
                name: "no more blocks in segments",
                segments: vec![],
                start_block: todo!(),
                range_size: todo!(),
                result: todo!(),
            },
            Case {
                name: "segments no completed",
                segments: vec![],
                start_block: todo!(),
                range_size: todo!(),
                result: todo!(),
            },
        ];

        for case in cases.into_iter() {
            let Case {
                name,
                segments,
                start_block,
                range_size,
                result,
            } = case;
            let segments = segments
                .into_iter()
                .map(|(start, stop, current)| SegmentDetails {
                    id: crate::components::store::SubgraphSegmentId(0),
                    deployment: DeploymentId(0),
                    start_block: start,
                    stop_block: stop,
                    current_block: current,
                })
                .collect();

            let range = super::next_segment_block_range(&segments, start_block, range_size)
                .await
                .unwrap();
            assert_eq!(
                range, result,
                "case failed: {}. Expected {:?} and got {:?}",
                name, result, range
            );
        }
    }
}
