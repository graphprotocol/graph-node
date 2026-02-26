mod error;
mod record_batch;

use std::{
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
};

use anyhow::{anyhow, Result};
use arrow::array::RecordBatch;
use futures03::{stream::BoxStream, Stream, StreamExt, TryStreamExt};
use slog::{debug, info, Logger};

use self::record_batch::Buffer;
use crate::{
    amp::{client::ResponseBatch, error::IsDeterministic, log::Logger as _},
    cheap_clone::CheapClone,
};

pub use self::{
    error::Error,
    record_batch::{RecordBatchGroup, RecordBatchGroups, StreamRecordBatch},
};

/// Reads record batches from multiple streams and groups them by block number and hash pairs.
///
/// Processes each row in the response record batches and groups them by block number
/// and hash. When processing starts for a new block, all data from previous blocks
/// is grouped and streamed in batches.
///
/// The reason the aggregation is required is to ensure compatibility with the existing
/// subgraph storage implementation.
///
/// # Stream requirements
///
/// - Every record batch must have valid block number and hash columns
/// - Every record batch must contain blocks in ascending order
///
/// # Performance
///
/// To ensure data consistency and ordered output, the aggregator waits for slower streams
/// to catch up with faster streams. The output stream speed matches the slowest input stream.
pub struct StreamAggregator {
    named_streams: Vec<(Arc<str>, BoxStream<'static, Result<RecordBatch, Error>>)>,
    buffer: Buffer,
    logger: Logger,

    /// Indicates whether all streams are fully consumed.
    is_finalized: bool,

    /// Indicates whether any stream has produced an error.
    ///
    /// When `true`, the stream aggregator stops polling all other streams.
    is_failed: bool,
}

impl StreamAggregator {
    /// Creates a new stream aggregator from the `streams` with a bounded buffer.
    pub fn new<E>(
        logger: &Logger,
        named_streams: impl IntoIterator<Item = (String, BoxStream<'static, Result<ResponseBatch, E>>)>,
        buffer_size: usize,
    ) -> Self
    where
        E: std::error::Error + IsDeterministic + Send + Sync + 'static,
    {
        let logger = logger.component("AmpStreamAggregator");

        let named_streams = named_streams
            .into_iter()
            .map(|(stream_name, stream)| {
                let stream_name: Arc<str> = stream_name.into();
                (
                    stream_name.cheap_clone(),
                    stream
                        .map_err({
                            let stream_name = stream_name.cheap_clone();
                            move |e| Error::stream(stream_name.cheap_clone(), e)
                        })
                        .try_filter_map({
                            let stream_name = stream_name.cheap_clone();
                            move |response_batch| {
                                let stream_name = stream_name.cheap_clone();
                                async move {
                                    match response_batch {
                                        ResponseBatch::Batch { data } => Ok(Some(data)),
                                        ResponseBatch::Reorg(_) => Err(Error::Stream {
                                            stream_name: stream_name.cheap_clone(),
                                            source: anyhow!("chain reorg"),
                                            is_deterministic: false,
                                        }),
                                    }
                                }
                            }
                        })
                        .boxed(),
                )
            })
            .collect::<Vec<_>>();

        let num_streams = named_streams.len();

        Self {
            named_streams,
            buffer: Buffer::new(num_streams, buffer_size),
            logger,
            is_finalized: false,
            is_failed: false,
        }
    }

    fn poll_all_streams(
        &mut self,
        cx: &mut task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatchGroups, Error>>> {
        let mut made_progress = false;
        let mut needs_repoll = false;

        for (stream_index, (stream_name, stream)) in self.named_streams.iter_mut().enumerate() {
            let logger = self.logger.new(slog::o!(
                "stream_index" => stream_index,
                "stream_name" => stream_name.cheap_clone()
            ));

            if self.buffer.is_finalized(stream_index) {
                continue;
            }

            if self.buffer.is_blocked(stream_index) {
                self.is_failed = true;

                return Poll::Ready(Some(Err(Error::Buffer {
                    stream_name: stream_name.cheap_clone(),
                    source: anyhow!("buffer is blocked"),
                })));
            }

            if !self.buffer.has_capacity(stream_index) {
                continue;
            }

            match stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(record_batch))) if record_batch.num_rows() != 0 => {
                    let buffer_result =
                        self.buffer
                            .extend(stream_index, record_batch)
                            .map_err(|e| Error::Buffer {
                                stream_name: stream_name.cheap_clone(),
                                source: e,
                            });

                    match buffer_result {
                        Ok(()) => {
                            made_progress = true;
                            needs_repoll = true;

                            debug!(logger, "Buffered record batch";
                                "buffer_size" => self.buffer.size(stream_index),
                                "has_capacity" => self.buffer.has_capacity(stream_index)
                            );
                        }
                        Err(e) => {
                            self.is_failed = true;

                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }
                Poll::Ready(Some(Ok(_empty_record_batch))) => {
                    debug!(logger, "Received an empty record batch");
                    needs_repoll = true;
                }
                Poll::Ready(Some(Err(e))) => {
                    self.is_failed = true;

                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(None) => {
                    self.buffer.finalize(stream_index);

                    if self.buffer.all_finalized() {
                        self.is_finalized = true;
                    }

                    made_progress = true;

                    info!(logger, "Stream completed";
                        "buffer_size" => self.buffer.size(stream_index)
                    );
                }
                Poll::Pending => {
                    //
                }
            }
        }

        if made_progress {
            if let Some(completed_groups) =
                self.buffer.completed_groups().map_err(Error::Aggregation)?
            {
                debug!(self.logger, "Sending completed record batch groups";
                    "num_completed_groups" => completed_groups.len()
                );

                return Poll::Ready(Some(Ok(completed_groups)));
            }
        }

        if self.is_finalized {
            info!(self.logger, "All streams completed");
            return Poll::Ready(None);
        }

        // When any stream returned `Poll::Ready` but we couldn't produce
        // output (e.g. empty batch, or data buffered but no completed
        // groups yet), the waker was consumed by that stream's poll call
        // and won't be re-registered until we poll it again. Schedule an
        // immediate re-poll so those streams get polled again and their
        // wakers are properly re-registered.
        if needs_repoll {
            cx.waker().wake_by_ref();
        }

        Poll::Pending
    }
}

impl Stream for StreamAggregator {
    type Item = Result<RecordBatchGroups, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_finalized || self.is_failed {
            return Poll::Ready(None);
        }

        self.poll_all_streams(cx)
    }
}
