mod error;
mod record_batch;

use std::{
    pin::Pin,
    task::{self, Poll},
};

use anyhow::{anyhow, Result};
use arrow::array::RecordBatch;
use futures03::{stream::BoxStream, Stream, StreamExt, TryStreamExt};
use slog::{debug, info, Logger};

use self::record_batch::Buffer;
use crate::amp::{client::ResponseBatch, error::IsDeterministic, log::Logger as _};

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
    streams: Vec<BoxStream<'static, Result<RecordBatch, Error>>>,
    buffer: Buffer,
    logger: Logger,
    is_finalized: bool,
    is_failed: bool,
}

impl StreamAggregator {
    /// Creates a new stream aggregator from the `streams` with a bounded buffer.
    pub fn new<E>(
        logger: &Logger,
        streams: impl IntoIterator<Item = BoxStream<'static, Result<ResponseBatch, E>>>,
        max_buffer_size: usize,
    ) -> Self
    where
        E: std::error::Error + IsDeterministic + Send + Sync + 'static,
    {
        let logger = logger.component("AmpStreamAggregator");

        let streams = streams
            .into_iter()
            .enumerate()
            .map(|(stream_index, stream)| {
                stream
                    .map_err(move |e| Error::stream(stream_index, e))
                    .try_filter_map(move |response_batch| async move {
                        match response_batch {
                            ResponseBatch::Batch { data } => Ok(Some(data)),
                            ResponseBatch::Reorg(_) => Err(Error::Stream {
                                stream_index,
                                source: anyhow!("chain reorg"),
                                is_deterministic: false,
                            }),
                        }
                    })
                    .boxed()
            })
            .collect::<Vec<_>>();

        let num_streams = streams.len();

        info!(logger, "Initializing stream aggregator";
            "num_streams" => num_streams,
            "max_buffer_size" => max_buffer_size
        );

        Self {
            streams,
            buffer: Buffer::new(num_streams, max_buffer_size),
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

        for (stream_index, stream) in self.streams.iter_mut().enumerate() {
            if self.buffer.is_finalized(stream_index) {
                continue;
            }

            if self.buffer.is_blocked(stream_index) {
                self.is_failed = true;

                return Poll::Ready(Some(Err(Error::Buffer {
                    stream_index,
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
                                stream_index,
                                source: e,
                            });

                    match buffer_result {
                        Ok(()) => {
                            made_progress = true;

                            debug!(self.logger, "Buffered record batch";
                                "stream_index" => stream_index,
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
                    debug!(self.logger, "Received an empty record batch";
                        "stream_index" => stream_index
                    );
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

                    info!(self.logger, "Stream completed";
                        "stream_index" => stream_index,
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
