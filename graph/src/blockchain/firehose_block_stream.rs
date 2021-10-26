use futures03::{FutureExt, Stream, StreamExt};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::firehose::endpoints::FirehoseEndpoint;
use crate::prelude::*;

use super::block_stream::{BlockStream, BlockStreamEvent, FirehoseMapper};
use super::Blockchain;
use crate::firehose::bstream;

pub struct FirehoseBlockStreamContext<C, F>
where
    C: Blockchain,
    F: FirehoseMapper<C>,
{
    active_cursor: Option<String>,
    last_seen_cursor: Option<String>,
    mapper: Arc<F>,
    adapter: Arc<C::TriggersAdapter>,
    filter: Arc<C::TriggerFilter>,
    start_blocks: Vec<BlockNumber>,
    logger: Logger,
}

enum BlockStreamState {
    Disconnected,
    Connecting(
        Pin<
            Box<
                dyn futures03::Future<
                    Output = Result<tonic::Streaming<bstream::BlockResponseV2>, anyhow::Error>,
                >,
            >,
        >,
    ),
    Connected(tonic::Streaming<bstream::BlockResponseV2>),
}

pub struct FirehoseBlockStream<C: Blockchain, F: FirehoseMapper<C>> {
    endpoint: Arc<FirehoseEndpoint>,
    state: BlockStreamState,
    ctx: FirehoseBlockStreamContext<C, F>,
    connection_attempts: u64,
}

impl<C, F> FirehoseBlockStream<C, F>
where
    C: Blockchain,
    F: FirehoseMapper<C>,
{
    pub fn new(
        endpoint: Arc<FirehoseEndpoint>,
        cursor: Option<String>,
        mapper: Arc<F>,
        adapter: Arc<C::TriggersAdapter>,
        filter: Arc<C::TriggerFilter>,
        start_blocks: Vec<BlockNumber>,
        logger: Logger,
    ) -> Self {
        FirehoseBlockStream {
            endpoint,
            state: BlockStreamState::Disconnected,
            ctx: FirehoseBlockStreamContext {
                active_cursor: cursor,
                last_seen_cursor: None,
                mapper,
                logger,
                adapter,
                filter,
                start_blocks,
            },
            connection_attempts: 0,
        }
    }
}

impl<C: Blockchain, F: FirehoseMapper<C>> BlockStream<C> for FirehoseBlockStream<C, F> {
    fn notify_block_consumed(&mut self) {
        if self.ctx.last_seen_cursor.is_none() {
            info!(
                self.ctx.logger,
                "Received block consumed notification without a last seen cursor present, skipping"
            );
            return;
        }

        // Last seen cursor becomes active by swapping it with active
        std::mem::swap(&mut self.ctx.active_cursor, &mut self.ctx.last_seen_cursor);
        self.ctx.last_seen_cursor = None;
    }
}

impl<C: Blockchain, F: FirehoseMapper<C>> Stream for FirehoseBlockStream<C, F> {
    type Item = Result<BlockStreamEvent<C>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                BlockStreamState::Disconnected => {
                    let start_block_num: BlockNumber = self
                        .ctx
                        .start_blocks
                        .iter()
                        .min()
                        .map(|x| *x)
                        // Firehose knows where to start the stream for the specific chain, 0 here means
                        // start at Genesis block.
                        .unwrap_or(0);

                    info!(
                        self.ctx.logger,
                        "Blockstream disconnected, connecting";
                        "endpoint_uri" => format_args!("{}", self.endpoint),
                        "start_block" => start_block_num,
                        "cursor" => match &self.ctx.active_cursor {
                            Some(v) => v.clone(),
                            None => "<None>".to_string(),
                        },
                    );

                    let future = self
                        .endpoint
                        .clone()
                        .stream_blocks(bstream::BlocksRequestV2 {
                            start_block_num: start_block_num as i64,
                            start_cursor: match &self.ctx.active_cursor {
                                Some(c) => c.clone(),
                                None => "".to_string(),
                            },
                            fork_steps: vec![
                                bstream::ForkStep::StepNew as i32,
                                bstream::ForkStep::StepUndo as i32,
                            ],
                            ..Default::default()
                        });
                    let mut stream_connection = Box::pin(future);

                    match stream_connection.poll_unpin(cx) {
                        Poll::Ready(Ok(streaming)) => {
                            self.state = BlockStreamState::Connected(streaming);
                            self.connection_attempts = 0;
                            info!(self.ctx.logger, "Blockstream connected");

                            // Re-loop to next state
                            continue;
                        }

                        Poll::Ready(Err(e)) => {
                            error!(self.ctx.logger, "Unable to connect to endpoint: {}", e);
                            self.state = BlockStreamState::Disconnected;

                            return self.schedule_error_retry(cx);
                        }

                        Poll::Pending => {
                            trace!(
                                self.ctx.logger,
                                "Connection is still pending when being created"
                            );
                            self.state = BlockStreamState::Connecting(stream_connection);
                            return Poll::Pending;
                        }
                    }
                }

                BlockStreamState::Connecting(stream_connection) => {
                    match stream_connection.poll_unpin(cx) {
                        Poll::Ready(Ok(streaming)) => {
                            self.state = BlockStreamState::Connected(streaming);
                            info!(self.ctx.logger, "Blockstream connected");

                            // Re-loop to next state
                            continue;
                        }

                        Poll::Ready(Err(e)) => {
                            error!(self.ctx.logger, "Unable to connect to endpoint: {}", e);
                            self.state = BlockStreamState::Disconnected;

                            return self.schedule_error_retry(cx);
                        }

                        Poll::Pending => {
                            trace!(
                                self.ctx.logger,
                                "Connection is still pending when being wake up"
                            );
                            return Poll::Pending;
                        }
                    }
                }

                BlockStreamState::Connected(streaming) => match streaming.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(response))) => {
                        match self.ctx.mapper.to_block_stream_event(
                            &self.ctx.logger,
                            &response,
                            &self.ctx.adapter,
                            &self.ctx.filter,
                        ) {
                            Ok(event) => {
                                self.ctx.last_seen_cursor = Some(response.cursor);
                                return Poll::Ready(Some(Ok(event)));
                            }
                            Err(e) => {
                                error!(
                                    self.ctx.logger,
                                    "Mapping block to BlockStreamEvent failed {}", e
                                );
                                self.state = BlockStreamState::Disconnected;

                                return self.schedule_error_retry(cx);
                            }
                        }
                    }

                    Poll::Ready(Some(Err(e))) => {
                        error!(self.ctx.logger, "Stream disconnected from endpoint {}", e);
                        self.state = BlockStreamState::Disconnected;

                        return self.schedule_error_retry(cx);
                    }

                    Poll::Ready(None) => {
                        error!(self.ctx.logger, "Stream has terminated blocks range, we expect never ending stream right now");
                        self.state = BlockStreamState::Disconnected;

                        return self.schedule_error_retry(cx);
                    }

                    Poll::Pending => {
                        trace!(
                            self.ctx.logger,
                            "Stream is pending, no item available yet, going to be wake up later to check again"
                        );

                        return Poll::Pending;
                    }
                },
            }
        }
    }
}

impl<C: Blockchain, F: FirehoseMapper<C>> FirehoseBlockStream<C, F> {
    /// Schedule a delayed function that will wake us later in time. This implementation
    /// uses an exponential backoff strategy to retry with incremental longer delays.
    fn schedule_error_retry<T>(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.connection_attempts += 1;
        let wait_duration = wait_duration(self.connection_attempts);

        let waker = cx.waker().clone();
        tokio::spawn(async move {
            tokio::time::sleep(wait_duration).await;
            waker.wake();
        });

        Poll::Pending
    }
}

fn wait_duration(attempt_number: u64) -> Duration {
    let pow = if attempt_number > 5 {
        5
    } else {
        attempt_number
    };

    Duration::from_secs(2 << pow)
}
