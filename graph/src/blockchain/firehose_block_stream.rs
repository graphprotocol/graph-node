use futures03::{FutureExt, Stream, StreamExt};
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::prelude::*;
use crate::sf::endpoints::FirehoseEndpoint;

use super::block_stream::{BlockStreamEvent, FirehoseMapper};
use super::Blockchain;
use crate::blockchain::block_stream::BlockStream;
use crate::sf::bstream;

pub struct FirehoseBlockStreamContext<C>
where
    C: Blockchain,
{
    cursor: String,
    mapper: Arc<C::FirehoseMapper>,
    node_id: NodeId,
    subgraph_id: DeploymentHash,
    filter: Arc<C::TriggerFilter>,
    start_blocks: Vec<BlockNumber>,
    logger: Logger,
}

impl<C: Blockchain> Clone for FirehoseBlockStreamContext<C> {
    fn clone(&self) -> Self {
        Self {
            cursor: self.cursor.clone(),
            mapper: self.mapper.clone(),
            node_id: self.node_id.clone(),
            subgraph_id: self.subgraph_id.clone(),
            filter: self.filter.clone(),
            start_blocks: self.start_blocks.clone(),
            logger: self.logger.clone(),
        }
    }
}

enum BlockStreamState {
    Disconneted,
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

pub struct FirehoseBlockStream<C: Blockchain> {
    endpoint: Arc<FirehoseEndpoint>,
    state: BlockStreamState,
    ctx: FirehoseBlockStreamContext<C>,
    connection_attempts: u64,
}

impl<C> FirehoseBlockStream<C>
where
    C: Blockchain,
{
    pub fn new(
        endpoint: Arc<FirehoseEndpoint>,
        cursor: String,
        mapper: Arc<C::FirehoseMapper>,
        node_id: NodeId,
        subgraph_id: DeploymentHash,
        filter: Arc<C::TriggerFilter>,
        start_blocks: Vec<BlockNumber>,
        logger: Logger,
    ) -> Self {
        FirehoseBlockStream {
            endpoint,
            state: BlockStreamState::Disconneted,
            ctx: FirehoseBlockStreamContext {
                cursor,
                mapper,
                node_id,
                subgraph_id,
                logger,
                filter,
                start_blocks,
            },
            connection_attempts: 0,
        }
    }
}

impl<C: Blockchain> BlockStream<C> for FirehoseBlockStream<C> {}

impl<C: Blockchain> Stream for FirehoseBlockStream<C> {
    type Item = Result<BlockStreamEvent<C>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                BlockStreamState::Disconneted => {
                    info!(
                        self.ctx.logger,
                        "Blockstream disconnected, (re-)connecting"; "endpoint uri" => format_args!("{}", self.endpoint),
                    );

                    let start_block_num: BlockNumber = self
                        .ctx
                        .start_blocks
                        .clone()
                        .into_iter()
                        .min()
                        .unwrap_or(BLOCK_NUMBER_MAX);

                    let future = self
                        .endpoint
                        .clone()
                        .stream_blocks(bstream::BlocksRequestV2 {
                            start_block_num: start_block_num as i64,
                            start_cursor: self.ctx.cursor.clone(),
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
                            error!(self.ctx.logger, "Unable to connect to endpoint {}", e);
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
                            error!(self.ctx.logger, "Unable to connect to endpoint {}", e);
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
                    Poll::Ready(Some(Ok(b))) => {
                        match self.ctx.mapper.to_block_stream_event(&b, &self.ctx.filter) {
                            Ok(event) => {
                                return Poll::Ready(Some(Ok(event)));
                            }
                            Err(e) => {
                                error!(
                                    self.ctx.logger,
                                    "Mapping block to BlockStreamEvent failed {}", e
                                );
                                self.state = BlockStreamState::Disconneted;

                                return self.schedule_error_retry(cx);
                            }
                        }
                    }

                    Poll::Ready(Some(Err(e))) => {
                        error!(self.ctx.logger, "Stream disconnected from endpoint {}", e);
                        self.state = BlockStreamState::Disconneted;

                        return self.schedule_error_retry(cx);
                    }

                    Poll::Ready(None) => {
                        error!(self.ctx.logger, "Stream has terminated blocks range, we expect never ending stream right now");
                        self.state = BlockStreamState::Disconneted;

                        return self.schedule_error_retry(cx);
                    }

                    Poll::Pending => {
                        trace!(
                            self.ctx.logger,
                            "Stream is pending, no item available yet will being wake up"
                        );

                        return Poll::Pending;
                    }
                },
            }
        }
    }
}

impl<C: Blockchain> FirehoseBlockStream<C> {
    /// Schedule a delayed function that will wake us later in time. This implementation
    /// uses an exponential backoff strategy to retry with incremental longer delays.
    fn schedule_error_retry<T>(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.connection_attempts += 1;
        let wait_duration =
            std::time::Duration::from_secs(wait_duration_secs(self.connection_attempts.clone()));

        let waker = cx.waker().clone();
        tokio::spawn(async move {
            tokio::time::sleep(wait_duration).await;
            waker.wake();
        });

        Poll::Pending
    }
}

fn wait_duration_secs(attempt_number: u64) -> u64 {
    let mut pow = attempt_number;
    if pow > 5 {
        pow = 5;
    }
    return 2 << pow;
}
