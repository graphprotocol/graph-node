use async_stream::try_stream;
use futures03::{Stream, StreamExt};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::firehose::endpoints::FirehoseEndpoint;
use crate::prelude::*;
use crate::util::backoff::ExponentialBackoff;

use super::block_stream::{BlockStream, BlockStreamEvent, FirehoseMapper};
use super::Blockchain;
use crate::firehose::bstream;

pub struct FirehoseBlockStream<C: Blockchain> {
    stream: Pin<Box<dyn Stream<Item = Result<BlockStreamEvent<C>, Error>>>>,
}

impl<C> FirehoseBlockStream<C>
where
    C: Blockchain,
{
    pub fn new<F>(
        endpoint: Arc<FirehoseEndpoint>,
        cursor: Option<String>,
        mapper: Arc<F>,
        adapter: Arc<C::TriggersAdapter>,
        filter: Arc<C::TriggerFilter>,
        start_blocks: Vec<BlockNumber>,
        logger: Logger,
    ) -> Self
    where
        F: FirehoseMapper<C> + 'static,
    {
        let start_block_num: BlockNumber = start_blocks
            .into_iter()
            .min()
            // Firehose knows where to start the stream for the specific chain, 0 here means
            // start at Genesis block.
            .unwrap_or(0);

        FirehoseBlockStream {
            stream: Box::pin(stream_blocks(
                endpoint,
                cursor,
                mapper,
                adapter,
                filter,
                start_block_num,
                logger,
            )),
        }
    }
}

fn stream_blocks<C: Blockchain, F: FirehoseMapper<C>>(
    endpoint: Arc<FirehoseEndpoint>,
    cursor: Option<String>,
    mapper: Arc<F>,
    adapter: Arc<C::TriggersAdapter>,
    filter: Arc<C::TriggerFilter>,
    start_block_num: BlockNumber,
    logger: Logger,
) -> impl Stream<Item = Result<BlockStreamEvent<C>, Error>> {
    use bstream::ForkStep::*;

    try_stream! {
        let mut latest_cursor = cursor.unwrap_or_else(|| "".to_string());
        let mut backoff = ExponentialBackoff::new(Duration::from_millis(500), Duration::from_secs(45));

        loop {
            info!(
                &logger,
                "Blockstream disconnected, connecting";
                "endpoint_uri" => format_args!("{}", endpoint),
                "start_block" => start_block_num,
                "cursor" => &latest_cursor,
            );

            let result = endpoint
            .clone()
            .stream_blocks(bstream::BlocksRequestV2 {
                start_block_num: start_block_num as i64,
                start_cursor: latest_cursor.clone(),
                fork_steps: vec![StepNew as i32, StepUndo as i32],
                ..Default::default()
            }).await;

            match result {
                Ok(stream) => {
                    info!(&logger, "Blockstream connected");
                    backoff.reset();

                    for await response in stream {
                        match response {
                            Ok(v) => {
                                match mapper.to_block_stream_event(&logger, &v, &adapter, &filter) {
                                    Ok(event) => {
                                        yield event;

                                        latest_cursor = v.cursor;
                                    },
                                    Err(e) => {
                                        error!(
                                            logger,
                                            "Mapping block to BlockStreamEvent failed: {:?}", e
                                        );
                                        break;
                                    }
                                }
                            },
                            Err(e) => {
                                info!(logger, "An error occurred while streaming blocks: {:?}", e);
                                break;
                            }
                        }
                    }

                    error!(logger, "Stream blocks complete unexpectedly, expecting stream to always stream blocks");
                },
                Err(e) => {
                    error!(logger, "Unable to connect to endpoint: {:?}", e);
                }
            }

            // If we reach this point, we must wait a bit before retrying
            backoff.sleep_async().await;
        }
    }
}

impl<C: Blockchain> Stream for FirehoseBlockStream<C> {
    type Item = Result<BlockStreamEvent<C>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        return self.stream.poll_next_unpin(cx);
    }
}

impl<C: Blockchain> BlockStream<C> for FirehoseBlockStream<C> {}
