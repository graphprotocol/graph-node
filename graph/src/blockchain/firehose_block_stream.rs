use async_stream::try_stream;
use futures03::{Stream, StreamExt};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tonic::Status;

use crate::blockchain::TriggerFilter;
use crate::prelude::*;
use crate::util::backoff::ExponentialBackoff;

use super::block_stream::{BlockStream, BlockStreamEvent, FirehoseMapper};
use super::Blockchain;
use crate::{firehose, firehose::FirehoseEndpoint};

pub struct FirehoseBlockStream<C: Blockchain> {
    stream: Pin<Box<dyn Stream<Item = Result<BlockStreamEvent<C>, Error>> + Send>>,
}

impl<C> FirehoseBlockStream<C>
where
    C: Blockchain,
{
    pub fn new<F>(
        endpoint: Arc<FirehoseEndpoint>,
        subgraph_current_block: Option<BlockPtr>,
        cursor: Option<String>,
        mapper: Arc<F>,
        adapter: Arc<C::TriggersAdapter>,
        filter: Arc<C::TriggerFilter>,
        start_blocks: Vec<BlockNumber>,
        logger: Logger,
        grpc_filters: bool,
    ) -> Self
    where
        F: FirehoseMapper<C> + 'static,
    {
        let manifest_start_block_num = start_blocks
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
                manifest_start_block_num,
                subgraph_current_block,
                grpc_filters,
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
    manifest_start_block_num: BlockNumber,
    subgraph_current_block: Option<BlockPtr>,
    grpc_filters: bool,
    logger: Logger,
) -> impl Stream<Item = Result<BlockStreamEvent<C>, Error>> {
    use firehose::ForkStep::*;

    let mut latest_cursor = cursor.unwrap_or_else(|| "".to_string());
    let mut backoff = ExponentialBackoff::new(Duration::from_millis(500), Duration::from_secs(45));
    let mut subgraph_current_block = subgraph_current_block;
    let mut start_block_num = subgraph_current_block
        .as_ref()
        .map(|ptr| {
            // Firehose start block is inclusive while the subgraph_current_block is where the actual
            // subgraph is currently at. So to process the actual next block, we must start one block
            // further in the chain.
            ptr.block_number() + 1 as BlockNumber
        })
        .unwrap_or(manifest_start_block_num);

    // Seems the `try_stream!` macro interfer and don't see we are actually reading/writing this
    #[allow(unused_assignments)]
    let mut skip_backoff = false;

    // Sanity check when starting from a subgraph block ptr directly. When
    // this happens, we must ensure that Firehose first picked block directly follows the
    // subgraph block ptr. So we check that Firehose first picked block's parent is
    // equal to subgraph block ptr.
    //
    // This can happen for example when rewinding, unfailing a deterministic error or
    // when switching from RPC to Firehose on Ethereum.
    //
    // What could go wrong is that the subgraph block ptr points to a forked block but
    // since Firehose only accepts `block_number`, it could pick right away the canonical
    // block of the longuest chain creating inconsistencies in the data (because it would
    // not revert the forked the block).
    //
    // We should perform that only if subgraph actually started from a subgraph block ptr
    // and no Firehose cursor was present. If a Firehose cursor is present, it's used to
    // resume and as such, there is no need to perform this check (at the same time, it's
    // not a bad check to make).
    let mut check_subgraph_continuity = latest_cursor == "" && subgraph_current_block.is_some();

    try_stream! {
        loop {
            info!(
                &logger,
                "Blockstream disconnected, connecting";
                "endpoint_uri" => format_args!("{}", endpoint),
                "start_block" => start_block_num,
                "cursor" => &latest_cursor,
            );
            skip_backoff = false;

            let mut request = firehose::Request {
                start_block_num: start_block_num as i64,
                start_cursor: latest_cursor.clone(),
                fork_steps: vec![StepNew as i32, StepUndo as i32],
                ..Default::default()
            };

            if grpc_filters {
                request.transforms = filter.as_ref().clone().to_firehose_filter();
            }

            let result = endpoint
            .clone()
            .stream_blocks(request).await;

            match result {
                Ok(stream) => {
                    info!(&logger, "Blockstream connected");
                    backoff.reset();

                    let mut expected_stream_end = false;

                    for await response in stream {
                        match process_firehose_response(
                            response,
                            &mut check_subgraph_continuity,
                            manifest_start_block_num,
                            subgraph_current_block.as_ref(),
                            mapper.as_ref(),
                            &adapter,
                            &filter,
                            &logger,
                        ).await {
                            Ok(BlockResponse::Proceed(event, cursor)) => {
                                yield event;

                                latest_cursor = cursor;
                            },
                            Ok(BlockResponse::Rewind(revert_to)) => {
                                // It's totally correct to pass the None as the cursor here, if we are here, there
                                // was no cursor before anyway, so it's totally fine to pass `None`
                                yield BlockStreamEvent::Revert(revert_to.clone(), None);

                                latest_cursor = "".to_string();
                                skip_backoff = true;

                                // We must restart the stream to ensure we now send block from revert_to point
                                // and we add + 1 to start block num because Firehose is inclusive and as such,
                                // we need to move to "next" block.
                                start_block_num = revert_to.number + 1;
                                subgraph_current_block = Some(revert_to);
                                expected_stream_end = true;
                                break;
                            },
                            Err(err) => {
                                error!(logger, "{:#}", err);
                                expected_stream_end = true;
                                break;
                            }
                        }
                    }

                    if !expected_stream_end {
                        error!(logger, "Stream blocks complete unexpectedly, expecting stream to always stream blocks");
                    }
                },
                Err(e) => {
                    error!(logger, "Unable to connect to endpoint: {:?}", e);
                }
            }

            // If we reach this point, we must wait a bit before retrying, unless `skip_backoff` is true
            if !skip_backoff {
                backoff.sleep_async().await;
            }
        }
    }
}

enum BlockResponse<C: Blockchain> {
    Proceed(BlockStreamEvent<C>, String),
    Rewind(BlockPtr),
}

async fn process_firehose_response<C: Blockchain, F: FirehoseMapper<C>>(
    result: Result<firehose::Response, Status>,
    check_subgraph_continuity: &mut bool,
    manifest_start_block_num: BlockNumber,
    subgraph_current_block: Option<&BlockPtr>,
    mapper: &F,
    adapter: &C::TriggersAdapter,
    filter: &C::TriggerFilter,
    logger: &Logger,
) -> Result<BlockResponse<C>, Error> {
    let response = match result {
        Ok(v) => v,
        Err(e) => return Err(anyhow!("An error occurred while streaming blocks: {:?}", e)),
    };

    let event = mapper
        .to_block_stream_event(logger, &response, adapter, filter)
        .await
        .context("Mapping block to BlockStreamEvent failed")?;

    if *check_subgraph_continuity {
        info!(logger, "Firehose started from a subgraph pointer without an existing cursor, ensuring chain continuity");

        if let BlockStreamEvent::ProcessBlock(ref block, _) = event {
            let previous_block_ptr = block.parent_ptr();
            if previous_block_ptr.is_some() && previous_block_ptr.as_ref() != subgraph_current_block
            {
                warn!(&logger,
                    "Firehose selected first streamed block's parent should match subgraph start block, reverting to last know final chain segment";
                    "subgraph_current_block" => &subgraph_current_block.unwrap(),
                    "firehose_start_block" => &previous_block_ptr.unwrap(),
                );

                let mut revert_to = mapper
                    .final_block_ptr_for(logger, &block.block)
                    .await
                    .context("Could not fetch final block to revert to")?;

                if revert_to.number < manifest_start_block_num {
                    warn!(&logger, "We would return before subgraph manifest's start block, limiting rewind to manifest's start block");

                    // We must revert up to parent's of manifest start block to ensure we delete everything "including" the start
                    // block that was processed.
                    let mut block_num = manifest_start_block_num - 1;
                    if block_num < 0 {
                        block_num = 0;
                    }

                    revert_to = mapper
                        .block_ptr_for_number(logger, block_num)
                        .await
                        .context("Could not fetch manifest start block to revert to")?;
                }

                return Ok(BlockResponse::Rewind(revert_to));
            }
        }

        info!(
            logger,
            "Subgraph chain continuity is respected, proceeding normally"
        );
        *check_subgraph_continuity = false;
    }

    Ok(BlockResponse::Proceed(event, response.cursor))
}

impl<C: Blockchain> Stream for FirehoseBlockStream<C> {
    type Item = Result<BlockStreamEvent<C>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

impl<C: Blockchain> BlockStream<C> for FirehoseBlockStream<C> {}
