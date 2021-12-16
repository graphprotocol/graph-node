use std::{marker::PhantomData, sync::Arc, time::Duration};

use crate::{
    blockchain::Block as BlockchainBlock,
    components::store::ChainStore,
    firehose::{bstream, decode_firehose_block, endpoints::FirehoseEndpoint},
    prelude::{error, info, Logger},
    util::backoff::ExponentialBackoff,
};
use anyhow::{Context, Error};
use futures03::StreamExt;
use slog::trace;
use tonic::Streaming;

pub struct FirehoseBlockIngestor<M>
where
    M: prost::Message + BlockchainBlock + Default + 'static,
{
    chain_store: Arc<dyn ChainStore>,
    endpoint: Arc<FirehoseEndpoint>,
    logger: Logger,

    phantom: PhantomData<M>,
}

impl<M> FirehoseBlockIngestor<M>
where
    M: prost::Message + BlockchainBlock + Default + 'static,
{
    pub fn new(
        chain_store: Arc<dyn ChainStore>,
        endpoint: Arc<FirehoseEndpoint>,
        logger: Logger,
    ) -> FirehoseBlockIngestor<M> {
        FirehoseBlockIngestor {
            chain_store,
            endpoint,
            logger,
            phantom: PhantomData {},
        }
    }

    pub async fn run(self) {
        let mut latest_cursor = self.fetch_head_cursor().await;
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(250), Duration::from_secs(30));

        loop {
            info!(
                self.logger,
                "Blockstream disconnected, connecting"; "endpoint uri" => format_args!("{}", self.endpoint), "cursor" => format_args!("{}", latest_cursor),
            );

            let result = self
                .endpoint
                .clone()
                .stream_blocks(bstream::BlocksRequestV2 {
                    // Starts at current HEAD block of the chain (viewed from Firehose side)
                    start_block_num: -1,
                    start_cursor: latest_cursor.clone(),
                    fork_steps: vec![
                        bstream::ForkStep::StepNew as i32,
                        bstream::ForkStep::StepUndo as i32,
                    ],
                    ..Default::default()
                })
                .await;

            match result {
                Ok(stream) => {
                    // Consume the stream of blocks until an error is hit
                    latest_cursor = self.process_blocks(latest_cursor, stream).await
                }
                Err(e) => {
                    error!(self.logger, "Unable to connect to endpoint: {:?}", e);
                }
            }

            // If we reach this point, we must wait a bit before retrying
            backoff.sleep_async().await;
        }
    }

    async fn fetch_head_cursor(&self) -> String {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(250), Duration::from_secs(30));
        loop {
            match self.chain_store.clone().chain_head_cursor() {
                Ok(cursor) => return cursor.unwrap_or_else(|| "".to_string()),
                Err(e) => {
                    error!(self.logger, "Fetching chain head cursor failed: {:?}", e);

                    backoff.sleep_async().await;
                }
            }
        }
    }

    /// Consumes the incoming stream of blocks infinitely until it hits an error. In which case
    /// the error is logged right away and the latest available cursor is returned
    /// upstream for future consumption.
    async fn process_blocks(
        &self,
        cursor: String,
        mut stream: Streaming<bstream::BlockResponseV2>,
    ) -> String {
        use bstream::ForkStep::*;

        let mut latest_cursor = cursor;

        while let Some(message) = stream.next().await {
            match message {
                Ok(v) => {
                    let step = bstream::ForkStep::from_i32(v.step)
                        .expect("Fork step should always match to known value");

                    let result = match step {
                        StepNew => self.process_new_block(&v).await,
                        StepUndo => {
                            trace!(self.logger, "Received undo block to ingest, skipping");
                            Ok(())
                        }
                        StepIrreversible | StepUnknown => panic!(
                            "We explicitly requested StepNew|StepUndo but received something else"
                        ),
                    };

                    if let Err(e) = result {
                        error!(self.logger, "Process block failed: {:?}", e);
                        break;
                    }

                    latest_cursor = v.cursor;
                }
                Err(e) => {
                    info!(
                        self.logger,
                        "An error occurred while streaming blocks: {}", e
                    );
                    break;
                }
            }
        }

        error!(
            self.logger,
            "Stream blocks complete unexpectedly, expecting stream to always stream blocks"
        );
        latest_cursor
    }

    async fn process_new_block(&self, response: &bstream::BlockResponseV2) -> Result<(), Error> {
        let block = decode_firehose_block::<M>(response)
            .context("Mapping firehose block to blockchain::Block")?;

        trace!(self.logger, "Received new block to ingest {}", block.ptr());

        self.chain_store
            .clone()
            .set_chain_head(block, response.cursor.clone())
            .await
            .context("Updating chain head")?;

        Ok(())
    }
}
