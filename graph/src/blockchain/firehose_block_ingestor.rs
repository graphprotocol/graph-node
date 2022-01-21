use crate::prelude::BlockNumber;
use crate::{
    blockchain::Block as BlockchainBlock,
    components::store::ChainStore,
    firehose::{self, decode_firehose_block, FirehoseEndpoint},
    prelude::{error, info, Logger},
    util::backoff::ExponentialBackoff,
};
use anyhow::{Context, Error};
use futures03::StreamExt;
use slog::trace;
use std::{marker::PhantomData, sync::Arc, time::Duration};
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
        use firehose::ForkStep::*;

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
                .stream_blocks(firehose::Request {
                    // Starts at current HEAD block of the chain (viewed from Firehose side)
                    start_block_num: -1,
                    start_cursor: latest_cursor.clone(),
                    fork_steps: vec![StepNew as i32, StepUndo as i32],
                    ..Default::default()
                })
                .await;

            match result {
                Ok(stream) => {
                    info!(self.logger, "Blockstream connected, consuming blocks");

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

    pub async fn run_backfill(self) {
        use firehose::ForkStep::*;

        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(250), Duration::from_secs(30));

        let mut backfill_cursor = self.fetch_backfill_cursor().await;

        let mut backfill_target_block_number = self.fetch_backfill_target_block_num().await;
        if backfill_target_block_number.is_none() {
            backfill_target_block_number = Some(self.initialize_backfill_target_block_num().await);
        }

        loop {
            let backfill_completed = self.fetch_backfill_is_completed().await;
            if backfill_completed {
                info!(self.logger, "Backfill completed");
                break;
            }

            let result = self
                .endpoint
                .clone()
                .stream_blocks(firehose::Request {
                    start_block_num: 0,
                    stop_block_num: backfill_target_block_number.unwrap() as u64,
                    start_cursor: backfill_cursor.clone(),
                    fork_steps: vec![StepIrreversible as i32],
                    ..Default::default()
                })
                .await;

            match result {
                Ok(stream) => {
                    backfill_cursor = self
                        .process_backfill_blocks(
                            backfill_cursor,
                            stream,
                            backfill_target_block_number.unwrap(),
                        )
                        .await
                }
                Err(e) => {
                    error!(
                        self.logger,
                        "Unable to connect to backfill endpoint: {:?}", e
                    )
                }
            }

            // If we reach this point, we must wait a bit before retrying
            backoff.sleep_async().await;
        }
    }

    async fn fetch_backfill_is_completed(&self) -> bool {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(250), Duration::from_secs(30));
        loop {
            match self.chain_store.clone().chain_backfill_is_completed() {
                Ok(b) => {
                    return b;
                }
                Err(e) => {
                    error!(
                        self.logger,
                        "Fetching chain backfill completion failed: {:?}", e
                    );

                    backoff.sleep_async().await;
                }
            }
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

    async fn fetch_backfill_cursor(&self) -> String {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(250), Duration::from_secs(30));
        loop {
            match self.chain_store.clone().chain_backfill_cursor() {
                Ok(cursor) => return cursor.unwrap_or_else(|| "".to_string()),
                Err(e) => {
                    error!(
                        self.logger,
                        "Fetching chain backfill cursor failed: {:?}", e
                    );

                    backoff.sleep_async().await;
                }
            }
        }
    }

    async fn initialize_backfill_target_block_num(&self) -> BlockNumber {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(250), Duration::from_secs(30));

        loop {
            match self.determine_backfill_target_block_num().await {
                None => {
                    info!(self.logger, "Could not yet determine backfill target block number: no blocks set yet for this chain");
                    backoff.sleep_async().await;
                    continue;
                }
                Some(block_num) => {
                    match self
                        .chain_store
                        .clone()
                        .set_chain_backfill_target_block_num(block_num)
                        .await
                    {
                        Ok(_) => {}
                        Err(e) => {
                            error!(self.logger, "Setting chain backfill target failed: {:?}", e);
                            backoff.sleep_async().await;
                        }
                    }

                    return block_num;
                }
            }
        }
    }

    async fn fetch_backfill_target_block_num(&self) -> Option<BlockNumber> {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(250), Duration::from_secs(30));
        loop {
            match self.chain_store.clone().chain_backfill_target_block_num() {
                Ok(opt) => {
                    return opt;
                }
                Err(e) => {
                    error!(
                        self.logger,
                        "Fetching chain backfill target failed: {:?}", e
                    );
                    backoff.sleep_async().await;
                }
            }
        }
    }

    async fn determine_backfill_target_block_num(&self) -> Option<BlockNumber> {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(250), Duration::from_secs(30));

        loop {
            match self.chain_store.chain_initial_backfill_target_block_num() {
                Ok(opt) => {
                    return match opt {
                        None => None,
                        Some(t) => Some(t),
                    }
                }
                Err(e) => {
                    error!(self.logger, "Setting chain backfill target failed: {:?}", e);
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
        mut stream: Streaming<firehose::Response>,
    ) -> String {
        use firehose::ForkStep;
        use firehose::ForkStep::*;

        let mut latest_cursor = cursor;

        while let Some(message) = stream.next().await {
            match message {
                Ok(v) => {
                    let step = ForkStep::from_i32(v.step)
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

    async fn process_new_block(&self, response: &firehose::Response) -> Result<(), Error> {
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

    async fn process_backfill_blocks(
        &self,
        cursor: String,
        mut stream: Streaming<firehose::Response>,
        backfill_target_block_number: BlockNumber,
    ) -> String {
        let mut latest_cursor = cursor;

        while let Some(message) = stream.next().await {
            match message {
                Ok(v) => {
                    if let Err(e) = self
                        .process_backfill_block(&v, backfill_target_block_number)
                        .await
                    {
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

    async fn process_backfill_block(
        &self,
        response: &firehose::Response,
        backfill_target_block_number: BlockNumber,
    ) -> Result<(), Error> {
        let block = decode_firehose_block::<M>(response)
            .context("Mapping firehose block to blockchain::Block")?;

        trace!(
            self.logger,
            "Received block to ingest in backfill {}",
            block.ptr()
        );

        let block_number = block.number();

        self.chain_store
            .clone()
            .upsert_block(block)
            .await
            .context("Inserting blockchain::Block in chain store")?;

        self.chain_store
            .clone()
            .set_chain_backfill_cursor(response.cursor.clone())
            .await
            .context("Updating chain backfill cursor")?;

        if block_number >= backfill_target_block_number.into() {
            self.chain_store
                .clone()
                .set_chain_backfill_as_completed()
                .await
                .context("Setting backfill completion")?;
        }

        Ok(())
    }
}
