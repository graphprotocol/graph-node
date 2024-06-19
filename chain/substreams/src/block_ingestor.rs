use std::{sync::Arc, time::Duration};

use crate::mapper::Mapper;
use anyhow::{Context, Error};
use graph::blockchain::block_stream::{BlockStreamError, FirehoseCursor};
use graph::blockchain::BlockchainKind;
use graph::blockchain::{
    client::ChainClient, substreams_block_stream::SubstreamsBlockStream, BlockIngestor,
};
use graph::components::adapter::ChainId;
use graph::prelude::MetricsRegistry;
use graph::slog::trace;
use graph::substreams::Package;
use graph::tokio_stream::StreamExt;
use graph::{
    blockchain::block_stream::BlockStreamEvent,
    cheap_clone::CheapClone,
    components::store::ChainStore,
    prelude::{async_trait, error, info, DeploymentHash, Logger},
    util::backoff::ExponentialBackoff,
};
use prost::Message;

const SUBSTREAMS_HEAD_TRACKER_BYTES: &[u8; 89935] = include_bytes!(
    "../../../substreams/substreams-head-tracker/substreams-head-tracker-v1.0.0.spkg"
);

pub struct SubstreamsBlockIngestor {
    chain_store: Arc<dyn ChainStore>,
    client: Arc<ChainClient<super::Chain>>,
    logger: Logger,
    chain_name: ChainId,
    metrics: Arc<MetricsRegistry>,
}

impl SubstreamsBlockIngestor {
    pub fn new(
        chain_store: Arc<dyn ChainStore>,
        client: Arc<ChainClient<super::Chain>>,
        logger: Logger,
        chain_name: ChainId,
        metrics: Arc<MetricsRegistry>,
    ) -> SubstreamsBlockIngestor {
        SubstreamsBlockIngestor {
            chain_store,
            client,
            logger,
            chain_name,
            metrics,
        }
    }

    async fn fetch_head_cursor(&self) -> String {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(250), Duration::from_secs(30));
        loop {
            match self.chain_store.clone().chain_head_cursor() {
                Ok(cursor) => return cursor.unwrap_or_default(),
                Err(e) => {
                    error!(self.logger, "Fetching chain head cursor failed: {:#}", e);

                    backoff.sleep_async().await;
                }
            }
        }
    }

    /// Consumes the incoming stream of blocks infinitely until it hits an error. In which case
    /// the error is logged right away and the latest available cursor is returned
    /// upstream for future consumption.
    /// If an error is returned it indicates a fatal/deterministic error which should not be retried.
    async fn process_blocks(
        &self,
        cursor: FirehoseCursor,
        mut stream: SubstreamsBlockStream<super::Chain>,
    ) -> Result<FirehoseCursor, BlockStreamError> {
        let mut latest_cursor = cursor;

        while let Some(message) = stream.next().await {
            let (block, cursor) = match message {
                Ok(BlockStreamEvent::ProcessWasmBlock(
                    _block_ptr,
                    _block_time,
                    _data,
                    _handler,
                    _cursor,
                )) => {
                    unreachable!("Block ingestor should never receive raw blocks");
                }
                Ok(BlockStreamEvent::ProcessBlock(triggers, cursor)) => {
                    (Arc::new(triggers.block), cursor)
                }
                Ok(BlockStreamEvent::Revert(_ptr, _cursor)) => {
                    trace!(self.logger, "Received undo block to ingest, skipping");
                    continue;
                }
                Err(e) if e.is_deterministic() => {
                    return Err(e);
                }
                Err(e) => {
                    info!(
                        self.logger,
                        "An error occurred while streaming blocks: {}", e
                    );
                    break;
                }
            };

            let res = self.process_new_block(block, cursor.to_string()).await;
            if let Err(e) = res {
                error!(self.logger, "Process block failed: {:#}", e);
                break;
            }

            latest_cursor = cursor
        }

        error!(
            self.logger,
            "Stream blocks complete unexpectedly, expecting stream to always stream blocks"
        );

        Ok(latest_cursor)
    }

    async fn process_new_block(
        &self,
        block: Arc<super::Block>,
        cursor: String,
    ) -> Result<(), Error> {
        trace!(self.logger, "Received new block to ingest {:?}", block);

        self.chain_store
            .clone()
            .set_chain_head(block, cursor)
            .await
            .context("Updating chain head")?;

        Ok(())
    }
}

#[async_trait]
impl BlockIngestor for SubstreamsBlockIngestor {
    async fn run(self: Box<Self>) {
        let mapper = Arc::new(Mapper {
            schema: None,
            skip_empty_blocks: false,
        });
        let mut latest_cursor = FirehoseCursor::from(self.fetch_head_cursor().await);
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(250), Duration::from_secs(30));
        let package = Package::decode(SUBSTREAMS_HEAD_TRACKER_BYTES.to_vec().as_ref()).unwrap();

        loop {
            let stream = SubstreamsBlockStream::<super::Chain>::new(
                DeploymentHash::default(),
                self.client.cheap_clone(),
                None,
                latest_cursor.clone(),
                mapper.cheap_clone(),
                package.modules.clone().unwrap_or_default(),
                "map_blocks".to_string(),
                vec![-1],
                vec![],
                self.logger.cheap_clone(),
                self.metrics.cheap_clone(),
            );

            // Consume the stream of blocks until an error is hit
            // If the error is retryable it will print the error and return the cursor
            // therefore if we get an error here it has to be a fatal error.
            // This is a bit brittle and should probably be improved at some point.
            let res = self.process_blocks(latest_cursor.clone(), stream).await;
            match res {
                Ok(cursor) => {
                    if cursor.as_ref() != latest_cursor.as_ref() {
                        backoff.reset();
                        latest_cursor = cursor;
                    }
                }
                Err(BlockStreamError::Fatal(e)) => {
                    error!(
                        self.logger,
                        "fatal error while ingesting substream blocks: {}", e
                    );
                    return;
                }
                _ => unreachable!("Nobody should ever see this error message, something is wrong"),
            }

            // If we reach this point, we must wait a bit before retrying
            backoff.sleep_async().await;
        }
    }

    fn network_name(&self) -> ChainId {
        self.chain_name.clone()
    }
    fn kind(&self) -> BlockchainKind {
        BlockchainKind::Substreams
    }
}
