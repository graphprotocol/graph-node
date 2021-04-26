use lazy_static;
use std::{sync::Arc, time::Duration};

use graph::prelude::{
    error, info, o, tokio, trace, warn, web3::types::H256, BlockNumber, ChainStore,
    ComponentLoggerConfig, ElasticComponentLoggerConfig, Error, EthereumAdapter,
    EthereumAdapterError, EthereumBlock, Future01CompatExt, LogCode, Logger, LoggerFactory,
};

lazy_static! {
    // graph_node::config disallows setting this in a store with multiple
    // shards. See 8b6ad0c64e244023ac20ced7897fe666 for the reason
    pub static ref CLEANUP_BLOCKS: bool = std::env::var("GRAPH_ETHEREUM_CLEANUP_BLOCKS")
        .ok()
        .map(|s| s.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
}

pub struct BlockIngestor<S>
where
    S: ChainStore,
{
    chain_store: Arc<S>,
    eth_adapter: Arc<dyn EthereumAdapter>,
    ancestor_count: BlockNumber,
    _network_name: String,
    logger: Logger,
    polling_interval: Duration,
}

impl<S> BlockIngestor<S>
where
    S: ChainStore,
{
    pub fn new(
        chain_store: Arc<S>,
        eth_adapter: Arc<dyn EthereumAdapter>,
        ancestor_count: BlockNumber,
        network_name: String,
        logger_factory: &LoggerFactory,
        polling_interval: Duration,
    ) -> Result<BlockIngestor<S>, Error> {
        let logger = logger_factory.component_logger(
            "BlockIngestor",
            Some(ComponentLoggerConfig {
                elastic: Some(ElasticComponentLoggerConfig {
                    index: String::from("block-ingestor-logs"),
                }),
            }),
        );

        let logger = logger.new(o!("provider" => eth_adapter.provider().to_string()));

        Ok(BlockIngestor {
            chain_store,
            eth_adapter,
            ancestor_count,
            _network_name: network_name,
            logger,
            polling_interval,
        })
    }

    pub async fn into_polling_stream(self) {
        loop {
            match self.do_poll().await {
                // Some polls will fail due to transient issues
                Err(err @ EthereumAdapterError::BlockUnavailable(_)) => {
                    trace!(
                        self.logger,
                        "Trying again after block polling failed: {}",
                        err
                    );
                }
                Err(EthereumAdapterError::Unknown(inner_err)) => {
                    warn!(
                        self.logger,
                        "Trying again after block polling failed: {}", inner_err
                    );
                }
                Ok(()) => (),
            }

            if *CLEANUP_BLOCKS {
                self.cleanup_cached_blocks()
            }

            tokio::time::delay_for(self.polling_interval).await;
        }
    }

    fn cleanup_cached_blocks(&self) {
        match self.chain_store.cleanup_cached_blocks(self.ancestor_count) {
            Ok((min_block, count)) => {
                if count > 0 {
                    info!(
                        self.logger,
                        "Cleaned {} blocks from the block cache. \
                                 Only blocks with number greater than {} remain",
                        count,
                        min_block
                    );
                }
            }
            Err(e) => warn!(
                self.logger,
                "Failed to clean blocks from block cache: {}", e
            ),
        }
    }

    async fn do_poll(&self) -> Result<(), EthereumAdapterError> {
        trace!(self.logger, "BlockIngestor::do_poll");

        // Get chain head ptr from store
        let head_block_ptr_opt = self.chain_store.chain_head_ptr()?;

        // To check if there is a new block or not, fetch only the block header since that's cheaper
        // than the full block. This is worthwhile because most of the time there won't be a new
        // block, as we expect the poll interval to be much shorter than the block time.
        let latest_block = self
            .eth_adapter
            .latest_block_header(&self.logger)
            .compat()
            .await?;

        // If latest block matches head block in store, nothing needs to be done
        if Some(latest_block.into()) == head_block_ptr_opt {
            return Ok(());
        }

        // Ask for latest block again, but now with full transactions
        let latest_block = self.eth_adapter.latest_block(&self.logger).compat().await?;

        // Compare latest block with head ptr, alert user if far behind
        match head_block_ptr_opt {
            None => {
                info!(
                    self.logger,
                    "Downloading latest blocks from Ethereum. \
                                    This may take a few minutes..."
                );
            }
            Some(head_block_ptr) => {
                let latest_number = latest_block.number.unwrap().as_u64() as i64;
                let head_number = head_block_ptr.number as i64;
                let distance = latest_number - head_number;
                let blocks_needed = (distance).min(self.ancestor_count as i64);
                let code = if distance >= 15 {
                    LogCode::BlockIngestionLagging
                } else {
                    LogCode::BlockIngestionStatus
                };
                if distance > 0 {
                    info!(
                        self.logger,
                        "Syncing {} blocks from Ethereum.",
                        blocks_needed;
                        "current_block_head" => head_number,
                        "latest_block_head" => latest_number,
                        "blocks_behind" => distance,
                        "blocks_needed" => blocks_needed,
                        "code" => code,
                    );
                }
            }
        }

        let latest_block = self
            .eth_adapter
            .load_full_block(&self.logger, latest_block)
            .compat()
            .await?;

        // Store latest block in block store.
        // Might be a no-op if latest block is one that we have seen.
        // ingest_blocks will return a (potentially incomplete) list of blocks that are
        // missing.
        let mut missing_block_hash = self.ingest_block(latest_block).await?;

        // Repeatedly fetch missing parent blocks, and ingest them.
        // ingest_blocks will continue to tell us about more missing parent
        // blocks until we have filled in all missing pieces of the
        // blockchain in the block number range we care about.
        //
        // Loop will terminate because:
        // - The number of blocks in the ChainStore in the block number
        //   range [latest - ancestor_count, latest] is finite.
        // - The missing parents in the first iteration have at most block
        //   number latest-1.
        // - Each iteration loads parents of all blocks in the range whose
        //   parent blocks are not already in the ChainStore, so blocks
        //   with missing parents in one iteration will not have missing
        //   parents in the next.
        // - Therefore, if the missing parents in one iteration have at
        //   most block number N, then the missing parents in the next
        //   iteration will have at most block number N-1.
        // - Therefore, the loop will iterate at most ancestor_count times.
        while missing_block_hash.is_some() {
            // Some blocks are missing: load them, ingest them, and repeat.
            let missing_block = self.get_block(missing_block_hash.unwrap()).await?;
            missing_block_hash = self.ingest_block(missing_block).await?;
        }
        Ok(())
    }

    /// Put some blocks into the block store (if they are not there already), and try to update the
    /// head block pointer. If missing blocks prevent such an update, return a Vec with at least
    /// one of the missing blocks' hashes.
    async fn ingest_block(
        &self,
        block: EthereumBlock,
    ) -> Result<Option<H256>, EthereumAdapterError> {
        self.chain_store.upsert_block(block).await?;

        self.chain_store
            .clone()
            .attempt_chain_head_update(self.ancestor_count)
            .await
            .map_err(|e| {
                error!(self.logger, "failed to update chain head");
                EthereumAdapterError::Unknown(e)
            })
    }

    /// Requests the specified blocks via web3, returning them in a stream (potentially out of
    /// order).
    async fn get_block(&self, block_hash: H256) -> Result<EthereumBlock, EthereumAdapterError> {
        let logger = self.logger.clone();
        let eth_adapter = self.eth_adapter.clone();

        let logger = logger.clone();
        let eth_adapter = eth_adapter.clone();

        let block = eth_adapter
            .block_by_hash(&logger, block_hash)
            .compat()
            .await?
            .ok_or_else(|| EthereumAdapterError::BlockUnavailable(block_hash))?;
        eth_adapter.load_full_block(&logger, block).compat().await
    }
}
