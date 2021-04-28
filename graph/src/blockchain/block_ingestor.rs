use std::{sync::Arc, time::Duration};

use crate::{
    blockchain::{Blockchain, IngestorAdapter, IngestorError},
    prelude::{info, lazy_static, tokio, trace, warn, Error, LogCode, Logger},
};

lazy_static! {
    // graph_node::config disallows setting this in a store with multiple
    // shards. See 8b6ad0c64e244023ac20ced7897fe666 for the reason
    pub static ref CLEANUP_BLOCKS: bool = std::env::var("GRAPH_ETHEREUM_CLEANUP_BLOCKS")
        .ok()
        .map(|s| s.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
}

pub struct BlockIngestor<C>
where
    C: Blockchain,
{
    adapter: Arc<C::IngestorAdapter>,
    logger: Logger,
    polling_interval: Duration,
}

impl<C> BlockIngestor<C>
where
    C: Blockchain,
{
    pub fn new(
        adapter: Arc<C::IngestorAdapter>,
        polling_interval: Duration,
    ) -> Result<BlockIngestor<C>, Error> {
        let logger = adapter.logger().clone();
        Ok(BlockIngestor {
            adapter,
            logger,
            polling_interval,
        })
    }

    pub async fn into_polling_stream(self) {
        loop {
            match self.do_poll().await {
                // Some polls will fail due to transient issues
                Err(err @ IngestorError::BlockUnavailable(_)) => {
                    trace!(
                        self.logger,
                        "Trying again after block polling failed: {}",
                        err
                    );
                }
                Err(IngestorError::Unknown(inner_err)) => {
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
        match self.adapter.cleanup_cached_blocks() {
            Ok(Some((min_block, count))) => {
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
            Ok(None) => { /* nothing was cleaned, ignore */ }
            Err(e) => warn!(
                self.logger,
                "Failed to clean blocks from block cache: {}", e
            ),
        }
    }

    async fn do_poll(&self) -> Result<(), IngestorError> {
        trace!(self.logger, "BlockIngestor::do_poll");

        // Get chain head ptr from store
        let head_block_ptr_opt = self.adapter.chain_head_ptr()?;

        // To check if there is a new block or not, fetch only the block header since that's cheaper
        // than the full block. This is worthwhile because most of the time there won't be a new
        // block, as we expect the poll interval to be much shorter than the block time.
        let latest_block = self.adapter.latest_block().await?;

        // If latest block matches head block in store, nothing needs to be done
        if Some(&latest_block) == head_block_ptr_opt.as_ref() {
            return Ok(());
        }

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
                let latest_number = latest_block.number;
                let head_number = head_block_ptr.number;
                let distance = latest_number - head_number;
                let blocks_needed = (distance).min(self.adapter.ancestor_count());
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

        // Store latest block in block store.
        // Might be a no-op if latest block is one that we have seen.
        // ingest_blocks will return a (potentially incomplete) list of blocks that are
        // missing.
        let mut missing_block_hash = self.adapter.ingest_block(&latest_block.hash).await?;

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
        while let Some(hash) = missing_block_hash {
            missing_block_hash = self.adapter.ingest_block(&hash).await?;
        }
        Ok(())
    }
}
