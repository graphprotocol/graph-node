use crate::{chain::BlockFinality, ENV_VARS};
use crate::{EthereumAdapter, EthereumAdapterTrait as _};
use graph::blockchain::client::ChainClient;
use graph::blockchain::BlockchainKind;
use graph::components::adapter::ChainId;
use graph::futures03::compat::Future01CompatExt as _;
use graph::slog::o;
use graph::util::backoff::ExponentialBackoff;
use graph::{
    blockchain::{BlockHash, BlockIngestor, BlockPtr, IngestorError},
    cheap_clone::CheapClone,
    prelude::{
        async_trait, error, info, tokio, trace, warn, web3::types::H256, ChainStore, Error,
        EthereumBlockWithCalls, LogCode, Logger,
    },
};
use std::{sync::Arc, time::Duration};

pub struct PollingBlockIngestor {
    logger: Logger,
    ancestor_count: i32,
    chain_client: Arc<ChainClient<crate::chain::Chain>>,
    chain_store: Arc<dyn ChainStore>,
    polling_interval: Duration,
    network_name: ChainId,
}

impl PollingBlockIngestor {
    pub fn new(
        logger: Logger,
        ancestor_count: i32,
        chain_client: Arc<ChainClient<crate::chain::Chain>>,
        chain_store: Arc<dyn ChainStore>,
        polling_interval: Duration,
        network_name: ChainId,
    ) -> Result<PollingBlockIngestor, Error> {
        Ok(PollingBlockIngestor {
            logger,
            ancestor_count,
            chain_client,
            chain_store,
            polling_interval,
            network_name,
        })
    }

    fn cleanup_cached_blocks(&self) {
        match self.chain_store.cleanup_cached_blocks(self.ancestor_count) {
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

    async fn do_poll(
        &self,
        logger: &Logger,
        eth_adapter: Arc<EthereumAdapter>,
    ) -> Result<(), IngestorError> {
        trace!(&logger, "BlockIngestor::do_poll");

        // Get chain head ptr from store
        let head_block_ptr_opt = self.chain_store.cheap_clone().chain_head_ptr().await?;

        // To check if there is a new block or not, fetch only the block header since that's cheaper
        // than the full block. This is worthwhile because most of the time there won't be a new
        // block, as we expect the poll interval to be much shorter than the block time.
        let latest_block = self.latest_block(logger, &eth_adapter).await?;

        if let Some(head_block) = head_block_ptr_opt.as_ref() {
            // If latest block matches head block in store, nothing needs to be done
            if &latest_block == head_block {
                return Ok(());
            }

            if latest_block.number < head_block.number {
                // An ingestor might wait or move forward, but it never
                // wavers and goes back. More seriously, this keeps us from
                // later trying to ingest a block with the same number again
                warn!(&logger,
                    "Provider went backwards - ignoring this latest block";
                    "current_block_head" => head_block.number,
                    "latest_block_head" => latest_block.number);
                return Ok(());
            }
        }

        // Compare latest block with head ptr, alert user if far behind
        match head_block_ptr_opt {
            None => {
                info!(
                    &logger,
                    "Downloading latest blocks from Ethereum, this may take a few minutes..."
                );
            }
            Some(head_block_ptr) => {
                let latest_number = latest_block.number;
                let head_number = head_block_ptr.number;
                let distance = latest_number - head_number;
                let blocks_needed = (distance).min(self.ancestor_count);
                let code = if distance >= 15 {
                    LogCode::BlockIngestionLagging
                } else {
                    LogCode::BlockIngestionStatus
                };
                if distance > 0 {
                    info!(
                    &logger,
                        "Syncing {} blocks from Ethereum",
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
        let mut missing_block_hash = self
            .ingest_block(&logger, &eth_adapter, &latest_block.hash)
            .await?;

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
            missing_block_hash = self.ingest_block(&logger, &eth_adapter, &hash).await?;
        }
        Ok(())
    }

    async fn ingest_block(
        &self,
        logger: &Logger,
        eth_adapter: &Arc<EthereumAdapter>,
        block_hash: &BlockHash,
    ) -> Result<Option<BlockHash>, IngestorError> {
        // TODO: H256::from_slice can panic
        let block_hash = H256::from_slice(block_hash.as_slice());

        // Get the fully populated block
        let block = eth_adapter
            .block_by_hash(logger, block_hash)
            .compat()
            .await?
            .ok_or(IngestorError::BlockUnavailable(block_hash))?;
        let ethereum_block = eth_adapter.load_full_block(&logger, block).await?;

        // We need something that implements `Block` to store the block; the
        // store does not care whether the block is final or not
        let ethereum_block = BlockFinality::NonFinal(EthereumBlockWithCalls {
            ethereum_block,
            calls: None,
        });

        // Store it in the database and try to advance the chain head pointer
        self.chain_store
            .upsert_block(Arc::new(ethereum_block))
            .await?;

        self.chain_store
            .cheap_clone()
            .attempt_chain_head_update(self.ancestor_count)
            .await
            .map(|missing| missing.map(|h256| h256.into()))
            .map_err(|e| {
                error!(logger, "failed to update chain head");
                IngestorError::Unknown(e)
            })
    }

    async fn latest_block(
        &self,
        logger: &Logger,
        eth_adapter: &Arc<EthereumAdapter>,
    ) -> Result<BlockPtr, IngestorError> {
        eth_adapter
            .latest_block_header(&logger)
            .compat()
            .await
            .map(|block| block.into())
    }

    async fn eth_adapter(&self) -> anyhow::Result<Arc<EthereumAdapter>> {
        self.chain_client
            .rpc()?
            .cheapest()
            .await
            .ok_or_else(|| graph::anyhow::anyhow!("unable to get eth adapter"))
    }
}

#[async_trait]
impl BlockIngestor for PollingBlockIngestor {
    async fn run(self: Box<Self>) {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(250), Duration::from_secs(30));

        loop {
            let eth_adapter = match self.eth_adapter().await {
                Ok(adapter) => {
                    backoff.reset();
                    adapter
                }
                Err(err) => {
                    error!(
                        &self.logger,
                        "unable to get ethereum adapter, backing off... error: {}",
                        err.to_string()
                    );
                    backoff.sleep_async().await;
                    continue;
                }
            };
            let logger = self
                .logger
                .new(o!("provider" => eth_adapter.provider().to_string()));

            match self.do_poll(&logger, eth_adapter).await {
                // Some polls will fail due to transient issues
                Err(err) => {
                    error!(logger, "Trying again after block polling failed: {}", err);
                }
                Ok(()) => (),
            }

            if ENV_VARS.cleanup_blocks {
                self.cleanup_cached_blocks()
            }

            tokio::time::sleep(self.polling_interval).await;
        }
    }

    fn network_name(&self) -> ChainId {
        self.network_name.clone()
    }

    fn kind(&self) -> BlockchainKind {
        BlockchainKind::Ethereum
    }
}
