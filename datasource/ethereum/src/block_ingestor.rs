use failure::Error;
use std::fmt::Debug;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;
use web3::api::Web3;
use web3::transports::batch::Batch;
use web3::types::Block;
use web3::types::BlockId;
use web3::types::BlockNumber;
use web3::types::H256;
use web3::types::Transaction;
use web3::BatchTransport;

use graph::prelude::*;

#[derive(Clone, Debug)]
pub struct BlockIngestorConfig<S: BlockStore, T: BatchTransport> {
    pub store: Arc<Mutex<S>>,
    pub network_name: String,
    pub web3_transport: T,
    pub ancestor_count: u64,
    pub logger: slog::Logger,
}

pub struct BlockIngestor<S: BlockStore, T: BatchTransport + Debug + Clone> {
    store: Arc<Mutex<S>>,
    network_name: String,
    web3_transport: T,
    ancestor_count: u64,
    logger: slog::Logger,
}

impl<S: BlockStore + Send + 'static, T: BatchTransport + Debug + Clone> BlockIngestor<S, T> {
    pub fn spawn(config: BlockIngestorConfig<S, T>) -> Result<(), Error>
    where
        T: Send + Sync + 'static,
    {
        // Extract config params
        let BlockIngestorConfig {
            store,
            network_name,
            web3_transport,
            ancestor_count,
            logger,
        } = config;

        // Add a head block pointer for this network name if one does not already exist
        store.lock().unwrap().add_network_if_missing(&network_name)?;

        // Start block ingestor
        let block_ingestor = BlockIngestor {
            store,
            network_name,
            web3_transport,
            ancestor_count,
            logger: logger.new(o!("component" => "BlockIngestor")),
        };
        tokio::spawn(block_ingestor.into_polling_stream());

        Ok(())
    }

    fn into_polling_stream(self) -> impl Future<Item = (), Error = ()> {
        let err_logger = self.logger.clone();

        tokio::timer::Interval::new(Instant::now(), Duration::from_secs(5))
            .map_err(Error::from)
            .map(move |_| {
                self.do_poll().unwrap_or_else(|e| {
                    error!(self.logger, "failed to poll for latest block: {:?}", e);
                });
            })
            .collect()
            .map(|_| ())
            .map_err(move |e| {
                error!(err_logger, "timer::Interval failed: {:?}", e);
            })
    }

    fn do_poll(&self) -> Result<(), Error> {
        let latest_block = self.get_latest_block()?;

        let mut new_blocks = vec![latest_block];
        while new_blocks.len() > 0 {
            let missing_block_hashes = self.ingest_blocks(&new_blocks)?;
            new_blocks = self.get_blocks(&missing_block_hashes)?;
        }

        Ok(())
    }

    fn get_latest_block(&self) -> Result<Block<Transaction>, Error> {
        let web3 = Web3::new(self.web3_transport.clone());
        web3.eth()
            .block_with_txs(BlockNumber::Latest.into())
            .wait()
            .map_err(|e| format_err!("could not get latest block from web3: {}", e))
    }

    fn ingest_blocks(&self, blocks: &[Block<Transaction>]) -> Result<Vec<H256>, Error> {
        let store = self.store.lock().unwrap();
        store.upsert_blocks(&self.network_name, blocks)?;
        store.attempt_head_update(&self.network_name, self.ancestor_count)
    }

    fn get_blocks(&self, block_hashes: &[H256]) -> Result<Vec<Block<Transaction>>, Error> {
        // Don't bother with a batch request if nothing to request
        if block_hashes.len() == 0 {
            return Ok(vec![]);
        }

        let web3 = Web3::new(Batch::new(self.web3_transport.clone()));

        // Add requests to batch
        let blocks = block_hashes
            .into_iter()
            .map(|block_hash| web3.eth().block_with_txs(BlockId::from(*block_hash)))
            .collect::<Vec<_>>();

        // Submit all requests in batch
        web3.transport()
            .submit_batch()
            .wait()
            .map_err(|e| format_err!("could not get block from web3: {}", e))?;

        // Receive request results
        blocks
            .into_iter()
            .map(|b| {
                b.wait()
                    .map_err(|e| format_err!("could not get block from web3: {}", e))
            })
            .collect()
    }
}
