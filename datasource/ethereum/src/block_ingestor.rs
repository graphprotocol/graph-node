use failure::Error;
use std::fmt::Debug;
use std::time::Duration;
use std::time::Instant;

use graph::prelude::*;
use graph::web3::api::Web3;
use graph::web3::transports::batch::Batch;
use graph::web3::types::{Block, BlockId, BlockNumber, Transaction, H256};
use graph::web3::BatchTransport;
use graph::web3::Transport;

pub struct BlockIngestor<S, T>
where
    S: ChainStore + 'static,
    T: BatchTransport + Send + Sync + Debug + Clone + 'static,
    <T as Transport>::Out: Send,
    <T as BatchTransport>::Batch: Send,
{
    store: Arc<S>,
    network_name: String,
    web3_transport: T,
    ancestor_count: u64,
    logger: slog::Logger,
    polling_interval: Duration,
}

impl<S, T> BlockIngestor<S, T>
where
    S: ChainStore + 'static,
    T: BatchTransport + Send + Sync + Debug + Clone + 'static,
    <T as Transport>::Out: Send,
    <T as BatchTransport>::Batch: Send,
{
    pub fn new(
        store: Arc<S>,
        network_name: String,
        web3_transport: T,
        ancestor_count: u64,
        logger: slog::Logger,
        polling_interval: Duration,
    ) -> Result<BlockIngestor<S, T>, Error> {
        Ok(BlockIngestor {
            store,
            network_name,
            web3_transport,
            ancestor_count,
            logger: logger.new(o!("component" => "BlockIngestor")),
            polling_interval,
        })
    }

    pub fn into_polling_stream(self) -> impl Future<Item = (), Error = ()> {
        // Currently, there is no way to stop block ingestion, so just leak self
        let static_self: &'static _ = Box::leak(Box::new(self));

        // First, add Ethereum network info it store
        static_self
            .add_network_to_store()
            .map_err(move |e| {
                panic!("Failed to load Ethereum network info: {}", e);
            }).and_then(move |()| {
                // Create stream that emits at polling interval
                tokio::timer::Interval::new(Instant::now(), static_self.polling_interval)
                    .map_err(move |e| {
                        error!(static_self.logger, "timer::Interval failed: {:?}", e);
                    }).for_each(move |_| {
                        // Attempt to poll
                        static_self.do_poll().then(move |result| {
                            if let Err(e) = result {
                                // Some polls will fail due to transient issues
                                warn!(
                                    static_self.logger,
                                    "failed to poll for latest block: {:?}", e
                                );
                            }

                            // Continue polling even if polling failed
                            future::ok(())
                        })
                    })
            })
    }

    fn add_network_to_store<'a>(&'a self) -> impl Future<Item = (), Error = Error> + 'a {
        let web3 = Web3::new(self.web3_transport.clone());

        // Ask Ethereum node for info to identify the network
        let net_ver_future = web3.net().version();
        let gen_block_future = web3.eth().block(BlockNumber::Earliest.into());
        net_ver_future
            .join(gen_block_future)
            .map_err(|e| format_err!("could not get network info from Ethereum: {}", e))
            .and_then(move |(net_version, gen_block)| {
                let gen_block_hash = gen_block
                    .expect("Ethereum node could not find genesis block")
                    .hash
                    .unwrap();

                // Add Ethereum network info to store
                self.store
                    .add_network_if_missing(&self.network_name, &net_version, gen_block_hash)
            })
    }

    fn do_poll<'a>(&'a self) -> impl Future<Item = (), Error = Error> + 'a {
        // Ask for latest block from Ethereum node
        self.get_latest_block()
            .and_then(move |latest_block: Block<Transaction>| {
                // Store latest block in block store.
                // Might be a no-op if latest block is one that we have seen.
                // ingest_blocks will return a (potentially incomplete) list of blocks that are
                // missing.
                self.ingest_blocks(stream::once(Ok(latest_block)))
            }).and_then(move |missing_block_hashes| {
                // Repeatedly fetch missing blocks, and ingest them.
                // ingest_blocks will continue to tell us about more missing blocks until we have
                // filled in all missing pieces of the blockchain (that we care about).
                future::loop_fn(
                    missing_block_hashes,
                    move |missing_block_hashes| -> Box<Future<Item = _, Error = _> + Send> {
                        if missing_block_hashes.is_empty() {
                            // If no blocks were missing, then the block head pointer was updated
                            // successfully, and this poll has completed.
                            Box::new(future::ok(future::Loop::Break(())))
                        } else {
                            // Some blocks are missing: load them, ingest them, and repeat.
                            let missing_blocks = self.get_blocks(&missing_block_hashes);
                            Box::new(self.ingest_blocks(missing_blocks).map(
                                |missing_block_hashes| future::Loop::Continue(missing_block_hashes),
                            ))
                        }
                    },
                )
            })
    }

    fn get_latest_block(&self) -> impl Future<Item = Block<Transaction>, Error = Error> {
        let web3 = Web3::new(self.web3_transport.clone());
        web3.eth()
            .block_with_txs(BlockNumber::Latest.into())
            .map_err(|e| format_err!("could not get latest block from Ethereum: {}", e))
            .and_then(|block| block.ok_or(format_err!("no block returned from Ethereum")))
    }

    /// Put some blocks into the block store (if they are not there already), and try to update the
    /// head block pointer. If missing blocks prevent such an update, return a Vec with at least
    /// one of the missing blocks' hashes.
    fn ingest_blocks<'a, B: Stream<Item = Block<Transaction>, Error = Error> + Send + 'a>(
        &'a self,
        blocks: B,
    ) -> impl Future<Item = Vec<H256>, Error = Error> + Send + 'a {
        self.store
            .upsert_blocks(&self.network_name, blocks)
            .and_then(move |()| {
                self.store
                    .attempt_chain_head_update(&self.network_name, self.ancestor_count)
            })
    }

    /// Requests the specified blocks via web3, returning them in a stream (potentially out of
    /// order).
    fn get_blocks<'a>(
        &'a self,
        block_hashes: &[H256],
    ) -> Box<Stream<Item = Block<Transaction>, Error = Error> + Send + 'a> {
        // Don't bother with a batch request if nothing to request
        if block_hashes.is_empty() {
            return Box::new(stream::empty());
        }

        let web3 = Web3::new(Batch::new(self.web3_transport.clone()));

        // Add requests to batch
        let block_futures = block_hashes
            .into_iter()
            .map(|block_hash| {
                web3.eth()
                    .block_with_txs(BlockId::from(*block_hash))
                    .map_err(|e| format_err!("could not get block from Ethereum: {}", e))
                    .and_then(|block| {
                        block.ok_or(format_err!("no block returned from Ethereum"))
                    })
            })
            // Collect to ensure that `block_with_txs` calls happen before `submit_batch`
            .collect::<Vec<_>>();

        // Submit all requests in batch
        Box::new(
            web3.transport()
                .submit_batch()
                .map_err(|e| format_err!("could not get blocks from Ethereum: {}", e))
                .map(|_| stream::futures_unordered(block_futures))
                .flatten_stream(),
        )
    }
}
