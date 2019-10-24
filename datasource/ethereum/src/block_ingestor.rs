use std::time::Duration;
use std::time::Instant;

use graph::prelude::*;
use web3::types::*;

pub struct BlockIngestor<S>
where
    S: ChainStore,
{
    chain_store: Arc<S>,
    eth_adapter: Arc<dyn EthereumAdapter>,
    ancestor_count: u64,
    network_name: String,
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
        ancestor_count: u64,
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

        Ok(BlockIngestor {
            chain_store,
            eth_adapter,
            ancestor_count,
            network_name,
            logger,
            polling_interval,
        })
    }

    pub fn into_polling_stream(self) -> impl Future<Item = (), Error = ()> {
        // Currently, there is no way to stop block ingestion, so just leak self
        let static_self: &'static _ = Box::leak(Box::new(self));

        // Create stream that emits at polling interval
        tokio::timer::Interval::new(Instant::now(), static_self.polling_interval)
            .map_err(move |e| {
                error!(static_self.logger, "timer::Interval failed: {:?}", e);
            })
            .for_each(move |_| {
                // Attempt to poll
                static_self.do_poll().then(move |result| {
                    if let Err(err) = result {
                        // Some polls will fail due to transient issues
                        match err {
                            EthereumAdapterError::BlockUnavailable(_) => {
                                trace!(
                                    static_self.logger,
                                    "Trying again after block polling failed: {}",
                                    err
                                );
                            }
                            EthereumAdapterError::Unknown(inner_err) => {
                                warn!(
                                    static_self.logger,
                                    "Trying again after block polling failed: {}", inner_err
                                );
                            }
                        }
                    }

                    // Continue polling even if polling failed
                    future::ok(())
                })
            })
    }

    fn do_poll<'a>(&'a self) -> impl Future<Item = (), Error = EthereumAdapterError> + 'a {
        trace!(self.logger, "BlockIngestor::do_poll");
        let network_name = self.network_name.clone();

        // Get chain head ptr from store
        future::result(self.chain_store.chain_head_ptr())
            .from_err()
            .and_then(move |head_block_ptr_opt| {
                // Ask for latest block from Ethereum node
                self.eth_adapter.latest_block(&self.logger)
                    // Compare latest block with head ptr, alert user if far behind
                    .and_then(move |latest_block: LightEthereumBlock| -> Box<dyn Future<Item=_, Error=_> + Send> {
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
                                        "network_name" => network_name,
                                        "code" => code,
                                    );
                                }
                            }
                        }

                        // If latest block matches head block in store
                        if Some((&latest_block).into()) == head_block_ptr_opt {
                            // We're done
                            return Box::new(future::ok(()));
                        }

                        Box::new(
                            self.eth_adapter.load_full_block(&self.logger, latest_block)
                            .and_then(move |latest_block: EthereumBlock| {
                                // Store latest block in block store.
                                // Might be a no-op if latest block is one that we have seen.
                                // ingest_blocks will return a (potentially incomplete) list of blocks that are
                                // missing.
                                self.ingest_blocks(stream::once(Ok(latest_block)))
                            }).and_then(move |missing_block_hashes| {
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
                                future::loop_fn(
                                    missing_block_hashes,
                                    move |missing_block_hashes| -> Box<dyn Future<Item = _, Error = _> + Send> {
                                        if missing_block_hashes.is_empty() {
                                            // If no blocks were missing, then the block head pointer was updated
                                            // successfully, and this poll has completed.
                                            Box::new(future::ok(future::Loop::Break(())))
                                        } else {
                                            // Some blocks are missing: load them, ingest them, and repeat.
                                            let missing_blocks = self.get_blocks(&missing_block_hashes);
                                            Box::new(self.ingest_blocks(missing_blocks).map(future::Loop::Continue))
                                        }
                                    },
                                )
                            })
                        )
                    })
            })
    }

    /// Put some blocks into the block store (if they are not there already), and try to update the
    /// head block pointer. If missing blocks prevent such an update, return a Vec with at least
    /// one of the missing blocks' hashes.
    fn ingest_blocks<
        'a,
        B: Stream<Item = EthereumBlock, Error = EthereumAdapterError> + Send + 'a,
    >(
        &'a self,
        blocks: B,
    ) -> impl Future<Item = Vec<H256>, Error = EthereumAdapterError> + Send + 'a {
        self.chain_store.upsert_blocks(blocks).and_then(move |()| {
            self.chain_store
                .attempt_chain_head_update(self.ancestor_count)
                .map_err(|e| {
                    error!(self.logger, "failed to update chain head");
                    EthereumAdapterError::Unknown(e)
                })
        })
    }

    /// Requests the specified blocks via web3, returning them in a stream (potentially out of
    /// order).
    fn get_blocks<'a>(
        &'a self,
        block_hashes: &[H256],
    ) -> Box<dyn Stream<Item = EthereumBlock, Error = EthereumAdapterError> + Send + 'a> {
        let logger = self.logger.clone();
        let eth_adapter = self.eth_adapter.clone();

        let block_futures = block_hashes.iter().map(move |&block_hash| {
            let logger = logger.clone();
            let eth_adapter = eth_adapter.clone();

            eth_adapter
                .block_by_hash(&logger, block_hash)
                .from_err()
                .and_then(move |block_opt| {
                    block_opt.ok_or_else(|| EthereumAdapterError::BlockUnavailable(block_hash))
                })
                .and_then(move |block| eth_adapter.load_full_block(&logger, block))
        });

        Box::new(stream::futures_unordered(block_futures))
    }
}
