use failure::Error;
use std::fmt::Debug;
use std::time::Duration;
use std::time::Instant;

use graph::prelude::*;
use graph::web3::api::Web3;
use graph::web3::transports::batch::Batch;
use graph::web3::types::*;
use graph::web3::BatchTransport;
use graph::web3::Transport;

pub struct BlockIngestor<S, T>
where
    S: ChainStore + 'static,
    T: BatchTransport + Send + Sync + Debug + Clone + 'static,
    <T as Transport>::Out: Send,
    <T as BatchTransport>::Batch: Send,
{
    chain_store: Arc<S>,
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
        chain_store: Arc<S>,
        web3_transport: T,
        ancestor_count: u64,
        logger: slog::Logger,
        polling_interval: Duration,
    ) -> Result<BlockIngestor<S, T>, Error> {
        Ok(BlockIngestor {
            chain_store,
            web3_transport,
            ancestor_count,
            logger: logger.new(o!("component" => "BlockIngestor")),
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
            }).for_each(move |_| {
                // Attempt to poll
                static_self.do_poll().then(move |result| {
                    if let Err(err) = result {
                        // Some polls will fail due to transient issues
                        match err {
                            BlockIngestorError::BlockUnavailable(_) => {
                                trace!(
                                    static_self.logger,
                                    "Trying again after block polling failed: {}",
                                    err
                                );
                            }
                            BlockIngestorError::Unknown(inner_err) => {
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

    fn do_poll<'a>(&'a self) -> impl Future<Item = (), Error = BlockIngestorError> + 'a {
        trace!(self.logger, "BlockIngestor::do_poll");

        // Get chain head ptr from store
        future::result(self.chain_store.chain_head_ptr())
            .from_err()
            .and_then(move |head_block_ptr_opt| {
                // Ask for latest block from Ethereum node
                self.get_latest_block()
                    // Compare latest block with head ptr, alert user if far behind
                    .and_then(move |latest_block: Block<Transaction>| -> Box<Future<Item=_, Error=_> + Send> {
                        match head_block_ptr_opt {
                            None => {
                                info!(self.logger, "Downloading latest blocks from Ethereum. This may take a few minutes...");
                            }
                            Some(head_block_ptr) => {
                                let latest_number = latest_block.number.unwrap().as_u64() as i64;
                                let head_number = head_block_ptr.number as i64;
                                let distance = latest_number - head_number;
                                if distance > 10 && distance <= 50 {
                                    info!(self.logger, "Downloading latest blocks from Ethereum. This may take a few seconds...");
                                } else if distance > 50 {
                                    info!(self.logger, "Downloading latest blocks from Ethereum. This may take a few minutes...");
                                }
                            }
                        }

                        // If latest block matches head block in store
                        if Some((&latest_block).into()) == head_block_ptr_opt {
                            // We're done
                            return Box::new(future::ok(()));
                        }

                        Box::new(
                            self.load_full_block(latest_block)
                            .and_then(move |latest_block: EthereumBlock| {
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
                        )
                    })
            })
    }

    fn get_latest_block<'a>(
        &'a self,
    ) -> impl Future<Item = Block<Transaction>, Error = BlockIngestorError> + 'a {
        let web3 = Web3::new(self.web3_transport.clone());
        web3.eth()
            .block_with_txs(BlockNumber::Latest.into())
            .map_err(|e| format_err!("could not get latest block from Ethereum: {}", e))
            .from_err()
            .and_then(|block_opt| {
                block_opt.ok_or(format_err!("no latest block returned from Ethereum").into())
            })
    }

    fn load_full_block<'a>(
        &'a self,
        block: Block<Transaction>,
    ) -> impl Future<Item = EthereumBlock, Error = BlockIngestorError> + 'a {
        let block_hash = block.hash.unwrap();

        // Load transaction receipts
        let receipt_futures = block
            .transactions
            .iter()
            .map(move |tx| self.get_transaction_receipt(block_hash, tx.hash))
            .collect::<Vec<_>>();

        // Merge receipts with Block<Transaction> to get EthereumBlock
        stream::futures_ordered(receipt_futures)
            .collect()
            .map(move |transaction_receipts| EthereumBlock {
                block,
                transaction_receipts,
            })
    }

    fn get_transaction_receipt<'a>(
        &'a self,
        block_hash: H256,
        tx_hash: H256,
    ) -> impl Future<Item = TransactionReceipt, Error = BlockIngestorError> + 'a {
        let web3 = Web3::new(self.web3_transport.clone());

        // Retry, but eventually give up.
        // The receipt might be missing because the block was uncled, and the transaction never
        // made it back into the main chain.
        retry(
            "block ingestor eth_getTransactionReceipt RPC call",
            self.logger.clone(),
        ).when_err()
        .limit(16)
        .no_logging()
        .timeout_secs(60)
        .run(move || {
            web3.eth()
                .transaction_receipt(tx_hash)
                .map_err(move |e| {
                    format_err!(
                        "could not get transaction receipt {} from Ethereum: {}",
                        tx_hash,
                        e
                    ).into()
                }).and_then(move |receipt_opt| {
                    receipt_opt.ok_or_else(move || {
                        // No receipt was returned.
                        //
                        // This can be because the Ethereum node no longer considers
                        // this block to be part of the main chain, and so the transaction is
                        // no longer in the main chain.  Nothing we can do from here except
                        // give up trying to ingest this block.
                        //
                        // This could also be because the receipt is simply not available yet.
                        // For that case, we should retry until it becomes available.
                        BlockIngestorError::BlockUnavailable(block_hash)
                    })
                })
        }).map_err(move |e| {
            e.into_inner().unwrap_or_else(move || {
                // Timed out
                format_err!(
                    "Ethereum node took too long to return transaction receipt {}",
                    tx_hash
                ).into()
            })
        }).and_then(move |receipt| {
            // Check if receipt is for the right block
            if receipt.block_hash != block_hash {
                // If the receipt came from a different block, then the Ethereum node
                // no longer considers this block to be in the main chain.
                // Nothing we can do from here except give up trying to ingest this
                // block.
                // There is no way to get the transaction receipt from this block.
                Err(BlockIngestorError::BlockUnavailable(block_hash))
            } else {
                Ok(receipt)
            }
        })
    }

    /// Put some blocks into the block store (if they are not there already), and try to update the
    /// head block pointer. If missing blocks prevent such an update, return a Vec with at least
    /// one of the missing blocks' hashes.
    fn ingest_blocks<
        'a,
        B: Stream<Item = EthereumBlock, Error = BlockIngestorError> + Send + 'a,
    >(
        &'a self,
        blocks: B,
    ) -> impl Future<Item = Vec<H256>, Error = BlockIngestorError> + Send + 'a {
        self.chain_store.upsert_blocks(blocks).and_then(move |()| {
            self.chain_store
                .attempt_chain_head_update(self.ancestor_count)
                .map_err(BlockIngestorError::from)
        })
    }

    /// Requests the specified blocks via web3, returning them in a stream (potentially out of
    /// order).
    fn get_blocks<'a>(
        &'a self,
        block_hashes: &[H256],
    ) -> Box<Stream<Item = EthereumBlock, Error = BlockIngestorError> + Send + 'a> {
        // Don't bother with a batch request if nothing to request
        if block_hashes.is_empty() {
            return Box::new(stream::empty());
        }

        let batching_web3 = Web3::new(Batch::new(self.web3_transport.clone()));

        // Add requests to batch
        let block_futures = block_hashes
            .into_iter()
            .map(|block_hash| {
                let block_hash = block_hash.to_owned();

                batching_web3
                    .eth()
                    .block_with_txs(BlockId::from(block_hash))
                    .map_err(|e| format_err!("could not get block from Ethereum: {}", e).into())
                    .and_then(move |block_opt| {
                        block_opt.ok_or(BlockIngestorError::BlockUnavailable(block_hash))
                    }).and_then(move |block| self.load_full_block(block))
            })
            // Collect to ensure that `block_with_txs` calls happen before `submit_batch`
            .collect::<Vec<_>>();

        // Submit all requests in batch
        Box::new(
            batching_web3
                .transport()
                .submit_batch()
                .map_err(|e| format_err!("could not get blocks from Ethereum: {}", e).into())
                .map(|_| stream::futures_unordered(block_futures))
                .flatten_stream(),
        )
    }
}

#[derive(Debug, Fail)]
enum BlockIngestorError {
    /// The Ethereum node does not know about this block for some reason, probably because it
    /// disappeared in a chain reorg.
    #[fail(
        display = "block data unavailable, block was likely uncled (block hash = {:?})",
        _0
    )]
    BlockUnavailable(H256),

    /// An unexpected error occurred.
    #[fail(display = "error in block ingestor: {}", _0)]
    Unknown(Error),
}

impl From<Error> for BlockIngestorError {
    fn from(e: Error) -> Self {
        BlockIngestorError::Unknown(e)
    }
}
