use failure::Error;
use std::fmt::Debug;
use std::time::Duration;
use std::time::Instant;

use graph::prelude::*;
use graph::tokio::timer::Delay;
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
                    if let Err(e) = result {
                        // Some polls will fail due to transient issues
                        warn!(
                            static_self.logger,
                            "Trying again after block polling failed: {:?}", e
                        );
                    }

                    // Continue polling even if polling failed
                    future::ok(())
                })
            })
    }

    fn do_poll<'a>(&'a self) -> impl Future<Item = (), Error = Error> + 'a {
        trace!(self.logger, "BlockIngestor::do_poll");

        // Get chain head ptr from store
        future::result(self.chain_store.chain_head_ptr())
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

                        // Load transaction receipts
                        let receipt_futures = latest_block
                            .transactions
                            .iter()
                            .map(move |tx| {
                                self.get_transaction_receipt(tx.hash)
                            }).collect::<Vec<_>>();

                        Box::new(
                            // Merge receipts with Block<Transaction> to get EthereumBlock
                            stream::futures_ordered(receipt_futures).collect().map(
                                move |transaction_receipts| EthereumBlock {
                                    block: latest_block,
                                    transaction_receipts,
                                },
                            ).and_then(move |latest_block: EthereumBlock| {
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
    ) -> impl Future<Item = Block<Transaction>, Error = Error> + 'a {
        let web3 = Web3::new(self.web3_transport.clone());
        web3.eth()
            .block_with_txs(BlockNumber::Latest.into())
            .map_err(|e| format_err!("could not get latest block from Ethereum: {}", e))
            .and_then(|block_opt| block_opt.ok_or(format_err!("no block returned from Ethereum")))
    }

    fn get_transaction_receipt<'a>(
        &'a self,
        tx_hash: H256,
    ) -> impl Future<Item = TransactionReceipt, Error = Error> + 'a {
        let web3 = Web3::new(self.web3_transport.clone());

        future::loop_fn(10, move |retries_left| {
            web3.eth()
                .transaction_receipt(tx_hash)
                .map_err(move |e| {
                    format_err!(
                        "could not get transaction receipt {} from Ethereum: {}",
                        tx_hash,
                        e
                    )
                }).and_then(move |receipt_opt| {
                    receipt_opt.ok_or_else(move || {
                        format_err!(
                            "Ethereum node could not find transaction receipt {}",
                            tx_hash
                        )
                    })
                }).then(
                    move |receipt_result| -> Box<Future<Item = _, Error = Error> + Send> {
                        match receipt_result {
                            Ok(receipt) => Box::new(future::ok(future::Loop::Break(receipt))),
                            Err(e) => {
                                if retries_left > 0 {
                                    trace!(
                                        self.logger,
                                        "Receipt retry #{} - {}",
                                        10 - retries_left + 1,
                                        tx_hash
                                    );
                                    Box::new(
                                        Delay::new(Instant::now() + Duration::from_millis(2000))
                                            .map(move |()| future::Loop::Continue(retries_left - 1))
                                            .map_err(|e| format_err!("tokio Delay error: {}", e)),
                                    )
                                } else {
                                    Box::new(future::err(e))
                                }
                            }
                        }
                    },
                )
        })
    }

    /// Put some blocks into the block store (if they are not there already), and try to update the
    /// head block pointer. If missing blocks prevent such an update, return a Vec with at least
    /// one of the missing blocks' hashes.
    fn ingest_blocks<'a, B: Stream<Item = EthereumBlock, Error = Error> + Send + 'a>(
        &'a self,
        blocks: B,
    ) -> impl Future<Item = Vec<H256>, Error = Error> + Send + 'a {
        self.chain_store.upsert_blocks(blocks).and_then(move |()| {
            self.chain_store
                .attempt_chain_head_update(self.ancestor_count)
        })
    }

    /// Requests the specified blocks via web3, returning them in a stream (potentially out of
    /// order).
    fn get_blocks<'a>(
        &'a self,
        block_hashes: &[H256],
    ) -> Box<Stream<Item = EthereumBlock, Error = Error> + Send + 'a> {
        // Don't bother with a batch request if nothing to request
        if block_hashes.is_empty() {
            return Box::new(stream::empty());
        }

        let batching_web3 = Web3::new(Batch::new(self.web3_transport.clone()));

        // Add requests to batch
        let block_futures = block_hashes
            .into_iter()
            .map(|block_hash| {
                batching_web3.eth()
                    .block_with_txs(BlockId::from(*block_hash))
                    .map_err(|e| format_err!("could not get block from Ethereum: {}", e))
                    .and_then(|block_opt| {
                        block_opt.ok_or(format_err!("no block returned from Ethereum"))
                    })
                    .and_then(move |block| {
                        let receipt_futures = block.transactions
                            .iter()
                            .map(move |tx| {
                                self.get_transaction_receipt(tx.hash)
                            })
                            .collect::<Vec<_>>();

                        stream::futures_ordered(receipt_futures).collect()
                            .map(move |transaction_receipts| EthereumBlock {
                                block, transaction_receipts
                            })
                    })
            })
            // Collect to ensure that `block_with_txs` calls happen before `submit_batch`
            .collect::<Vec<_>>();

        // Submit all requests in batch
        Box::new(
            batching_web3
                .transport()
                .submit_batch()
                .map_err(|e| format_err!("could not get blocks from Ethereum: {}", e))
                .map(|_| stream::futures_unordered(block_futures))
                .flatten_stream(),
        )
    }
}
