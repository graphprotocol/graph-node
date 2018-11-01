use futures::future;
use futures::prelude::*;
use graph::ethabi::Token;
use std::collections::HashSet;
use std::sync::Arc;

use graph::components::ethereum::{EthereumAdapter as EthereumAdapterTrait, *};
use graph::prelude::*;
use graph::web3;
use graph::web3::api::Web3;
use graph::web3::transports::batch::Batch;
use graph::web3::types::{Filter, *};

#[derive(Clone)]
pub struct EthereumAdapter<T: web3::Transport> {
    web3: Arc<Web3<T>>,
}

/// Number of chunks to request in parallel when streaming logs.
const LOG_STREAM_PARALLEL_CHUNKS: u64 = 5;

/// Number of blocks to request in each chunk.
const LOG_STREAM_CHUNK_SIZE_IN_BLOCKS: u64 = 10000;

impl<T> EthereumAdapter<T>
where
    T: web3::BatchTransport + Send + Sync + 'static,
    T::Out: Send,
{
    pub fn new(transport: T) -> Self {
        EthereumAdapter {
            web3: Arc::new(Web3::new(transport)),
        }
    }

    fn logs_with_sigs(
        &self,
        logger: &Logger,
        from: u64,
        to: u64,
        addresses: Vec<H160>,
        event_signatures: Vec<H256>,
    ) -> impl Future<Item = Vec<Log>, Error = Error> {
        let eth_adapter = self.clone();
        let logger = logger.to_owned();
        let event_sig_count = event_signatures.len();

        retry("eth_getLogs RPC call", &logger)
            .no_limit()
            .timeout_secs(60)
            .run(move || {
                // Create a log filter
                let log_filter: Filter = FilterBuilder::default()
                    .from_block(from.into())
                    .to_block(to.into())
                    .address(addresses.clone())
                    .topics(Some(event_signatures.clone()), None, None, None)
                    .build();

                // Request logs from client
                let logger = logger.clone();
                eth_adapter
                    .web3
                    .eth()
                    .logs(log_filter)
                    .map(move |logs| {
                        debug!(logger, "Received logs for [{}, {}].", from, to);
                        logs
                    }).map_err(SyncFailure::new)
                    .from_err()
            }).map_err(move |e| {
                e.into_inner().unwrap_or_else(move || {
                    format_err!(
                        "Ethereum node took too long to respond to eth_getLogs \
                         (from block {}, to block {}, {} event signatures)",
                        from,
                        to,
                        event_sig_count
                    )
                })
            })
    }

    fn log_stream(
        &self,
        logger: &Logger,
        from: u64,
        to: u64,
        log_filter: EthereumLogFilter,
    ) -> impl Stream<Item = Vec<Log>, Error = Error> + Send {
        if from > to {
            panic!(
                "cannot produce a log stream on a backwards block range (from={}, to={})",
                from, to
            );
        }

        // Collect all event sigs
        let event_sigs = log_filter
            .contract_address_and_event_sig_pairs
            .iter()
            .map(|(_addr, sig)| *sig)
            .collect::<HashSet<H256>>()
            .into_iter()
            .collect::<Vec<H256>>();

        // Collect all contract addresses
        let addresses = log_filter
            .contract_address_and_event_sig_pairs
            .iter()
            .map(|(addr, _sig)| *addr)
            .collect::<HashSet<H160>>()
            .into_iter()
            .collect::<Vec<H160>>();

        let eth_adapter = self.clone();
        let logger = logger.to_owned();
        stream::unfold(from, move |mut chunk_offset| {
            if chunk_offset <= to {
                let mut chunk_futures = vec![];

                if chunk_offset < 4_000_000 {
                    let chunk_end = (chunk_offset + 100_000 - 1).min(to).min(4_000_000);

                    debug!(
                        logger,
                        "Starting request for logs in block range [{},{}]", chunk_offset, chunk_end
                    );
                    let log_filter = log_filter.clone();
                    let chunk_future = eth_adapter
                        .logs_with_sigs(
                            &logger,
                            chunk_offset,
                            chunk_end,
                            addresses.clone(),
                            event_sigs.clone(),
                        ).map(move |logs| {
                            logs.into_iter()
                                // Filter out false positives
                                .filter(move |log| log_filter.matches(log))
                                .collect()
                        });
                    chunk_futures
                        .push(Box::new(chunk_future)
                            as Box<Future<Item = Vec<Log>, Error = _> + Send>);

                    chunk_offset = chunk_end + 1;
                } else {
                    for _ in 0..LOG_STREAM_PARALLEL_CHUNKS {
                        // Last chunk may be shorter than CHUNK_SIZE, so needs special handling
                        let is_last_chunk = (chunk_offset + LOG_STREAM_CHUNK_SIZE_IN_BLOCKS) > to;

                        // Determine the upper bound on the chunk
                        // Note: chunk_end is inclusive
                        let chunk_end = if is_last_chunk {
                            to
                        } else {
                            // Subtract 1 to make range inclusive
                            chunk_offset + LOG_STREAM_CHUNK_SIZE_IN_BLOCKS - 1
                        };

                        // Start request for this chunk of logs
                        // Note: this function filters only on event sigs,
                        // and will therefore return false positives
                        debug!(
                            logger,
                            "Starting request for logs in block range [{},{}]",
                            chunk_offset,
                            chunk_end
                        );
                        let log_filter = log_filter.clone();
                        let chunk_future = eth_adapter
                            .logs_with_sigs(
                                &logger,
                                chunk_offset,
                                chunk_end,
                                addresses.clone(),
                                event_sigs.clone(),
                            ).map(move |logs| {
                                logs.into_iter()
                                    // Filter out false positives
                                    .filter(move |log| log_filter.matches(log))
                                    .collect()
                            });

                        // Save future for later
                        chunk_futures.push(Box::new(chunk_future)
                            as Box<Future<Item = Vec<Log>, Error = _> + Send>);

                        // If last chunk, will push offset past `to`. That's fine.
                        chunk_offset += LOG_STREAM_CHUNK_SIZE_IN_BLOCKS;

                        if is_last_chunk {
                            break;
                        }
                    }
                }

                // Combine chunk futures into one future (Vec<Log>, u64)
                Some(stream::futures_ordered(chunk_futures).collect().map(
                    move |chunks: Vec<Vec<Log>>| {
                        let flattened = chunks.into_iter().flat_map(|v| v).collect::<Vec<Log>>();
                        (flattened, chunk_offset)
                    },
                ))
            } else {
                None
            }
        }).filter(|chunk| !chunk.is_empty())
    }

    fn call(
        &self,
        logger: &Logger,
        contract_address: Address,
        call_data: Bytes,
        block_number_opt: Option<BlockNumber>,
    ) -> impl Future<Item = Bytes, Error = Error> + Send {
        let web3 = self.web3.clone();
        let logger = logger.clone();

        // Outer retry used only for 0-byte responses,
        // where we can't guarantee the problem is temporary.
        // If we keep getting back 0-byte responses,
        // eventually we assume it's right and return it.
        retry("eth_call RPC call (outer)", &logger)
            .when(|result: &Result<Bytes, _>| {
                match result {
                    // Retry only if zero-length response received
                    Ok(bytes) => bytes.0.is_empty(),

                    // Errors are retried in the inner retry
                    Err(_) => false,
                }
            }).limit(16)
            .no_logging()
            .no_timeout()
            .run(move || {
                let web3 = web3.clone();
                let call_data = call_data.clone();

                retry("eth_call RPC call", &logger)
                    .no_limit()
                    .timeout_secs(60)
                    .run(move || {
                        let req = CallRequest {
                            from: None,
                            to: contract_address,
                            gas: None,
                            gas_price: None,
                            value: None,
                            data: Some(call_data.clone()),
                        };
                        web3.eth()
                            .call(req, block_number_opt)
                            .map_err(SyncFailure::new)
                            .from_err()
                    }).map_err(|e| {
                        e.into_inner().unwrap_or_else(|| {
                            format_err!("Ethereum node took too long to perform function call")
                        })
                    })
            })
    }
}

impl<T> EthereumAdapterTrait for EthereumAdapter<T>
where
    T: web3::BatchTransport + Send + Sync + 'static,
    T::Batch: Send,
    T::Out: Send,
{
    fn net_identifiers(
        &self,
        logger: &Logger,
    ) -> Box<Future<Item = EthereumNetworkIdentifier, Error = Error> + Send> {
        let logger = logger.clone();

        let web3 = self.web3.clone();
        let net_version_future = retry("net_version RPC call", &logger)
            .no_limit()
            .timeout_secs(20)
            .run(move || web3.net().version().map_err(SyncFailure::new).from_err());

        let web3 = self.web3.clone();
        let gen_block_hash_future = retry("eth_getBlockByNumber(0, false) RPC call", &logger)
            .no_limit()
            .timeout_secs(30)
            .run(move || {
                web3.eth()
                    .block(BlockNumber::Earliest.into())
                    .map_err(SyncFailure::new)
                    .from_err()
                    .and_then(|gen_block_opt| {
                        future::result(
                            gen_block_opt
                                .ok_or_else(|| {
                                    format_err!("Ethereum node could not find genesis block")
                                }).map(|gen_block| gen_block.hash.unwrap()),
                        )
                    })
            });

        Box::new(
            net_version_future
                .join(gen_block_hash_future)
                .map(
                    |(net_version, genesis_block_hash)| EthereumNetworkIdentifier {
                        net_version,
                        genesis_block_hash,
                    },
                ).map_err(|e| {
                    e.into_inner().unwrap_or_else(|| {
                        format_err!("Ethereum node took too long to read network identifiers")
                    })
                }),
        )
    }

    fn block_by_hash(
        &self,
        logger: &Logger,
        block_hash: H256,
    ) -> Box<Future<Item = Option<EthereumBlock>, Error = Error> + Send> {
        let web3 = self.web3.clone();
        let logger = logger.clone();

        let block_opt_future = retry("eth_getBlockByHash RPC call", &logger)
            .no_limit()
            .timeout_secs(60)
            .run(move || {
                web3.eth()
                    .block_with_txs(BlockId::Hash(block_hash))
                    .map_err(SyncFailure::new)
                    .from_err()
            }).map_err(move |e| {
                e.into_inner().unwrap_or_else(move || {
                    format_err!("Ethereum node took too long to return block {}", block_hash)
                })
            });

        let web3 = self.web3.clone();
        Box::new(block_opt_future.and_then(move |block_opt| {
            let web3 = web3.clone();

            let block = block_opt?;

            // Retry, but eventually give up.
            // The receipt might be missing because the block was uncled, and the
            // transaction never made it back into the main chain.
            Some(
                retry("batch eth_getTransactionReceipt RPC call", &logger)
                    .limit(32)
                    .timeout_secs(60)
                    .run(move || {
                        let block = block.clone();
                        let batching_web3 = Web3::new(Batch::new(web3.transport().clone()));

                        let receipt_futures = block
                            .transactions
                            .iter()
                            .map(|tx| {
                                let tx_hash = tx.hash;

                                batching_web3
                                    .eth()
                                    .transaction_receipt(tx_hash)
                                    .map_err(SyncFailure::new)
                                    .from_err()
                                    .and_then(move |receipt_opt| {
                                        // Might be transient, but might be permanent due to reorg
                                        receipt_opt.ok_or_else(move || {
                                            format_err!(
                                                "Ethereum node is missing transaction receipt: {}",
                                                tx_hash
                                            )
                                        })
                                    }).and_then(move |receipt| {
                                        // Check if receipt is for the right block
                                        if receipt.block_hash != block_hash {
                                            // If the receipt came from a different block, then the Ethereum
                                            // node no longer considers this block to be in the main chain.
                                            // Nothing we can do from here except give up trying to ingest this
                                            // block.
                                            // There is no way to get the transaction receipt from this block.
                                            Err(format_err!(
                                                "could not get receipt for block {:?} \
                                                 because block is off the main chain",
                                                block_hash
                                            ))
                                        } else {
                                            Ok(receipt)
                                        }
                                    })
                            }).collect::<Vec<_>>();

                        batching_web3
                            .transport()
                            .submit_batch()
                            .map_err(SyncFailure::new)
                            .from_err()
                            .and_then(move |_| {
                                stream::futures_ordered(receipt_futures).collect().map(
                                    move |transaction_receipts| EthereumBlock {
                                        block,
                                        transaction_receipts,
                                    },
                                )
                            })
                    }).map_err(move |e| {
                        e.into_inner().unwrap_or_else(move || {
                            format_err!(
                                "Ethereum node took too long to return receipts for block {}",
                                block_hash
                            )
                        })
                    }),
            )
        }))
    }

    fn block_hash_by_block_number(
        &self,
        logger: &Logger,
        block_number: u64,
    ) -> Box<Future<Item = Option<H256>, Error = Error> + Send> {
        let web3 = self.web3.clone();

        Box::new(
            retry("eth_getBlockByNumber RPC call", &logger)
                .no_limit()
                .timeout_secs(60)
                .run(move || {
                    web3.eth()
                        .block(BlockId::Number(block_number.into()))
                        .map_err(SyncFailure::new)
                        .from_err()
                        .map(|block_opt| block_opt.map(|block| block.hash.unwrap()))
                }).map_err(move |e| {
                    e.into_inner().unwrap_or_else(move || {
                        format_err!(
                            "Ethereum node took too long to return data for block #{}",
                            block_number
                        )
                    })
                }),
        )
    }

    fn is_on_main_chain(
        &self,
        logger: &Logger,
        block_ptr: EthereumBlockPointer,
    ) -> Box<Future<Item = bool, Error = Error> + Send> {
        Box::new(
            self.block_hash_by_block_number(&logger, block_ptr.number)
                .and_then(move |block_hash_opt| {
                    block_hash_opt
                        .ok_or_else(|| {
                            format_err!("Ethereum node is missing block #{}", block_ptr.number)
                        }).map(|block_hash| block_hash == block_ptr.hash)
                }),
        )
    }

    fn find_first_blocks_with_logs(
        &self,
        logger: &Logger,
        from: u64,
        to: u64,
        log_filter: EthereumLogFilter,
    ) -> Box<Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send> {
        Box::new(
            // Get a stream of all relevant logs in range
            self.log_stream(&logger, from, to, log_filter)
                // Get first chunk of logs
                .take(1)
                // Collect 0 or 1 vecs of logs
                .collect()
                // Produce Vec<block ptr> or None
                .map(|chunks| match chunks.len() {
                    0 => vec![],
                    1 => {
                        let mut block_ptrs = vec![];
                        for log in chunks[0].iter() {
                            let hash = log
                                .block_hash
                                .expect("log from Eth node is missing block hash");
                            let number = log
                                .block_number
                                .expect("log from Eth node is missing block number")
                                .as_u64();
                            let block_ptr = EthereumBlockPointer::from((hash, number));

                            if !block_ptrs.contains(&block_ptr) {
                                if let Some(prev) = block_ptrs.last() {
                                    assert!(prev.number < number);
                                }
                                block_ptrs.push(block_ptr);
                            }
                        }
                        block_ptrs
                    }
                    _ => unreachable!(),
                }),
        )
    }

    fn contract_call(
        &self,
        logger: &Logger,
        call: EthereumContractCall,
    ) -> Box<Future<Item = Vec<Token>, Error = EthereumContractCallError> + Send> {
        // Emit custom error for type mismatches.
        for (token, kind) in call
            .args
            .iter()
            .zip(call.function.inputs.iter().map(|p| &p.kind))
        {
            if !token.type_check(kind) {
                return Box::new(future::err(EthereumContractCallError::TypeError(
                    token.clone(),
                    kind.clone(),
                )));
            }
        }

        // Encode the call parameters according to the ABI
        let call_data = call.function.encode_input(&call.args).unwrap();

        Box::new(
            // Make the actual function call
            self.call(
                logger,
                call.address,
                Bytes(call_data),
                Some(call.block_ptr.number.into()),
            ).map_err(EthereumContractCallError::from)
            .and_then(move |output| {
                // Decode the return values according to the ABI
                call.function
                    .decode_output(&output.0)
                    .map_err(EthereumContractCallError::from)
            }),
        )
    }
}
