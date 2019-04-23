use futures::future;
use futures::prelude::*;
use graph::ethabi::Token;
use lazy_static::lazy_static;
use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use tiny_keccak::{keccak256};

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

lazy_static! {
    static ref LOG_STREAM_FAST_SCAN_END: u64 = ::std::env::var("ETHEREUM_FAST_SCAN_END")
        .unwrap_or("4000000".into())
        .parse::<u64>()
        .expect("invalid fast scan end block number");

    /// Number of chunks to request in parallel when streaming logs.
    static ref LOG_STREAM_PARALLEL_CHUNKS: u64 = ::std::env::var("ETHEREUM_PARALLEL_BLOCK_RANGES")
        .unwrap_or("5".into())
        .parse::<u64>()
        .expect("invalid number of parallel Ethereum block ranges to scan");

    /// Number of blocks to request in each chunk.
    static ref LOG_STREAM_CHUNK_SIZE_IN_BLOCKS: u64 = ::std::env::var("ETHEREUM_BLOCK_RANGE_SIZE")
        .unwrap_or("10000".into())
        .parse::<u64>()
        .expect("invalid Ethereum block range size");
}

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

    fn calls(
        &self,
        logger: &Logger,
        from: u64,
        to: u64,
        addresses: Vec<H160>,
    ) -> impl Future<Item = Vec<EthereumCall>, Error = Error> {
        let eth = self.clone();
        let logger = logger.to_owned();

        retry("trace_filter RPC call", &logger)
            .no_limit()
            .timeout_secs(60)
            .run(move || {
                let trace_filter: TraceFilter = match addresses.len() {
                    0 => {
                        TraceFilterBuilder::default()
                            .from_block(from.into())
                            .to_block(to.into())
                            .build()
                    }
                    _ => {
                        TraceFilterBuilder::default()
                            .from_block(from.into())
                            .to_block(to.into())
                            .to_address(addresses.clone())
                            .build()
                    }
                };

                let logger = logger.clone();
                eth
                    .web3
                    .trace()
                    .filter(trace_filter)
                    .map(move |traces| {
                        debug!(logger, "Received traces for block range [{}, {}]", from, to);
                        // Transform traces into calls
                        traces
                            .iter()
                            .filter(|trace| match trace.action {
                                Action::Call(_) => true,
                                _ => false,
                            })
                            .map(EthereumCall::from)
                            .collect()
                    })
                    .map_err(SyncFailure::new)
                    .from_err()
            })
            .map_err(move |e| {
                e.into_inner().unwrap_or_else(move || {
                    format_err!(
                        "Ethereum node took too long to respond to trace_filter \
                         (from block {}, to block {})",
                        from,
                        to
                    )
                })
            })
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
                    })
                    .map_err(SyncFailure::new)
                    .from_err()
            })
            .map_err(move |e| {
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

    fn call_stream(
        self,
        logger: &Logger,
        from: u64,
        to: u64,
        call_filter: EthereumCallFilter,
    ) -> impl Stream<Item = Vec<EthereumCall>, Error = Error> + Send {
        if from > to {
            panic!(
                "Can not produce a call stream on a backwards block range: from = {}, to = {}",
                from, to,
            );
        }
        
        let eth = self.clone();
        let logger = logger.to_owned();
        let call_filter: HashMap<Address, HashSet<[u8; 4]>> = call_filter
            .contract_addresses_function_signatures
            .into_iter()
            .map(|(addr, fsigs)| {
                (addr, fsigs.into_iter()
                 .map(|signature| {
                     let hash = keccak256(signature.as_bytes());
                     [hash[0], hash[1], hash[2], hash[3]]
                 }).collect::<HashSet<[u8; 4]>>())
            }).collect();
        let addresses: Vec<H160> = call_filter
            .iter()
            .map(|(addr, _fsigs)| *addr)
            .collect::<HashSet<H160>>()
            .into_iter()
            .collect::<Vec<H160>>();

        stream::unfold(from, move |start| {
            if start > to {
                return None
            }
            let end = (start + 10_000 - 1).min(to);
            let new_start = end + 1;
            let call_filter = call_filter.clone();
            debug!(logger, "Starting request for method calls in block range: [{}, {}]", start, end);
            Some(eth.calls(&logger, start, end, addresses.clone())
                 .map(move |calls| {
                     calls
                         .into_iter()
                         .filter(|call| {
                             // Ensure the call is to a contract the filter expressed an interest in
                             if !call_filter.contains_key(&call.to) {
                                 return false
                             }
                             // If the call is to a contract with no specified functions, keep the call
                             if call_filter.get(&call.to).unwrap().is_empty() {
                                 return true
                             }
                             // Ensure the call is to run a function the filter expressed an interest in
                             let input = &call.input.0;
                             if input.len() < 4 { return false }
                             call_filter
                                 .get(&call.to)
                                 .unwrap()
                                 .contains(&input[..4])
                         })
                         .collect()
                 })
                 .map(move |calls| {
                     (calls, new_start)
                 }))
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

        // Collect all contract addresses; if we have a data source without a contract
        // address, we can't add addresses to the filter because it would only match
        // the contracts for which we _have_ addresses; therefore if we have a data source
        // without a contract address, we perform a broader logs scan and filter out
        // irrelevant events ourselves.
        //
        // Our own filtering is performed later when the events are passed to
        // subgraphs and runtime hosts for processing:
        // - At the top level in `BlockStreamContext::do_step`
        // - At the subgraph level in `SubgraphInstance::matches_log`
        // - At the data source level in `RuntimeHost::matches_log`
        let addresses = if log_filter
            .contract_address_and_event_sig_pairs
            .iter()
            .any(|(addr, _)| addr.is_none())
        {
            vec![]
        } else {
            log_filter
                .contract_address_and_event_sig_pairs
                .iter()
                .map(|(addr, _sig)| match addr {
                    None => unreachable!(
                        "shouldn't include addresses in Ethereum logs filter \
                         if there are data sources without a contract address"
                    ),
                    Some(addr) => *addr,
                })
                .collect::<HashSet<H160>>()
                .into_iter()
                .collect::<Vec<H160>>()
        };

        let eth_adapter = self.clone();
        let logger = logger.to_owned();
        stream::unfold(from, move |start| {
            if start > to {
                return None
            }
            let end = if start < *LOG_STREAM_FAST_SCAN_END {
                (start + 100_000 - 1)
                    .min(to)
                    .min(*LOG_STREAM_FAST_SCAN_END)
            } else {
                (start + 10_000 - 1).min(to)
            };
            let new_start = end + 1;
            
            debug!(logger, "Starting request for logs in block range: [{}, {}]", start, end);
            
            let log_filter = log_filter.clone();
            Some(eth
                .logs_with_sigs(
                    &logger,
                    start,
                    end,
                    addresses.clone(),
                    event_sigs.clone(),
                )
                .map(move |logs| {
                    logs.into_iter()
                        .filter(move |log| log_filter.matches(log))
                        .collect()
                })
                .map(move |logs| {
                    (logs, new_start)
                }))
        })
        .filter(|chunk| !chunk.is_empty())
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
            })
            .limit(16)
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
                    })
                    .map_err(|e| {
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
                                })
                                .map(|gen_block| gen_block.hash.unwrap()),
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
                )
                .map_err(|e| {
                    e.into_inner().unwrap_or_else(|| {
                        format_err!("Ethereum node took too long to read network identifiers")
                    })
                }),
        )
    }

    fn latest_block(
        &self,
        logger: &Logger,
    ) -> Box<Future<Item = Block<Transaction>, Error = EthereumAdapterError> + Send> {
        let web3 = self.web3.clone();

        Box::new(
            retry("eth_getBlockByNumber(latest) RPC call", logger)
                .no_limit()
                .timeout_secs(60)
                .run(move || {
                    web3.eth()
                        .block_with_txs(BlockNumber::Latest.into())
                        .map_err(|e| format_err!("could not get latest block from Ethereum: {}", e))
                        .from_err()
                        .and_then(|block_opt| {
                            block_opt.ok_or_else(|| {
                                format_err!("no latest block returned from Ethereum").into()
                            })
                        })
                })
                .map_err(move |e| {
                    e.into_inner().unwrap_or_else(move || {
                        format_err!("Ethereum node took too long to return latest block").into()
                    })
                }),
        )
    }

    fn block_by_hash(
        &self,
        logger: &Logger,
        block_hash: H256,
    ) -> Box<Future<Item = Option<Block<Transaction>>, Error = Error> + Send> {
        let web3 = self.web3.clone();
        let logger = logger.clone();

        Box::new(
            retry("eth_getBlockByHash RPC call", &logger)
                .no_limit()
                .timeout_secs(60)
                .run(move || {
                    web3.eth()
                        .block_with_txs(BlockId::Hash(block_hash))
                        .map_err(SyncFailure::new)
                        .from_err()
                })
                .map_err(move |e| {
                    e.into_inner().unwrap_or_else(move || {
                        format_err!("Ethereum node took too long to return block {}", block_hash)
                    })
                }),
        )
    }

    fn load_full_block(
        &self,
        logger: &Logger,
        block: Block<Transaction>,
    ) -> Box<Future<Item = EthereumBlock, Error = EthereumAdapterError> + Send> {
        let logger = logger.clone();
        let web3 = self.web3.clone();

        let block_hash = block.hash.expect("block is missing block hash");

        // Retry, but eventually give up.
        // A receipt might be missing because the block was uncled, and the
        // transaction never made it back into the main chain.
        Box::new(
            retry("batch eth_getTransactionReceipt RPC call", &logger)
                .limit(16)
                .no_logging()
                .timeout_secs(60)
                .run(move || {
                    let block = block.clone();
                    let batching_web3 = Web3::new(Batch::new(web3.transport().clone()));

                    let receipt_futures = block
                        .transactions
                        .iter()
                        .map(|tx| {
                            let logger = logger.clone();
                            let tx_hash = tx.hash;

                            batching_web3
                                .eth()
                                .transaction_receipt(tx_hash)
                                .map_err(SyncFailure::new)
                                .from_err()
                                .map_err(EthereumAdapterError::Unknown)
                                .and_then(move |receipt_opt| {
                                    receipt_opt.ok_or_else(move || {
                                        // No receipt was returned.
                                        //
                                        // This can be because the Ethereum node no longer
                                        // considers this block to be part of the main chain,
                                        // and so the transaction is no longer in the main
                                        // chain.  Nothing we can do from here except give up
                                        // trying to ingest this block.
                                        //
                                        // This could also be because the receipt is simply not
                                        // available yet.  For that case, we should retry until
                                        // it becomes available.
                                        EthereumAdapterError::BlockUnavailable(block_hash)
                                    })
                                })
                                .and_then(move |receipt| {
                                    // Parity nodes seem to return receipts with no block hash
                                    // when a transaction is no longer in the main chain, so
                                    // treat that case the same as a receipt being absent
                                    // entirely.
                                    let receipt_block_hash =
                                        receipt.block_hash.ok_or_else(|| {
                                            EthereumAdapterError::BlockUnavailable(block_hash)
                                        })?;

                                    // Check if receipt is for the right block
                                    if receipt_block_hash != block_hash {
                                        trace!(
                                            logger, "receipt block mismatch";
                                            "receipt_block_hash" =>
                                                receipt_block_hash.to_string(),
                                            "block_hash" =>
                                                block_hash.to_string()
                                        );

                                        // If the receipt came from a different block, then the
                                        // Ethereum node no longer considers this block to be
                                        // in the main chain.  Nothing we can do from here
                                        // except give up trying to ingest this block.
                                        // There is no way to get the transaction receipt from
                                        // this block.
                                        Err(EthereumAdapterError::BlockUnavailable(block_hash))
                                    } else {
                                        Ok(receipt)
                                    }
                                })
                        })
                        .collect::<Vec<_>>();

                    batching_web3
                        .transport()
                        .submit_batch()
                        .map_err(SyncFailure::new)
                        .from_err()
                        .map_err(EthereumAdapterError::Unknown)
                        .and_then(move |_| {
                            stream::futures_ordered(receipt_futures).collect().map(
                                move |transaction_receipts| EthereumBlock {
                                    block,
                                    transaction_receipts,
                                },
                            )
                        })
                })
                .map_err(move |e| {
                    e.into_inner().unwrap_or_else(move || {
                        format_err!(
                            "Ethereum node took too long to return receipts for block {}",
                            block_hash
                        )
                        .into()
                    })
                }),
        )
    }

    fn block_parent_hash_by_block_hash(
        &self,
        logger: &Logger,
        descendant_block_hash: H256,
    ) -> Box<Future<Item = Option<H256>, Error = Error> + Send> {
        let web3 = self.web3.clone();

        Box::new(
            retry("eth_getBlockByHash RPC call", &logger)
                .no_limit()
                .timeout_secs(60)
                .run(move || {
                    web3.eth()
                        .block(BlockId::Hash(descendant_block_hash))
                        .map_err(SyncFailure::new)
                        .from_err()
                        .map(|block_opt| block_opt.map(|block| block.parent_hash))
                })
                .map_err(move |e| {
                    e.into_inner().unwrap_or_else(move || {
                        format_err!(
                            "Ethereum node took too long to return data for block hash = {}",
                            descendant_block_hash
                        )
                    })
                }),
        )
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
                })
                .map_err(move |e| {
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
                        })
                        .map(|block_hash| block_hash == block_ptr.hash)
                }),
        )
    }

    fn calls_in_block(
        &self,
        logger: &Logger,
        block_number: u64,
        block_hash: H256,
        call_filter: EthereumCallFilter,
    ) -> Box<Future<Item = Vec<EthereumCall>, Error = Error> + Send> {
        let eth = self.clone();
        Box::new(
            eth.call_stream(&logger, block_number, block_number, call_filter)
                .collect()
                .map(|call_chunks| match call_chunks.len() {
                    0 => vec![],
                    _ => {
                        call_chunks
                            .into_iter()
                            .flatten()
                            .collect()
                    }
                })
                .and_then(move |calls| {
                    // Ensure the `EthereumCall` objects are for the correct Ethereum block
                    // by checking the call's block hash against the desired block hash.
                    // 
                    // DISCLAIMER: If the call stream returns calls for the correct block_number but the
                    // incorrect block_hash we can catch this and error. But if the call streams returns no calls
                    // for the correct block_number but the incorrect block_hash, there is no way to tell.
                    // This method should only be used when populating triggers before the reorg threshold.
                    for call in calls.iter() {
                        if call.block_hash != block_hash {
                            return future::err(format_err!(
                                "Call stream returned calls for an unexpected block: number = {}, hash = {}",
                                block_number,
                                block_hash,
                            ))
                        }
                    }
                    future::ok(calls)
                })
        )
    }

    fn blocks_with_triggers(
        &self,
        logger: &Logger,
        from: u64,
        to: u64,
        log_filter_opt: Option<EthereumLogFilter>,
        call_filter_opt: Option<EthereumCallFilter>,
        block_filter_opt: Option<EthereumBlockFilter>,
    ) -> Box<Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send> {
        // If no filters are provided, return an empty vector of blocks.
        if log_filter_opt.is_none() && call_filter_opt.is_none() && block_filter_opt.is_none() {
            return Box::new(future::ok(vec![]))
        }

        // Each trigger filter needs to be queried for the same block range
        // and the blocks yielded need to be deduped. If any error occurs while searching for a trigger type, the
        // entire operation fails.
        let eth = self.clone();
        let mut block_futs: Vec<Box<Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send>> = vec![];
        if log_filter_opt.is_some() {
            block_futs.push(Box::new(eth.blocks_with_logs(&logger, from, to, log_filter_opt.unwrap())));
        }
        if call_filter_opt.is_some() {
            block_futs.push(Box::new(eth.blocks_with_calls(&logger, from, to, call_filter_opt.unwrap())));
        }
        if block_filter_opt.is_some() {
            let block_filter = block_filter_opt.unwrap();
            match block_filter.contract_addresses.len() {
                0 => block_futs.push(Box::new(eth.blocks(&logger, from, to))),
                _ => {
                    let call_filter = EthereumCallFilter {
                        contract_addresses_function_signatures: block_filter
                            .contract_addresses
                            .into_iter()
                            .map(|address| {
                                (address, HashSet::default())
                            }).collect::<HashMap<Address, HashSet<String>>>(),
                    };
                    block_futs.push(Box::new(eth.blocks_with_calls(&logger, from, to, call_filter)));
                }
            }
        }
        Box::new(future::join_all(block_futs)
            .and_then(|block_pointer_chunks| {
                let mut blocks = block_pointer_chunks
                    .into_iter()
                    .flatten()
                    .collect::<Vec<EthereumBlockPointer>>();
                blocks.dedup();
                blocks.sort_by(|a, b| {
                    a.number.cmp(&b.number)
                });
                future::ok(blocks)
            }))
    }

    fn blocks(
        &self,
        logger: &Logger,
        from: u64,
        to: u64,
    ) -> Box<Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send> {
        let eth = self.clone();
        let logger = logger.clone();

        // Generate `EthereumBlockPointers` from `to` backwards to `from`
        Box::new(
            self.block_hash_by_block_number(&logger, to)
                .map(move |block_hash_opt| {
                    EthereumBlockPointer {
                        hash: block_hash_opt.unwrap(),
                        number: to,
                    }
                })
                .and_then(move |block_pointer| {
                    stream::unfold(block_pointer, move |descendant_block_pointer| {
                        if descendant_block_pointer.number < from {
                            return None
                        }
                        // Populate the parent block pointer
                        Some(eth
                             .block_parent_hash_by_block_hash(&logger, descendant_block_pointer.hash)
                             .map(move |block_hash_opt| {
                                 let parent_block_pointer = EthereumBlockPointer {
                                     hash: block_hash_opt.unwrap(),
                                     number: descendant_block_pointer.number - 1,
                                 };
                                 (descendant_block_pointer, parent_block_pointer)
                             }))
                    }).collect()
                })
                .map(move |mut block_pointers| {
                    block_pointers.reverse();
                    block_pointers
                }))
    }

    fn blocks_with_logs(
        &self,
        logger: &Logger,
        from: u64,
        to: u64,
        log_filter: EthereumLogFilter,
    ) -> Box<Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send> {
        let eth = self.clone();
        Box::new(
            // Get a stream of all relevant logs in range
            eth.log_stream(&logger, from, to, log_filter)
                .collect()
                .map(|log_chunks| match log_chunks.len() {
                    0 => vec![],
                    _ => {
                        let logs: Vec<Log> = log_chunks
                            .into_iter()
                            .flatten()
                            .collect();
                        let mut block_ptrs = vec![];
                        for log in logs.iter() {
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
                }))
    }

    fn blocks_with_calls(
        &self,
        logger: &Logger,
        from: u64,
        to: u64,
        call_filter: EthereumCallFilter,
    ) -> Box<Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send> {
        let eth = self.clone();
        
        Box::new(
            eth.call_stream(&logger, from, to, call_filter)
                .collect()
                .map(|call_chunks| match call_chunks.len() {
                    0 => vec![],
                    _ => {
                        let calls: Vec<EthereumCall> = call_chunks
                            .into_iter()
                            .flatten()
                            .collect();
                        let mut block_ptrs = vec![];
                        for call in calls.iter() {
                            let hash = call.block_hash;
                            let number = call.block_number;
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
                }))
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
            )
            .map_err(EthereumContractCallError::from)
            .and_then(move |output| {
                // Decode the return values according to the ABI
                call.function
                    .decode_output(&output.0)
                    .map_err(EthereumContractCallError::from)
            }),
        )
    }
}
