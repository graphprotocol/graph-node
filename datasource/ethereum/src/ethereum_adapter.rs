use ethabi::Token;
use futures::future;
use futures::prelude::*;
use lazy_static::lazy_static;
use std::collections::HashSet;
use std::sync::Arc;

use ethabi::ParamType;
use graph::components::ethereum::{EthereumAdapter as EthereumAdapterTrait, *};
use graph::prelude::*;
use web3;
use web3::api::Web3;
use web3::transports::batch::Batch;
use web3::types::{Filter, *};

#[derive(Clone)]
pub struct EthereumAdapter<T: web3::Transport> {
    web3: Arc<Web3<T>>,
    start_block: u64,
}

lazy_static! {
    static ref TRACE_STREAM_STEP_SIZE: u64 = std::env::var("ETHEREUM_TRACE_STREAM_STEP_SIZE")
        .unwrap_or("200".into())
        .parse::<u64>()
        .expect("invalid trace stream step size");

    /// Maximum number of chunks to request in parallel when streaming logs.
    static ref LOG_STREAM_PARALLEL_CHUNKS: u64 = std::env::var("ETHEREUM_PARALLEL_BLOCK_RANGES")
        .unwrap_or("100".into())
        .parse::<u64>()
        .expect("invalid number of parallel Ethereum block ranges to scan");

    /// Maximum range size for `eth.getLogs` requests that dont filter on
    /// contract address, only event signature, and are therefore expensive.
    ///
    /// According to Ethereum node operators, size 500 is reasonable here.
    static ref MAX_EVENT_ONLY_RANGE: u64 = std::env::var("GRAPH_ETHEREUM_MAX_EVENT_ONLY_RANGE")
        .unwrap_or("500".into())
        .parse::<u64>()
        .expect("invalid number of parallel Ethereum block ranges to scan");
}

impl<T> EthereumAdapter<T>
where
    T: web3::BatchTransport + Send + Sync + 'static,
    T::Out: Send,
{
    pub fn new(transport: T, start_block: u64) -> Self {
        EthereumAdapter {
            web3: Arc::new(Web3::new(transport)),
            start_block,
        }
    }

    fn traces(
        &self,
        logger: &Logger,
        from: u64,
        to: u64,
        addresses: Vec<H160>,
    ) -> impl Future<Item = Vec<Trace>, Error = Error> {
        let eth = self.clone();
        let logger = logger.to_owned();

        retry("trace_filter RPC call", &logger)
            .no_limit()
            .timeout_secs(60)
            .run(move || {
                let trace_filter: TraceFilter = match addresses.len() {
                    0 => TraceFilterBuilder::default()
                        .from_block(from.into())
                        .to_block(to.into())
                        .build(),
                    _ => TraceFilterBuilder::default()
                        .from_block(from.into())
                        .to_block(to.into())
                        .to_address(addresses.clone())
                        .build(),
                };

                let logger_for_triggers = logger.clone();
                let logger_for_error = logger.clone();
                eth.web3
                    .trace()
                    .filter(trace_filter)
                    .map(move |traces| {
                        if traces.len() > 0 {
                            if to == from {
                                debug!(
                                    logger_for_triggers,
                                    "Received {} traces for block {}",
                                    traces.len(),
                                    to
                                );
                            } else {
                                debug!(
                                    logger_for_triggers,
                                    "Received {} traces for blocks [{}, {}]",
                                    traces.len(),
                                    from,
                                    to
                                );
                            }
                        }
                        traces
                    })
                    .from_err()
                    .then(move |result| {
                        if result.is_err() {
                            debug!(
                                logger_for_error,
                                "Error querying traces error = {:?} from = {:?} to = {:?}",
                                result,
                                from,
                                to
                            );
                        }
                        result
                    })
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
        too_many_logs_fingerprint: &'static str,
    ) -> impl Future<Item = Vec<Log>, Error = tokio_timer::timeout::Error<web3::error::Error>> {
        let eth_adapter = self.clone();

        retry("eth_getLogs RPC call", &logger)
            .when(move |res: &Result<_, web3::error::Error>| match res {
                Ok(_) => false,
                Err(e) => !e.to_string().contains(too_many_logs_fingerprint),
            })
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
                eth_adapter.web3.eth().logs(log_filter)
            })
    }

    fn trace_stream(
        self,
        logger: &Logger,
        from: u64,
        to: u64,
        addresses: Vec<H160>,
    ) -> impl Stream<Item = Trace, Error = Error> + Send {
        if from > to {
            panic!(
                "Can not produce a call stream on a backwards block range: from = {}, to = {}",
                from, to,
            );
        }

        let eth = self.clone();
        let logger = logger.to_owned();
        stream::unfold(from, move |start| {
            if start > to {
                return None;
            }
            let end = (start + *TRACE_STREAM_STEP_SIZE - 1).min(to);
            let new_start = end + 1;
            if start == end {
                debug!(logger, "Requesting traces for block {}", start);
            } else {
                debug!(logger, "Requesting traces for blocks [{}, {}]", start, end);
            }
            Some(
                eth.traces(&logger, start, end, addresses.clone())
                    .map(move |traces| (traces, new_start)),
            )
        })
        .map(stream::iter_ok)
        .flatten()
    }

    fn log_stream(
        &self,
        logger: &Logger,
        from: u64,
        to: u64,
        log_filter: EthereumLogFilter,
    ) -> impl Future<Item = Vec<Log>, Error = Error> {
        // Code returned by Infura if a request returns too many logs.
        // web3 doesn't seem to offer a better way of checking the error code.
        const TOO_MANY_LOGS_FINGERPRINT: &str = "ServerError(-32005)";

        if from > to {
            panic!(
                "cannot produce a log stream on a backwards block range (from={}, to={})",
                from, to
            );
        }

        // Collect all event sigs
        let eth = self.clone();
        let logger = logger.to_owned();

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

        let logger = logger.to_owned();
        let step = match addresses.is_empty() {
            // `to - from` is the size of the full range.
            false => to - from,
            true => (to - from).min(*MAX_EVENT_ONLY_RANGE),
        };

        stream::unfold((from, step), move |(start, step)| {
            if start > to {
                return None;
            }

            // Make as many parallel requests of size `step` as necessary,
            // respecting `LOG_STREAM_PARALLEL_CHUNKS`.
            let mut chunk_futures = vec![];
            let mut low = start;
            for _ in 0..*LOG_STREAM_PARALLEL_CHUNKS {
                if low == to + 1 {
                    break;
                }
                let log_filter = log_filter.clone();
                let high = (low + step).min(to);
                debug!(logger, "Requesting logs for blocks [{}, {}]", low, high);
                chunk_futures.push(
                    eth.logs_with_sigs(
                        &logger,
                        low,
                        high,
                        addresses.clone(),
                        event_sigs.clone(),
                        TOO_MANY_LOGS_FINGERPRINT,
                    )
                    .map(move |logs| {
                        logs.into_iter()
                            .filter(|log| log_filter.matches(log))
                            .collect::<Vec<Log>>()
                    }),
                );
                low = high + 1;
            }
            let logger = logger.clone();
            Some(
                stream::futures_ordered(chunk_futures)
                    .collect()
                    .map(|chunks| chunks.into_iter().flatten().collect::<Vec<Log>>())
                    .then(move |res| match res {
                        Err(e) => {
                            let string_err = e.to_string();

                            // If the step is already 0, we're hitting the log
                            // limit even for a single block. We hope this never
                            // happens, but if it does, make sure to error.
                            if string_err.contains(TOO_MANY_LOGS_FINGERPRINT) && step > 0 {
                                // The range size for a request is `step + 1`.
                                // So it's ok if the step goes down to 0, in
                                // that case we'll request one block at a time.
                                let new_step = step / 10;
                                debug!(logger, "Reducing block range size to scan for events";
                                               "new_size" => new_step + 1);
                                Ok((vec![], (start, new_step)))
                            } else {
                                warn!(logger, "Unexpected RPC error"; "error" => &string_err);
                                Err(err_msg(string_err))
                            }
                        }
                        Ok(logs) => Ok((logs, (low, step))),
                    }),
            )
        })
        .concat2()
    }

    fn call(
        &self,
        logger: &Logger,
        contract_address: Address,
        call_data: Bytes,
        block_number_opt: Option<BlockNumber>,
    ) -> impl Future<Item = Bytes, Error = EthereumContractCallError> + Send {
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
                    .when(|result| match result {
                        Ok(_) | Err(EthereumContractCallError::Revert(_)) => false,
                        Err(_) => true,
                    })
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
                        web3.eth().call(req, block_number_opt).then(|result| {
                            // Try to check if the call was reverted. The JSON-RPC response
                            // for reverts is not standardized, the current situation for
                            // the tested clients is:
                            //
                            // - Parity/Alchemy returns a specific RPC error for reverts.
                            // - Geth/Infura will return an `0x` RPC result on a revert,
                            //   which cannot be differentiated from random failures.
                            //   However for Solidity `revert` and `require` calls with a
                            //   reason string can be detected.

                            const PARITY_VM_EXECUTION_ERROR: i64 = -32015;
                            const PARITY_REVERT_PREFIX: &str = "Reverted 0x";

                            let as_solidity_revert_with_reason = |bytes: &[u8]| {
                                let solidity_revert_function_selector =
                                    &tiny_keccak::keccak256(b"Error(string)")[..4];

                                match &bytes[..4] == solidity_revert_function_selector {
                                    false => None,
                                    true => ethabi::decode(&[ParamType::String], &bytes[4..])
                                        .ok()
                                        .and_then(|tokens| tokens[0].clone().to_string()),
                                }
                            };

                            match result {
                                // Check for Geth revert.
                                Ok(bytes) => match as_solidity_revert_with_reason(&bytes.0) {
                                    None => Ok(bytes),
                                    Some(reason) => Err(EthereumContractCallError::Revert(reason)),
                                },

                                // Check for Parity revert.
                                Err(web3::Error::Rpc(ref rpc_error))
                                    if rpc_error.code.code() == PARITY_VM_EXECUTION_ERROR =>
                                {
                                    match rpc_error.data.as_ref().and_then(|d| d.as_str()) {
                                        Some(data) if data.starts_with(PARITY_REVERT_PREFIX) => {
                                            let payload =
                                                data.trim_start_matches(PARITY_REVERT_PREFIX);
                                            Err(EthereumContractCallError::Revert(
                                                hex::decode(payload)
                                                    .ok()
                                                    .and_then(|payload| {
                                                        as_solidity_revert_with_reason(&payload)
                                                    })
                                                    .unwrap_or("no reason".to_owned()),
                                            ))
                                        }
                                        _ => Err(EthereumContractCallError::Web3Error(
                                            web3::Error::Rpc(rpc_error.clone()),
                                        )),
                                    }
                                }
                                Err(err) => Err(EthereumContractCallError::Web3Error(err)),
                            }
                        })
                    })
                    .map_err(|e| e.into_inner().unwrap_or(EthereumContractCallError::Timeout))
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
    ) -> Box<dyn Future<Item = EthereumNetworkIdentifier, Error = Error> + Send> {
        let logger = logger.clone();
        let start_block = self.start_block;

        let web3 = self.web3.clone();
        let net_version_future = retry("net_version RPC call", &logger)
            .no_limit()
            .timeout_secs(20)
            .run(move || web3.net().version().from_err());

        let web3 = self.web3.clone();
        let gen_block_hash_future = retry("eth_getBlockByNumber(0, false) RPC call", &logger)
            .no_limit()
            .timeout_secs(30)
            .run(move || {
                web3.eth()
                    .block(if start_block > 0 {
                        BlockNumber::Number(start_block).into()
                    } else {
                        BlockNumber::Earliest.into()
                    })
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
    ) -> Box<dyn Future<Item = Block<Transaction>, Error = EthereumAdapterError> + Send> {
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
    ) -> Box<dyn Future<Item = Option<Block<Transaction>>, Error = Error> + Send> {
        let web3 = self.web3.clone();
        let logger = logger.clone();

        Box::new(
            retry("eth_getBlockByHash RPC call", &logger)
                .no_limit()
                .timeout_secs(60)
                .run(move || {
                    web3.eth()
                        .block_with_txs(BlockId::Hash(block_hash))
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
    ) -> Box<dyn Future<Item = EthereumBlock, Error = EthereumAdapterError> + Send> {
        let logger = logger.clone();
        let block_hash = block.hash.expect("block is missing block hash");

        // The early return is necessary for correctness, otherwise we'll
        // request an empty batch which is not valid in JSON-RPC.
        if block.transactions.is_empty() {
            info!(logger, "Block {} contains no transactions", block_hash);
            return Box::new(future::ok(EthereumBlock {
                block,
                transaction_receipts: Vec::new(),
            }));
        }
        let web3 = self.web3.clone();

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
                                                block_hash.to_string(),
                                            "tx_hash" => tx_hash.to_string(),
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

    fn block_parent_hash(
        &self,
        logger: &Logger,
        descendant_block_hash: H256,
    ) -> Box<dyn Future<Item = Option<H256>, Error = Error> + Send> {
        let web3 = self.web3.clone();

        Box::new(
            retry("eth_getBlockByHash RPC call", &logger)
                .no_limit()
                .timeout_secs(60)
                .run(move || {
                    web3.eth()
                        .block(BlockId::Hash(descendant_block_hash))
                        .from_err()
                        .map(|block_opt| block_opt.map(|block| block.parent_hash))
                })
                .map_err(move |e| {
                    e.into_inner().unwrap_or_else(move || {
                        format_err!(
                            "Ethereum node took too long to return data for block hash = `{}`",
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
    ) -> Box<dyn Future<Item = Option<H256>, Error = Error> + Send> {
        let web3 = self.web3.clone();

        Box::new(
            retry("eth_getBlockByNumber RPC call", &logger)
                .no_limit()
                .timeout_secs(60)
                .run(move || {
                    web3.eth()
                        .block(BlockId::Number(block_number.into()))
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
    ) -> Box<dyn Future<Item = bool, Error = Error> + Send> {
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
    ) -> Box<dyn Future<Item = Vec<EthereumCall>, Error = Error> + Send> {
        let eth = self.clone();
        let addresses = Vec::new();
        let calls = eth
            .trace_stream(&logger, block_number, block_number, addresses)
            .collect()
            .and_then(move |traces| {
                // `trace_stream` returns all of the traces for the block, and this
                // includes a trace for the block reward which every block should have.
                // If there are no traces something has gone wrong.
                if traces.is_empty() {
                    return future::err(format_err!(
                        "Trace stream returned no traces for block: number = `{}`, hash = `{}`",
                        block_number,
                        block_hash,
                    ));
                }
                // Since we can only pull traces by block number and we have
                // all the traces for the block, we need to ensure that the
                // block hash for the traces is equal to the desired block hash.
                // Assume all traces are for the same block.
                if traces.iter().nth(0).unwrap().block_hash != block_hash {
                    return future::err(format_err!(
                        "Trace stream returned traces for an unexpected block: \
                         number = `{}`, hash = `{}`",
                        block_number,
                        block_hash,
                    ));
                }
                future::ok(traces)
            })
            .map(move |traces| {
                traces
                    .iter()
                    .filter_map(EthereumCall::try_from_trace)
                    .collect()
            });
        Box::new(calls)
    }

    fn blocks_with_triggers(
        &self,
        logger: &Logger,
        from: u64,
        to: u64,
        log_filter: EthereumLogFilter,
        call_filter: EthereumCallFilter,
        block_filter: EthereumBlockFilter,
    ) -> Box<dyn Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send> {
        // Each trigger filter needs to be queried for the same block range
        // and the blocks yielded need to be deduped. If any error occurs
        // while searching for a trigger type, the entire operation fails.
        let eth = self.clone();
        let mut block_futs: futures::stream::FuturesUnordered<
            Box<dyn Future<Item = HashSet<EthereumBlockPointer>, Error = Error> + Send>,
        > = futures::stream::FuturesUnordered::new();
        if block_filter.trigger_every_block {
            // All blocks in the range contain a trigger
            block_futs.push(Box::new(
                eth.blocks(&logger, from, to)
                    .map(|block_ptrs| block_ptrs.into_iter().collect()),
            ));
        } else {
            // Scan the block range from triggers to find relevant blocks
            if !log_filter.is_empty() {
                block_futs.push(Box::new(
                    eth.blocks_with_logs(&logger, from, to, log_filter)
                        .map(|block_ptrs| block_ptrs.into_iter().collect()),
                ));
            }
            if !call_filter.is_empty() {
                block_futs.push(Box::new(eth.blocks_with_calls(
                    &logger,
                    from,
                    to,
                    call_filter,
                )));
            }

            match block_filter.contract_addresses.len() {
                0 => (),
                _ => {
                    // To determine which blocks include a call to addresses
                    // in the block filter, transform the `block_filter` into
                    // a `call_filter` and run `blocks_with_calls`
                    let call_filter = EthereumCallFilter::from(block_filter);
                    block_futs.push(Box::new(eth.blocks_with_calls(
                        &logger,
                        from,
                        to,
                        call_filter,
                    )));
                }
            }
        }
        Box::new(block_futs.concat2().and_then(|block_ptrs| {
            let mut blocks = block_ptrs.into_iter().collect::<Vec<_>>();
            blocks.sort_by(|a, b| a.number.cmp(&b.number));
            future::ok(blocks)
        }))
    }

    fn blocks(
        &self,
        logger: &Logger,
        from: u64,
        to: u64,
    ) -> Box<dyn Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send> {
        let eth = self.clone();
        let logger = logger.clone();

        // Generate `EthereumBlockPointers` from `to` backwards to `from`
        Box::new(
            self.block_hash_by_block_number(&logger, to)
                .map(move |block_hash_opt| EthereumBlockPointer {
                    hash: block_hash_opt.unwrap(),
                    number: to,
                })
                .and_then(move |block_pointer| {
                    stream::unfold(block_pointer, move |descendant_block_pointer| {
                        if descendant_block_pointer.number < from {
                            return None;
                        }
                        // Populate the parent block pointer
                        Some(
                            eth.block_parent_hash(&logger, descendant_block_pointer.hash)
                                .map(move |block_hash_opt| {
                                    let parent_block_pointer = EthereumBlockPointer {
                                        hash: block_hash_opt.unwrap(),
                                        number: descendant_block_pointer.number - 1,
                                    };
                                    (descendant_block_pointer, parent_block_pointer)
                                }),
                        )
                    })
                    .collect()
                })
                .map(move |mut block_pointers| {
                    block_pointers.reverse();
                    block_pointers
                }),
        )
    }

    fn blocks_with_logs(
        &self,
        logger: &Logger,
        from: u64,
        to: u64,
        log_filter: EthereumLogFilter,
    ) -> Box<dyn Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send> {
        let eth = self.clone();
        Box::new(
            // Get a stream of all relevant logs in range
            eth.log_stream(&logger, from, to, log_filter).map(|logs| {
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
            }),
        )
    }

    fn blocks_with_calls(
        &self,
        logger: &Logger,
        from: u64,
        to: u64,
        call_filter: EthereumCallFilter,
    ) -> Box<dyn Future<Item = HashSet<EthereumBlockPointer>, Error = Error> + Send> {
        let eth = self.clone();

        let addresses: Vec<H160> = call_filter
            .contract_addresses_function_signatures
            .iter()
            .map(|(addr, _fsigs)| *addr)
            .collect::<HashSet<H160>>()
            .into_iter()
            .collect::<Vec<H160>>();
        Box::new(
            eth.trace_stream(&logger, from, to, addresses)
                .filter_map(|trace| EthereumCall::try_from_trace(&trace))
                .filter(move |call| {
                    // `trace_filter` can only filter by calls `to` an address and
                    // a block range. Since subgraphs are subscribing to calls
                    // for a specific contract function an additional filter needs
                    // to be applied
                    call_filter.matches(&call)
                })
                .fold(HashSet::new(), |mut block_ptrs, call| {
                    block_ptrs.insert(EthereumBlockPointer::from((
                        call.block_hash,
                        call.block_number,
                    )));
                    future::ok::<_, Error>(block_ptrs)
                }),
        )
    }

    fn contract_call(
        &self,
        logger: &Logger,
        call: EthereumContractCall,
    ) -> Box<dyn Future<Item = Vec<Token>, Error = EthereumContractCallError> + Send> {
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
            .and_then(move |output| {
                // Decode the return values according to the ABI
                call.function
                    .decode_output(&output.0)
                    .map_err(EthereumContractCallError::from)
            }),
        )
    }
}
