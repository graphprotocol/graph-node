use ethabi::{RawLog, Token};
use ethereum_types::H256;
use futures::future;
use futures::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use web3;
use web3::api::{Eth, Web3};
use web3::helpers::CallResult;
use web3::types::{Filter, *};

use graph::components::ethereum::{EthereumAdapter as EthereumAdapterTrait, *};
use graph::components::store::EthereumBlockPointer;
use graph::prelude::*;

pub struct EthereumAdapterConfig<T: web3::Transport> {
    pub transport: T,
    pub logger: Logger,
}

pub struct EthereumAdapter<T: web3::Transport> {
    eth_client: Arc<Web3<T>>,
    logger: Logger,
}

/// Number of chunks to request in parallel when streaming logs.
const LOG_STREAM_PARALLEL_CHUNKS: u64 = 20;

/// Number of blocks to request in each chunk.
const LOG_STREAM_CHUNK_SIZE_IN_BLOCKS: u64 = 10000;

impl<T> EthereumAdapter<T>
where
    T: web3::Transport + Send + Sync + 'static,
    T::Out: Send,
{
    pub fn new(config: EthereumAdapterConfig<T>) -> Self {
        EthereumAdapter {
            eth_client: Arc::new(Web3::new(config.transport)),
            logger: config.logger,
        }
    }

    pub fn block(eth: Eth<T>, block_id: BlockId) -> CallResult<Block<H256>, T::Out> {
        eth.block(block_id)
    }

    fn logs_with_sigs<'a>(
        &'a self,
        from: u64,
        to: u64,
        event_signatures: Vec<H256>,
    ) -> impl Future<Item = Vec<Log>, Error = Error> + 'a {
        with_retry(self.logger.clone(), move || {
            // Create a log filter
            let log_filter: Filter = FilterBuilder::default()
                .from_block(from.into())
                .to_block(to.into())
                .topics(Some(event_signatures.clone()), None, None, None)
                .build();

            // Request logs from client
            debug!(&self.logger, "REQUESTING logs for [{}, {}]", from, to);
            Box::new(
                self.eth_client
                    .eth()
                    .logs(log_filter)
                    .map(move |logs| {
                        debug!(self.logger, "GOT logs for [{}, {}]", from, to);
                        logs
                    })
                    .map_err(SyncFailure::new)
                    .from_err(),
            )
        })
    }

    fn log_stream<'a>(
        &'a self,
        from: u64,
        to: u64,
        event_filter: EthereumEventFilter,
    ) -> impl Stream<Item = Log, Error = Error> + 'a {
        if from > to {
            panic!(
                "cannot produce a log stream on a backwards block range (from={}, to={})",
                from, to
            );
        }

        // Find all event sigs
        let event_sigs = event_filter
            .event_types_by_contract_address_and_sig
            .values()
            .flat_map(|event_types_by_sig| event_types_by_sig.keys())
            .map(|sig| sig.to_owned())
            .collect::<Vec<H256>>();

        stream::unfold(from, move |mut chunk_offset| {
            if chunk_offset <= to {
                let mut chunk_futures = vec![];
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
                        self.logger,
                        "Starting request for logs in block range [{},{}]", chunk_offset, chunk_end
                    );
                    let event_filter = event_filter.clone();
                    let chunk_future = self
                        .logs_with_sigs(chunk_offset, chunk_end, event_sigs.clone())
                        .map(move |logs| {
                            logs.into_iter()
                                // Filter out false positives
                                .filter(move |log| event_filter.match_event(log).is_some())
                        });

                    // Save future for later
                    chunk_futures.push(chunk_future);

                    // If last chunk, will push offset past `to`. That's fine.
                    chunk_offset += LOG_STREAM_CHUNK_SIZE_IN_BLOCKS;

                    if is_last_chunk {
                        break;
                    }
                }

                // Combine chunk futures into one stream of logs
                let log_stream_for_chunks = stream::futures_ordered(chunk_futures)
                    .map(|logs_chunk| stream::iter_ok(logs_chunk))
                    .flatten();

                // Yield a stream from the chunks and a new chunk_offset.
                // Stream will be consumed entirely before next group of chunks is requested.
                Some(future::ok::<_, Error>((
                    log_stream_for_chunks,
                    chunk_offset,
                )))
            } else {
                None
            }
        }).flatten()
    }

    fn call(
        eth: Eth<T>,
        contract_address: Address,
        call_data: Bytes,
        block_number: Option<BlockNumber>,
    ) -> CallResult<Bytes, T::Out> {
        let req = CallRequest {
            from: None,
            to: contract_address,
            gas: None,
            gas_price: None,
            value: None,
            data: Some(call_data),
        };
        eth.call(req, block_number)
    }
}

impl<T> EthereumAdapterTrait for EthereumAdapter<T>
where
    T: web3::Transport + Send + Sync + 'static,
    T::Out: Send,
{
    fn block_by_hash(
        &self,
        block_hash: H256,
    ) -> Box<Future<Item = Block<Transaction>, Error = Error> + Send> {
        Box::new(
            self.eth_client
                .eth()
                .block_with_txs(BlockId::Hash(block_hash))
                .map_err(SyncFailure::new)
                .from_err(),
        )
    }

    fn block_by_number(
        &self,
        block_number: u64,
    ) -> Box<Future<Item = Block<Transaction>, Error = Error> + Send> {
        Box::new(
            self.eth_client
                .eth()
                .block_with_txs(BlockId::Number(block_number.into()))
                .map_err(SyncFailure::new)
                .from_err(),
        )
    }

    fn is_on_main_chain(
        &self,
        block_ptr: EthereumBlockPointer,
    ) -> Box<Future<Item = bool, Error = Error> + Send> {
        Box::new(
            self.eth_client
                .eth()
                .block(BlockId::Number(block_ptr.number.into()))
                .map(move |b| b.hash.unwrap() == block_ptr.hash)
                .map_err(SyncFailure::new)
                .from_err(),
        )
    }

    fn find_first_block_with_event<'a>(
        &'a self,
        from: u64,
        to: u64,
        event_filter: EthereumEventFilter,
    ) -> Box<Future<Item = Option<EthereumBlockPointer>, Error = Error> + Send + 'a> {
        Box::new(
            // Get a stream of all relevant events in range
            self.log_stream(from, to, event_filter)

                // Find first relevant event (or zero events)
                .take(1)

                // Collect 0 or 1 logs
                .collect()

                // Produce Some(block ptr) or None
                .map(|logs| {
                    match logs.len() {
                        0 => None,
                        1 => {
                            let hash = logs[0]
                                .block_hash
                                .expect("log from Eth node is missing block hash");
                            let number = logs[0]
                                .block_number
                                .expect("log from Eth node is missing block number");
                            Some((hash, number.as_u64()).into())
                        },
                        _ => unreachable!(),
                    }
                }),
        )
    }

    // TODO investigate storing receipts in DB and moving this fn to BlockStore
    fn get_events_in_block<'a>(
        &'a self,
        block: Block<Transaction>,
        event_filter: EthereumEventFilter,
    ) -> Box<Stream<Item = EthereumEvent, Error = EthereumSubscriptionError> + 'a> {
        if !event_filter.check_bloom(block.logs_bloom) {
            return Box::new(stream::empty());
        }

        let tx_receipt_futures = block.transactions.into_iter().map(|tx| {
            self.eth_client
                .eth()
                .transaction_receipt(tx.hash)
                .map(move |opt| opt.expect(&format!("missing receipt for TX {:?}", tx.hash)))
                .map_err(EthereumSubscriptionError::from)
        });

        Box::new(
            stream::futures_ordered(tx_receipt_futures)
                .map(move |receipt| {
                    let event_filter = event_filter.clone();

                    stream::iter_result(receipt.logs.into_iter().filter_map(move |log| {
                        // Check log against event filter
                        event_filter
                                .match_event(&log)

                                // If matched: convert Log into an EthereumEvent
                                .map(|event_type| {
                                    // Try to parse log data into an Ethereum event
                                    event_type
                                        .parse_log(RawLog {
                                            topics: log.topics.clone(),
                                            data: log.data.0.clone(),
                                        })
                                        .map_err(EthereumSubscriptionError::from)
                                        .map(|log_data| EthereumEvent {
                                            address: log.address,
                                            event_signature: log.topics[0],
                                            block_hash: log.block_hash.unwrap(),
                                            params: log_data.params,
                                            removed: log.is_removed(), // TODO is this obsolete?
                                        })
                                })
                    }))
                })
                .flatten(),
        )
    }

    fn contract_call(
        &mut self,
        call: EthereumContractCall,
    ) -> Box<Future<Item = Vec<Token>, Error = EthereumContractCallError>> {
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

        // Obtain a handle on the Ethereum client
        let eth_client = self.eth_client.clone();

        // Prepare for the function call, encoding the call parameters according
        // to the ABI
        let call_address = call.address;
        let call_data = call.function.encode_input(&call.args).unwrap();

        Box::new(
            // Resolve the block ID into a block number
            Self::block(eth_client.eth(), call.block_id.clone())
                .map_err(EthereumContractCallError::from)
                .and_then(move |block| {
                    // Make the actual function call
                    Self::call(
                        eth_client.eth(),
                        call_address,
                        Bytes(call_data),
                        block
                            .number
                            .map(|number| number.as_u64())
                            .map(BlockNumber::Number),
                    ).map_err(EthereumContractCallError::from)
                })
                // Decode the return values according to the ABI
                .and_then(move |output| {
                    call.function
                        .decode_output(&output.0)
                        .map_err(EthereumContractCallError::from)
                }),
        )
    }
}

fn with_retry<'a, I, T>(
    logger: Logger,
    try_it: T,
) -> Box<Future<Item = I, Error = Error> + Send + 'a>
where
    I: Send + 'a,
    T: Fn() -> Box<Future<Item = I, Error = Error> + Send + 'a> + Send + 'a,
{
    Box::new(future::loop_fn((), move |()| {
        let logger = logger.clone();

        try_it()
            .deadline(Instant::now() + Duration::from_secs(30))
            .then(move |result| match result {
                Ok(ret) => Ok(future::Loop::Break(ret)),
                Err(deadline_err) => match deadline_err.into_inner() {
                    Some(e) => Err(e),
                    None => {
                        info!(
                            logger,
                            "Ethereum RPC call is taking more than 30 seconds. Retrying..."
                        );
                        Ok(future::Loop::Continue(()))
                    }
                },
            })
    }))
}
