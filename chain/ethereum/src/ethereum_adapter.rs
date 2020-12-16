use ethabi::Token;
use futures::future;
use futures::prelude::*;
use lazy_static::lazy_static;
use std::collections::HashSet;
use std::iter::FromIterator;
use std::sync::Arc;
use std::time::Instant;

use ethabi::ParamType;
use graph::components::ethereum::{EthereumAdapter as EthereumAdapterTrait, *};
use graph::prelude::{
    anyhow, debug, error, ethabi,
    futures03::{self, compat::Future01CompatExt, FutureExt, StreamExt, TryStreamExt},
    hex, retry, stream, tiny_keccak, trace, warn, web3, ChainStore, CheapClone, DynTryFuture,
    Error, EthereumCallCache, Logger, TimeoutError,
};
use web3::api::Web3;
use web3::transports::batch::Batch;
use web3::types::{Filter, *};

#[derive(Clone)]
pub struct EthereumAdapter<T: web3::Transport> {
    url_hostname: Arc<String>,
    web3: Arc<Web3<T>>,
    metrics: Arc<ProviderEthRpcMetrics>,
    is_ganache: bool,
}

lazy_static! {
    static ref TRACE_STREAM_STEP_SIZE: u64 = std::env::var("ETHEREUM_TRACE_STREAM_STEP_SIZE")
        .unwrap_or("200".into())
        .parse::<u64>()
        .expect("invalid trace stream step size");

    /// Maximum range size for `eth.getLogs` requests that dont filter on
    /// contract address, only event signature, and are therefore expensive.
    ///
    /// According to Ethereum node operators, size 500 is reasonable here.
    static ref MAX_EVENT_ONLY_RANGE: u64 = std::env::var("GRAPH_ETHEREUM_MAX_EVENT_ONLY_RANGE")
        .unwrap_or("500".into())
        .parse::<u64>()
        .expect("invalid number of parallel Ethereum block ranges to scan");

    static ref BLOCK_BATCH_SIZE: usize = std::env::var("ETHEREUM_BLOCK_BATCH_SIZE")
            .unwrap_or("10".into())
            .parse::<usize>()
            .expect("invalid ETHEREUM_BLOCK_BATCH_SIZE env var");

    /// This should not be too large that it causes requests to timeout without us catching it, nor
    /// too small that it causes us to timeout requests that would've succeeded. We've seen
    /// successful `eth_getLogs` requests take over 120 seconds.
    static ref JSON_RPC_TIMEOUT: u64 = std::env::var("GRAPH_ETHEREUM_JSON_RPC_TIMEOUT")
            .unwrap_or("180".into())
            .parse::<u64>()
            .expect("invalid GRAPH_ETHEREUM_JSON_RPC_TIMEOUT env var");


    /// This is used for requests that will not fail the subgraph if the limit is reached, but will
    /// simply restart the syncing step, so it can be low. This limit guards against scenarios such
    /// as requesting a block hash that has been reorged.
    static ref REQUEST_RETRIES: usize = std::env::var("GRAPH_ETHEREUM_REQUEST_RETRIES")
            .unwrap_or("10".into())
            .parse::<usize>()
            .expect("invalid GRAPH_ETHEREUM_REQUEST_RETRIES env var");

    /// Log eth_call data and target address at trace level. Turn on for debugging.
    static ref ETH_CALL_FULL_LOG: bool = std::env::var("GRAPH_ETH_CALL_FULL_LOG").is_ok();

    /// This is not deterministic and will be removed after the testnet.
    static ref ETH_CALL_BY_NUMBER: bool = std::env::var("GRAPH_ETH_CALL_BY_NUMBER").is_ok();
}

impl<T: web3::Transport> CheapClone for EthereumAdapter<T> {
    fn cheap_clone(&self) -> Self {
        Self {
            url_hostname: self.url_hostname.cheap_clone(),
            web3: self.web3.cheap_clone(),
            metrics: self.metrics.cheap_clone(),
            is_ganache: self.is_ganache,
        }
    }
}

impl<T> EthereumAdapter<T>
where
    T: web3::BatchTransport + Send + Sync + 'static,
    T::Batch: Send,
    T::Out: Send,
{
    pub async fn new(
        url: &str,
        transport: T,
        provider_metrics: Arc<ProviderEthRpcMetrics>,
    ) -> Self {
        // Unwrap: The transport was constructed with this url, so it is valid and has a host.
        let hostname = graph::url::Url::parse(url)
            .unwrap()
            .host_str()
            .unwrap()
            .to_string();

        let web3 = Arc::new(Web3::new(transport));

        // Use the client version to check if it is ganache. For compatibility with unit tests, be
        // are lenient with errors, defaulting to false.
        let is_ganache = web3
            .web3()
            .client_version()
            .compat()
            .await
            .map(|s| s.contains("TestRPC"))
            .unwrap_or(false);

        EthereumAdapter {
            url_hostname: Arc::new(hostname),
            web3,
            metrics: provider_metrics,
            is_ganache,
        }
    }

    fn traces(
        &self,
        logger: &Logger,
        subgraph_metrics: Arc<SubgraphEthRpcMetrics>,
        from: u64,
        to: u64,
        addresses: Vec<H160>,
    ) -> impl Future<Item = Vec<Trace>, Error = Error> {
        let eth = self.clone();
        let logger = logger.to_owned();

        retry("trace_filter RPC call", &logger)
            .limit(*REQUEST_RETRIES)
            .timeout_secs(*JSON_RPC_TIMEOUT)
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
                let start = Instant::now();
                let subgraph_metrics = subgraph_metrics.clone();
                let provider_metrics = eth.metrics.clone();
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
                        let elapsed = start.elapsed().as_secs_f64();
                        provider_metrics.observe_request(elapsed, "trace_filter");
                        subgraph_metrics.observe_request(elapsed, "trace_filter");
                        if result.is_err() {
                            provider_metrics.add_error("trace_filter");
                            subgraph_metrics.add_error("trace_filter");
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
                    anyhow::anyhow!(
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
        subgraph_metrics: Arc<SubgraphEthRpcMetrics>,
        from: u64,
        to: u64,
        filter: Arc<EthGetLogsFilter>,
        too_many_logs_fingerprints: &'static [&'static str],
    ) -> impl Future<Item = Vec<Log>, Error = TimeoutError<web3::error::Error>> {
        let eth_adapter = self.clone();

        retry("eth_getLogs RPC call", &logger)
            .when(move |res: &Result<_, web3::error::Error>| match res {
                Ok(_) => false,
                Err(e) => !too_many_logs_fingerprints
                    .iter()
                    .any(|f| e.to_string().contains(f)),
            })
            .limit(*REQUEST_RETRIES)
            .timeout_secs(*JSON_RPC_TIMEOUT)
            .run(move || {
                let start = Instant::now();
                let subgraph_metrics = subgraph_metrics.clone();
                let provider_metrics = eth_adapter.metrics.clone();

                // Create a log filter
                let log_filter: Filter = FilterBuilder::default()
                    .from_block(from.into())
                    .to_block(to.into())
                    .address(filter.contracts.clone())
                    .topics(Some(filter.event_signatures.clone()), None, None, None)
                    .build();

                // Request logs from client
                eth_adapter.web3.eth().logs(log_filter).then(move |result| {
                    let elapsed = start.elapsed().as_secs_f64();
                    provider_metrics.observe_request(elapsed, "eth_getLogs");
                    subgraph_metrics.observe_request(elapsed, "eth_getLogs");
                    if result.is_err() {
                        provider_metrics.add_error("eth_getLogs");
                        subgraph_metrics.add_error("eth_getLogs");
                    }
                    result
                })
            })
    }

    fn trace_stream(
        self,
        logger: &Logger,
        subgraph_metrics: Arc<SubgraphEthRpcMetrics>,
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

        // Filters with no address can be more expensive, so use a reduced step size.
        let step_size = match addresses.is_empty() {
            false => *TRACE_STREAM_STEP_SIZE,
            true => *TRACE_STREAM_STEP_SIZE / 4,
        }
        .max(1);

        let eth = self.clone();
        let logger = logger.to_owned();
        stream::unfold(from, move |start| {
            if start > to {
                return None;
            }
            let end = (start + step_size - 1).min(to);
            let new_start = end + 1;
            if start == end {
                debug!(logger, "Requesting traces for block {}", start);
            } else {
                debug!(logger, "Requesting traces for blocks [{}, {}]", start, end);
            }
            Some(
                eth.traces(
                    &logger,
                    subgraph_metrics.clone(),
                    start,
                    end,
                    addresses.clone(),
                )
                .map(move |traces| (traces, new_start)),
            )
        })
        .map(stream::iter_ok)
        .flatten()
    }

    fn log_stream(
        &self,
        logger: Logger,
        subgraph_metrics: Arc<SubgraphEthRpcMetrics>,
        from: u64,
        to: u64,
        filter: EthGetLogsFilter,
    ) -> DynTryFuture<'static, Vec<Log>, Error> {
        // Codes returned by Ethereum node providers if an eth_getLogs request is too heavy.
        // The first one is for Infura when it hits the log limit, the rest for Alchemy timeouts.
        const TOO_MANY_LOGS_FINGERPRINTS: &[&str] = &[
            "ServerError(-32005)",
            "503 Service Unavailable",
            "ServerError(-32000)",
        ];

        if from > to {
            panic!(
                "cannot produce a log stream on a backwards block range (from={}, to={})",
                from, to
            );
        }

        // Collect all event sigs
        let eth = self.cheap_clone();
        let filter = Arc::new(filter);

        let step = match filter.contracts.is_empty() {
            // `to - from + 1`  blocks will be scanned.
            false => to - from,
            true => (to - from).min(*MAX_EVENT_ONLY_RANGE - 1),
        };

        // Typically this will loop only once and fetch the entire range in one request. But if the
        // node returns an error that signifies the request is to heavy to process, the range will
        // be broken down to smaller steps.
        futures03::stream::try_unfold((from, step), move |(start, step)| {
            let logger = logger.cheap_clone();
            let filter = filter.cheap_clone();
            let eth = eth.cheap_clone();
            let subgraph_metrics = subgraph_metrics.cheap_clone();

            async move {
                if start > to {
                    return Ok(None);
                }

                let end = (start + step).min(to);
                debug!(
                    logger,
                    "Requesting logs for blocks [{}, {}], {}", start, end, filter
                );
                let res = eth
                    .logs_with_sigs(
                        &logger,
                        subgraph_metrics.cheap_clone(),
                        start,
                        end,
                        filter.cheap_clone(),
                        TOO_MANY_LOGS_FINGERPRINTS,
                    )
                    .compat()
                    .await;

                match res {
                    Err(e) => {
                        let string_err = e.to_string();

                        // If the step is already 0, the request is too heavy even for a single
                        // block. We hope this never happens, but if it does, make sure to error.
                        if TOO_MANY_LOGS_FINGERPRINTS
                            .iter()
                            .any(|f| string_err.contains(f))
                            && step > 0
                        {
                            // The range size for a request is `step + 1`. So it's ok if the step
                            // goes down to 0, in that case we'll request one block at a time.
                            let new_step = step / 10;
                            debug!(logger, "Reducing block range size to scan for events";
                                               "new_size" => new_step + 1);
                            Ok(Some((vec![], (start, new_step))))
                        } else {
                            warn!(logger, "Unexpected RPC error"; "error" => &string_err);
                            Err(anyhow!("{}", string_err))
                        }
                    }
                    Ok(logs) => Ok(Some((logs, (end + 1, step)))),
                }
            }
        })
        .try_concat()
        .boxed()
    }

    fn call(
        &self,
        logger: Logger,
        contract_address: Address,
        call_data: Bytes,
        block_ptr: EthereumBlockPointer,
    ) -> impl Future<Item = Bytes, Error = EthereumContractCallError> + Send {
        let web3 = self.web3.clone();

        // Ganache does not support calls by block hash.
        // See https://github.com/trufflesuite/ganache-cli/issues/745
        let block_id = if self.is_ganache || *ETH_CALL_BY_NUMBER {
            BlockId::Number(block_ptr.number.into())
        } else {
            BlockId::Hash(block_ptr.hash)
        };

        retry("eth_call RPC call", &logger)
            .when(|result| match result {
                Ok(_) | Err(EthereumContractCallError::Revert(_)) => false,
                Err(_) => true,
            })
            .limit(10)
            .timeout_secs(*JSON_RPC_TIMEOUT)
            .run(move || {
                let req = CallRequest {
                    from: None,
                    to: contract_address,
                    gas: None,
                    gas_price: None,
                    value: None,
                    data: Some(call_data.clone()),
                };
                web3.eth().call(req, Some(block_id)).then(|result| {
                    // Try to check if the call was reverted. The JSON-RPC response for
                    // reverts is not standardized, the current situation for the tested
                    // clients is:
                    //
                    // - Parity returns a reliable RPC error response for reverts.
                    // - Ganache also returns a reliable RPC error.
                    // - Geth now also returns an RPC error. It used to return `0x` on a
                    //   revert with no reason string, or a Solidity encoded `Error(string)`
                    //   call from `revert` and `require` calls with a reason string. We
                    //   still have support for those but that can be removed on the next
                    //   hard fork (Berlin).

                    // 0xfe is the "designated bad instruction" of the EVM, and Solidity
                    // uses it for asserts.
                    const PARITY_BAD_INSTRUCTION_FE: &str = "Bad instruction fe";

                    // 0xfd is REVERT, but on some contracts, and only on older blocks,
                    // this happens. Makes sense to consider it a revert as well.
                    const PARITY_BAD_INSTRUCTION_FD: &str = "Bad instruction fd";

                    const PARITY_BAD_JUMP_PREFIX: &str = "Bad jump";
                    const GANACHE_VM_EXECUTION_ERROR: i64 = -32000;
                    const GANACHE_REVERT_MESSAGE: &str =
                        "VM Exception while processing transaction: revert";
                    const PARITY_VM_EXECUTION_ERROR: i64 = -32015;
                    const PARITY_REVERT_PREFIX: &str = "Reverted 0x";

                    // Deterministic Geth execution errors. We might need to expand this as
                    // subgraphs come across other errors. See
                    // https://github.com/ethereum/go-ethereum/blob/cd57d5cd38ef692de8fbedaa56598b4e9fbfbabc/core/vm/errors.go
                    const GETH_EXECUTION_ERRORS: &[&str] = &[
                        "execution reverted",
                        "invalid jump destination",
                        "invalid opcode",
                    ];

                    let as_solidity_revert_with_reason = |bytes: &[u8]| {
                        let solidity_revert_function_selector =
                            &tiny_keccak::keccak256(b"Error(string)")[..4];

                        match bytes.len() >= 4 && &bytes[..4] == solidity_revert_function_selector {
                            false => None,
                            true => ethabi::decode(&[ParamType::String], &bytes[4..])
                                .ok()
                                .and_then(|tokens| tokens[0].clone().to_string()),
                        }
                    };

                    match result {
                        // Check for old Geth revert with reason.
                        Ok(bytes) => match as_solidity_revert_with_reason(&bytes.0) {
                            None => Ok(bytes),
                            Some(reason) => Err(EthereumContractCallError::Revert(reason)),
                        },

                        // Check for Geth revert.
                        Err(web3::Error::Rpc(rpc_error))
                            if GETH_EXECUTION_ERRORS
                                .iter()
                                .any(|e| rpc_error.message.contains(e)) =>
                        {
                            Err(EthereumContractCallError::Revert(rpc_error.message))
                        }

                        // Check for Parity revert.
                        Err(web3::Error::Rpc(ref rpc_error))
                            if rpc_error.code.code() == PARITY_VM_EXECUTION_ERROR =>
                        {
                            match rpc_error.data.as_ref().and_then(|d| d.as_str()) {
                                Some(data)
                                    if data.starts_with(PARITY_REVERT_PREFIX)
                                        || data.starts_with(PARITY_BAD_JUMP_PREFIX)
                                        || data == PARITY_BAD_INSTRUCTION_FE
                                        || data == PARITY_BAD_INSTRUCTION_FD =>
                                {
                                    let reason = if data == PARITY_BAD_INSTRUCTION_FE {
                                        PARITY_BAD_INSTRUCTION_FE.to_owned()
                                    } else {
                                        let payload = data.trim_start_matches(PARITY_REVERT_PREFIX);
                                        hex::decode(payload)
                                            .ok()
                                            .and_then(|payload| {
                                                as_solidity_revert_with_reason(&payload)
                                            })
                                            .unwrap_or("no reason".to_owned())
                                    };
                                    Err(EthereumContractCallError::Revert(reason))
                                }

                                // The VM execution error was not identified as a revert.
                                _ => Err(EthereumContractCallError::Web3Error(web3::Error::Rpc(
                                    rpc_error.clone(),
                                ))),
                            }
                        }

                        // Check for Ganache revert.
                        Err(web3::Error::Rpc(ref rpc_error))
                            if rpc_error.code.code() == GANACHE_VM_EXECUTION_ERROR
                                && rpc_error.message.starts_with(GANACHE_REVERT_MESSAGE) =>
                        {
                            Err(EthereumContractCallError::Revert(rpc_error.message.clone()))
                        }

                        // The error was not identified as a revert.
                        Err(err) => Err(EthereumContractCallError::Web3Error(err)),
                    }
                })
            })
            .map_err(|e| e.into_inner().unwrap_or(EthereumContractCallError::Timeout))
    }

    /// Request blocks by hash through JSON-RPC.
    fn load_blocks_rpc(
        &self,
        logger: Logger,
        ids: Vec<H256>,
    ) -> impl Stream<Item = LightEthereumBlock, Error = Error> + Send {
        let web3 = self.web3.clone();

        stream::iter_ok::<_, Error>(ids.into_iter().map(move |hash| {
            let web3 = web3.clone();
            retry(format!("load block {}", hash), &logger)
                .limit(*REQUEST_RETRIES)
                .timeout_secs(*JSON_RPC_TIMEOUT)
                .run(move || {
                    web3.eth()
                        .block_with_txs(BlockId::Hash(hash))
                        .from_err::<Error>()
                        .and_then(move |block| {
                            block.ok_or_else(|| {
                                anyhow::anyhow!("Ethereum node did not find block {:?}", hash)
                            })
                        })
                })
                .from_err()
        }))
        .buffered(*BLOCK_BATCH_SIZE)
    }

    /// Request blocks ptrs for numbers through JSON-RPC.
    ///
    /// Reorg safety: If ids are numbers, they must be a final blocks.
    fn load_block_ptrs_rpc(
        &self,
        logger: Logger,
        block_nums: Vec<u64>,
    ) -> impl Stream<Item = EthereumBlockPointer, Error = Error> + Send {
        let web3 = self.web3.clone();

        stream::iter_ok::<_, Error>(block_nums.into_iter().map(move |block_num| {
            let web3 = web3.clone();
            retry(format!("load block ptr {}", block_num), &logger)
                .no_limit()
                .timeout_secs(*JSON_RPC_TIMEOUT)
                .run(move || {
                    web3.eth()
                        .block(BlockId::Number(BlockNumber::Number(block_num.into())))
                        .from_err::<Error>()
                        .and_then(move |block| {
                            block.ok_or_else(|| {
                                anyhow!("Ethereum node did not find block {:?}", block_num)
                            })
                        })
                })
                .from_err()
        }))
        .buffered(*BLOCK_BATCH_SIZE)
        .map(|b| b.into())
    }
}

impl<T> EthereumAdapterTrait for EthereumAdapter<T>
where
    T: web3::BatchTransport + Send + Sync + 'static,
    T::Batch: Send,
    T::Out: Send,
{
    fn url_hostname(&self) -> &str {
        &self.url_hostname
    }

    fn net_identifiers(
        &self,
        logger: &Logger,
    ) -> Box<dyn Future<Item = EthereumNetworkIdentifier, Error = Error> + Send> {
        let logger = logger.clone();

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
                    .block(BlockId::Number(BlockNumber::Number(0.into())))
                    .from_err()
                    .and_then(|gen_block_opt| {
                        future::result(
                            gen_block_opt
                                .and_then(|gen_block| gen_block.hash)
                                .ok_or_else(|| {
                                    anyhow!("Ethereum node could not find genesis block")
                                }),
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
                        anyhow!("Ethereum node took too long to read network identifiers")
                    })
                }),
        )
    }

    fn latest_block_header(
        &self,
        logger: &Logger,
    ) -> Box<dyn Future<Item = web3::types::Block<H256>, Error = EthereumAdapterError> + Send> {
        let web3 = self.web3.clone();

        Box::new(
            retry("eth_getBlockByNumber(latest) no txs RPC call", logger)
                .no_limit()
                .timeout_secs(*JSON_RPC_TIMEOUT)
                .run(move || {
                    web3.eth()
                        .block(BlockNumber::Latest.into())
                        .map_err(|e| anyhow!("could not get latest block from Ethereum: {}", e))
                        .from_err()
                        .and_then(|block_opt| {
                            block_opt.ok_or_else(|| {
                                anyhow!("no latest block returned from Ethereum").into()
                            })
                        })
                })
                .map_err(move |e| {
                    e.into_inner().unwrap_or_else(move || {
                        anyhow!("Ethereum node took too long to return latest block").into()
                    })
                }),
        )
    }

    fn latest_block(
        &self,
        logger: &Logger,
    ) -> Box<dyn Future<Item = LightEthereumBlock, Error = EthereumAdapterError> + Send + Unpin>
    {
        let web3 = self.web3.clone();

        Box::new(
            retry("eth_getBlockByNumber(latest) with txs RPC call", logger)
                .no_limit()
                .timeout_secs(*JSON_RPC_TIMEOUT)
                .run(move || {
                    web3.eth()
                        .block_with_txs(BlockNumber::Latest.into())
                        .map_err(|e| anyhow!("could not get latest block from Ethereum: {}", e))
                        .from_err()
                        .and_then(|block_opt| {
                            block_opt.ok_or_else(|| {
                                anyhow!("no latest block returned from Ethereum").into()
                            })
                        })
                })
                .map_err(move |e| {
                    e.into_inner().unwrap_or_else(move || {
                        anyhow!("Ethereum node took too long to return latest block").into()
                    })
                }),
        )
    }

    fn load_block(
        &self,
        logger: &Logger,
        block_hash: H256,
    ) -> Box<dyn Future<Item = LightEthereumBlock, Error = Error> + Send> {
        Box::new(
            self.block_by_hash(&logger, block_hash)
                .and_then(move |block_opt| {
                    block_opt.ok_or_else(move || {
                        anyhow!(
                            "Ethereum node could not find block with hash {}",
                            block_hash
                        )
                    })
                }),
        )
    }

    fn block_by_hash(
        &self,
        logger: &Logger,
        block_hash: H256,
    ) -> Box<dyn Future<Item = Option<LightEthereumBlock>, Error = Error> + Send> {
        let web3 = self.web3.clone();
        let logger = logger.clone();

        Box::new(
            retry("eth_getBlockByHash RPC call", &logger)
                .limit(*REQUEST_RETRIES)
                .timeout_secs(*JSON_RPC_TIMEOUT)
                .run(move || {
                    web3.eth()
                        .block_with_txs(BlockId::Hash(block_hash))
                        .from_err()
                })
                .map_err(move |e| {
                    e.into_inner().unwrap_or_else(move || {
                        anyhow!("Ethereum node took too long to return block {}", block_hash)
                    })
                }),
        )
    }

    fn block_by_number(
        &self,
        logger: &Logger,
        block_number: u64,
    ) -> Box<dyn Future<Item = Option<LightEthereumBlock>, Error = Error> + Send> {
        let web3 = self.web3.clone();
        let logger = logger.clone();

        Box::new(
            retry("eth_getBlockByNumber RPC call", &logger)
                .no_limit()
                .timeout_secs(*JSON_RPC_TIMEOUT)
                .run(move || {
                    web3.eth()
                        .block_with_txs(BlockId::Number(block_number.into()))
                        .from_err()
                })
                .map_err(move |e| {
                    e.into_inner().unwrap_or_else(move || {
                        anyhow!(
                            "Ethereum node took too long to return block {}",
                            block_number
                        )
                    })
                }),
        )
    }

    fn load_full_block(
        &self,
        logger: &Logger,
        block: LightEthereumBlock,
    ) -> Box<dyn Future<Item = EthereumBlock, Error = EthereumAdapterError> + Send> {
        let logger = logger.clone();
        let block_hash = block.hash.expect("block is missing block hash");

        // The early return is necessary for correctness, otherwise we'll
        // request an empty batch which is not valid in JSON-RPC.
        if block.transactions.is_empty() {
            trace!(logger, "Block {} contains no transactions", block_hash);
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
                .timeout_secs(*JSON_RPC_TIMEOUT)
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
                        anyhow!(
                            "Ethereum node took too long to return receipts for block {}",
                            block_hash
                        )
                        .into()
                    })
                }),
        )
    }

    fn block_pointer_from_number(
        &self,
        logger: &Logger,
        chain_store: Arc<dyn ChainStore>,
        block_number: u64,
    ) -> Box<dyn Future<Item = EthereumBlockPointer, Error = EthereumAdapterError> + Send> {
        Box::new(
            // When this method is called (from the subgraph registrar), we don't
            // know yet whether the block with the given number is final, it is
            // therefore safer to assume it is not final
            self.block_hash_by_block_number(logger, chain_store.clone(), block_number, false)
                .and_then(move |block_hash_opt| {
                    block_hash_opt.ok_or_else(|| {
                        anyhow!(
                            "Ethereum node could not find start block hash by block number {}",
                            &block_number
                        )
                    })
                })
                .from_err()
                .map(move |block_hash| EthereumBlockPointer {
                    hash: block_hash,
                    number: block_number,
                }),
        )
    }

    fn block_hash_by_block_number(
        &self,
        logger: &Logger,
        chain_store: Arc<dyn ChainStore>,
        block_number: u64,
        block_is_final: bool,
    ) -> Box<dyn Future<Item = Option<H256>, Error = Error> + Send> {
        let web3 = self.web3.clone();

        let mut hashes = match chain_store.block_hashes_by_block_number(block_number) {
            Ok(hashes) => hashes,
            Err(e) => return Box::new(future::result(Err(e))),
        };
        let num_hashes = hashes.len();
        let logger1 = logger.clone();
        let confirm_block_hash = move |hash: &Option<H256>| {
            // If there was more than one hash, now that we know what the
            // 'right' one is, get rid of all the others
            if let Some(hash) = hash {
                if block_is_final && num_hashes > 1 {
                    chain_store
                        .confirm_block_hash(block_number, hash)
                        .map(|_| ())
                        .unwrap_or_else(|e| {
                            warn!(
                                logger1,
                                "Failed to remove {} ommers for block number {} \
                                 (hash `0x{:x}`): {}",
                                num_hashes - 1,
                                block_number,
                                hash,
                                e
                            );
                        });
                }
            } else {
                warn!(
                    logger1,
                    "Failed to fetch block hash for block number";
                    "number" => block_number
                );
            }
        };

        if hashes.len() == 1 {
            Box::new(future::result(Ok(hashes.pop())))
        } else {
            Box::new(
                retry("eth_getBlockByNumber RPC call", &logger)
                    .no_limit()
                    .timeout_secs(*JSON_RPC_TIMEOUT)
                    .run(move || {
                        web3.eth()
                            .block(BlockId::Number(block_number.into()))
                            .from_err()
                            .map(|block_opt| block_opt.map(|block| block.hash).flatten())
                    })
                    .inspect(confirm_block_hash)
                    .map_err(move |e| {
                        e.into_inner().unwrap_or_else(move || {
                            anyhow!(
                                "Ethereum node took too long to return data for block #{}",
                                block_number
                            )
                        })
                    }),
            )
        }
    }

    fn uncles(
        &self,
        logger: &Logger,
        block: &LightEthereumBlock,
    ) -> Box<dyn Future<Item = Vec<Option<Block<H256>>>, Error = Error> + Send> {
        let block_hash = match block.hash {
            Some(hash) => hash,
            None => {
                return Box::new(future::result(Err(anyhow!(
                    "could not get uncle for block '{}' because block has null hash",
                    block
                        .number
                        .map(|num| num.to_string())
                        .unwrap_or(String::from("null"))
                ))))
            }
        };
        let n = block.uncles.len();

        Box::new(
            futures::stream::futures_ordered((0..n).map(move |index| {
                let web3 = self.web3.clone();

                retry("eth_getUncleByBlockHashAndIndex RPC call", &logger)
                    .no_limit()
                    .timeout_secs(60)
                    .run(move || {
                        web3.eth()
                            .uncle(block_hash.clone().into(), index.into())
                            .map_err(move |e| {
                                anyhow!(
                                    "could not get uncle {} for block {:?} ({} uncles): {}",
                                    index,
                                    block_hash,
                                    n,
                                    e
                                )
                            })
                    })
                    .map_err(move |e| {
                        e.into_inner().unwrap_or_else(move || {
                            anyhow!("Ethereum node took too long to return uncle")
                        })
                    })
            }))
            .collect(),
        )
    }

    fn is_on_main_chain(
        &self,
        logger: &Logger,
        _: Arc<SubgraphEthRpcMetrics>,
        chain_store: Arc<dyn ChainStore>,
        block_ptr: EthereumBlockPointer,
    ) -> Box<dyn Future<Item = bool, Error = Error> + Send> {
        Box::new(
            self.block_hash_by_block_number(&logger, chain_store, block_ptr.number, true)
                .and_then(move |block_hash_opt| {
                    block_hash_opt
                        .ok_or_else(|| {
                            anyhow!("Ethereum node is missing block #{}", block_ptr.number)
                        })
                        .map(|block_hash| block_hash == block_ptr.hash)
                }),
        )
    }

    fn calls_in_block(
        &self,
        logger: &Logger,
        subgraph_metrics: Arc<SubgraphEthRpcMetrics>,
        block_number: u64,
        block_hash: H256,
    ) -> Box<dyn Future<Item = Vec<EthereumCall>, Error = Error> + Send> {
        let eth = self.clone();
        let addresses = Vec::new();
        let calls = eth
            .trace_stream(
                &logger,
                subgraph_metrics.clone(),
                block_number,
                block_number,
                addresses,
            )
            .collect()
            .and_then(move |traces| {
                // `trace_stream` returns all of the traces for the block, and this
                // includes a trace for the block reward which every block should have.
                // If there are no traces something has gone wrong.
                if traces.is_empty() {
                    return future::err(anyhow!(
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
                    return future::err(anyhow!(
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

    fn logs_in_block_range(
        &self,
        logger: &Logger,
        subgraph_metrics: Arc<SubgraphEthRpcMetrics>,
        from: u64,
        to: u64,
        log_filter: EthereumLogFilter,
    ) -> DynTryFuture<'static, Vec<Log>, Error> {
        let eth: Self = self.cheap_clone();
        let logger = logger.clone();

        futures03::stream::iter(log_filter.eth_get_logs_filters().map(move |filter| {
            eth.cheap_clone().log_stream(
                logger.cheap_clone(),
                subgraph_metrics.cheap_clone(),
                from,
                to,
                filter,
            )
        }))
        // Real limits on the number of parallel requests are imposed within the adapter.
        .buffered(1000)
        .try_concat()
        .boxed()
    }

    fn calls_in_block_range(
        &self,
        logger: &Logger,
        subgraph_metrics: Arc<SubgraphEthRpcMetrics>,
        from: u64,
        to: u64,
        call_filter: EthereumCallFilter,
    ) -> Box<dyn Stream<Item = EthereumCall, Error = Error> + Send> {
        let eth = self.clone();

        let addresses: Vec<H160> = call_filter
            .contract_addresses_function_signatures
            .iter()
            .filter(|(_addr, (start_block, _fsigs))| start_block <= &to)
            .map(|(addr, (_start_block, _fsigs))| *addr)
            .collect::<HashSet<H160>>()
            .into_iter()
            .collect::<Vec<H160>>();
        Box::new(
            eth.trace_stream(&logger, subgraph_metrics, from, to, addresses)
                .filter_map(|trace| EthereumCall::try_from_trace(&trace))
                .filter(move |call| {
                    // `trace_filter` can only filter by calls `to` an address and
                    // a block range. Since subgraphs are subscribing to calls
                    // for a specific contract function an additional filter needs
                    // to be applied
                    call_filter.matches(&call)
                }),
        )
    }

    fn contract_call(
        &self,
        logger: &Logger,
        call: EthereumContractCall,
        cache: Arc<dyn EthereumCallCache>,
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
        let call_data = match call.function.encode_input(&call.args) {
            Ok(data) => data,
            Err(e) => return Box::new(future::err(EthereumContractCallError::EncodingError(e))),
        };

        if *ETH_CALL_FULL_LOG {
            trace!(logger, "eth_call";
                "address" => hex::encode(&call.address),
                "data" => hex::encode(&call_data)
            );
        }

        // Check if we have it cached, if not do the call and cache.
        Box::new(
            match cache
                .get_call(call.address, &call_data, call.block_ptr)
                .map_err(|e| error!(logger, "call cache get error"; "error" => e.to_string()))
                .ok()
                .flatten()
            {
                Some(result) => {
                    Box::new(future::ok(result)) as Box<dyn Future<Item = _, Error = _> + Send>
                }
                None => {
                    let cache = cache.clone();
                    let call = call.clone();
                    let logger = logger.clone();
                    Box::new(
                        self.call(
                            logger.clone(),
                            call.address,
                            Bytes(call_data.clone()),
                            call.block_ptr,
                        )
                        .map(move |result| {
                            let _ = cache
                                .set_call(call.address, &call_data, call.block_ptr, &result.0)
                                .map_err(|e| {
                                    error!(logger, "call cache set error";
                                                   "error" => e.to_string())
                                });
                            result.0
                        }),
                    )
                }
            }
            // Decode the return values according to the ABI
            .and_then(move |output| {
                if output.is_empty() {
                    // We got a `0x` response. For old Geth, this can mean a revert. It can also be
                    // that the contract actually returned an empty response. A view call is meant
                    // to return something, so we treat empty responses the same as reverts.
                    Err(EthereumContractCallError::Revert("empty response".into()))
                } else {
                    // Decode failures are reverts. The reasoning is that if Solidity fails to
                    // decode an argument, that's a revert, so the same goes for the output.
                    call.function.decode_output(&output).map_err(|e| {
                        EthereumContractCallError::Revert(format!("failed to decode output: {}", e))
                    })
                }
            }),
        )
    }

    /// Load Ethereum blocks in bulk, returning results as they come back as a Stream.
    fn load_blocks(
        &self,
        logger: Logger,
        chain_store: Arc<dyn ChainStore>,
        block_hashes: HashSet<H256>,
    ) -> Box<dyn Stream<Item = LightEthereumBlock, Error = Error> + Send> {
        // Search for the block in the store first then use json-rpc as a backup.
        let mut blocks = chain_store
            .blocks(block_hashes.iter().cloned().collect())
            .map_err(|e| error!(&logger, "Error accessing block cache {}", e))
            .unwrap_or_default();

        let missing_blocks = Vec::from_iter(
            block_hashes
                .into_iter()
                .filter(|hash| !blocks.iter().any(|b| b.hash == Some(*hash))),
        );

        // Return a stream that lazily loads batches of blocks.
        debug!(logger, "Requesting {} block(s)", missing_blocks.len());
        Box::new(
            self.load_blocks_rpc(logger.clone(), missing_blocks.into_iter().collect())
                .collect()
                .map(move |new_blocks| {
                    if let Err(e) = chain_store.upsert_light_blocks(new_blocks.clone()) {
                        error!(logger, "Error writing to block cache {}", e);
                    }
                    blocks.extend(new_blocks);
                    blocks.sort_by_key(|block| block.number);
                    stream::iter_ok(blocks)
                })
                .flatten_stream(),
        )
    }

    /// Reorg safety: `to` must be a final block.
    fn block_range_to_ptrs(
        &self,
        logger: Logger,
        from: u64,
        to: u64,
    ) -> Box<dyn Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send> {
        // Currently we can't go to the DB for this because there might be duplicate entries for
        // the same block number.
        debug!(&logger, "Requesting hashes for blocks [{}, {}]", from, to);
        Box::new(
            self.load_block_ptrs_rpc(logger, (from..=to).collect())
                .collect(),
        )
    }
}
