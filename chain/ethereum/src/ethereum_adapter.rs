use async_trait::async_trait;
use futures03::{future::BoxFuture, stream::FuturesUnordered};
use graph::abi;
use graph::abi::DynSolValueExt;
use graph::abi::FunctionExt;
use graph::blockchain::client::ChainClient;
use graph::blockchain::BlockHash;
use graph::blockchain::ChainIdentifier;
use graph::blockchain::ExtendedBlockPtr;
use graph::components::ethereum::*;
use graph::components::transaction_receipt::LightTransactionReceipt;
use graph::data::store::ethereum::call;
use graph::data::store::scalar;
use graph::data::subgraph::UnifiedMappingApiVersion;
use graph::data::subgraph::API_VERSION_0_0_7;
use graph::data_source::common::ContractCall;
use graph::futures01::stream;
use graph::futures01::Future;
use graph::futures01::Stream;
use graph::futures03::future::try_join_all;
use graph::futures03::{
    self, compat::Future01CompatExt, FutureExt, StreamExt, TryFutureExt, TryStreamExt,
};
use graph::prelude::{
    alloy::{
        self,
        network::{AnyNetwork, TransactionResponse},
        primitives::{Address, B256},
        providers::{
            ext::TraceApi,
            fillers::{
                BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller,
            },
            Identity, Provider, RootProvider,
        },
        rpc::types::{
            trace::{filter::TraceFilter as AlloyTraceFilter, parity::LocalizedTransactionTrace},
            TransactionInput, TransactionRequest,
        },
        transports::{RpcError, TransportErrorKind},
    },
    tokio::try_join,
};
use graph::slog::o;
use graph::{
    blockchain::{block_stream::BlockWithTriggers, BlockPtr, IngestorError},
    prelude::{
        anyhow::{self, anyhow, bail, ensure, Context},
        debug, error, hex, info, retry, trace, warn, BlockNumber, ChainStore, CheapClone,
        DynTryFuture, Error, EthereumCallCache, Logger, TimeoutError,
    },
};
use itertools::Itertools;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::convert::TryFrom;
use std::iter::FromIterator;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tokio::time::timeout;

use crate::adapter::EthGetLogsFilter;
use crate::adapter::EthereumRpcError;
use crate::adapter::ProviderStatus;
use crate::call_helper::interpret_eth_call_error;
use crate::chain::BlockFinality;
use crate::json_block::EthereumJsonBlock;
use crate::trigger::{LogPosition, LogRef};
use crate::Chain;
use crate::NodeCapabilities;
use crate::TriggerFilter;
use crate::{
    adapter::{
        ContractCallError, EthereumAdapter as EthereumAdapterTrait, EthereumBlockFilter,
        EthereumCallFilter, EthereumLogFilter, ProviderEthRpcMetrics, SubgraphEthRpcMetrics,
    },
    transport::Transport,
    trigger::{EthereumBlockTriggerType, EthereumTrigger},
    ENV_VARS,
};

type AlloyProvider = FillProvider<
    JoinFill<
        Identity,
        JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
    >,
    RootProvider<AnyNetwork>,
    AnyNetwork,
>;

#[derive(Clone)]
pub struct EthereumAdapter {
    logger: Logger,
    provider: String,
    alloy: Arc<AlloyProvider>,
    metrics: Arc<ProviderEthRpcMetrics>,
    supports_eip_1898: bool,
    call_only: bool,
    supports_block_receipts: Arc<RwLock<Option<bool>>>,
}

impl std::fmt::Debug for EthereumAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EthereumAdapter")
            .field("logger", &self.logger)
            .field("provider", &self.provider)
            .field("alloy", &"<Provider>")
            .field("metrics", &self.metrics)
            .field("supports_eip_1898", &self.supports_eip_1898)
            .field("call_only", &self.call_only)
            .field("supports_block_receipts", &self.supports_block_receipts)
            .finish()
    }
}

impl CheapClone for EthereumAdapter {
    fn cheap_clone(&self) -> Self {
        Self {
            logger: self.logger.clone(),
            provider: self.provider.clone(),
            alloy: self.alloy.clone(),
            metrics: self.metrics.cheap_clone(),
            supports_eip_1898: self.supports_eip_1898,
            call_only: self.call_only,
            supports_block_receipts: self.supports_block_receipts.cheap_clone(),
        }
    }
}

impl EthereumAdapter {
    pub fn is_call_only(&self) -> bool {
        self.call_only
    }

    pub async fn new(
        logger: Logger,
        provider: String,
        transport: Transport,
        provider_metrics: Arc<ProviderEthRpcMetrics>,
        supports_eip_1898: bool,
        call_only: bool,
    ) -> Self {
        let alloy = match &transport {
            Transport::RPC(client) => Arc::new(
                alloy::providers::ProviderBuilder::<_, _, AnyNetwork>::default()
                    .network::<AnyNetwork>()
                    .with_recommended_fillers()
                    .connect_client(client.clone()),
            ),
            Transport::IPC(ipc_connect) => Arc::new(
                alloy::providers::ProviderBuilder::<_, _, AnyNetwork>::default()
                    .network::<AnyNetwork>()
                    .with_recommended_fillers()
                    .connect_ipc(ipc_connect.clone())
                    .await
                    .expect("Failed to connect to Ethereum IPC"),
            ),
            Transport::WS(ws_connect) => Arc::new(
                alloy::providers::ProviderBuilder::<_, _, AnyNetwork>::default()
                    .network::<AnyNetwork>()
                    .with_recommended_fillers()
                    .connect_ws(ws_connect.clone())
                    .await
                    .expect("Failed to connect to Ethereum WS"),
            ),
        };

        EthereumAdapter {
            logger,
            provider,
            alloy,
            metrics: provider_metrics,
            supports_eip_1898,
            call_only,
            supports_block_receipts: Arc::new(RwLock::new(None)),
        }
    }

    async fn traces(
        self,
        logger: Logger,
        subgraph_metrics: Arc<SubgraphEthRpcMetrics>,
        from: BlockNumber,
        to: BlockNumber,
        addresses: Vec<Address>,
    ) -> Result<Vec<LocalizedTransactionTrace>, Error> {
        assert!(!self.call_only);

        let retry_log_message =
            format!("trace_filter RPC call for block range: [{}..{}]", from, to);
        let eth = self.clone();

        retry(retry_log_message, &logger)
            .redact_log_urls(true)
            .limit(ENV_VARS.request_retries)
            .timeout_secs(ENV_VARS.json_rpc_timeout.as_secs())
            .run(move || {
                let eth = eth.clone();
                let logger = logger.clone();
                let subgraph_metrics = subgraph_metrics.clone();
                let addresses = addresses.clone();
                async move {
                    eth.execute_trace_filter_request(logger, subgraph_metrics, from, to, addresses)
                        .await
                }
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
            .await
    }

    async fn execute_trace_filter_request(
        &self,
        logger: Logger,
        subgraph_metrics: Arc<SubgraphEthRpcMetrics>,
        from: BlockNumber,
        to: BlockNumber,
        addresses: Vec<Address>,
    ) -> Result<Vec<LocalizedTransactionTrace>, Error> {
        let alloy_trace_filter = Self::build_trace_filter(from, to, &addresses);
        let start = Instant::now();

        let result = self.alloy.trace_filter(&alloy_trace_filter).await;

        if let Ok(traces) = &result {
            self.log_trace_results(&logger, from, to, traces.len());
        }

        self.record_trace_metrics(
            &subgraph_metrics,
            start.elapsed().as_secs_f64(),
            &result,
            from,
            to,
            &logger,
        );

        result.map_err(Error::from)
    }

    fn build_trace_filter(
        from: BlockNumber,
        to: BlockNumber,
        addresses: &[Address],
    ) -> AlloyTraceFilter {
        let filter = AlloyTraceFilter::default()
            .from_block(from as u64)
            .to_block(to as u64);

        if !addresses.is_empty() {
            filter.to_address(addresses.to_vec())
        } else {
            filter
        }
    }

    fn log_trace_results(
        &self,
        logger: &Logger,
        from: BlockNumber,
        to: BlockNumber,
        trace_len: usize,
    ) {
        if trace_len > 0 {
            if to == from {
                debug!(logger, "Received {} traces for block {}", trace_len, to);
            } else {
                debug!(
                    logger,
                    "Received {} traces for blocks [{}, {}]", trace_len, from, to
                );
            }
        }
    }

    fn record_trace_metrics(
        &self,
        subgraph_metrics: &Arc<SubgraphEthRpcMetrics>,
        elapsed: f64,
        result: &Result<Vec<LocalizedTransactionTrace>, RpcError<TransportErrorKind>>,
        from: BlockNumber,
        to: BlockNumber,
        logger: &Logger,
    ) {
        self.metrics
            .observe_request(elapsed, "trace_filter", &self.provider);
        subgraph_metrics.observe_request(elapsed, "trace_filter", &self.provider);

        if let Err(e) = result {
            self.metrics.add_error("trace_filter", &self.provider);
            subgraph_metrics.add_error("trace_filter", &self.provider);
            debug!(
                logger,
                "Error querying traces error = {:#} from = {} to = {}", e, from, to
            );
        }
    }

    // This is a lazy check for block receipt support. It is only called once and then the result is
    // cached. The result is not used for anything critical, so it is fine to be lazy.
    async fn check_block_receipt_support_and_update_cache(
        &self,
        alloy: Arc<AlloyProvider>,
        block_hash: B256,
        supports_eip_1898: bool,
        call_only: bool,
        logger: Logger,
    ) -> bool {
        // This is the lazy part. If the result is already in `supports_block_receipts`, we don't need
        // to check again.
        {
            let supports_block_receipts = self.supports_block_receipts.read().await;
            if let Some(supports_block_receipts) = *supports_block_receipts {
                return supports_block_receipts;
            }
        }

        info!(logger, "Checking eth_getBlockReceipts support");
        let result = timeout(
            ENV_VARS.block_receipts_check_timeout,
            check_block_receipt_support(alloy, block_hash, supports_eip_1898, call_only),
        )
        .await;

        let result = match result {
            Ok(Ok(_)) => {
                info!(logger, "Provider supports block receipts");
                true
            }
            Ok(Err(err)) => {
                warn!(logger, "Skipping use of block receipts, reason: {}", err);
                false
            }
            Err(_) => {
                warn!(
                    logger,
                    "Skipping use of block receipts, reason: Timeout after {} seconds",
                    ENV_VARS.block_receipts_check_timeout.as_secs()
                );
                false
            }
        };

        // We set the result in `self.supports_block_receipts` so that the next time this function is called, we don't
        // need to check again.
        let mut supports_block_receipts = self.supports_block_receipts.write().await;
        if supports_block_receipts.is_none() {
            *supports_block_receipts = Some(result);
        }

        result
    }

    /// Alloy-exclusive version of logs_with_sigs using alloy types and methods
    async fn logs_with_sigs(
        &self,
        logger: Logger,
        subgraph_metrics: Arc<SubgraphEthRpcMetrics>,
        from: BlockNumber,
        to: BlockNumber,
        filter: Arc<EthGetLogsFilter>,
        too_many_logs_fingerprints: &'static [&'static str],
    ) -> Result<
        Vec<alloy::rpc::types::Log>,
        TimeoutError<RpcError<alloy::transports::TransportErrorKind>>,
    > {
        assert!(!self.call_only);

        let eth_adapter = self.clone();
        let retry_log_message = format!("eth_getLogs RPC call for block range: [{}..{}]", from, to);
        retry(retry_log_message, &logger)
            .redact_log_urls(true)
            .when(
                move |res: &Result<_, RpcError<alloy::transports::TransportErrorKind>>| match res {
                    Ok(_) => false,
                    Err(e) => !too_many_logs_fingerprints
                        .iter()
                        .any(|f| e.to_string().contains(f)),
                },
            )
            .limit(ENV_VARS.request_retries)
            .timeout_secs(ENV_VARS.json_rpc_timeout.as_secs())
            .run(move || {
                let eth_adapter = eth_adapter.cheap_clone();
                let subgraph_metrics = subgraph_metrics.clone();
                let provider_metrics = eth_adapter.metrics.clone();
                let filter = filter.clone();
                let provider = eth_adapter.provider.clone();

                async move {
                    let start = Instant::now();

                    let alloy_filter = filter.to_alloy_filter(from, to);

                    let result = eth_adapter.alloy.get_logs(&alloy_filter).await;
                    let elapsed = start.elapsed().as_secs_f64();
                    provider_metrics.observe_request(elapsed, "eth_getLogs", &provider);
                    subgraph_metrics.observe_request(elapsed, "eth_getLogs", &provider);
                    if result.is_err() {
                        provider_metrics.add_error("eth_getLogs", &provider);
                        subgraph_metrics.add_error("eth_getLogs", &provider);
                    }
                    result
                }
            })
            .await
    }

    fn trace_stream(
        self,
        logger: &Logger,
        subgraph_metrics: Arc<SubgraphEthRpcMetrics>,
        from: BlockNumber,
        to: BlockNumber,
        addresses: Vec<Address>,
    ) -> impl futures03::Stream<Item = Result<LocalizedTransactionTrace, Error>> + Send {
        if from > to {
            panic!(
                "Can not produce a call stream on a backwards block range: from = {}, to = {}",
                from, to,
            );
        }

        // Go one block at a time if requesting all traces, to not overload the RPC.
        let step_size = match addresses.is_empty() {
            false => ENV_VARS.trace_stream_step_size,
            true => 1,
        };

        let ranges: Vec<(BlockNumber, BlockNumber)> = {
            let mut ranges = Vec::new();
            let mut start = from;
            while start <= to {
                let end = (start + step_size - 1).min(to);
                ranges.push((start, end));
                start = end + 1;
            }
            ranges
        };

        let eth = self;
        let logger = logger.clone();

        futures03::stream::iter(ranges.into_iter().map(move |(start, end)| {
            let eth = eth.clone();
            let logger = logger.clone();
            let subgraph_metrics = subgraph_metrics.clone();
            let addresses = addresses.clone();

            async move {
                if start == end {
                    debug!(logger, "Requesting traces for block {}", start);
                } else {
                    debug!(logger, "Requesting traces for blocks [{}, {}]", start, end);
                }

                eth.traces(logger, subgraph_metrics, start, end, addresses)
                    .await
            }
        }))
        .buffered(ENV_VARS.block_batch_size)
        .map_ok(|traces| futures03::stream::iter(traces.into_iter().map(Ok)))
        .try_flatten()
    }

    fn log_stream(
        &self,
        logger: Logger,
        subgraph_metrics: Arc<SubgraphEthRpcMetrics>,
        from: BlockNumber,
        to: BlockNumber,
        filter: EthGetLogsFilter,
    ) -> DynTryFuture<'static, Vec<alloy::rpc::types::Log>, Error> {
        // Codes returned by Ethereum node providers if an eth_getLogs request is too heavy.
        const TOO_MANY_LOGS_FINGERPRINTS: &[&str] = &[
            "ServerError(-32005)",       // Infura
            "503 Service Unavailable",   // Alchemy
            "ServerError(-32000)",       // Alchemy
            "Try with this block range", // zKSync era
            "block range too large",     // Monad
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
            true => (to - from).min(ENV_VARS.max_event_only_range - 1),
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
                        logger.cheap_clone(),
                        subgraph_metrics.cheap_clone(),
                        start,
                        end,
                        filter.cheap_clone(),
                        TOO_MANY_LOGS_FINGERPRINTS,
                    )
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

    fn block_ptr_to_id(&self, block_ptr: &BlockPtr) -> alloy::rpc::types::BlockId {
        if !self.supports_eip_1898 {
            alloy::rpc::types::BlockId::number(block_ptr.number as u64)
        } else {
            alloy::rpc::types::BlockId::hash(block_ptr.hash.as_b256())
        }
    }

    async fn code(
        &self,
        logger: &Logger,
        address: Address,
        block_ptr: BlockPtr,
    ) -> Result<alloy::primitives::Bytes, EthereumRpcError> {
        let alloy = self.alloy.clone();
        let logger = Logger::new(logger, o!("provider" => self.provider.clone()));

        let block_id = self.block_ptr_to_id(&block_ptr);
        let retry_log_message = format!("eth_getCode RPC call for block {}", block_ptr);

        retry(retry_log_message, &logger)
            .redact_log_urls(true)
            .when(|result| result.is_err())
            .limit(ENV_VARS.request_retries)
            .timeout_secs(ENV_VARS.json_rpc_timeout.as_secs())
            .run(move || {
                let alloy = alloy.cheap_clone();
                async move {
                    let result = alloy.get_code_at(address).block_id(block_id).await;
                    match result {
                        Ok(code) => Ok(code),
                        Err(err) => Err(EthereumRpcError::AlloyError(err)),
                    }
                }
            })
            .await
            .map_err(|e| e.into_inner().unwrap_or(EthereumRpcError::Timeout))
    }

    async fn balance(
        &self,
        logger: &Logger,
        address: Address,
        block_ptr: BlockPtr,
    ) -> Result<alloy::primitives::U256, EthereumRpcError> {
        let alloy = self.alloy.clone();
        let logger = Logger::new(logger, o!("provider" => self.provider.clone()));

        let block_id = self.block_ptr_to_id(&block_ptr);
        let retry_log_message = format!("eth_getBalance RPC call for block {}", block_ptr);

        retry(retry_log_message, &logger)
            .redact_log_urls(true)
            .when(|result| result.is_err())
            .limit(ENV_VARS.request_retries)
            .timeout_secs(ENV_VARS.json_rpc_timeout.as_secs())
            .run(move || {
                let alloy = alloy.cheap_clone();
                async move {
                    let result = alloy.get_balance(address).block_id(block_id).await;
                    match result {
                        Ok(balance) => Ok(balance),
                        Err(err) => Err(EthereumRpcError::AlloyError(err)),
                    }
                }
            })
            .await
            .map_err(|e| e.into_inner().unwrap_or(EthereumRpcError::Timeout))
    }

    async fn call(
        &self,
        logger: Logger,
        call_data: call::Request,
        block_ptr: BlockPtr,
        gas: Option<u32>,
    ) -> Result<call::Retval, ContractCallError> {
        let alloy = self.alloy.clone();
        let logger = Logger::new(&logger, o!("provider" => self.provider.clone()));

        let alloy_block_id = self.block_ptr_to_id(&block_ptr);
        let retry_log_message = format!("eth_call RPC call for block {}", block_ptr);
        retry(retry_log_message, &logger)
            .redact_log_urls(true)
            .limit(ENV_VARS.request_retries)
            .timeout_secs(ENV_VARS.json_rpc_timeout.as_secs())
            .run(move || {
                let call_data = call_data.clone();
                let alloy = alloy.cheap_clone();
                let logger = logger.cheap_clone();
                async move {
                    let mut req = TransactionRequest::default()
                        .input(TransactionInput::both(alloy::primitives::Bytes::from(
                            call_data.encoded_call.to_vec(),
                        )))
                        .to(call_data.address);

                    if let Some(gas) = gas {
                        req = req.gas_limit(gas as u64);
                    }

                    let result = alloy.call(req.into()).block(alloy_block_id).await;

                    match result {
                        Ok(bytes) => Ok(call::Retval::Value(scalar::Bytes::from(bytes))),
                        Err(err) => interpret_eth_call_error(&logger, err),
                    }
                }
            })
            .await
            .map_err(|e| e.into_inner().unwrap_or(ContractCallError::Timeout))
    }

    async fn call_and_cache(
        &self,
        logger: &Logger,
        call: &ContractCall,
        req: call::Request,
        cache: Arc<dyn EthereumCallCache>,
    ) -> Result<call::Response, ContractCallError> {
        let result = self
            .call(
                logger.clone(),
                req.cheap_clone(),
                call.block_ptr.clone(),
                call.gas,
            )
            .await?;
        if let Err(e) = cache
            .set_call(
                logger,
                req.cheap_clone(),
                call.block_ptr.cheap_clone(),
                result.clone(),
            )
            .await
        {
            error!(logger, "EthereumAdapter: call cache set error";
                        "contract_address" => format!("{:?}", req.address),
                        "error" => e.to_string());
        }

        Ok(req.response(result, call::Source::Rpc))
    }
    /// Request blocks by hash through JSON-RPC.
    fn load_blocks_rpc(
        &self,
        logger: Logger,
        ids: Vec<B256>,
    ) -> impl futures03::Stream<Item = Result<Arc<LightEthereumBlock>, Error>> + Send {
        let alloy = self.alloy.clone();

        futures03::stream::iter(ids.into_iter().map(move |hash| {
            let alloy = alloy.clone();
            let logger = logger.clone();

            async move {
                retry(format!("load block {}", hash), &logger)
                    .redact_log_urls(true)
                    .limit(ENV_VARS.request_retries)
                    .timeout_secs(ENV_VARS.json_rpc_timeout.as_secs())
                    .run(move || {
                        let alloy = alloy.cheap_clone();
                        async move {
                            alloy
                                .get_block_by_hash(hash)
                                .full()
                                .await
                                .map_err(Error::from)
                                .and_then(|block| {
                                    block
                                        .map(|b| Arc::new(LightEthereumBlock::new(b)))
                                        .ok_or_else(|| {
                                            anyhow::anyhow!(
                                                "Ethereum node did not find block {:?}",
                                                hash
                                            )
                                        })
                                })
                        }
                    })
                    .await
                    .map_err(|e| {
                        e.into_inner().unwrap_or_else(|| {
                            anyhow::anyhow!("Ethereum node took too long to return block {}", hash)
                        })
                    })
            }
        }))
        .buffered(ENV_VARS.block_batch_size)
    }

    /// Request blocks by number through JSON-RPC.
    pub fn load_block_ptrs_by_numbers_rpc(
        &self,
        logger: Logger,
        numbers: Vec<BlockNumber>,
    ) -> impl futures03::Stream<Item = Result<Arc<ExtendedBlockPtr>, Error>> + Send {
        let alloy = self.alloy.clone();

        futures03::stream::iter(numbers.into_iter().map(move |number| {
            let alloy = alloy.clone();
            let logger = logger.clone();

            async move {
                retry(format!("load block {}", number), &logger)
                    .redact_log_urls(true)
                    .limit(ENV_VARS.request_retries)
                    .timeout_secs(ENV_VARS.json_rpc_timeout.as_secs())
                    .run(move || {
                        let alloy = alloy.cheap_clone();

                        async move {
                            let block_result = alloy
                                .get_block_by_number(alloy::rpc::types::BlockNumberOrTag::Number(
                                    number as u64,
                                ))
                                .await;

                            match block_result {
                                Ok(Some(block)) => {
                                    let ptr = ExtendedBlockPtr::try_from((
                                        block.header.hash,
                                        i32::try_from(block.header.number).unwrap(),
                                        block.header.parent_hash,
                                        block.header.timestamp,
                                    ))
                                    .map_err(|e| {
                                        anyhow::anyhow!("Failed to convert block: {}", e)
                                    })?;
                                    Ok(Arc::new(ptr))
                                }
                                Ok(None) => Err(anyhow::anyhow!(
                                    "Ethereum node did not find block with number {:?}",
                                    number
                                )),
                                Err(e) => Err(anyhow::anyhow!("Failed to fetch block: {}", e)),
                            }
                        }
                    })
                    .await
                    .map_err(|e| match e {
                        TimeoutError::Elapsed => {
                            anyhow::anyhow!("Timeout while fetching block {}", number)
                        }
                        TimeoutError::Inner(e) => e,
                    })
            }
        }))
        .buffered(ENV_VARS.block_ptr_batch_size)
    }

    /// Request blocks ptrs for numbers through JSON-RPC.
    ///
    /// Reorg safety: If ids are numbers, they must be a final blocks.
    fn load_block_ptrs_rpc(
        &self,
        logger: Logger,
        block_nums: Vec<BlockNumber>,
    ) -> impl Stream<Item = BlockPtr, Error = Error> + Send {
        let alloy = self.alloy.clone();

        stream::iter_ok::<_, Error>(block_nums.into_iter().map(move |block_num| {
            let alloy = alloy.clone();
            retry(format!("load block ptr {}", block_num), &logger)
                .redact_log_urls(true)
                .when(|res| !res.is_ok() && !detect_null_block(res))
                .no_limit()
                .timeout_secs(ENV_VARS.json_rpc_timeout.as_secs())
                .run(move || {
                    let alloy = alloy.cheap_clone();
                    async move {
                        let block = alloy
                            .get_block_by_number(alloy::rpc::types::BlockNumberOrTag::Number(
                                block_num as u64,
                            ))
                            .await?;

                        block.ok_or_else(|| {
                            anyhow!("Ethereum node did not find block {:?}", block_num)
                        })
                    }
                })
                .boxed()
                .compat()
                .from_err()
                .then(|res| {
                    if detect_null_block(&res) {
                        Ok(None)
                    } else {
                        Some(res).transpose()
                    }
                })
        }))
        .buffered(ENV_VARS.block_batch_size)
        .filter_map(|b| b)
        .map(|b| BlockPtr::from((b.header.hash, b.header.number)))
    }

    /// Check if `block_ptr` refers to a block that is on the main chain, according to the Ethereum
    /// node.
    ///
    /// Careful: don't use this function without considering race conditions.
    /// Chain reorgs could happen at any time, and could affect the answer received.
    /// Generally, it is only safe to use this function with blocks that have received enough
    /// confirmations to guarantee no further reorgs, **and** where the Ethereum node is aware of
    /// those confirmations.
    /// If the Ethereum node is far behind in processing blocks, even old blocks can be subject to
    /// reorgs.
    pub(crate) async fn is_on_main_chain(
        &self,
        logger: &Logger,
        block_ptr: BlockPtr,
    ) -> Result<bool, Error> {
        // TODO: This considers null blocks, but we could instead bail if we encounter one as a
        // small optimization.
        let canonical_block = self
            .next_existing_ptr_to_number(logger, block_ptr.number)
            .await?;
        Ok(canonical_block == block_ptr)
    }

    pub(crate) fn logs_in_block_range(
        &self,
        logger: &Logger,
        subgraph_metrics: Arc<SubgraphEthRpcMetrics>,
        from: BlockNumber,
        to: BlockNumber,
        log_filter: EthereumLogFilter,
    ) -> DynTryFuture<'static, Vec<alloy::rpc::types::Log>, Error> {
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
        .buffered(ENV_VARS.block_ingestor_max_concurrent_json_rpc_calls)
        .try_concat()
        .boxed()
    }

    pub(crate) fn calls_in_block_range<'a>(
        &self,
        logger: &Logger,
        subgraph_metrics: Arc<SubgraphEthRpcMetrics>,
        from: BlockNumber,
        to: BlockNumber,
        call_filter: &'a EthereumCallFilter,
    ) -> Box<dyn Stream<Item = EthereumCall, Error = Error> + Send + 'a> {
        let eth = self.clone();

        let EthereumCallFilter {
            contract_addresses_function_signatures,
            wildcard_signatures,
        } = call_filter;

        let mut addresses: Vec<Address> = contract_addresses_function_signatures
            .iter()
            .filter(|(_addr, (start_block, _fsigs))| start_block <= &to)
            .map(|(addr, (_start_block, _fsigs))| *addr)
            .collect::<HashSet<Address>>()
            .into_iter()
            .collect::<Vec<Address>>();

        if addresses.is_empty() && wildcard_signatures.is_empty() {
            // The filter has no started data sources in the requested range, nothing to do.
            // This prevents an expensive call to `trace_filter` with empty `addresses`.
            return Box::new(stream::empty());
        }

        // if wildcard_signatures is on, we can't filter by topic so we need to get all the traces.
        if addresses.len() > 100 || !wildcard_signatures.is_empty() {
            // If the address list is large, request all traces, this avoids generating huge
            // requests and potentially getting 413 errors.
            addresses = vec![];
        }

        Box::new(
            eth.trace_stream(logger, subgraph_metrics, from, to, addresses)
                .try_filter_map(move |trace| {
                    let maybe_call = EthereumCall::try_from_trace(&trace)
                        .filter(|call| call_filter.matches(call));
                    futures03::future::ready(Ok(maybe_call))
                })
                .boxed()
                .compat(),
        )
    }

    // Used to get the block triggers with a `polling` or `once` filter
    /// `polling_filter_type` is used to differentiate between `polling` and `once` filters
    /// A `polling_filter_type` value of  `BlockPollingFilterType::Once` is the case for
    /// intialization triggers
    /// A `polling_filter_type` value of  `BlockPollingFilterType::Polling` is the case for
    /// polling triggers
    pub(crate) fn blocks_matching_polling_intervals(
        &self,
        logger: Logger,
        from: i32,
        to: i32,
        filter: &EthereumBlockFilter,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<Vec<EthereumTrigger>, anyhow::Error>>
                + std::marker::Send,
        >,
    > {
        // Create a HashMap of block numbers to Vec<EthereumBlockTriggerType>
        let matching_blocks = (from..=to)
            .filter_map(|block_number| {
                filter
                    .polling_intervals
                    .iter()
                    .find_map(|(start_block, interval)| {
                        let has_once_trigger = (*interval == 0) && (block_number == *start_block);
                        let has_polling_trigger = block_number >= *start_block
                            && *interval > 0
                            && ((block_number - start_block) % *interval) == 0;

                        if has_once_trigger || has_polling_trigger {
                            let mut triggers = Vec::new();
                            if has_once_trigger {
                                triggers.push(EthereumBlockTriggerType::Start);
                            }
                            if has_polling_trigger {
                                triggers.push(EthereumBlockTriggerType::End);
                            }
                            Some((block_number, triggers))
                        } else {
                            None
                        }
                    })
            })
            .collect::<HashMap<_, _>>();

        let blocks_matching_polling_filter = self.load_ptrs_for_blocks(
            logger.clone(),
            matching_blocks.keys().cloned().collect_vec(),
        );

        let block_futures = blocks_matching_polling_filter.map(move |ptrs| {
            ptrs.into_iter()
                .flat_map(|ptr| {
                    let triggers = matching_blocks
                        .get(&ptr.number)
                        // Safe to unwrap since we are iterating over ptrs which was created from
                        // the keys of matching_blocks
                        .unwrap()
                        .iter()
                        .map(move |trigger| EthereumTrigger::Block(ptr.clone(), trigger.clone()));

                    triggers
                })
                .collect::<Vec<_>>()
        });

        block_futures.compat().boxed()
    }

    pub(crate) async fn calls_in_block(
        &self,
        logger: &Logger,
        subgraph_metrics: Arc<SubgraphEthRpcMetrics>,
        block_number: BlockNumber,
        block_hash: alloy::primitives::B256,
    ) -> Result<Vec<EthereumCall>, Error> {
        let eth = self.clone();
        let addresses = Vec::new();
        let traces: Vec<LocalizedTransactionTrace> = eth
            .trace_stream(
                logger,
                subgraph_metrics.clone(),
                block_number,
                block_number,
                addresses,
            )
            .try_collect()
            .await?;

        // `trace_stream` returns all of the traces for the block, and this
        // includes a trace for the block reward which every block should have.
        // If there are no traces something has gone wrong.
        if traces.is_empty() {
            return Err(anyhow!(
                "Trace stream returned no traces for block: number = `{}`, hash = `{}`",
                block_number,
                block_hash,
            ));
        }

        // Since we can only pull traces by block number and we have
        // all the traces for the block, we need to ensure that the
        // block hash for the traces is equal to the desired block hash.
        // Assume all traces are for the same block.
        if traces.first().unwrap().block_hash != Some(block_hash) {
            return Err(anyhow!(
                "Trace stream returned traces for an unexpected block: \
                         number = `{}`, hash = `{}`",
                block_number,
                block_hash,
            ));
        }

        Ok(traces
            .iter()
            .filter_map(EthereumCall::try_from_trace)
            .collect())
    }

    /// Reorg safety: `to` must be a final block.
    pub(crate) fn block_range_to_ptrs(
        &self,
        logger: Logger,
        from: BlockNumber,
        to: BlockNumber,
    ) -> Box<dyn Future<Item = Vec<BlockPtr>, Error = Error> + Send> {
        // Currently we can't go to the DB for this because there might be duplicate entries for
        // the same block number.
        debug!(&logger, "Requesting hashes for blocks [{}, {}]", from, to);
        Box::new(
            self.load_block_ptrs_rpc(logger, (from..=to).collect())
                .collect(),
        )
    }

    pub(crate) fn load_ptrs_for_blocks(
        &self,
        logger: Logger,
        blocks: Vec<BlockNumber>,
    ) -> Box<dyn Future<Item = Vec<BlockPtr>, Error = Error> + Send> {
        // Currently we can't go to the DB for this because there might be duplicate entries for
        // the same block number.
        debug!(&logger, "Requesting hashes for blocks {:?}", blocks);
        Box::new(self.load_block_ptrs_rpc(logger, blocks).collect())
    }

    pub async fn chain_id(&self) -> Result<u64, Error> {
        let logger = self.logger.clone();
        let alloy = self.alloy.clone();
        retry("chain_id RPC call", &logger)
            .redact_log_urls(true)
            .no_limit()
            .timeout_secs(ENV_VARS.json_rpc_timeout.as_secs())
            .run(move || {
                let alloy = alloy.cheap_clone();
                async move { alloy.get_chain_id().await.map_err(Error::from) }
            })
            .await
            .map_err(|e| e.into_inner().unwrap_or(EthereumRpcError::Timeout.into()))
    }
}

// Detects null blocks as can occur on Filecoin EVM chains, by checking for the FEVM-specific
// error returned when requesting such a null round. Ideally there should be a defined reponse or
// message for this case, or a check that is less dependent on the Filecoin implementation.
fn detect_null_block<T>(res: &Result<T, Error>) -> bool {
    match res {
        Ok(_) => false,
        Err(e) => e.to_string().contains("requested epoch was a null round"),
    }
}

#[async_trait]
impl EthereumAdapterTrait for EthereumAdapter {
    fn provider(&self) -> &str {
        &self.provider
    }

    async fn net_identifiers(&self) -> Result<ChainIdentifier, Error> {
        let logger = self.logger.clone();

        let alloy = self.alloy.clone();
        let metrics = self.metrics.clone();
        let provider = self.provider().to_string();
        let net_version_future = retry("net_version RPC call", &logger)
            .redact_log_urls(true)
            .no_limit()
            .timeout_secs(20)
            .run(move || {
                let alloy = alloy.cheap_clone();
                let metrics = metrics.cheap_clone();
                let provider = provider.clone();
                async move {
                    alloy.get_net_version().await.map_err(|e| {
                        metrics.set_status(ProviderStatus::VersionFail, &provider);
                        e.into()
                    })
                }
            })
            .map_err(|e| {
                self.metrics
                    .set_status(ProviderStatus::VersionTimeout, self.provider());
                e
            })
            .boxed();

        let alloy_provider = self.alloy.clone();
        let metrics = self.metrics.clone();
        let provider = self.provider().to_string();
        let retry_log_message = format!(
            "eth_getBlockByNumber({}, false) RPC call",
            ENV_VARS.genesis_block_number
        );
        let gen_block_hash_future = retry(retry_log_message, &logger)
            .redact_log_urls(true)
            .no_limit()
            .timeout_secs(30)
            .run(move || {
                let alloy_genesis = alloy_provider.cheap_clone();
                let metrics = metrics.cheap_clone();
                let provider = provider.clone();
                async move {
                    alloy_genesis
                        .get_block_by_number(alloy::rpc::types::BlockNumberOrTag::Number(
                            ENV_VARS.genesis_block_number,
                        ))
                        .await
                        .inspect_err(|_| {
                            metrics.set_status(ProviderStatus::GenesisFail, &provider);
                        })?
                        .map(|gen_block| BlockHash::from(gen_block.header.hash))
                        .ok_or_else(|| anyhow!("Ethereum node could not find genesis block"))
                }
            })
            .map_err(|e| {
                self.metrics
                    .set_status(ProviderStatus::GenesisTimeout, self.provider());
                e
            });

        let (net_version, genesis_block_hash) =
            try_join!(net_version_future, gen_block_hash_future).map_err(|e| {
                anyhow!(
                    "Ethereum node took too long to read network identifiers: {}",
                    e
                )
            })?;

        let ident = ChainIdentifier {
            net_version: net_version.to_string(),
            genesis_block_hash,
        };

        self.metrics
            .set_status(ProviderStatus::Working, self.provider());
        Ok(ident)
    }

    async fn latest_block_ptr(&self, logger: &Logger) -> Result<BlockPtr, IngestorError> {
        let alloy = self.alloy.clone();
        retry("eth_getBlockByNumber(latest) no txs RPC call", logger)
            .redact_log_urls(true)
            .no_limit()
            .timeout_secs(ENV_VARS.json_rpc_timeout.as_secs())
            .run(move || {
                let alloy = alloy.cheap_clone();
                async move {
                    let block_opt = alloy
                        .get_block_by_number(alloy::rpc::types::BlockNumberOrTag::Latest)
                        .await
                        .map_err(|e| anyhow!("could not get latest block from Ethereum: {}", e))?;

                    let block = block_opt
                        .ok_or_else(|| anyhow!("no latest block returned from Ethereum"))?;

                    Ok(BlockPtr::from((block.header.hash, block.header.number)))
                }
            })
            .map_err(move |e| {
                e.into_inner().unwrap_or_else(move || {
                    anyhow!("Ethereum node took too long to return latest block").into()
                })
            })
            .await
    }

    async fn block_by_hash(
        &self,
        logger: &Logger,
        block_hash: B256,
    ) -> Result<Option<AnyBlock>, Error> {
        let alloy = self.alloy.clone();
        let logger = logger.clone();
        let retry_log_message = format!(
            "eth_getBlockByHash RPC call for block hash {:?}",
            block_hash
        );

        retry(retry_log_message, &logger)
            .redact_log_urls(true)
            .limit(ENV_VARS.request_retries)
            .timeout_secs(ENV_VARS.json_rpc_timeout.as_secs())
            .run(move || {
                let alloy = alloy.cheap_clone();
                async move {
                    alloy
                        .get_block_by_hash(block_hash)
                        .full()
                        .await
                        .map_err(Error::from)
                }
            })
            .map_err(move |e| {
                e.into_inner().unwrap_or_else(move || {
                    anyhow!("Ethereum node took too long to return block {}", block_hash)
                })
            })
            .await
    }

    async fn block_by_number(
        &self,
        logger: &Logger,
        block_number: BlockNumber,
    ) -> Result<Option<AnyBlock>, Error> {
        let alloy = self.alloy.clone();
        let logger = logger.clone();
        let retry_log_message = format!(
            "eth_getBlockByNumber RPC call for block number {}",
            block_number
        );
        retry(retry_log_message, &logger)
            .redact_log_urls(true)
            .no_limit()
            .timeout_secs(ENV_VARS.json_rpc_timeout.as_secs())
            .run(move || {
                let alloy = alloy.clone();
                async move {
                    alloy
                        .get_block_by_number(alloy::rpc::types::BlockNumberOrTag::Number(
                            block_number as u64,
                        ))
                        .full()
                        .await
                        .map_err(Error::from)
                }
            })
            .map_err(move |e| {
                e.into_inner().unwrap_or_else(move || {
                    anyhow!(
                        "Ethereum node took too long to return block {}",
                        block_number
                    )
                })
            })
            .await
    }

    async fn load_full_block(
        &self,
        logger: &Logger,
        block: AnyBlock,
    ) -> Result<EthereumBlock, IngestorError> {
        let alloy = self.alloy.clone();
        let logger = logger.clone();
        let block_hash = block.header.hash;

        // The early return is necessary for correctness, otherwise we'll
        // request an empty batch which is not valid in JSON-RPC.
        if block.transactions.is_empty() {
            trace!(logger, "Block {} contains no transactions", block_hash);
            return Ok(EthereumBlock {
                block: Arc::new(LightEthereumBlock::new(block)),
                transaction_receipts: Vec::new(),
            });
        }
        let hashes: Vec<_> = block.transactions.hashes().collect();

        let supports_block_receipts = self
            .check_block_receipt_support_and_update_cache(
                alloy.clone(),
                block_hash,
                self.supports_eip_1898,
                self.call_only,
                logger.clone(),
            )
            .await;

        fetch_receipts_with_retry(alloy, hashes, block_hash, logger, supports_block_receipts)
            .await
            .map(|transaction_receipts| EthereumBlock {
                block: Arc::new(LightEthereumBlock::new(block)),
                transaction_receipts,
            })
    }

    async fn get_balance(
        &self,
        logger: &Logger,
        address: Address,
        block_ptr: BlockPtr,
    ) -> Result<alloy::primitives::U256, EthereumRpcError> {
        debug!(
            logger, "eth_getBalance";
            "address" => format!("{}", address),
            "block" => format!("{}", block_ptr)
        );
        self.balance(logger, address, block_ptr).await
    }

    async fn get_code(
        &self,
        logger: &Logger,
        address: Address,
        block_ptr: BlockPtr,
    ) -> Result<alloy::primitives::Bytes, EthereumRpcError> {
        debug!(
            logger, "eth_getCode";
            "address" => format!("{}", address),
            "block" => format!("{}", block_ptr)
        );
        self.code(logger, address, block_ptr).await
    }

    async fn next_existing_ptr_to_number(
        &self,
        logger: &Logger,
        block_number: BlockNumber,
    ) -> Result<BlockPtr, Error> {
        let mut next_number = block_number;
        loop {
            let retry_log_message = format!(
                "eth_getBlockByNumber RPC call for block number {}",
                next_number
            );
            let alloy = self.alloy.clone();
            let logger = logger.clone();
            let res = retry(retry_log_message, &logger)
                .redact_log_urls(true)
                .when(|res| !res.is_ok() && !detect_null_block(res))
                .no_limit()
                .timeout_secs(ENV_VARS.json_rpc_timeout.as_secs())
                .run(move || {
                    let alloy = alloy.cheap_clone();
                    async move {
                        alloy
                            .get_block_by_number(alloy::rpc::types::BlockNumberOrTag::Number(
                                next_number as u64,
                            ))
                            .await
                            .map(|block_opt| {
                                block_opt.map(|block| BlockHash::from(block.header.hash.0.to_vec()))
                            })
                            .map_err(Error::from)
                    }
                })
                .await
                .map_err(move |e| {
                    e.into_inner().unwrap_or_else(move || {
                        anyhow!(
                            "Ethereum node took too long to return data for block #{}",
                            next_number
                        )
                    })
                });
            if detect_null_block(&res) {
                next_number += 1;
                continue;
            }
            return match res {
                Ok(Some(hash)) => Ok(BlockPtr::new(hash, next_number)),
                Ok(None) => Err(anyhow!("Block {} does not contain hash", next_number)),
                Err(e) => Err(e),
            };
        }
    }

    async fn contract_call(
        &self,
        logger: &Logger,
        inp_call: &ContractCall,
        cache: Arc<dyn EthereumCallCache>,
    ) -> Result<(Option<Vec<abi::DynSolValue>>, call::Source), ContractCallError> {
        let mut result = self.contract_calls(logger, &[inp_call], cache).await?;
        // unwrap: self.contract_calls returns as many results as there were calls
        Ok(result.pop().unwrap())
    }

    async fn contract_calls(
        &self,
        logger: &Logger,
        calls: &[&ContractCall],
        cache: Arc<dyn EthereumCallCache>,
    ) -> Result<Vec<(Option<Vec<abi::DynSolValue>>, call::Source)>, ContractCallError> {
        fn as_req(
            logger: &Logger,
            call: &ContractCall,
            index: u32,
        ) -> Result<call::Request, ContractCallError> {
            // Emit custom error for type mismatches.
            for (val, kind) in call
                .args
                .iter()
                .zip(call.function.inputs.iter().map(|p| p.selector_type()))
            {
                let kind: abi::DynSolType = kind.parse().map_err(|err| {
                    ContractCallError::ABIError(anyhow!(
                        "failed to parse function input type '{kind}': {err}"
                    ))
                })?;

                if !val.type_check(&kind) {
                    return Err(ContractCallError::TypeError(val.clone(), kind.clone()));
                }
            }

            // Encode the call parameters according to the ABI
            let req = {
                let encoded_call = call
                    .function
                    .abi_encode_input(&call.args)
                    .map_err(ContractCallError::EncodingError)?;
                call::Request::new(call.address, encoded_call, index)
            };

            trace!(logger, "eth_call";
                "fn" => &call.function.name,
                "address" => hex::encode(call.address),
                "data" => hex::encode(req.encoded_call.as_ref()),
                "block_hash" => call.block_ptr.hash_hex(),
                "block_number" => call.block_ptr.block_number()
            );
            Ok(req)
        }

        fn decode(
            logger: &Logger,
            resp: call::Response,
            call: &ContractCall,
        ) -> (Option<Vec<abi::DynSolValue>>, call::Source) {
            let call::Response {
                retval,
                source,
                req: _,
            } = resp;
            match retval {
                call::Retval::Value(output) => match call.function.abi_decode_output(&output) {
                    Ok(tokens) => (Some(tokens), source),
                    Err(e) => {
                        // Decode failures are reverts. The reasoning is that if Solidity fails to
                        // decode an argument, that's a revert, so the same goes for the output.
                        let reason = format!("failed to decode output: {}", e);
                        info!(logger, "Contract call reverted"; "reason" => reason);
                        (None, call::Source::Rpc)
                    }
                },
                call::Retval::Null => {
                    // We got a `0x` response. For old Geth, this can mean a revert. It can also be
                    // that the contract actually returned an empty response. A view call is meant
                    // to return something, so we treat empty responses the same as reverts.
                    info!(logger, "Contract call reverted"; "reason" => "empty response");
                    (None, call::Source::Rpc)
                }
            }
        }

        fn log_call_error(logger: &Logger, e: &ContractCallError, call: &ContractCall) {
            match e {
                ContractCallError::AlloyError(e) => error!(logger,
                    "Ethereum node returned an error when calling function \"{}\" of contract \"{}\": {}",
                    call.function.name, call.contract_name, e),
                ContractCallError::Timeout => error!(logger,
                    "Ethereum node did not respond when calling function \"{}\" of contract \"{}\"",
                    call.function.name, call.contract_name),
                _ => error!(logger,
                    "Failed to call function \"{}\" of contract \"{}\": {}",
                    call.function.name, call.contract_name, e),
            }
        }

        if calls.is_empty() {
            return Ok(Vec::new());
        }

        let block_ptr = calls.first().unwrap().block_ptr.clone();
        if calls.iter().any(|call| call.block_ptr != block_ptr) {
            return Err(ContractCallError::Internal(
                "all calls must have the same block pointer".to_string(),
            ));
        }

        let reqs: Vec<_> = calls
            .iter()
            .enumerate()
            .map(|(index, call)| as_req(logger, call, index as u32))
            .collect::<Result<_, _>>()?;

        let (mut resps, missing) = cache
            .get_calls(&reqs, block_ptr)
            .await
            .map_err(|e| error!(logger, "call cache get error"; "error" => e.to_string()))
            .unwrap_or_else(|_| (Vec::new(), reqs));

        let futs = missing.into_iter().map(|req| {
            let cache = cache.clone();
            async move {
                let call = calls[req.index as usize];
                match self.call_and_cache(logger, call, req, cache.clone()).await {
                    Ok(resp) => Ok(resp),
                    Err(e) => {
                        log_call_error(logger, &e, call);
                        Err(e)
                    }
                }
            }
        });
        resps.extend(try_join_all(futs).await?);

        // If we make it here, we have a response for every call.
        debug_assert_eq!(resps.len(), calls.len());

        // Bring the responses into the same order as the calls
        resps.sort_by_key(|resp| resp.req.index);

        let decoded: Vec<_> = resps
            .into_iter()
            .map(|res| {
                let call = &calls[res.req.index as usize];
                decode(logger, res, call)
            })
            .collect();

        Ok(decoded)
    }

    /// Load Ethereum blocks in bulk, returning results as they come back as a Stream.
    async fn load_blocks(
        &self,
        logger: Logger,
        chain_store: Arc<dyn ChainStore>,
        block_hashes: HashSet<B256>,
    ) -> Result<Vec<Arc<LightEthereumBlock>>, Error> {
        let block_hashes: Vec<_> = block_hashes.iter().cloned().collect();
        // Search for the block in the store first then use json-rpc as a backup.
        let mut blocks: Vec<_> = chain_store
            .cheap_clone()
            .blocks(block_hashes.iter().map(|&b| b.into()).collect::<Vec<_>>())
            .await
            .map_err(|e| error!(&logger, "Error accessing block cache {}", e))
            .unwrap_or_default()
            .into_iter()
            .filter_map(|value| {
                let json_block = EthereumJsonBlock::new(value);
                if json_block.is_shallow() {
                    return None;
                }
                json_block
                    .into_light_block()
                    .map_err(|e| {
                        warn!(
                            &logger,
                            "Failed to deserialize cached block: {}. Block will be re-fetched from RPC.",
                            e
                        );
                    })
                    .ok()
            })
            .map(Arc::new)
            .collect();

        let missing_blocks = Vec::from_iter(
            block_hashes
                .into_iter()
                .filter(|hash| !blocks.iter().any(|b| b.hash() == *hash)),
        );

        // Return a stream that lazily loads batches of blocks.
        debug!(logger, "Requesting {} block(s)", missing_blocks.len());
        let new_blocks: Vec<_> = self
            .load_blocks_rpc(logger.clone(), missing_blocks)
            .try_collect()
            .await?;
        let upsert_blocks: Vec<_> = new_blocks
            .iter()
            .map(|block| BlockFinality::Final(block.clone()))
            .collect();
        let block_refs: Vec<_> = upsert_blocks
            .iter()
            .map(|block| block as &dyn graph::blockchain::Block)
            .collect();
        if let Err(e) = chain_store.upsert_light_blocks(block_refs.as_slice()).await {
            error!(logger, "Error writing to block cache {}", e);
        }
        blocks.extend(new_blocks);
        blocks.sort_by_key(|block| block.number());
        Ok(blocks)
    }
}

/// Returns blocks with triggers, corresponding to the specified range and filters; and the resolved
/// `to` block, which is the nearest non-null block greater than or equal to the passed `to` block.
/// If a block contains no triggers, there may be no corresponding item in the stream.
/// However the (resolved) `to` block will always be present, even if triggers are empty.
///
/// Careful: don't use this function without considering race conditions.
/// Chain reorgs could happen at any time, and could affect the answer received.
/// Generally, it is only safe to use this function with blocks that have received enough
/// confirmations to guarantee no further reorgs, **and** where the Ethereum node is aware of
/// those confirmations.
/// If the Ethereum node is far behind in processing blocks, even old blocks can be subject to
/// reorgs.
/// It is recommended that `to` be far behind the block number of latest block the Ethereum
/// node is aware of.
pub(crate) async fn blocks_with_triggers(
    adapter: Arc<EthereumAdapter>,
    logger: Logger,
    chain_store: Arc<dyn ChainStore>,
    subgraph_metrics: Arc<SubgraphEthRpcMetrics>,
    from: BlockNumber,
    to: BlockNumber,
    filter: &TriggerFilter,
    unified_api_version: UnifiedMappingApiVersion,
) -> Result<(Vec<BlockWithTriggers<crate::Chain>>, BlockNumber), Error> {
    // Each trigger filter needs to be queried for the same block range
    // and the blocks yielded need to be deduped. If any error occurs
    // while searching for a trigger type, the entire operation fails.
    let eth = adapter.clone();
    let call_filter = EthereumCallFilter::from(&filter.block);

    // Scan the block range to find relevant triggers
    let trigger_futs: FuturesUnordered<BoxFuture<Result<Vec<EthereumTrigger>, anyhow::Error>>> =
        FuturesUnordered::new();

    // Resolve the nearest non-null "to" block
    debug!(logger, "Finding nearest valid `to` block to {}", to);

    let to_ptr = eth.next_existing_ptr_to_number(&logger, to).await?;
    let to_hash = to_ptr.hash.as_b256();
    let to = to_ptr.block_number();

    // This is for `start` triggers which can be initialization handlers which needs to be run
    // before all other triggers
    if filter.block.trigger_every_block {
        let block_future = eth
            .block_range_to_ptrs(logger.clone(), from, to)
            .map(move |ptrs| {
                ptrs.into_iter()
                    .flat_map(|ptr| {
                        vec![
                            EthereumTrigger::Block(ptr.clone(), EthereumBlockTriggerType::Start),
                            EthereumTrigger::Block(ptr, EthereumBlockTriggerType::End),
                        ]
                    })
                    .collect()
            })
            .compat()
            .boxed();
        trigger_futs.push(block_future)
    } else if !filter.block.polling_intervals.is_empty() {
        let block_futures_matching_once_filter =
            eth.blocks_matching_polling_intervals(logger.clone(), from, to, &filter.block);
        trigger_futs.push(block_futures_matching_once_filter);
    }

    // Scan for Logs
    if !filter.log.is_empty() {
        let logs_future = get_logs_and_transactions(
            &eth,
            &logger,
            subgraph_metrics.clone(),
            from,
            to,
            filter.log.clone(),
            &unified_api_version,
        )
        .boxed();
        trigger_futs.push(logs_future)
    }
    // Scan for Calls
    if !filter.call.is_empty() {
        let calls_future = eth
            .calls_in_block_range(&logger, subgraph_metrics.clone(), from, to, &filter.call)
            .map(Arc::new)
            .map(EthereumTrigger::Call)
            .collect()
            .compat()
            .boxed();
        trigger_futs.push(calls_future)
    }

    if !filter.block.contract_addresses.is_empty() {
        // To determine which blocks include a call to addresses
        // in the block filter, transform the `block_filter` into
        // a `call_filter` and run `blocks_with_calls`
        let block_future = eth
            .calls_in_block_range(&logger, subgraph_metrics.clone(), from, to, &call_filter)
            .map(|call| {
                EthereumTrigger::Block(
                    BlockPtr::from(&call),
                    EthereumBlockTriggerType::WithCallTo(call.to),
                )
            })
            .collect()
            .compat()
            .boxed();
        trigger_futs.push(block_future)
    }

    // Join on triggers, unpack and handle possible errors
    let triggers = trigger_futs
        .try_concat()
        .await
        .with_context(|| format!("Failed to obtain triggers for block {}", to))?;

    let mut block_hashes: HashSet<B256> = triggers
        .iter()
        .map(|trigger| trigger.block_hash())
        .collect();
    let mut triggers_by_block: HashMap<BlockNumber, Vec<EthereumTrigger>> =
        triggers.into_iter().fold(HashMap::new(), |mut map, t| {
            map.entry(t.block_number()).or_default().push(t);
            map
        });

    debug!(logger, "Found {} relevant block(s)", block_hashes.len());

    // Make sure `to` is included, even if empty.
    block_hashes.insert(to_hash);
    triggers_by_block.entry(to).or_default();

    let logger2 = logger.cheap_clone();

    let blocks: Vec<_> = eth
        .load_blocks(logger.cheap_clone(), chain_store.clone(), block_hashes)
        .await?
        .into_iter()
        .map(
            move |block| match triggers_by_block.remove(&(block.number())) {
                Some(triggers) => Ok(BlockWithTriggers::new(
                    BlockFinality::Final(block),
                    triggers,
                    &logger2,
                )),
                None => Err(anyhow!(
                    "block {} not found in `triggers_by_block`",
                    block.block_ptr()
                )),
            },
        )
        .collect::<Result<_, _>>()?;

    // Filter out call triggers that come from unsuccessful transactions
    let futures = blocks.into_iter().map(|block| {
        filter_call_triggers_from_unsuccessful_transactions(block, &eth, &chain_store, &logger)
    });
    let mut blocks = futures03::future::try_join_all(futures).await?;

    blocks.sort_by_key(|block| block.ptr().number);

    // Sanity check that the returned blocks are in the correct range.
    // Unwrap: `blocks` always includes at least `to`.
    let first = blocks.first().unwrap().ptr().number;
    let last = blocks.last().unwrap().ptr().number;
    if first < from {
        return Err(anyhow!(
            "block {} returned by the Ethereum node is before {}, the first block of the requested range",
            first,
            from,
        ));
    }
    if last > to {
        return Err(anyhow!(
            "block {} returned by the Ethereum node is after {}, the last block of the requested range",
            last,
            to,
        ));
    }

    Ok((blocks, to))
}

pub(crate) async fn get_calls(
    client: &Arc<ChainClient<Chain>>,
    logger: Logger,
    subgraph_metrics: Arc<SubgraphEthRpcMetrics>,
    capabilities: &NodeCapabilities,
    requires_traces: bool,
    block: BlockFinality,
) -> Result<BlockFinality, Error> {
    // For final blocks, or nonfinal blocks where we already checked
    // (`calls.is_some()`), do nothing; if we haven't checked for calls, do
    // that now
    match block {
        BlockFinality::Final(_)
        | BlockFinality::NonFinal(EthereumBlockWithCalls {
            ethereum_block: _,
            calls: Some(_),
        }) => Ok(block),
        BlockFinality::NonFinal(EthereumBlockWithCalls {
            ethereum_block,
            calls: None,
        }) => {
            let calls = if !requires_traces || ethereum_block.transaction_receipts.is_empty() {
                vec![]
            } else {
                client
                    .rpc()?
                    .cheapest_with(capabilities)
                    .await?
                    .calls_in_block(
                        &logger,
                        subgraph_metrics.clone(),
                        ethereum_block.block.number(),
                        ethereum_block.block.hash(),
                    )
                    .await?
            };
            Ok(BlockFinality::NonFinal(EthereumBlockWithCalls {
                ethereum_block,
                calls: Some(calls),
            }))
        }
        BlockFinality::Ptr(_) => {
            unreachable!("get_calls called with BlockFinality::Ptr")
        }
    }
}

pub(crate) fn parse_log_triggers(
    log_filter: &EthereumLogFilter,
    block: &EthereumBlock,
) -> Vec<EthereumTrigger> {
    if log_filter.is_empty() {
        return vec![];
    }

    block
        .transaction_receipts
        .iter()
        .flat_map(move |receipt| {
            receipt.logs().iter().enumerate().map(move |(index, log)| {
                let requires_transaction_receipt = log
                    .topics()
                    .first()
                    .map(|signature| {
                        log_filter.requires_transaction_receipt(
                            signature,
                            Some(&log.address()),
                            log.topics(),
                        )
                    })
                    .unwrap_or(false);

                EthereumTrigger::Log(LogRef::LogPosition(LogPosition {
                    index,
                    receipt: receipt.cheap_clone(),
                    requires_transaction_receipt,
                }))
            })
        })
        .collect()
}

pub(crate) fn parse_call_triggers(
    call_filter: &EthereumCallFilter,
    block: &EthereumBlockWithCalls,
) -> anyhow::Result<Vec<EthereumTrigger>> {
    if call_filter.is_empty() {
        return Ok(vec![]);
    }

    match &block.calls {
        Some(calls) => calls
            .iter()
            .filter(move |call| call_filter.matches(call))
            .map(
                move |call| match block.transaction_for_call_succeeded(call) {
                    Ok(true) => Ok(Some(EthereumTrigger::Call(Arc::new(call.clone())))),
                    Ok(false) => Ok(None),
                    Err(e) => Err(e),
                },
            )
            .filter_map_ok(|some_trigger| some_trigger)
            .collect(),
        None => Ok(vec![]),
    }
}

/// This method does not parse block triggers with `once` filters.
/// This is because it is to be run before any other triggers are run.
/// So we have `parse_initialization_triggers` for that.
pub(crate) fn parse_block_triggers(
    block_filter: &EthereumBlockFilter,
    block: &EthereumBlockWithCalls,
) -> Vec<EthereumTrigger> {
    if block_filter.is_empty() {
        return vec![];
    }

    let block_ptr = block.ethereum_block.block.block_ptr();
    let trigger_every_block = block_filter.trigger_every_block;
    let call_filter = EthereumCallFilter::from(block_filter);
    let block_ptr2 = block_ptr.cheap_clone();
    let block_ptr3 = block_ptr.cheap_clone();
    let block_number = block_ptr.number;

    let mut triggers = match &block.calls {
        Some(calls) => calls
            .iter()
            .filter(move |call| call_filter.matches(call))
            .map(move |call| {
                EthereumTrigger::Block(
                    block_ptr2.clone(),
                    EthereumBlockTriggerType::WithCallTo(call.to),
                )
            })
            .collect::<Vec<EthereumTrigger>>(),
        None => vec![],
    };
    if trigger_every_block {
        triggers.push(EthereumTrigger::Block(
            block_ptr.clone(),
            EthereumBlockTriggerType::Start,
        ));
        triggers.push(EthereumTrigger::Block(
            block_ptr,
            EthereumBlockTriggerType::End,
        ));
    } else if !block_filter.polling_intervals.is_empty() {
        let has_polling_trigger =
            &block_filter
                .polling_intervals
                .iter()
                .any(|(start_block, interval)| match interval {
                    0 => false,
                    _ => {
                        block_number >= *start_block
                            && (block_number - *start_block) % *interval == 0
                    }
                });

        let has_once_trigger =
            &block_filter
                .polling_intervals
                .iter()
                .any(|(start_block, interval)| match interval {
                    0 => block_number == *start_block,
                    _ => false,
                });

        if *has_once_trigger {
            triggers.push(EthereumTrigger::Block(
                block_ptr3.clone(),
                EthereumBlockTriggerType::Start,
            ));
        }

        if *has_polling_trigger {
            triggers.push(EthereumTrigger::Block(
                block_ptr3,
                EthereumBlockTriggerType::End,
            ));
        }
    }
    triggers
}

async fn fetch_receipt_from_ethereum_client(
    eth: &EthereumAdapter,
    transaction_hash: B256,
) -> anyhow::Result<alloy::network::AnyTransactionReceipt> {
    match eth.alloy.get_transaction_receipt(transaction_hash).await {
        Ok(Some(receipt)) => Ok(receipt),
        Ok(None) => bail!("Could not find transaction receipt"),
        Err(error) => bail!("Failed to fetch transaction receipt: {}", error),
    }
}

async fn filter_call_triggers_from_unsuccessful_transactions(
    mut block: BlockWithTriggers<crate::Chain>,
    eth: &EthereumAdapter,
    chain_store: &Arc<dyn ChainStore>,
    logger: &Logger,
) -> anyhow::Result<BlockWithTriggers<crate::Chain>> {
    // Return early if there is no trigger data
    if block.trigger_data.is_empty() {
        return Ok(block);
    }

    let initial_number_of_triggers = block.trigger_data.len();

    // Get the transaction hash from each call trigger
    let transaction_hashes: BTreeSet<B256> = block
        .trigger_data
        .iter()
        .filter_map(|trigger| match trigger.as_chain() {
            Some(EthereumTrigger::Call(call_trigger)) => Some(call_trigger.transaction_hash),
            _ => None,
        })
        .collect::<Option<BTreeSet<B256>>>()
        .ok_or(anyhow!(
            "failed to obtain transaction hash from call triggers"
        ))?;

    // Return early if there are no transaction hashes
    if transaction_hashes.is_empty() {
        return Ok(block);
    }

    // And obtain all Transaction values for the calls in this block.
    let transactions: Vec<&AnyTransaction> = {
        match &block.block {
            BlockFinality::Final(ref block) => block
                .transactions()
                .ok_or_else(|| anyhow!("Block transactions not available"))?
                .iter()
                .filter(|transaction| transaction_hashes.contains(&transaction.tx_hash()))
                .collect(),
            BlockFinality::NonFinal(_block_with_calls) => {
                unreachable!(
                    "this function should not be called when dealing with non-final blocks"
                )
            }
            BlockFinality::Ptr(_block) => {
                unreachable!(
                    "this function should not be called when dealing with header-only blocks"
                )
            }
        }
    };

    // Confidence check: Did we collect all transactions for the current call triggers?
    if transactions.len() != transaction_hashes.len() {
        bail!("failed to find transactions in block for the given call triggers")
    }

    // We'll also need the receipts for those transactions. In this step we collect all receipts
    // we have in store for the current block.
    let mut receipts: BTreeMap<B256, LightTransactionReceipt> = chain_store
        .transaction_receipts_in_block(&block.ptr().hash.as_b256())
        .await?
        .into_iter()
        .map(|receipt| (receipt.transaction_hash, receipt))
        .collect::<BTreeMap<B256, LightTransactionReceipt>>();

    // Do we have a receipt for each transaction under analysis?
    let mut receipts_and_transactions: Vec<(&AnyTransaction, LightTransactionReceipt)> = Vec::new();
    let mut transactions_without_receipt: Vec<&AnyTransaction> = Vec::new();
    for transaction in transactions.iter() {
        if let Some(receipt) = receipts.remove(&transaction.tx_hash()) {
            receipts_and_transactions.push((*transaction, receipt));
        } else {
            transactions_without_receipt.push(*transaction);
        }
    }

    // When some receipts are missing, we then try to fetch them from our client.
    let futures = transactions_without_receipt
        .iter()
        .map(|transaction| async move {
            fetch_receipt_from_ethereum_client(eth, transaction.tx_hash())
                .await
                .map(|receipt| (transaction, receipt))
        });
    futures03::future::try_join_all(futures)
        .await?
        .into_iter()
        .for_each(|(transaction, receipt)| {
            receipts_and_transactions.push((transaction, receipt.into()))
        });

    // TODO: We should persist those fresh transaction receipts into the store, so we don't incur
    // additional Ethereum API calls for future scans on this block.

    // With all transactions and receipts in hand, we can evaluate the success of each transaction
    let mut transaction_success: BTreeMap<B256, bool> = BTreeMap::new();
    for (transaction, receipt) in receipts_and_transactions.into_iter() {
        transaction_success.insert(transaction.tx_hash(), receipt.status);
    }

    // Confidence check: Did we inspect the status of all transactions?
    if !transaction_hashes
        .iter()
        .all(|tx| transaction_success.contains_key(tx))
    {
        bail!("Not all transactions status were inspected")
    }

    // Filter call triggers from unsuccessful transactions
    block.trigger_data.retain(|trigger| {
        if let Some(EthereumTrigger::Call(call_trigger)) = trigger.as_chain() {
            // Unwrap: We already checked that those values exist
            transaction_success[&call_trigger.transaction_hash.unwrap()]
        } else {
            // We are not filtering other types of triggers
            true
        }
    });

    // Log if any call trigger was filtered out
    let final_number_of_triggers = block.trigger_data.len();
    let number_of_filtered_triggers = initial_number_of_triggers - final_number_of_triggers;
    if number_of_filtered_triggers != 0 {
        let noun = {
            if number_of_filtered_triggers == 1 {
                "call trigger"
            } else {
                "call triggers"
            }
        };
        info!(&logger,
              "Filtered {} {} from failed transactions", number_of_filtered_triggers, noun ;
              "block_number" => block.ptr().block_number());
    }
    Ok(block)
}

/// Deprecated. Wraps the [`fetch_transaction_receipts_in_batch`] in a retry loop.
async fn fetch_transaction_receipts_in_batch_with_retry(
    alloy: Arc<AlloyProvider>,
    hashes: Vec<B256>,
    block_hash: B256,
    logger: Logger,
) -> Result<Vec<Arc<alloy::network::AnyTransactionReceipt>>, IngestorError> {
    let retry_log_message = format!(
        "batch eth_getTransactionReceipt RPC call for block {:?}",
        block_hash
    );
    retry(retry_log_message, &logger)
        .redact_log_urls(true)
        .limit(ENV_VARS.request_retries)
        .no_logging()
        .timeout_secs(ENV_VARS.json_rpc_timeout.as_secs())
        .run(move || {
            let alloy = alloy.cheap_clone();
            let hashes = hashes.clone();
            let logger = logger.cheap_clone();
            fetch_transaction_receipts_in_batch(alloy, hashes, block_hash, logger).boxed()
        })
        .await
        .map_err(|_timeout| anyhow!(block_hash).into())
}

/// Deprecated. Attempts to fetch multiple transaction receipts in a batching context.
async fn fetch_transaction_receipts_in_batch(
    alloy: Arc<AlloyProvider>,
    hashes: Vec<B256>,
    block_hash: B256,
    logger: Logger,
) -> Result<Vec<Arc<alloy::network::AnyTransactionReceipt>>, IngestorError> {
    // Use the batch method to get all receipts at once
    let receipts = batch_get_transaction_receipts(alloy, hashes.clone())
        .await
        .map_err(|e| {
            IngestorError::Unknown(anyhow::anyhow!("Batch receipt fetch failed: {}", e))
        })?;

    let mut result = Vec::new();
    for (receipt, hash) in receipts.into_iter().zip(hashes.iter()) {
        if let Some(receipt) = receipt {
            let validated_receipt = resolve_transaction_receipt(
                Some(receipt),
                *hash,
                block_hash,
                logger.cheap_clone(),
            )?;
            result.push(Arc::new(validated_receipt));
        } else {
            return Err(IngestorError::ReceiptUnavailable(block_hash, *hash));
        }
    }

    Ok(result)
}

async fn batch_get_transaction_receipts(
    provider: Arc<AlloyProvider>,
    tx_hashes: Vec<B256>,
) -> Result<Vec<Option<alloy::network::AnyTransactionReceipt>>, Box<dyn std::error::Error>> {
    let mut batch = alloy::rpc::client::BatchRequest::new(provider.client());
    let mut receipt_futures = Vec::new();

    // Add all receipt requests to batch
    for tx_hash in &tx_hashes {
        let receipt_future = batch
            .add_call::<(B256,), Option<alloy::network::AnyTransactionReceipt>>(
                "eth_getTransactionReceipt",
                &(*tx_hash,),
            )?;
        receipt_futures.push(receipt_future);
    }

    // Execute batch
    batch.send().await?;

    // Collect results in order
    let mut results = Vec::new();
    for receipt_future in receipt_futures {
        let receipt = receipt_future.await?;
        results.push(receipt);
    }

    Ok(results)
}

pub(crate) async fn check_block_receipt_support(
    alloy: Arc<AlloyProvider>,
    block_hash: B256,
    supports_eip_1898: bool,
    call_only: bool,
) -> Result<(), Error> {
    use alloy::rpc::types::BlockId;
    if call_only {
        return Err(anyhow!("Provider is call-only"));
    }

    if !supports_eip_1898 {
        return Err(anyhow!("Provider does not support EIP 1898"));
    }

    // Fetch block receipts from the provider for the latest block.
    let block_receipts_result = alloy.get_block_receipts(BlockId::from(block_hash)).await;

    // Determine if the provider supports block receipts based on the fetched result.
    match block_receipts_result {
        Ok(Some(receipts)) if !receipts.is_empty() => Ok(()),
        Ok(_) => Err(anyhow!("Block receipts are empty")),
        Err(err) => Err(anyhow!("Error fetching block receipts: {}", err)),
    }
}

// Fetches transaction receipts with retries. This function acts as a dispatcher
// based on whether block receipts are supported or individual transaction receipts
// need to be fetched.
async fn fetch_receipts_with_retry(
    alloy: Arc<AlloyProvider>,
    hashes: Vec<B256>,
    block_hash: B256,
    logger: Logger,
    supports_block_receipts: bool,
) -> Result<Vec<Arc<alloy::network::AnyTransactionReceipt>>, IngestorError> {
    if supports_block_receipts {
        return fetch_block_receipts_with_retry(alloy, hashes, block_hash, logger).await;
    }
    fetch_individual_receipts_with_retry(alloy, hashes, block_hash, logger).await
}

// Fetches receipts for each transaction in the block individually.
async fn fetch_individual_receipts_with_retry(
    alloy: Arc<AlloyProvider>,
    hashes: Vec<B256>,
    block_hash: B256,
    logger: Logger,
) -> Result<Vec<Arc<alloy::network::AnyTransactionReceipt>>, IngestorError> {
    if ENV_VARS.fetch_receipts_in_batches {
        return fetch_transaction_receipts_in_batch_with_retry(alloy, hashes, block_hash, logger)
            .await;
    }

    // Use a stream to fetch receipts individually
    let hash_stream = tokio_stream::iter(hashes);
    let receipt_stream = hash_stream
        .map(move |tx_hash| {
            fetch_transaction_receipt_with_retry(
                alloy.cheap_clone(),
                tx_hash,
                block_hash,
                logger.cheap_clone(),
            )
        })
        .buffered(ENV_VARS.block_ingestor_max_concurrent_json_rpc_calls);

    tokio_stream::StreamExt::collect::<
        Result<Vec<Arc<alloy::network::AnyTransactionReceipt>>, IngestorError>,
    >(receipt_stream)
    .await
}

/// Fetches transaction receipts of all transactions in a block with `eth_getBlockReceipts` call.
async fn fetch_block_receipts_with_retry(
    alloy: Arc<AlloyProvider>,
    hashes: Vec<B256>,
    block_hash: B256,
    logger: Logger,
) -> Result<Vec<Arc<alloy::network::AnyTransactionReceipt>>, IngestorError> {
    use graph::prelude::alloy::rpc::types::BlockId;
    let logger = logger.cheap_clone();
    let retry_log_message = format!("eth_getBlockReceipts RPC call for block {:?}", block_hash);

    // Perform the retry operation
    let receipts_option = retry(retry_log_message, &logger)
        .redact_log_urls(true)
        .limit(ENV_VARS.request_retries)
        .timeout_secs(ENV_VARS.json_rpc_timeout.as_secs())
        .run(move || alloy.get_block_receipts(BlockId::from(block_hash)).boxed())
        .await
        .map_err(|_timeout| -> IngestorError { anyhow!(block_hash).into() })?;

    // Check if receipts are available, and transform them if they are
    match receipts_option {
        Some(receipts) => {
            // Create a HashSet from the transaction hashes of the receipts
            let receipt_hashes_set: HashSet<_> =
                receipts.iter().map(|r| r.transaction_hash).collect();

            // Check if the set contains all the hashes and has the same length as the hashes vec
            if hashes.len() == receipt_hashes_set.len()
                && hashes.iter().all(|hash| receipt_hashes_set.contains(hash))
            {
                let transformed_receipts = receipts.into_iter().map(Arc::new).collect();
                Ok(transformed_receipts)
            } else {
                // If there's a mismatch in numbers or a missing hash, return an error
                Err(IngestorError::BlockReceiptsMismatched(block_hash))
            }
        }
        None => {
            // If no receipts are found, return an error
            Err(IngestorError::BlockReceiptsUnavailable(block_hash))
        }
    }
}

/// Retries fetching a single transaction receipt using alloy, then converts to web3 format.
async fn fetch_transaction_receipt_with_retry(
    alloy: Arc<AlloyProvider>,
    transaction_hash: B256,
    block_hash: B256,
    logger: Logger,
) -> Result<Arc<alloy::network::AnyTransactionReceipt>, IngestorError> {
    let logger = logger.cheap_clone();
    let retry_log_message = format!(
        "eth_getTransactionReceipt RPC call for transaction {:?}",
        transaction_hash
    );

    retry(retry_log_message, &logger)
        .redact_log_urls(true)
        .limit(ENV_VARS.request_retries)
        .timeout_secs(ENV_VARS.json_rpc_timeout.as_secs())
        .run(move || {
            let alloy_clone = alloy.clone();
            async move { alloy_clone.get_transaction_receipt(transaction_hash).await }.boxed()
        })
        .await
        .map_err(|_timeout| anyhow!(block_hash).into())
        .and_then(move |some_receipt| {
            resolve_transaction_receipt(some_receipt, transaction_hash, block_hash, logger)
        })
        .map(Arc::new)
}

fn resolve_transaction_receipt(
    transaction_receipt: Option<alloy::network::AnyTransactionReceipt>,
    transaction_hash: B256,
    block_hash: B256,
    logger: Logger,
) -> Result<alloy::network::AnyTransactionReceipt, IngestorError> {
    match transaction_receipt {
        // A receipt might be missing because the block was uncled, and the transaction never
        // made it back into the main chain.
        Some(receipt) => {
            // Check if the receipt has a block hash and is for the right block. Parity nodes seem
            // to return receipts with no block hash when a transaction is no longer in the main
            // chain, so treat that case the same as a receipt being absent entirely.
            //
            // Also as a sanity check against provider nonsense, check that the receipt transaction
            // hash and the requested transaction hash match.
            if receipt.block_hash != Some(block_hash)
                || transaction_hash != receipt.transaction_hash
            {
                info!(
                    logger, "receipt block mismatch";
                    "receipt_block_hash" =>
                    receipt.block_hash.unwrap_or_default().to_string(),
                    "block_hash" =>
                        block_hash.to_string(),
                    "tx_hash" => transaction_hash.to_string(),
                    "receipt_tx_hash" => receipt.transaction_hash.to_string(),
                );

                // If the receipt came from a different block, then the Ethereum node no longer
                // considers this block to be in the main chain. Nothing we can do from here except
                // give up trying to ingest this block. There is no way to get the transaction
                // receipt from this block.
                Err(IngestorError::BlockUnavailable(block_hash))
            } else {
                Ok(receipt)
            }
        }
        None => {
            // No receipt was returned.
            //
            // This can be because the Ethereum node no longer considers this block to be part of
            // the main chain, and so the transaction is no longer in the main chain. Nothing we can
            // do from here except give up trying to ingest this block.
            //
            // This could also be because the receipt is simply not available yet. For that case, we
            // should retry until it becomes available.
            Err(IngestorError::ReceiptUnavailable(
                block_hash,
                transaction_hash,
            ))
        }
    }
}

/// Retrieves logs and the associated transaction receipts, if required by the [`EthereumLogFilter`].
async fn get_logs_and_transactions(
    adapter: &Arc<EthereumAdapter>,
    logger: &Logger,
    subgraph_metrics: Arc<SubgraphEthRpcMetrics>,
    from: BlockNumber,
    to: BlockNumber,
    log_filter: EthereumLogFilter,
    unified_api_version: &UnifiedMappingApiVersion,
) -> Result<Vec<EthereumTrigger>, anyhow::Error> {
    // Obtain logs externally
    let logs = adapter
        .logs_in_block_range(
            logger,
            subgraph_metrics.cheap_clone(),
            from,
            to,
            log_filter.clone(),
        )
        .await?;

    // Not all logs have associated transaction hashes, nor do all triggers require them.
    // We also restrict receipts retrieval for some api versions.
    let transaction_hashes_by_block: HashMap<B256, HashSet<B256>> = logs
        .iter()
        .filter(|_| unified_api_version.equal_or_greater_than(&API_VERSION_0_0_7))
        .filter(|log| {
            if let Some(signature) = log.topics().first() {
                log_filter.requires_transaction_receipt(
                    signature,
                    Some(&log.address()),
                    log.topics(),
                )
            } else {
                false
            }
        })
        .filter_map(|log| {
            if let (Some(block), Some(txn)) = (log.block_hash, log.transaction_hash) {
                Some((block, txn))
            } else {
                // Absent block and transaction data might happen for pending transactions, which we
                // don't handle.
                None
            }
        })
        .fold(
            HashMap::<B256, HashSet<B256>>::new(),
            |mut acc, (block_hash, txn_hash)| {
                acc.entry(block_hash).or_default().insert(txn_hash);
                acc
            },
        );

    // Obtain receipts externally
    let transaction_receipts_by_hash = get_transaction_receipts_for_transaction_hashes(
        adapter,
        &transaction_hashes_by_block,
        subgraph_metrics,
        logger.cheap_clone(),
    )
    .await?;

    // Associate each log with its receipt, when possible
    let mut log_triggers = Vec::new();
    for log in logs.into_iter() {
        let optional_receipt = log
            .transaction_hash
            .and_then(|txn| transaction_receipts_by_hash.get(&txn).cloned());

        let value = EthereumTrigger::Log(LogRef::FullLog(Arc::new(log), optional_receipt));
        log_triggers.push(value);
    }

    Ok(log_triggers)
}

/// Tries to retrive all transaction receipts for a set of transaction hashes.
async fn get_transaction_receipts_for_transaction_hashes(
    adapter: &EthereumAdapter,
    transaction_hashes_by_block: &HashMap<B256, HashSet<B256>>,
    subgraph_metrics: Arc<SubgraphEthRpcMetrics>,
    logger: Logger,
) -> Result<HashMap<B256, Arc<alloy::network::AnyTransactionReceipt>>, anyhow::Error> {
    use std::collections::hash_map::Entry::Vacant;

    let mut receipts_by_hash: HashMap<B256, Arc<alloy::network::AnyTransactionReceipt>> =
        HashMap::new();

    // Return early if input set is empty
    if transaction_hashes_by_block.is_empty() {
        return Ok(receipts_by_hash);
    }

    // Keep a record of all unique transaction hashes for which we'll request receipts. We will
    // later use this to check if we have collected the receipts from all required transactions.
    let mut unique_transaction_hashes: HashSet<&B256> = HashSet::new();

    // Request transaction receipts concurrently
    let receipt_futures = FuturesUnordered::new();

    let alloy = Arc::clone(&adapter.alloy);
    for (block_hash, transaction_hashes) in transaction_hashes_by_block {
        for transaction_hash in transaction_hashes {
            unique_transaction_hashes.insert(transaction_hash);
            let receipt_future = fetch_transaction_receipt_with_retry(
                alloy.cheap_clone(),
                *transaction_hash,
                *block_hash,
                logger.cheap_clone(),
            );
            receipt_futures.push(receipt_future)
        }
    }

    // Execute futures while monitoring elapsed time
    let start = Instant::now();
    let receipts: Vec<_> = match receipt_futures.try_collect().await {
        Ok(receipts) => {
            let elapsed = start.elapsed().as_secs_f64();
            subgraph_metrics.observe_request(
                elapsed,
                "eth_getTransactionReceipt",
                &adapter.provider,
            );
            receipts
        }
        Err(ingestor_error) => {
            subgraph_metrics.add_error("eth_getTransactionReceipt", &adapter.provider);
            debug!(
                logger,
                "Error querying transaction receipts: {}", ingestor_error
            );
            return Err(ingestor_error.into());
        }
    };

    // Build a map between transaction hashes and their receipts
    for receipt in receipts.into_iter() {
        if !unique_transaction_hashes.remove(&receipt.transaction_hash) {
            bail!("Received a receipt for a different transaction hash")
        }
        if let Vacant(entry) = receipts_by_hash.entry(receipt.transaction_hash) {
            entry.insert(receipt);
        } else {
            bail!("Received a duplicate transaction receipt")
        }
    }

    // Confidence check: all unique hashes should have been used
    ensure!(
        unique_transaction_hashes.is_empty(),
        "Didn't receive all necessary transaction receipts"
    );

    Ok(receipts_by_hash)
}

#[cfg(test)]
mod tests {

    use crate::trigger::{EthereumBlockTriggerType, EthereumTrigger};

    use super::{
        check_block_receipt_support, parse_block_triggers, EthereumBlock, EthereumBlockFilter,
        EthereumBlockWithCalls,
    };
    use graph::blockchain::BlockPtr;
    use graph::components::ethereum::AnyBlock;
    use graph::prelude::alloy::network::AnyNetwork;
    use graph::prelude::alloy::primitives::{Address, Bytes, B256};
    use graph::prelude::alloy::providers::mock::Asserter;
    use graph::prelude::alloy::providers::ProviderBuilder;
    use graph::prelude::{create_minimal_block_for_test, EthereumCall, LightEthereumBlock};
    use jsonrpc_core::serde_json::{self, Value};
    use std::collections::HashSet;
    use std::iter::FromIterator;
    use std::sync::Arc;

    #[test]
    fn parse_block_triggers_every_block() {
        let block = create_minimal_block_for_test(2, hash(2));

        let block = EthereumBlockWithCalls {
            ethereum_block: EthereumBlock {
                block: Arc::new(LightEthereumBlock::new(AnyBlock::from(block))),
                ..Default::default()
            },
            calls: Some(vec![EthereumCall {
                to: address(4),
                input: bytes(vec![1; 36]),
                ..Default::default()
            }]),
        };

        assert_eq!(
            vec![
                EthereumTrigger::Block(
                    BlockPtr::from((hash(2), 2)),
                    EthereumBlockTriggerType::Start
                ),
                EthereumTrigger::Block(BlockPtr::from((hash(2), 2)), EthereumBlockTriggerType::End)
            ],
            parse_block_triggers(
                &EthereumBlockFilter {
                    polling_intervals: HashSet::new(),
                    contract_addresses: HashSet::from_iter(vec![(10, address(1))]),
                    trigger_every_block: true,
                },
                &block
            ),
            "every block should generate a trigger even when address don't match"
        );
    }

    #[graph::test]
    async fn test_check_block_receipts_support() {
        let json_receipts = r#"[{
            "blockHash": "0x23f785604642e91613881fc3c9d16740ee416e340fd36f3fa2239f203d68fd33",
            "blockNumber": "0x12f7f81",
            "contractAddress": null,
            "cumulativeGasUsed": "0x26f66",
            "effectiveGasPrice": "0x140a1bd03",
            "from": "0x56fc0708725a65ebb633efdaec931c0600a9face",
            "gasUsed": "0x26f66",
            "logs": [],
            "logsBloom": "0x00000000010000000000000000000000000000000000000000000000040000000000000000000000000008000000000002000000080020000000040000000000000000000000000808000008000000000000000000040000000000000000000000000000000000000000000000000000000000000000000000000010000800000000000000000000000000000000000000000000010000000000000000000000000000000000200000000000000000000000000000000000002000000008000000000002000000000000000000000000000000000400000000000000000000000000200000000000000010000000000000000000000000000000000000000000",
            "status": "0x1",
            "to": "0x51c72848c68a965f66fa7a88855f9f7784502a7f",
            "transactionHash": "0xabfe9e82d71c843a91251fd1272b0dd80bc0b8d94661e3a42c7bb9e7f55789cf",
            "transactionIndex": "0x0",
            "type": "0x2"
        }]"#;

        let json_empty = r#"[]"#;

        // Helper function to run a single test case
        async fn run_test_case(
            json_response: &str,
            expected_err: Option<&str>,
            supports_eip_1898: bool,
            call_only: bool,
        ) -> Result<(), anyhow::Error> {
            let json_value: Value = serde_json::from_str(json_response).unwrap();

            let asserter = Asserter::new();
            let provider = ProviderBuilder::<_, _, AnyNetwork>::default()
                .network::<AnyNetwork>()
                .with_recommended_fillers()
                .connect_mocked_client(asserter.clone());

            asserter.push_success(&json_value);

            let result = check_block_receipt_support(
                Arc::new(provider),
                B256::ZERO,
                supports_eip_1898,
                call_only,
            )
            .await;

            match expected_err {
                Some(err_msg) => match result {
                    Ok(_) => panic!("Expected error but got Ok"),
                    Err(e) => {
                        assert!(e.to_string().contains(err_msg));
                    }
                },
                None => match result {
                    Ok(_) => (),
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        panic!("Unexpected error: {}", e);
                    }
                },
            }
            Ok(())
        }

        // Test case 1: Valid block receipts
        run_test_case(json_receipts, None, true, false)
            .await
            .unwrap();

        // Test case 2: Empty block receipts
        run_test_case(json_empty, Some("Block receipts are empty"), true, false)
            .await
            .unwrap();

        // Test case 3: Null response
        run_test_case("null", Some("Block receipts are empty"), true, false)
            .await
            .unwrap();

        // Test case 3: Simulating an RPC error
        // Note: In the context of this test, we cannot directly simulate an RPC error.
        // Instead, we simulate a response that would cause a decoding error, such as an unexpected key("error").
        // The function should handle this as an error case.
        run_test_case(
            r#"{"error":"RPC Error"}"#,
            Some("Error fetching block receipts:"),
            true,
            false,
        )
        .await
        .unwrap();

        // Test case 5: Does not support EIP-1898
        run_test_case(
            json_receipts,
            Some("Provider does not support EIP 1898"),
            false,
            false,
        )
        .await
        .unwrap();

        // Test case 5: Does not support Call only adapters
        run_test_case(json_receipts, Some("Provider is call-only"), true, true)
            .await
            .unwrap();
    }

    #[test]
    fn parse_block_triggers_specific_call_not_found() {
        let block = create_minimal_block_for_test(2, hash(2));

        #[allow(unreachable_code)]
        let block = EthereumBlockWithCalls {
            ethereum_block: EthereumBlock {
                block: Arc::new(LightEthereumBlock::new(AnyBlock::from(block))),
                ..Default::default()
            },
            calls: Some(vec![EthereumCall {
                to: address(4),
                input: bytes(vec![1; 36]),
                ..Default::default()
            }]),
        };

        assert_eq!(
            Vec::<EthereumTrigger>::new(),
            parse_block_triggers(
                &EthereumBlockFilter {
                    polling_intervals: HashSet::new(),
                    contract_addresses: HashSet::from_iter(vec![(1, address(1))]),
                    trigger_every_block: false,
                },
                &block
            ),
            "block filter specifies address 1 but block does not contain any call to it"
        );
    }

    #[test]
    fn parse_block_triggers_specific_call_found() {
        let block = create_minimal_block_for_test(2, hash(2));

        #[allow(unreachable_code)]
        let block = EthereumBlockWithCalls {
            ethereum_block: EthereumBlock {
                block: Arc::new(LightEthereumBlock::new(AnyBlock::from(block))),
                ..Default::default()
            },
            calls: Some(vec![EthereumCall {
                to: address(4),
                input: bytes(vec![1; 36]),
                ..Default::default()
            }]),
        };

        assert_eq!(
            vec![EthereumTrigger::Block(
                BlockPtr::from((hash(2), 2)),
                EthereumBlockTriggerType::WithCallTo(address(4))
            )],
            parse_block_triggers(
                &EthereumBlockFilter {
                    polling_intervals: HashSet::new(),
                    contract_addresses: HashSet::from_iter(vec![(1, address(4))]),
                    trigger_every_block: false,
                },
                &block
            ),
            "block filter specifies address 4 and block has call to it"
        );
    }

    fn address(id: u64) -> Address {
        Address::left_padding_from(&id.to_be_bytes())
    }

    fn hash(id: u8) -> B256 {
        B256::from_slice(&[id; 32])
    }

    fn bytes(value: Vec<u8>) -> Bytes {
        Bytes::from(value)
    }
}
