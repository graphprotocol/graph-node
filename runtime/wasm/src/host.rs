use std::cmp::PartialEq;
use std::str::FromStr;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use ethabi::{LogParam, RawLog};
use futures::sync::mpsc::Sender;
use futures03::channel::oneshot::channel;
use graph::{components::store::CallCache, ensure};
use semver::Version;
use slog::{o, OwnedKV};
use strum::AsStaticRef as _;

use graph::components::arweave::ArweaveAdapter;
use graph::components::ethereum::*;
use graph::components::store::SubgraphStore;
use graph::components::subgraph::{MappingError, SharedProofOfIndexing};
use graph::components::three_box::ThreeBoxAdapter;
use graph::prelude::{
    RuntimeHost as RuntimeHostTrait, RuntimeHostBuilder as RuntimeHostBuilderTrait, *,
};
use graph::util;
use web3::types::Log;

use crate::mapping::{MappingContext, MappingRequest, MappingTrigger};
use crate::{host_exports::HostExports, module::ExperimentalFeatures};

lazy_static! {
    static ref TIMEOUT: Option<Duration> = std::env::var("GRAPH_MAPPING_HANDLER_TIMEOUT")
        .ok()
        .map(|s| u64::from_str(&s).expect("Invalid value for GRAPH_MAPPING_HANDLER_TIMEOUT"))
        .map(Duration::from_secs);
    static ref ALLOW_NON_DETERMINISTIC_IPFS: bool =
        std::env::var("GRAPH_ALLOW_NON_DETERMINISTIC_IPFS").is_ok();
    static ref ALLOW_NON_DETERMINISTIC_3BOX: bool =
        std::env::var("GRAPH_ALLOW_NON_DETERMINISTIC_3BOX").is_ok();
    static ref ALLOW_NON_DETERMINISTIC_ARWEAVE: bool =
        std::env::var("GRAPH_ALLOW_NON_DETERMINISTIC_ARWEAVE").is_ok();
}

pub struct RuntimeHostBuilder<S, CC> {
    ethereum_networks: EthereumNetworks,
    link_resolver: Arc<dyn LinkResolver>,
    store: Arc<S>,
    caches: Arc<CC>,
    arweave_adapter: Arc<dyn ArweaveAdapter>,
    three_box_adapter: Arc<dyn ThreeBoxAdapter>,
}

impl<S, CC> Clone for RuntimeHostBuilder<S, CC>
where
    S: SubgraphStore,
    CC: CallCache,
{
    fn clone(&self) -> Self {
        RuntimeHostBuilder {
            ethereum_networks: self.ethereum_networks.clone(),
            link_resolver: self.link_resolver.clone(),
            store: self.store.clone(),
            caches: self.caches.clone(),
            arweave_adapter: self.arweave_adapter.cheap_clone(),
            three_box_adapter: self.three_box_adapter.cheap_clone(),
        }
    }
}

impl<S, CC> RuntimeHostBuilder<S, CC>
where
    S: SubgraphStore,
    CC: CallCache,
{
    pub fn new(
        ethereum_networks: EthereumNetworks,
        link_resolver: Arc<dyn LinkResolver>,
        store: Arc<S>,
        caches: Arc<CC>,
        arweave_adapter: Arc<dyn ArweaveAdapter>,
        three_box_adapter: Arc<dyn ThreeBoxAdapter>,
    ) -> Self {
        RuntimeHostBuilder {
            ethereum_networks,
            link_resolver,
            store,
            caches,
            arweave_adapter,
            three_box_adapter,
        }
    }
}

impl<S, CC> RuntimeHostBuilderTrait for RuntimeHostBuilder<S, CC>
where
    S: SubgraphStore,
    CC: CallCache,
{
    type Host = RuntimeHost;
    type Req = MappingRequest;

    fn spawn_mapping(
        raw_module: Vec<u8>,
        logger: Logger,
        subgraph_id: SubgraphDeploymentId,
        metrics: Arc<HostMetrics>,
    ) -> Result<Sender<Self::Req>, Error> {
        let experimental_features = ExperimentalFeatures {
            allow_non_deterministic_arweave: *ALLOW_NON_DETERMINISTIC_ARWEAVE,
            allow_non_deterministic_3box: *ALLOW_NON_DETERMINISTIC_3BOX,
            allow_non_deterministic_ipfs: *ALLOW_NON_DETERMINISTIC_IPFS,
        };
        crate::mapping::spawn_module(
            raw_module,
            logger,
            subgraph_id,
            metrics,
            tokio::runtime::Handle::current(),
            *TIMEOUT,
            experimental_features,
        )
    }

    fn build(
        &self,
        network_name: String,
        subgraph_id: SubgraphDeploymentId,
        data_source: DataSource,
        templates: Arc<Vec<DataSourceTemplate>>,
        mapping_request_sender: Sender<MappingRequest>,
        metrics: Arc<HostMetrics>,
    ) -> Result<Self::Host, Error> {
        let cache = self
            .caches
            .ethereum_call_cache(&network_name)
            .ok_or_else(|| {
                anyhow!(
                    "No store found that matches subgraph network: \"{}\"",
                    &network_name
                )
            })?;

        let required_capabilities = data_source.mapping.required_capabilities();

        let ethereum_adapter = self
            .ethereum_networks
            .adapter_with_capabilities(network_name.clone(), &required_capabilities)?;

        RuntimeHost::new(
            ethereum_adapter.clone(),
            self.link_resolver.clone(),
            self.store.clone(),
            cache,
            network_name,
            subgraph_id,
            data_source,
            templates,
            mapping_request_sender,
            metrics,
            self.arweave_adapter.cheap_clone(),
            self.three_box_adapter.cheap_clone(),
        )
    }
}

#[derive(Debug)]
pub struct RuntimeHost {
    data_source: DataSource,
    mapping_request_sender: Sender<MappingRequest>,
    host_exports: Arc<HostExports>,
    metrics: Arc<HostMetrics>,
}

impl RuntimeHost {
    fn new(
        ethereum_adapter: Arc<dyn EthereumAdapter>,
        link_resolver: Arc<dyn LinkResolver>,
        store: Arc<dyn crate::RuntimeStore>,
        call_cache: Arc<dyn EthereumCallCache>,
        network_name: String,
        subgraph_id: SubgraphDeploymentId,
        data_source: DataSource,
        templates: Arc<Vec<DataSourceTemplate>>,
        mapping_request_sender: Sender<MappingRequest>,
        metrics: Arc<HostMetrics>,
        arweave_adapter: Arc<dyn ArweaveAdapter>,
        three_box_adapter: Arc<dyn ThreeBoxAdapter>,
    ) -> Result<Self, Error> {
        let api_version = Version::parse(&data_source.mapping.api_version)?;

        // Create new instance of externally hosted functions invoker. The `Arc` is simply to avoid
        // implementing `Clone` for `HostExports`.
        let host_exports = Arc::new(HostExports::new(
            subgraph_id,
            api_version,
            data_source.name.clone(),
            data_source.source.address.clone(),
            network_name,
            data_source.context.cheap_clone(),
            templates,
            data_source.mapping.abis.clone(),
            ethereum_adapter,
            link_resolver,
            store,
            call_cache,
            arweave_adapter,
            three_box_adapter,
        ));

        Ok(RuntimeHost {
            data_source,
            mapping_request_sender,
            host_exports,
            metrics,
        })
    }

    /// Sends a MappingRequest to the thread which owns the host,
    /// and awaits the result.
    async fn send_mapping_request<T: slog::SendSyncRefUnwindSafeKV>(
        &self,
        logger: &Logger,
        extra: OwnedKV<T>,
        state: BlockState,
        handler: &str,
        trigger: MappingTrigger,
        block: &Arc<LightEthereumBlock>,
        proof_of_indexing: SharedProofOfIndexing,
    ) -> Result<BlockState, MappingError> {
        let trigger_type = trigger.as_static();
        debug!(
            logger, "Start processing Ethereum trigger";
            &extra,
            "trigger_type" => trigger_type,
            "handler" => handler,
            "data_source" => &self.data_source.name,
        );

        let (result_sender, result_receiver) = channel();
        let start_time = Instant::now();
        let metrics = self.metrics.clone();

        self.mapping_request_sender
            .clone()
            .send(MappingRequest {
                ctx: MappingContext {
                    logger: logger.cheap_clone(),
                    state,
                    host_exports: self.host_exports.cheap_clone(),
                    block: block.cheap_clone(),
                    proof_of_indexing,
                },
                trigger,
                result_sender,
            })
            .compat()
            .await
            .context("Mapping terminated before passing in trigger")?;

        let (result, send_time) = result_receiver
            .await
            .context("Mapping terminated before handling trigger")?;

        let elapsed = start_time.elapsed();
        metrics.observe_handler_execution_time(elapsed.as_secs_f64(), handler);

        info!(
            logger, "Done processing Ethereum trigger";
            extra,

            "trigger_type" => trigger_type,
            "total_ms" => elapsed.as_millis(),
            "handler" => handler,

            // How much time the result spent in the channel,
            // waiting in the tokio threadpool queue. Anything
            // larger than 0 is bad here. The `.wait()` is instant.
            "waiting_ms" => send_time
                .wait()
                .unwrap()
                .elapsed()
                .as_millis(),
        );

        result
    }
}

#[async_trait]
impl RuntimeHostTrait for RuntimeHost {
    fn matches_log(&self, log: &Log) -> bool {
        self.data_source.matches_log(log)
    }

    fn matches_call(&self, call: &EthereumCall) -> bool {
        self.data_source.matches_call(call)
    }

    fn matches_block(&self, trigger_ty: &EthereumBlockTriggerType, block_num: BlockNumber) -> bool {
        self.data_source.matches_block(trigger_ty, block_num)
    }

    async fn process_call(
        &self,
        logger: &Logger,
        block: &Arc<LightEthereumBlock>,
        call: &Arc<EthereumCall>,
        state: BlockState,
        proof_of_indexing: SharedProofOfIndexing,
    ) -> Result<BlockState, MappingError> {
        // Identify the call handler for this call
        let call_handler = self.data_source.handler_for_call(&call)?;

        // Identify the function ABI in the contract
        let function_abi = util::ethereum::contract_function_with_signature(
            &self.data_source.contract_abi.contract,
            call_handler.function.as_str(),
        )
        .with_context(|| {
            anyhow!(
                "Function with the signature \"{}\" not found in \
                    contract \"{}\" of data source \"{}\"",
                call_handler.function,
                self.data_source.contract_abi.name,
                self.data_source.name
            )
        })?;

        // Parse the inputs
        //
        // Take the input for the call, chop off the first 4 bytes, then call
        // `function.decode_input` to get a vector of `Token`s. Match the `Token`s
        // with the `Param`s in `function.inputs` to create a `Vec<LogParam>`.
        let tokens = function_abi
            .decode_input(&call.input.0[4..])
            .with_context(|| {
                format!(
                    "Generating function inputs for the call {:?} failed, raw input: {}",
                    &function_abi,
                    hex::encode(&call.input.0)
                )
            })?;

        ensure!(
            tokens.len() == function_abi.inputs.len(),
            "Number of arguments in call does not match \
                    number of inputs in function signature."
        );

        let inputs = tokens
            .into_iter()
            .enumerate()
            .map(|(i, token)| LogParam {
                name: function_abi.inputs[i].name.clone(),
                value: token,
            })
            .collect::<Vec<_>>();

        // Parse the outputs
        //
        // Take the output for the call, then call `function.decode_output` to
        // get a vector of `Token`s. Match the `Token`s with the `Param`s in
        // `function.outputs` to create a `Vec<LogParam>`.
        let tokens = function_abi
            .decode_output(&call.output.0)
            .with_context(|| {
                format!(
                    "Decoding function outputs for the call {:?} failed, raw output: {}",
                    &function_abi,
                    hex::encode(&call.output.0)
                )
            })?;

        ensure!(
            tokens.len() == function_abi.outputs.len(),
            "Number of parameters in the call output does not match \
                        number of outputs in the function signature."
        );

        let outputs = tokens
            .into_iter()
            .enumerate()
            .map(|(i, token)| LogParam {
                name: function_abi.outputs[i].name.clone(),
                value: token,
            })
            .collect::<Vec<_>>();

        let transaction = Arc::new(
            block
                .transaction_for_call(&call)
                .context("Found no transaction for call")?,
        );

        self.send_mapping_request(
            logger,
            o! {
                "function" => &call_handler.function,
                "to" => format!("{}", &call.to),
            },
            state,
            &call_handler.handler,
            MappingTrigger::Call {
                transaction,
                call: call.cheap_clone(),
                inputs,
                outputs,
                handler: call_handler.clone(),
            },
            block,
            proof_of_indexing,
        )
        .await
    }

    async fn process_block(
        &self,
        logger: &Logger,
        block: &Arc<LightEthereumBlock>,
        trigger_type: &EthereumBlockTriggerType,
        state: BlockState,
        proof_of_indexing: SharedProofOfIndexing,
    ) -> Result<BlockState, MappingError> {
        let block_handler = self.data_source.handler_for_block(trigger_type)?;
        self.send_mapping_request(
            logger,
            o! {
                "hash" => block.hash.unwrap().to_string(),
                "number" => &block.number.unwrap().to_string(),
            },
            state,
            &block_handler.handler,
            MappingTrigger::Block {
                handler: block_handler.clone(),
            },
            block,
            proof_of_indexing,
        )
        .await
    }

    async fn process_log(
        &self,
        logger: &Logger,
        block: &Arc<LightEthereumBlock>,
        log: &Arc<Log>,
        state: BlockState,
        proof_of_indexing: SharedProofOfIndexing,
    ) -> Result<BlockState, MappingError> {
        let data_source_name = &self.data_source.name;
        let abi_name = &self.data_source.contract_abi.name;
        let contract = &self.data_source.contract_abi.contract;

        // If there are no matching handlers, fail processing the event
        let potential_handlers = self.data_source.handlers_for_log(&log)?;

        // Map event handlers to (event handler, event ABI) pairs; fail if there are
        // handlers that don't exist in the contract ABI
        let valid_handlers = potential_handlers
            .into_iter()
            .map(|event_handler| {
                // Identify the event ABI in the contract
                let event_abi = util::ethereum::contract_event_with_signature(
                    contract,
                    event_handler.event.as_str(),
                )
                .with_context(|| {
                    anyhow!(
                        "Event with the signature \"{}\" not found in \
                                contract \"{}\" of data source \"{}\"",
                        event_handler.event,
                        abi_name,
                        data_source_name,
                    )
                })?;
                Ok((event_handler, event_abi))
            })
            .collect::<Result<Vec<_>, anyhow::Error>>()?;

        // Filter out handlers whose corresponding event ABIs cannot decode the
        // params (this is common for overloaded events that have the same topic0
        // but have indexed vs. non-indexed params that are encoded differently).
        //
        // Map (handler, event ABI) pairs to (handler, decoded params) pairs.
        let mut matching_handlers = valid_handlers
            .into_iter()
            .filter_map(|(event_handler, event_abi)| {
                event_abi
                    .parse_log(RawLog {
                        topics: log.topics.clone(),
                        data: log.data.clone().0,
                    })
                    .map(|log| log.params)
                    .map_err(|e| {
                        info!(
                            logger,
                            "Skipping handler because the event parameters do not \
                            match the event signature. This is typically the case \
                            when parameters are indexed in the event but not in the \
                            signature or the other way around";
                            "handler" => &event_handler.handler,
                            "event" => &event_handler.event,
                            "error" => format!("{}", e),
                        );
                    })
                    .ok()
                    .map(|params| (event_handler, params))
            })
            .collect::<Vec<_>>();

        if matching_handlers.is_empty() {
            warn!(
                logger,
                "No matching handlers found for event with topic0 `{}`",
                log.topics
                .iter()
                .next()
                .map_or(String::from("none"), |topic0| format!("{:x}", topic0));
                "data_source" => &data_source_name,
            );
            return Ok(state);
        }

        // Process the event with the matching handler
        let (event_handler, params) = matching_handlers.pop().unwrap();

        ensure!(
            matching_handlers.is_empty(),
            format!(
                "Multiple handlers defined for event `{}`, only one is supported",
                &event_handler.event
            )
        );

        let transaction = Arc::new(
            block
                .transaction_for_log(&log)
                .context("Found no transaction for event")?,
        );

        self.send_mapping_request(
            logger,
            o! {
                "signature" => &event_handler.event,
                "address" => format!("{}", &log.address),
            },
            state,
            &event_handler.handler,
            MappingTrigger::Log {
                transaction,
                log: log.cheap_clone(),
                params,
                handler: event_handler.clone(),
            },
            block,
            proof_of_indexing,
        )
        .err_into()
        .await
    }

    fn creation_block_number(&self) -> Option<BlockNumber> {
        self.data_source.creation_block
    }
}

impl PartialEq for RuntimeHost {
    fn eq(&self, other: &Self) -> bool {
        self.data_source.is_duplicate_of(&other.data_source)
    }
}
