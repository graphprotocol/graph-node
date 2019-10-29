use futures::sync::mpsc::Sender;
use futures::sync::oneshot;
use semver::{Version, VersionReq};
use tiny_keccak::keccak256;

use std::collections::HashMap;
use std::str::FromStr;
use std::time::{Duration, Instant};

use crate::host_exports::HostExports;
use crate::mapping::{MappingContext, MappingRequest, MappingTrigger};
use ethabi::{LogParam, RawLog};
use graph::components::ethereum::*;
use graph::components::store::Store;
use graph::data::subgraph::{Mapping, Source};
use graph::prelude::{
    RuntimeHost as RuntimeHostTrait, RuntimeHostBuilder as RuntimeHostBuilderTrait, *,
};
use graph::util;
use web3::types::{Log, Transaction};

pub(crate) const TIMEOUT_ENV_VAR: &str = "GRAPH_MAPPING_HANDLER_TIMEOUT";

struct RuntimeHostConfig {
    subgraph_id: SubgraphDeploymentId,
    mapping: Mapping,
    data_source_network: String,
    data_source_name: String,
    contract: Source,
    templates: Vec<DataSourceTemplate>,
}

pub struct RuntimeHostBuilder<S> {
    ethereum_adapters: HashMap<String, Arc<dyn EthereumAdapter>>,
    link_resolver: Arc<dyn LinkResolver>,
    stores: HashMap<String, Arc<S>>,
}

impl<S> Clone for RuntimeHostBuilder<S>
where
    S: Store,
{
    fn clone(&self) -> Self {
        RuntimeHostBuilder {
            ethereum_adapters: self.ethereum_adapters.clone(),
            link_resolver: self.link_resolver.clone(),
            stores: self.stores.clone(),
        }
    }
}

impl<S> RuntimeHostBuilder<S>
where
    S: Store + SubgraphDeploymentStore + EthereumCallCache,
{
    pub fn new(
        ethereum_adapters: HashMap<String, Arc<dyn EthereumAdapter>>,
        link_resolver: Arc<dyn LinkResolver>,
        stores: HashMap<String, Arc<S>>,
    ) -> Self {
        RuntimeHostBuilder {
            ethereum_adapters,
            link_resolver,
            stores,
        }
    }
}

impl<S> RuntimeHostBuilderTrait for RuntimeHostBuilder<S>
where
    S: Send + Sync + 'static + Store + SubgraphDeploymentStore + EthereumCallCache,
{
    type Host = RuntimeHost;
    type Req = MappingRequest;

    fn spawn_mapping(
        parsed_module: parity_wasm::elements::Module,
        logger: Logger,
        subgraph_id: SubgraphDeploymentId,
        metrics: Arc<HostMetrics>,
    ) -> Result<Sender<Self::Req>, Error> {
        crate::mapping::spawn_module(parsed_module, logger, subgraph_id, metrics)
    }

    fn build(
        &self,
        network_name: String,
        subgraph_id: SubgraphDeploymentId,
        data_source: DataSource,
        top_level_templates: Vec<DataSourceTemplate>,
        mapping_request_sender: Sender<MappingRequest>,
        metrics: Arc<HostMetrics>,
    ) -> Result<Self::Host, Error> {
        let store = self.stores.get(&network_name).ok_or_else(|| {
            format_err!(
                "No store found that matches subgraph network: \"{}\"",
                &network_name
            )
        })?;

        let ethereum_adapter = self.ethereum_adapters.get(&network_name).ok_or_else(|| {
            format_err!(
                "No Ethereum adapter found that matches subgraph network: \"{}\"",
                &network_name
            )
        })?;

        // Detect whether the subgraph uses templates in data sources, which are
        // deprecated, or the top-level templates field.
        let templates = match top_level_templates.is_empty() {
            false => top_level_templates,
            true => data_source.templates,
        };

        RuntimeHost::new(
            ethereum_adapter.clone(),
            self.link_resolver.clone(),
            store.clone(),
            store.clone(),
            RuntimeHostConfig {
                subgraph_id,
                mapping: data_source.mapping,
                data_source_network: network_name,
                data_source_name: data_source.name,
                contract: data_source.source,
                templates,
            },
            mapping_request_sender,
            metrics,
        )
    }
}

#[derive(Debug)]
pub struct RuntimeHost {
    data_source_name: String,
    data_source_contract: Source,
    data_source_contract_abi: MappingABI,
    data_source_event_handlers: Vec<MappingEventHandler>,
    data_source_call_handlers: Vec<MappingCallHandler>,
    data_source_block_handlers: Vec<MappingBlockHandler>,
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
        config: RuntimeHostConfig,
        mapping_request_sender: Sender<MappingRequest>,
        metrics: Arc<HostMetrics>,
    ) -> Result<Self, Error> {
        let api_version = Version::parse(&config.mapping.api_version)?;
        if !VersionReq::parse("<= 0.0.3").unwrap().matches(&api_version) {
            return Err(format_err!(
                "This Graph Node only supports mapping API versions <= 0.0.3, but subgraph `{}` uses `{}`",
                config.subgraph_id,
                api_version
            ));
        }

        let data_source_contract_abi = config
            .mapping
            .abis
            .iter()
            .find(|abi| abi.name == config.contract.abi)
            .ok_or_else(|| {
                format_err!(
                    "No ABI entry found for the main contract of data source \"{}\": {}",
                    &config.data_source_name,
                    config.contract.abi,
                )
            })?
            .clone();

        let data_source_name = config.data_source_name;

        // Create new instance of externally hosted functions invoker. The `Arc` is simply to avoid
        // implementing `Clone` for `HostExports`.
        let host_exports = Arc::new(HostExports::new(
            config.subgraph_id.clone(),
            api_version,
            data_source_name.clone(),
            config.contract.address.clone(),
            Some(config.data_source_network),
            config.templates,
            config.mapping.abis,
            ethereum_adapter,
            link_resolver,
            store,
            call_cache,
            std::env::var(TIMEOUT_ENV_VAR)
                .ok()
                .and_then(|s| u64::from_str(&s).ok())
                .map(Duration::from_secs),
        ));

        Ok(RuntimeHost {
            data_source_name,
            data_source_contract: config.contract,
            data_source_contract_abi,
            data_source_event_handlers: config.mapping.event_handlers,
            data_source_call_handlers: config.mapping.call_handlers,
            data_source_block_handlers: config.mapping.block_handlers,
            mapping_request_sender,
            host_exports,
            metrics,
        })
    }

    fn matches_call_address(&self, call: &EthereumCall) -> bool {
        // The runtime host matches the contract address of the `EthereumCall`
        // if the data source contains the same contract address or
        // if the data source doesn't have a contract address at all
        self.data_source_contract
            .address
            .map_or(true, |addr| addr == call.to)
    }

    fn matches_call_function(&self, call: &EthereumCall) -> bool {
        let target_method_id = &call.input.0[..4];
        self.data_source_call_handlers.iter().any(|handler| {
            let fhash = keccak256(handler.function.as_bytes());
            let actual_method_id = [fhash[0], fhash[1], fhash[2], fhash[3]];
            target_method_id == actual_method_id
        })
    }

    fn matches_log_address(&self, log: &Log) -> bool {
        // The runtime host matches the contract address of the `Log`
        // if the data source contains the same contract address or
        // if the data source doesn't have a contract address at all
        self.data_source_contract
            .address
            .map_or(true, |addr| addr == log.address)
    }

    fn matches_log_signature(&self, log: &Log) -> bool {
        let topic0 = match log.topics.iter().next() {
            Some(topic0) => topic0,
            None => return false,
        };

        self.data_source_event_handlers
            .iter()
            .any(|handler| *topic0 == handler.topic0())
    }

    fn matches_block_trigger(&self, block_trigger_type: EthereumBlockTriggerType) -> bool {
        let source_address_matches = match block_trigger_type {
            EthereumBlockTriggerType::WithCallTo(address) => {
                self.data_source_contract
                    .address
                    // Do not match if this datasource has no address
                    .map_or(false, |addr| addr == address)
            }
            EthereumBlockTriggerType::Every => true,
        };
        source_address_matches && self.handler_for_block(block_trigger_type).is_ok()
    }

    fn handlers_for_log(&self, log: &Arc<Log>) -> Result<Vec<MappingEventHandler>, Error> {
        // Get signature from the log
        let topic0 = match log.topics.iter().next() {
            Some(topic0) => topic0,
            None => return Err(format_err!("Ethereum event has no topics")),
        };

        let handlers = self
            .data_source_event_handlers
            .iter()
            .filter(|handler| *topic0 == handler.topic0())
            .cloned()
            .collect::<Vec<_>>();

        if !handlers.is_empty() {
            Ok(handlers)
        } else {
            Err(format_err!(
                "No event handler found for event in data source \"{}\"",
                self.data_source_name,
            ))
        }
    }

    fn handler_for_call(&self, call: &Arc<EthereumCall>) -> Result<MappingCallHandler, Error> {
        // First four bytes of the input for the call are the first four
        // bytes of hash of the function signature
        if call.input.0.len() < 4 {
            return Err(format_err!(
                "Ethereum call has input with less than 4 bytes"
            ));
        }

        let target_method_id = &call.input.0[..4];

        self.data_source_call_handlers
            .iter()
            .find(move |handler| {
                let fhash = keccak256(handler.function.as_bytes());
                let actual_method_id = [fhash[0], fhash[1], fhash[2], fhash[3]];
                target_method_id == actual_method_id
            })
            .cloned()
            .ok_or_else(|| {
                format_err!(
                    "No call handler found for call in data source \"{}\"",
                    self.data_source_name,
                )
            })
    }

    fn handler_for_block(
        &self,
        trigger_type: EthereumBlockTriggerType,
    ) -> Result<MappingBlockHandler, Error> {
        match trigger_type {
            EthereumBlockTriggerType::Every => self
                .data_source_block_handlers
                .iter()
                .find(move |handler| handler.filter == None)
                .cloned()
                .ok_or_else(|| {
                    format_err!(
                        "No block handler for `Every` block trigger \
                         type found in data source \"{}\"",
                        self.data_source_name,
                    )
                }),
            EthereumBlockTriggerType::WithCallTo(_address) => self
                .data_source_block_handlers
                .iter()
                .find(move |handler| {
                    handler.filter.is_some()
                        && handler.filter.clone().unwrap() == BlockHandlerFilter::Call
                })
                .cloned()
                .ok_or_else(|| {
                    format_err!(
                        "No block handler for `WithCallTo` block trigger \
                         type found in data source \"{}\"",
                        self.data_source_name,
                    )
                }),
        }
    }
}

impl RuntimeHostTrait for RuntimeHost {
    fn matches_log(&self, log: &Log) -> bool {
        self.matches_log_address(log)
            && self.matches_log_signature(log)
            && self.data_source_contract.start_block <= log.block_number.unwrap().as_u64()
    }

    fn matches_call(&self, call: &EthereumCall) -> bool {
        self.matches_call_address(call)
            && self.matches_call_function(call)
            && self.data_source_contract.start_block <= call.block_number
    }

    fn matches_block(
        &self,
        block_trigger_type: EthereumBlockTriggerType,
        block_number: u64,
    ) -> bool {
        self.matches_block_trigger(block_trigger_type)
            && self.data_source_contract.start_block <= block_number
    }

    fn process_call(
        &self,
        logger: Logger,
        block: Arc<LightEthereumBlock>,
        transaction: Arc<Transaction>,
        call: Arc<EthereumCall>,
        state: BlockState,
    ) -> Box<dyn Future<Item = BlockState, Error = Error> + Send> {
        // Identify the call handler for this call
        let call_handler = match self.handler_for_call(&call) {
            Ok(handler) => handler,
            Err(e) => return Box::new(future::err(e)),
        };

        // Identify the function ABI in the contract
        let function_abi = match util::ethereum::contract_function_with_signature(
            &self.data_source_contract_abi.contract,
            call_handler.function.as_str(),
        ) {
            Some(function_abi) => function_abi,
            None => {
                return Box::new(future::err(format_err!(
                    "Function with the signature \"{}\" not found in \
                     contract \"{}\" of data source \"{}\"",
                    call_handler.function,
                    self.data_source_contract_abi.name,
                    self.data_source_name
                )));
            }
        };

        // Parse the inputs
        //
        // Take the input for the call, chop off the first 4 bytes, then call
        // `function.decode_output` to get a vector of `Token`s. Match the `Token`s
        // with the `Param`s in `function.inputs` to create a `Vec<LogParam>`.
        let inputs = match function_abi
            .decode_input(&call.input.0[4..])
            .map_err(|err| {
                format_err!(
                    "Generating function inputs for an Ethereum call failed = {}",
                    err,
                )
            })
            .and_then(|tokens| {
                if tokens.len() != function_abi.inputs.len() {
                    return Err(format_err!(
                        "Number of arguments in call does not match \
                         number of inputs in function signature."
                    ));
                }
                let inputs = tokens
                    .into_iter()
                    .enumerate()
                    .map(|(i, token)| LogParam {
                        name: function_abi.inputs[i].name.clone(),
                        value: token,
                    })
                    .collect::<Vec<LogParam>>();
                Ok(inputs)
            }) {
            Ok(params) => params,
            Err(e) => return Box::new(future::err(e)),
        };

        // Parse the outputs
        //
        // Take the ouput for the call, then call `function.decode_output` to
        // get a vector of `Token`s. Match the `Token`s with the `Param`s in
        // `function.outputs` to create a `Vec<LogParam>`.
        let outputs = match function_abi
            .decode_output(&call.output.0)
            .map_err(|err| {
                format_err!(
                    "Generating function outputs for an Ethereum call failed = {}",
                    err,
                )
            })
            .and_then(|tokens| {
                if tokens.len() != function_abi.outputs.len() {
                    return Err(format_err!(
                        "Number of paramters in the call output does not match \
                         number of outputs in the function signature."
                    ));
                }
                let outputs = tokens
                    .into_iter()
                    .enumerate()
                    .map(|(i, token)| LogParam {
                        name: function_abi.outputs[i].name.clone(),
                        value: token,
                    })
                    .collect::<Vec<LogParam>>();
                Ok(outputs)
            }) {
            Ok(outputs) => outputs,
            Err(e) => return Box::new(future::err(e)),
        };

        debug!(
            logger, "Start processing Ethereum call";
            "function" => &call_handler.function,
            "handler" => &call_handler.handler,
            "data_source" => &self.data_source_name,
            "to" => format!("{}", &call.to),
        );

        // Execute the call handler and asynchronously wait for the result
        let (result_sender, result_receiver) = oneshot::channel();
        let start_time = Instant::now();
        let metrics = self.metrics.clone();
        Box::new(
            self.mapping_request_sender
                .clone()
                .send(MappingRequest {
                    ctx: MappingContext {
                        logger: logger.clone(),
                        state,
                        host_exports: self.host_exports.clone(),
                        block: block.clone(),
                    },
                    trigger: MappingTrigger::Call {
                        transaction: transaction.clone(),
                        call: call.clone(),
                        inputs,
                        outputs,
                        handler: call_handler.clone(),
                    },
                    result_sender,
                })
                .map_err(move |_| format_err!("Mapping terminated before passing in Ethereum call"))
                .and_then(|_| {
                    result_receiver.map_err(move |_| {
                        format_err!("Mapping terminated before finishing to handle")
                    })
                })
                .and_then(move |(result, _)| {
                    let elapsed = start_time.elapsed();
                    metrics.observe_handler_execution_time(
                        elapsed.as_secs_f64(),
                        call_handler.handler.clone(),
                    );
                    info!(
                        logger, "Done processing Ethereum call";
                        "function" => &call_handler.function,
                        "handler" => &call_handler.handler,
                        "ms" => elapsed.as_millis(),
                    );
                    result
                }),
        )
    }

    fn process_block(
        &self,
        logger: Logger,
        block: Arc<LightEthereumBlock>,
        trigger_type: EthereumBlockTriggerType,
        state: BlockState,
    ) -> Box<dyn Future<Item = BlockState, Error = Error> + Send> {
        let block_handler = match self.handler_for_block(trigger_type) {
            Ok(handler) => handler,
            Err(e) => return Box::new(future::err(e)),
        };

        debug!(
            logger, "Start processing Ethereum block";
            "hash" => block.hash.unwrap().to_string(),
            "number" => &block.number.unwrap().to_string(),
            "handler" => &block_handler.handler,
            "data_source" => &self.data_source_name,
        );

        // Execute the call handler and asynchronously wait for the result
        let (result_sender, result_receiver) = oneshot::channel();
        let start_time = Instant::now();
        let metrics = self.metrics.clone();
        Box::new(
            self.mapping_request_sender
                .clone()
                .send(MappingRequest {
                    ctx: MappingContext {
                        logger: logger.clone(),
                        state,
                        host_exports: self.host_exports.clone(),
                        block: block.clone(),
                    },
                    trigger: MappingTrigger::Block {
                        handler: block_handler.clone(),
                    },
                    result_sender,
                })
                .map_err(move |_| {
                    format_err!("Mapping terminated before passing in Ethereum block")
                })
                .and_then(|_| {
                    result_receiver.map_err(move |_| {
                        format_err!("Mapping terminated before finishing to handle block trigger")
                    })
                })
                .and_then(move |(result, _)| {
                    let elapsed = start_time.elapsed();
                    metrics.observe_handler_execution_time(
                        elapsed.as_secs_f64(),
                        block_handler.handler.clone(),
                    );
                    info!(
                        logger, "Done processing Ethereum block";
                        "hash" => block.hash.unwrap().to_string(),
                        "number" => &block.number.unwrap().to_string(),
                        "handler" => &block_handler.handler,
                        "ms" => elapsed.as_millis(),
                    );
                    result
                }),
        )
    }

    fn process_log(
        &self,
        logger: Logger,
        block: Arc<LightEthereumBlock>,
        transaction: Arc<Transaction>,
        log: Arc<Log>,
        state: BlockState,
    ) -> Box<dyn Future<Item = BlockState, Error = Error> + Send> {
        let logger = logger.clone();

        let block = block.clone();
        let transaction = transaction.clone();
        let log = log.clone();

        let data_source_name = self.data_source_name.clone();
        let abi_name = self.data_source_contract_abi.name.clone();
        let contract = self.data_source_contract_abi.contract.clone();

        // If there are no matching handlers, fail processing the event
        let potential_handlers = match self.handlers_for_log(&log) {
            Ok(handlers) => handlers,
            Err(e) => {
                return Box::new(future::err(e)) as Box<dyn Future<Item = _, Error = _> + Send>
            }
        };

        // Map event handlers to (event handler, event ABI) pairs; fail if there are
        // handlers that don't exist in the contract ABI
        let valid_handlers =
            potential_handlers
                .into_iter()
                .try_fold(vec![], |mut acc, event_handler| {
                    // Identify the event ABI in the contract
                    let event_abi = match util::ethereum::contract_event_with_signature(
                        &contract,
                        event_handler.event.as_str(),
                    ) {
                        Some(event_abi) => event_abi,
                        None => {
                            return Err(format_err!(
                                "Event with the signature \"{}\" not found in \
                                 contract \"{}\" of data source \"{}\"",
                                event_handler.event,
                                abi_name,
                                data_source_name,
                            ))
                        }
                    };

                    acc.push((event_handler, event_abi));
                    Ok(acc)
                });

        // If there are no valid handlers, fail processing the event
        let valid_handlers = match valid_handlers {
            Ok(valid_handlers) => valid_handlers,
            Err(e) => {
                return Box::new(future::err(e)) as Box<dyn Future<Item = _, Error = _> + Send>
            }
        };

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

        // If none of the handlers match the event params, log a warning and
        // skip the event. See #1021.
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

            return Box::new(future::ok(state));
        }

        // Process the event with the matching handler
        let (event_handler, params) = matching_handlers.pop().unwrap();

        // Fail if there is more than one matching handler
        if !matching_handlers.is_empty() {
            return Box::new(future::err(format_err!(
                "Multiple handlers defined for event `{}`, only one is suported",
                &event_handler.event
            )));
        }

        debug!(
            logger, "Start processing Ethereum event";
            "signature" => &event_handler.event,
            "handler" => &event_handler.handler,
            "data_source" => &data_source_name,
            "address" => format!("{}", &log.address),
        );

        // Call the event handler and asynchronously wait for the result
        let (result_sender, result_receiver) = oneshot::channel();

        let before_event_signature = event_handler.event.clone();
        let event_signature = event_handler.event.clone();
        let start_time = Instant::now();
        let metrics = self.metrics.clone();
        Box::new(
            self.mapping_request_sender
                .clone()
                .send(MappingRequest {
                    ctx: MappingContext {
                        logger: logger.clone(),
                        state,
                        host_exports: self.host_exports.clone(),
                        block: block.clone(),
                    },
                    trigger: MappingTrigger::Log {
                        transaction: transaction.clone(),
                        log: log.clone(),
                        params,
                        handler: event_handler.clone(),
                    },
                    result_sender,
                })
                .map_err(move |_| {
                    format_err!(
                        "Mapping terminated before passing in Ethereum event: {}",
                        before_event_signature
                    )
                })
                .and_then(|_| {
                    result_receiver.map_err(move |_| {
                        format_err!(
                            "Mapping terminated before finishing to handle \
                             Ethereum event: {}",
                            event_signature,
                        )
                    })
                })
                .and_then(move |(result, send_time)| {
                    let elapsed = start_time.elapsed();
                    let logger = logger.clone();
                    metrics.observe_handler_execution_time(
                        elapsed.as_secs_f64(),
                        event_handler.handler.clone(),
                    );
                    info!(
                        logger, "Done processing Ethereum event";
                        "signature" => &event_handler.event,
                        "handler" => &event_handler.handler,
                        "total_ms" => elapsed.as_millis(),

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
                }),
        )
    }
}
