use futures::sync::mpsc::{channel, Sender};
use futures::sync::oneshot;
use semver::{Version, VersionReq};
use tiny_keccak::keccak256;

use std::thread;
use std::time::Instant;

use super::MappingContext;
use crate::module::{ValidModule, WasmiModule, WasmiModuleConfig};
use graph::components::ethereum::*;
use graph::components::store::Store;
use graph::data::subgraph::{DataSource, Source};
use graph::ethabi::{LogParam, RawLog};
use graph::prelude::{
    RuntimeHost as RuntimeHostTrait, RuntimeHostBuilder as RuntimeHostBuilderTrait, *,
};
use graph::util;
use graph::web3::types::{Log, Transaction};

pub struct RuntimeHostConfig {
    subgraph_id: SubgraphDeploymentId,
    data_source: DataSource,
}

pub struct RuntimeHostBuilder<T, L, S> {
    ethereum_adapter: Arc<T>,
    link_resolver: Arc<L>,
    store: Arc<S>,
}

impl<T, L, S> Clone for RuntimeHostBuilder<T, L, S>
where
    T: EthereumAdapter,
    L: LinkResolver,
    S: Store,
{
    fn clone(&self) -> Self {
        RuntimeHostBuilder {
            ethereum_adapter: self.ethereum_adapter.clone(),
            link_resolver: self.link_resolver.clone(),
            store: self.store.clone(),
        }
    }
}

impl<T, L, S> RuntimeHostBuilder<T, L, S>
where
    T: EthereumAdapter,
    L: LinkResolver,
    S: Store,
{
    pub fn new(ethereum_adapter: Arc<T>, link_resolver: Arc<L>, store: Arc<S>) -> Self {
        RuntimeHostBuilder {
            ethereum_adapter,
            link_resolver,
            store,
        }
    }
}

impl<T, L, S> RuntimeHostBuilderTrait for RuntimeHostBuilder<T, L, S>
where
    T: EthereumAdapter,
    L: LinkResolver,
    S: Store,
{
    type Host = RuntimeHost;

    fn build(
        &self,
        logger: &Logger,
        subgraph_id: SubgraphDeploymentId,
        data_source: DataSource,
    ) -> Result<Self::Host, Error> {
        RuntimeHost::new(
            logger,
            self.ethereum_adapter.clone(),
            self.link_resolver.clone(),
            self.store.clone(),
            RuntimeHostConfig {
                subgraph_id,
                data_source,
            },
        )
    }
}

type MappingResponse = (Result<BlockState, Error>, futures::Finished<Instant, Error>);

#[derive(Debug)]
struct MappingRequest {
    logger: Logger,
    block: Arc<EthereumBlock>,
    state: BlockState,
    trigger: MappingTrigger,
    result_sender: oneshot::Sender<MappingResponse>,
}

#[derive(Debug)]
enum MappingTrigger {
    Log {
        transaction: Arc<Transaction>,
        log: Arc<Log>,
        params: Vec<LogParam>,
        handler: MappingEventHandler,
    },
    Call {
        transaction: Arc<Transaction>,
        call: Arc<EthereumCall>,
        inputs: Vec<LogParam>,
        outputs: Vec<LogParam>,
        handler: MappingCallHandler,
    },
    Block {
        handler: MappingBlockHandler,
    },
}

#[derive(Debug)]
pub struct RuntimeHost {
    data_source_name: String,
    data_source_contract: Source,
    data_source_contract_abi: MappingABI,
    data_source_event_handlers: Option<Vec<MappingEventHandler>>,
    data_source_call_handlers: Option<Vec<MappingCallHandler>>,
    data_source_block_handlers: Option<Vec<MappingBlockHandler>>,
    mapping_request_sender: Sender<MappingRequest>,
    _guard: oneshot::Sender<()>,
}

impl RuntimeHost {
    pub fn new<T, L, S>(
        logger: &Logger,
        ethereum_adapter: Arc<T>,
        link_resolver: Arc<L>,
        store: Arc<S>,
        config: RuntimeHostConfig,
    ) -> Result<Self, Error>
    where
        T: EthereumAdapter,
        L: LinkResolver,
        S: Store,
    {
        let logger = logger.new(o!(
            "component" => "RuntimeHost",
            "data_source" => config.data_source.name.clone(),
        ));

        let api_version = Version::parse(&config.data_source.mapping.api_version)?;
        if !VersionReq::parse("<= 0.0.2").unwrap().matches(&api_version) {
            return Err(format_err!(
                "This Graph Node only supports mapping API versions <= 0.0.2, but subgraph `{}` uses `{}`",
                config.subgraph_id,
                api_version
            ));
        }

        // Create channel for canceling the module
        let (cancel_sender, cancel_receiver) = oneshot::channel();

        // Create channel for event handling requests
        let (mapping_request_sender, mapping_request_receiver) = channel(100);

        // wasmi modules are not `Send` therefore they cannot be scheduled by
        // the regular tokio executor, so we create a dedicated thread.
        //
        // This thread can spawn tasks on the runtime by sending them to
        // `task_receiver`.
        let (task_sender, task_receiver) = channel(100);
        tokio::spawn(task_receiver.for_each(tokio::spawn));

        // Start the WASMI module runtime
        let module_logger = logger.clone();
        let data_source_name = config.data_source.name.clone();
        let data_source_contract = config.data_source.source.clone();
        let data_source_event_handlers = config.data_source.mapping.event_handlers.clone();
        let data_source_call_handlers = config.data_source.mapping.call_handlers.clone();
        let data_source_block_handlers = config.data_source.mapping.block_handlers.clone();
        let data_source_contract_abi = config
            .data_source
            .mapping
            .abis
            .iter()
            .find(|abi| abi.name == config.data_source.source.abi)
            .ok_or_else(|| {
                format_err!(
                    "No ABI entry found for the main contract of data source \"{}\": {}",
                    data_source_name,
                    config.data_source.source.abi,
                )
            })?
            .clone();

        // Spawn a dedicated thread for the runtime.
        //
        // In case of failure, this thread may panic or simply terminate,
        // dropping the `mapping_request_receiver` which ultimately causes the
        // subgraph to fail the next time it tries to handle an event.
        let conf = thread::Builder::new().name(format!(
            "mapping-{}-{}",
            config.subgraph_id, data_source_name
        ));
        conf.spawn(move || {
            debug!(module_logger, "Start WASM runtime");
            let wasmi_config = WasmiModuleConfig {
                subgraph_id: config.subgraph_id,
                data_source: config.data_source,
                ethereum_adapter: ethereum_adapter.clone(),
                link_resolver: link_resolver.clone(),
                store: store.clone(),
            };
            let valid_module = ValidModule::new(&module_logger, wasmi_config, task_sender)
                .expect("Failed to validate module");
            let valid_module = Arc::new(valid_module);

            // Pass incoming triggers to the WASM module and return entity changes;
            // Stop when cancelled.
            let canceler = cancel_receiver.into_stream();
            mapping_request_receiver
                .select(
                    canceler
                        .map(|_| panic!("WASM module thread cancelled"))
                        .map_err(|_| ()),
                )
                .map_err(|()| err_msg("Cancelled"))
                .for_each(move |request: MappingRequest| -> Result<(), Error> {
                    let MappingRequest {
                        logger,
                        block,
                        state,
                        trigger,
                        result_sender,
                    } = request;

                    let ctx = MappingContext {
                        logger,
                        block,
                        state,
                    };

                    let module =
                        WasmiModule::from_valid_module_with_ctx(valid_module.clone(), ctx)?;

                    let result = match trigger {
                        MappingTrigger::Log {
                            transaction,
                            log,
                            params,
                            handler,
                        } => module.handle_ethereum_log(
                            handler.handler.as_str(),
                            transaction,
                            log,
                            params,
                        ),
                        MappingTrigger::Call {
                            transaction,
                            call,
                            inputs,
                            outputs,
                            handler,
                        } => module.handle_ethereum_call(
                            handler.handler.as_str(),
                            transaction,
                            call,
                            inputs,
                            outputs,
                        ),
                        MappingTrigger::Block { handler } => {
                            module.handle_ethereum_block(handler.handler.as_str())
                        }
                    };

                    result_sender
                        .send((result, future::ok(Instant::now())))
                        .map_err(|_| err_msg("WASM module result receiver dropped."))
                })
                .wait()
                .unwrap_or_else(|e| {
                    debug!(module_logger, "WASM runtime thread terminating"; 
                           "reason" => e.to_string())
                });
        })
        .expect("Spawning WASM runtime thread failed.");

        Ok(RuntimeHost {
            data_source_name,
            data_source_contract,
            data_source_contract_abi,
            data_source_event_handlers,
            data_source_call_handlers,
            data_source_block_handlers,
            mapping_request_sender,
            _guard: cancel_sender,
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
        self.data_source_call_handlers
            .as_ref()
            .map_or(false, |handlers| {
                handlers.iter().any(|handler| {
                    let fhash = keccak256(handler.function.as_bytes());
                    let actual_method_id = [fhash[0], fhash[1], fhash[2], fhash[3]];
                    target_method_id == actual_method_id
                })
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
            .as_ref()
            .map_or(false, |handlers| {
                handlers.iter().any(|handler| *topic0 == handler.topic0())
            })
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
            .as_ref()
            .ok_or_else(|| {
                format_err!(
                    "No event handlers found in data source \"{}\"",
                    self.data_source_name
                )
            })?
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
            .as_ref()
            .ok_or_else(|| {
                format_err!(
                    "No call handlers found in data source \"{}\"",
                    self.data_source_name
                )
            })?
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
                .as_ref()
                .ok_or_else(|| {
                    format_err!(
                        "No block handlers found in data source \"{}\"",
                        self.data_source_name
                    )
                })?
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
                .as_ref()
                .ok_or_else(|| {
                    format_err!(
                        "No block handlers found in data source \"{}\"",
                        self.data_source_name
                    )
                })?
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
        self.matches_log_address(log) && self.matches_log_signature(log)
    }

    fn matches_call(&self, call: &EthereumCall) -> bool {
        self.matches_call_address(call) && self.matches_call_function(call)
    }

    fn matches_block(&self, block_trigger_type: EthereumBlockTriggerType) -> bool {
        self.matches_block_trigger(block_trigger_type)
    }

    fn process_call(
        &self,
        logger: Logger,
        block: Arc<EthereumBlock>,
        transaction: Arc<Transaction>,
        call: Arc<EthereumCall>,
        state: BlockState,
    ) -> Box<Future<Item = BlockState, Error = Error> + Send> {
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
        Box::new(
            self.mapping_request_sender
                .clone()
                .send(MappingRequest {
                    logger: logger.clone(),
                    block: block.clone(),
                    trigger: MappingTrigger::Call {
                        transaction: transaction.clone(),
                        call: call.clone(),
                        inputs,
                        outputs,
                        handler: call_handler.clone(),
                    },
                    state,
                    result_sender,
                })
                .map_err(move |_| format_err!("Mapping terminated before passing in Ethereum call"))
                .and_then(|_| {
                    result_receiver.map_err(move |_| {
                        format_err!("Mapping terminated before finishing to handle")
                    })
                })
                .and_then(move |(result, _)| {
                    info!(
                        logger, "Done processing Ethereum call";
                        "function" => &call_handler.function,
                        "handler" => &call_handler.handler,
                        "ms" => start_time.elapsed().as_millis(),
                    );
                    result
                }),
        )
    }

    fn process_block(
        &self,
        logger: Logger,
        block: Arc<EthereumBlock>,
        trigger_type: EthereumBlockTriggerType,
        state: BlockState,
    ) -> Box<Future<Item = BlockState, Error = Error> + Send> {
        let block_handler = match self.handler_for_block(trigger_type) {
            Ok(handler) => handler,
            Err(e) => return Box::new(future::err(e)),
        };

        debug!(
            logger, "Start processing Ethereum block";
            "hash" => block.block.hash.unwrap().to_string(),
            "number" => &block.block.number.unwrap().to_string(),
            "handler" => &block_handler.handler,
            "data_source" => &self.data_source_name,
        );

        // Execute the call handler and asynchronously wait for the result
        let (result_sender, result_receiver) = oneshot::channel();
        let start_time = Instant::now();
        Box::new(
            self.mapping_request_sender
                .clone()
                .send(MappingRequest {
                    logger: logger.clone(),
                    block: block.clone(),
                    trigger: MappingTrigger::Block {
                        handler: block_handler.clone(),
                    },
                    state,
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
                    info!(
                        logger, "Done processing Ethereum block";
                        "hash" => block.block.hash.unwrap().to_string(),
                        "number" => &block.block.number.unwrap().to_string(),
                        "handler" => &block_handler.handler,
                        "ms" => start_time.elapsed().as_millis(),
                    );
                    result
                }),
        )
    }

    fn process_log(
        &self,
        logger: Logger,
        block: Arc<EthereumBlock>,
        transaction: Arc<Transaction>,
        log: Arc<Log>,
        state: BlockState,
    ) -> Box<Future<Item = BlockState, Error = Error> + Send> {
        let logger = logger.clone();
        let mapping_request_sender = self.mapping_request_sender.clone();

        let block = block.clone();
        let transaction = transaction.clone();
        let log = log.clone();

        let data_source_name = self.data_source_name.clone();
        let abi_name = self.data_source_contract_abi.name.clone();
        let contract = self.data_source_contract_abi.contract.clone();

        Box::new(
            future::result(self.handlers_for_log(&log)).and_then(|event_handlers| {
                stream::iter_ok(event_handlers).fold(state, move |state, event_handler| {
                    let logger = logger.clone();
                    let logger_for_parsing = logger.clone();
                    let mapping_request_sender = mapping_request_sender.clone();
                    let data_source_name = data_source_name.clone();

                    let block = block.clone();
                    let transaction = transaction.clone();

                    let event_handler = event_handler.clone();

                    // Identify the event ABI in the contract
                    let event_abi = match util::ethereum::contract_event_with_signature(
                        &contract,
                        event_handler.event.as_str(),
                    ) {
                        Some(event_abi) => event_abi,
                        None => {
                            return Box::new(future::err(format_err!(
                                "Event with the signature \"{}\" not found in \
                                 contract \"{}\" of data source \"{}\"",
                                event_handler.event,
                                abi_name,
                                data_source_name,
                            )))
                                as Box<dyn Future<Item = _, Error = _> + Send>;
                        }
                    };

                    // Parse the log into an event
                    let params = match event_abi.parse_log(RawLog {
                        topics: log.topics.clone(),
                        data: log.data.clone().0,
                    }) {
                        Ok(log) => log.params,
                        Err(e) => {
                            info!(
                                logger_for_parsing,
                                "Skipping handler because the event parameters do not \
                                 match the event signature. This is typically the case \
                                 when parameters are indexed in the event but not in the \
                                 signature or the other way around";
                                "handler" => &event_handler.handler,
                                "event" => &event_handler.event,
                                "error" => format!("{}", e),
                            );
                            return Box::new(future::ok(state))
                                as Box<dyn Future<Item = _, Error = _> + Send>;
                        }
                    };

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

                    Box::new(
                        mapping_request_sender
                            .clone()
                            .send(MappingRequest {
                                logger: logger.clone(),
                                block: block.clone(),
                                trigger: MappingTrigger::Log {
                                    transaction: transaction.clone(),
                                    log: log.clone(),
                                    params,
                                    handler: event_handler.clone(),
                                },
                                state,
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
                                let logger = logger.clone();
                                info!(
                                    logger, "Done processing Ethereum event";
                                    "signature" => &event_handler.event,
                                    "handler" => &event_handler.handler,
                                    "total_ms" => start_time.elapsed().as_millis(),

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
                })
            }),
        )
    }
}
