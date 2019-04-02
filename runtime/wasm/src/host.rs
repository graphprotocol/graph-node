use futures::sync::mpsc::{channel, Sender};
use futures::sync::oneshot;
use semver::{Version, VersionReq};
use std::thread;
use std::time::Instant;

use graph::components::ethereum::*;
use graph::components::store::Store;
use graph::data::subgraph::{DataSource, Source};
use graph::ethabi::LogParam;
use graph::ethabi::RawLog;
use graph::prelude::{
    RuntimeHost as RuntimeHostTrait, RuntimeHostBuilder as RuntimeHostBuilderTrait, *,
};
use graph::util;
use graph::web3::types::{Log, Transaction};

use super::EventHandlerContext;
use crate::module::{ValidModule, WasmiModule, WasmiModuleConfig};

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

type HandleEventResponse = Result<Vec<EntityOperation>, Error>;

#[derive(Debug)]
struct HandleEventRequest {
    handler: MappingEventHandler,
    logger: Logger,
    block: Arc<EthereumBlock>,
    transaction: Arc<Transaction>,
    log: Arc<Log>,
    params: Vec<LogParam>,
    entity_operations: Vec<EntityOperation>,
    result_sender: oneshot::Sender<HandleEventResponse>,
}

#[derive(Debug)]
pub struct RuntimeHost {
    data_source_name: String,
    data_source_contract: Source,
    data_source_contract_abi: MappingABI,
    data_source_event_handlers: Vec<MappingEventHandler>,
    handle_event_sender: Sender<HandleEventRequest>,
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
        let (handle_event_sender, handle_event_receiver) = channel(100);

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
        // dropping the `handle_event_receiver` which ultimately causes the
        // subgraph to fail the next time it tries to handle an event.
        let conf = thread::Builder::new().name(format!(
            "{}-{}-{}",
            util::log::MAPPING_THREAD_PREFIX,
            config.subgraph_id,
            data_source_name
        ));
        conf.spawn(move || {
            debug!(module_logger, "Start WASM runtime");

            // Load the mapping of the data source as a WASM module
            let wasmi_config = WasmiModuleConfig {
                subgraph_id: config.subgraph_id,
                data_source: config.data_source,
                ethereum_adapter: ethereum_adapter.clone(),
                link_resolver: link_resolver.clone(),
                store: store.clone(),
            };

            // Validate the mapping's WASM module
            let valid_module = ValidModule::new(&module_logger, wasmi_config, task_sender)
                .expect("Failed to validate module");

            // Pass incoming events to the WASM module and send entity changes back;
            // stop when cancelled from the outside.
            let canceler = cancel_receiver.into_stream();
            handle_event_receiver
                .select(canceler.map(|_| panic!("sent to canceler")).map_err(|_| ()))
                .map_err(|()| err_msg("Canceled"))
                .for_each(move |request: HandleEventRequest| -> Result<(), Error> {
                    let HandleEventRequest {
                        handler,
                        logger,
                        block,
                        transaction,
                        log,
                        params,
                        entity_operations,
                        result_sender,
                    } = request;

                    let ctx = EventHandlerContext {
                        logger,
                        block,
                        transaction,
                        entity_operations,
                    };

                    let result = WasmiModule::from_valid_module_with_ctx(&valid_module, ctx)?
                        .handle_ethereum_event(handler.handler.as_str(), log, params);
                    result_sender
                        .send(result)
                        .map_err(|_| err_msg("receiver dropped"))
                })
                .wait()
                .ok();
        })
        .expect("failed to spawn runtime thread");

        Ok(RuntimeHost {
            data_source_name,
            data_source_contract,
            data_source_contract_abi,
            data_source_event_handlers,
            handle_event_sender,
            _guard: cancel_sender,
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
        if log.topics.is_empty() {
            return false;
        }
        let signature = log.topics[0];

        self.data_source_event_handlers.iter().any(|event_handler| {
            signature == util::ethereum::string_to_h256(event_handler.event.as_str())
        })
    }

    fn event_handler_for_log(&self, log: &Arc<Log>) -> Result<MappingEventHandler, Error> {
        // Get signature from the log
        if log.topics.is_empty() {
            return Err(format_err!("Ethereum event has no topics"));
        }
        let signature = log.topics[0];

        self.data_source_event_handlers
            .iter()
            .find(|handler| signature == util::ethereum::string_to_h256(handler.event.as_str()))
            .cloned()
            .ok_or_else(|| {
                format_err!(
                    "No event handler found for event in data source \"{}\"",
                    self.data_source_name,
                )
            })
    }
}

impl RuntimeHostTrait for RuntimeHost {
    fn matches_log(&self, log: &Log) -> bool {
        self.matches_log_address(log) && self.matches_log_signature(log)
    }

    fn process_log(
        &self,
        logger: Logger,
        block: Arc<EthereumBlock>,
        transaction: Arc<Transaction>,
        log: Arc<Log>,
        entity_operations: Vec<EntityOperation>,
    ) -> Box<Future<Item = Vec<EntityOperation>, Error = Error> + Send> {
        // Identify event handler for this log
        let event_handler = match self.event_handler_for_log(&log) {
            Ok(handler) => handler,
            Err(e) => return Box::new(future::err(e)),
        };

        // Identify the event ABI in the contract
        let event_abi = match util::ethereum::contract_event_with_signature(
            &self.data_source_contract_abi.contract,
            event_handler.event.as_str(),
        ) {
            Some(event_abi) => event_abi,
            None => {
                return Box::new(future::err(format_err!(
                    "Event with the signature \"{}\" not found in \
                     contract \"{}\" of data source \"{}\"",
                    event_handler.event,
                    self.data_source_contract_abi.name,
                    self.data_source_name
                )));
            }
        };

        // Parse the log into an event
        let params = match event_abi.parse_log(RawLog {
            topics: log.topics.clone(),
            data: log.data.clone().0,
        }) {
            Ok(log) => log.params,
            Err(e) => {
                return Box::new(future::err(format_err!(
                    "Failed to parse parameters of event: {}: {}",
                    event_handler.event,
                    e
                )));
            }
        };

        debug!(
            logger, "Start processing Ethereum event";
            "signature" => &event_handler.event,
            "handler" => &event_handler.handler
        );

        // Call the event handler and asynchronously wait for the result
        let (result_sender, result_receiver) = oneshot::channel();

        let before_event_signature = event_handler.event.clone();
        let event_signature = event_handler.event.clone();
        let start_time = Instant::now();

        Box::new(
            self.handle_event_sender
                .clone()
                .send(HandleEventRequest {
                    handler: event_handler.clone(),
                    logger: logger.clone(),
                    block: block.clone(),
                    transaction: transaction.clone(),
                    log: log.clone(),
                    params,
                    entity_operations,
                    result_sender,
                })
                .map_err(move |_| {
                    format_err!(
                        "Mapping terminated before passing in Ethereum event: {}",
                        before_event_signature
                    )
                })
                .and_then(|_| {
                    result_receiver.map_err(move |e| {
                        format_err!(
                            "Mapping terminated with error when handling event `{}`: {}",
                            event_signature,
                            e
                        )
                    })
                })
                .and_then(move |result| {
                    info!(
                        logger, "Done processing Ethereum event";
                        "signature" => &event_handler.event,
                        "handler" => &event_handler.handler,
                        // Replace this when `as_millis` is stable.
                        "secs" => start_time.elapsed().as_secs(),
                        "ms" => start_time.elapsed().subsec_millis()
                    );
                    result
                }),
        )
    }
}
