use failure::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use futures::sync::oneshot;
use std::str::FromStr;
use std::sync::Mutex;
use std::thread;
use uuid::Uuid;

use graph::components::ethereum::*;
use graph::components::store::Store;
use graph::data::subgraph::DataSource;
use graph::prelude::{
    RuntimeHost as RuntimeHostTrait, RuntimeHostBuilder as RuntimeHostBuilderTrait, *,
};
use graph::util;
use graph::web3::types::Address;

use module::{WasmiModule, WasmiModuleConfig};

#[derive(Clone)]
pub struct RuntimeHostConfig {
    subgraph_manifest: SubgraphManifest,
    data_source: DataSource,
}

pub struct RuntimeHostBuilder<T, L, S> {
    logger: Logger,
    ethereum_adapter: Arc<Mutex<T>>,
    link_resolver: Arc<L>,
    store: Arc<Mutex<S>>,
}

impl<T, L, S> Clone for RuntimeHostBuilder<T, L, S>
where
    T: EthereumAdapter,
    L: LinkResolver,
    S: Store,
{
    fn clone(&self) -> Self {
        RuntimeHostBuilder {
            logger: self.logger.clone(),
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
    pub fn new(
        logger: &Logger,
        ethereum_adapter: Arc<Mutex<T>>,
        link_resolver: Arc<L>,
        store: Arc<Mutex<S>>,
    ) -> Self {
        RuntimeHostBuilder {
            logger: logger.new(o!("component" => "RuntimeHostBuilder")),
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
    S: Store + 'static,
{
    type Host = RuntimeHost;

    fn build(&self, subgraph_manifest: SubgraphManifest, data_source: DataSource) -> Self::Host {
        RuntimeHost::new(
            &self.logger,
            self.ethereum_adapter.clone(),
            self.link_resolver.clone(),
            self.store.clone(),
            RuntimeHostConfig {
                subgraph_manifest,
                data_source,
            },
        )
    }
}

type HandleEventResponse = Result<Vec<EntityOperation>, Error>;

type HandleEventRequest = (
    EthereumEvent,
    MappingEventHandler,
    oneshot::Sender<HandleEventResponse>,
);

pub struct RuntimeHost {
    config: RuntimeHostConfig,
    logger: Logger,
    handle_event_sender: Sender<HandleEventRequest>,
    _guard: oneshot::Sender<()>,
}

impl RuntimeHost {
    pub fn new<T, L, S>(
        logger: &Logger,
        ethereum_adapter: Arc<Mutex<T>>,
        link_resolver: Arc<L>,
        store: Arc<Mutex<S>>,
        config: RuntimeHostConfig,
    ) -> Self
    where
        T: EthereumAdapter,
        L: LinkResolver,
        S: Store + 'static,
    {
        let logger = logger.new(o!(
            "component" => "RuntimeHost",
            "subgraph_id" => config.subgraph_manifest.id.clone(),
            "data_source" => config.data_source.name.clone(),
        ));

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
        let module_config = config.clone();
        thread::spawn(move || {
            info!(module_logger, "Start WASM runtime");

            // Load the mapping of the data source as a WASM module
            let wasmi_config = WasmiModuleConfig {
                subgraph: module_config.subgraph_manifest.clone(),
                data_source: module_config.data_source.clone(),
                ethereum_adapter: ethereum_adapter.clone(),
                link_resolver: link_resolver.clone(),
                store: store.clone(),
            };

            let name = module_config.data_source.name.clone();

            // Start the mapping as a WASM module
            let module = WasmiModule::new(&module_logger, wasmi_config, task_sender);

            // Pass incoming events to the WASM module and send entity changes back;
            // stop when cancelled from the outside
            let event_logger = module_logger.clone();
            handle_event_receiver
                .map(Some)
                .select(cancel_receiver.into_stream().map(|_| None).map_err(|_| ()))
                .for_each(move |request: Option<HandleEventRequest>| {
                    if let Some(request) = request {
                        let (event, handler, response_sender) = request;

                        debug!(event_logger, "  Call event handler";
                               "name" => &handler.handler,
                               "signature" => &handler.event);

                        let result = module.handle_ethereum_event(handler.handler.as_str(), event);
                        future::result(response_sender.send(result).map_err(|_| ()))
                    } else {
                        future::err(())
                    }
                }).wait()
                .ok();
        });

        RuntimeHost {
            config,
            logger,
            handle_event_sender,
            _guard: cancel_sender,
        }
    }
}

impl RuntimeHostTrait for RuntimeHost {
    fn subgraph_manifest(&self) -> &SubgraphManifest {
        &self.config.subgraph_manifest
    }

    fn matches_event(&self, event: &EthereumEvent) -> bool {
        self.config
            .data_source
            .mapping
            .event_handlers
            .iter()
            .find(|event_handler| {
                event.event_signature
                    == util::ethereum::string_to_h256(event_handler.event.as_str())
            }).is_some()
    }

    fn process_event(
        &self,
        event: EthereumEvent,
    ) -> Box<Future<Item = Vec<EntityOperation>, Error = Error> + Send> {
        let handler = self
            .config
            .data_source
            .mapping
            .event_handlers
            .iter()
            .find(|event_handler| {
                event.event_signature
                    == util::ethereum::string_to_h256(event_handler.event.as_str())
            });

        match handler {
            Some(handler) => {
                debug!(self.logger, "Process Ethereum event"; "signature" => &handler.event);

                let (result_sender, result_receiver) = oneshot::channel();

                let before_event_signature = handler.event.clone();
                let event_signature = handler.event.clone();

                Box::new(
                    self.handle_event_sender
                        .clone()
                        .send((event, handler.clone(), result_sender))
                        .map_err(move |_| {
                            format_err!(
                                "Mapping terminated before passing in Ethereum event: {}",
                                before_event_signature
                            )
                        }).and_then(|_| {
                            result_receiver.map_err(move |_| {
                                format_err!(
                                    "Mapping terminated before finishing to handle \
                                     Ethereum event: {}",
                                    event_signature,
                                )
                            })
                        }).and_then(|result| result),
                )
            }
            None => Box::new(future::err(format_err!(
                "Ethereum event not mentioned in the data source"
            ))),
        }
    }
}
