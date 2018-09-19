use futures::sync::mpsc::{channel, Receiver};
use futures::sync::oneshot;
use std::str::FromStr;
use std::sync::Mutex;
use std::thread;
use uuid::Uuid;

use graph::components::ethereum::*;
use graph::components::store::Store;
use graph::components::subgraph::RuntimeHostEvent;
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

    fn build(
        &mut self,
        subgraph_manifest: SubgraphManifest,
        data_source: DataSource,
    ) -> Self::Host {
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

pub struct RuntimeHost {
    config: RuntimeHostConfig,
    output: Option<Receiver<RuntimeHostEvent>>,
    // Never accessed, it's purpose is to signal cancelation on drop.
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
        let logger = logger.new(o!("component" => "RuntimeHost"));

        // Create channel for sending runtime host events
        let (event_sender, event_receiver) = channel(100);

        let wasmi_config = WasmiModuleConfig {
            subgraph: config.subgraph_manifest.clone(),
            data_source: config.data_source.clone(),
            event_sink: event_sender,
            ethereum_adapter: ethereum_adapter.clone(),
            link_resolver: link_resolver.clone(),
            store: store.clone(),
        };

        let name = config.data_source.name.clone();
        info!(logger, "Loading WASM runtime"; "data_source" => &name);

        // Create channel as cancelation guard.
        let (cancel_sender, cancel_receiver) = oneshot::channel();

        // wasmi modules are not `Send` therefore they cannot be scheduled by
        // the regular tokio executor, so we create a dedicated thread.
        //
        // This thread can spawn tasks on the runtime by sending them to
        // `task_receiver`.
        let (task_sender, task_receiver) = channel(100);
        tokio::spawn(task_receiver.for_each(tokio::spawn));
        thread::spawn(move || {
            let data_source = wasmi_config.data_source.clone();

            // Load the mappings as a WASM module
            let module = WasmiModule::new(&logger, wasmi_config, task_sender);

            // Process one event at a time, blocking the thread when waiting for
            // the next event. Also check for a cancelation signal.
            Self::subscribe_to_events(&logger, data_source, module, ethereum_adapter)
                .select(
                    cancel_receiver
                        .into_stream()
                        .map(|_| panic!("sent into cancel guard"))
                        .map_err(|_| ()),
                ).for_each(|_| Ok(()))
                .wait()
                .ok();

            info!(logger, "shutting down WASM runtime"; "data_source" => name);
        });

        RuntimeHost {
            config,
            output: Some(event_receiver),
            _guard: cancel_sender,
        }
    }

    /// Subscribe to all smart contract events of `data_source` contained in
    /// `subgraph`.
    fn subscribe_to_events<T, L, S, U>(
        logger: &Logger,
        data_source: DataSource,
        mut module: WasmiModule<T, L, S, U>,
        ethereum_adapter: Arc<Mutex<T>>,
    ) -> impl Stream<Item = (), Error = ()> + 'static
    where
        T: EthereumAdapter + 'static,
        L: LinkResolver + 'static,
        S: Store + 'static,
        U: Sink<SinkItem = Box<Future<Item = (), Error = ()> + Send>> + Clone + 'static,
    {
        info!(logger, "Subscribe to events");

        // Obtain the contract address of the data set.
        let address = Address::from_str(data_source.source.address.as_str())
            .expect("Failed to parse contract address");

        // Load the main dataset contract.
        let contract = data_source
            .mapping
            .abis
            .iter()
            .find(|abi| abi.name == data_source.source.abi)
            .expect("No ABI entry found for the main contract of the dataset")
            .contract
            .clone();

        // Prepare subscriptions for all events.
        let event_stream = {
            let subscription_results = {
                data_source
                    .mapping
                    .event_handlers
                    .iter()
                    .map(|event_handler| {
                        debug!(logger, "Prepare subscription";
                                "event" => &event_handler.event,
                                "handler" => &event_handler.handler,
                            );
                        let subscription_id = Uuid::new_v4().simple().to_string();
                        let event = util::ethereum::contract_event_with_signature(
                            &contract,
                            event_handler.event.as_str(),
                        ).expect(
                            format!("Event not found in contract: {}", event_handler.event)
                                .as_str(),
                        );

                        EthereumEventSubscription {
                            address,
                            event: event.clone(),
                            range: BlockNumberRange {
                                from: BlockNumber::Number(0),
                                to: BlockNumber::Latest,
                            },
                            subscription_id: subscription_id.clone(),
                        }
                    })
            };

            // Merge all event streams.
            let mut event_stream: Box<Stream<Item = _, Error = _>> = Box::new(stream::empty());
            for subscription in subscription_results {
                info!(logger, "Subscribe to event";
                      "name" => &subscription.event.name,
                      "subscription_id" => &subscription.subscription_id);
                event_stream = Box::new(
                    event_stream.select(
                        ethereum_adapter
                            .lock()
                            .unwrap()
                            .subscribe_to_event(subscription),
                    ),
                );
            }
            event_stream
        };

        let event_logger = logger.clone();
        let error_logger = logger.clone();

        event_stream
            .map(move |event| {
                info!(event_logger, "Ethereum event received";
                      "signature" => event.event_signature.to_string(),
                    );

                if event.removed {
                    info!(event_logger, "Event removed";
                          "block" => event.block_hash.to_string());
                } else {
                    let event_handler = data_source
                        .mapping
                        .event_handlers
                        .iter()
                        .find(|event_handler| {
                            util::ethereum::string_to_h256(event_handler.event.as_str())
                                == event.event_signature
                        }).expect("Received an Ethereum event not mentioned in the data set")
                        .to_owned();

                    debug!(event_logger, "  Call event handler";
                           "name" => &event_handler.handler,
                           "signature" => &event_handler.event);

                    module.handle_ethereum_event(event_handler.handler.as_str(), event);
                }
            }).map_err(move |e| error!(error_logger, "Event subscription failed: {}", e))
    }
}

impl EventProducer<RuntimeHostEvent> for RuntimeHost {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<Stream<Item = RuntimeHostEvent, Error = ()> + Send>> {
        self.output
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = RuntimeHostEvent, Error = ()> + Send>)
    }
}

impl RuntimeHostTrait for RuntimeHost {
    fn subgraph_manifest(&self) -> &SubgraphManifest {
        &self.config.subgraph_manifest
    }
}
