use ethereum_types::Address;
use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver};
use slog::Logger;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tokio_core::reactor::Handle;
use uuid::Uuid;

use graph::components::ethereum::*;
use graph::components::subgraph::RuntimeHostEvent;
use graph::data::subgraph::DataSource;
use graph::prelude::{
    RuntimeHost as RuntimeHostTrait, RuntimeHostBuilder as RuntimeHostBuilderTrait, *,
};
use graph::util;

use module::{WasmiModule, WasmiModuleConfig};

#[derive(Clone)]
pub struct RuntimeHostConfig {
    subgraph_manifest: SubgraphManifest,
    data_source: DataSource,
}

pub struct RuntimeHostBuilder<T, L> {
    logger: Logger,
    runtime: Handle,
    ethereum_adapter: Arc<Mutex<T>>,
    link_resolver: Arc<L>,
}

impl<T, L> RuntimeHostBuilder<T, L>
where
    T: EthereumAdapter,
    L: LinkResolver,
{
    pub fn new(
        logger: &Logger,
        runtime: Handle,
        ethereum_adapter: Arc<Mutex<T>>,
        link_resolver: Arc<L>,
    ) -> Self {
        RuntimeHostBuilder {
            logger: logger.new(o!("component" => "RuntimeHostBuilder")),
            runtime,
            ethereum_adapter,
            link_resolver,
        }
    }
}

impl<T, L> RuntimeHostBuilderTrait for RuntimeHostBuilder<T, L>
where
    T: EthereumAdapter + 'static,
    L: LinkResolver + 'static,
{
    type Host = RuntimeHost;

    fn build(
        &mut self,
        subgraph_manifest: SubgraphManifest,
        data_source: DataSource,
    ) -> Self::Host {
        RuntimeHost::new(
            &self.logger,
            self.runtime.clone(),
            self.ethereum_adapter.clone(),
            self.link_resolver.clone(),
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
}

impl RuntimeHost {
    pub fn new<T, L>(
        logger: &Logger,
        runtime: Handle,
        ethereum_adapter: Arc<Mutex<T>>,
        link_resolver: Arc<L>,
        config: RuntimeHostConfig,
    ) -> Self
    where
        T: EthereumAdapter + 'static,
        L: LinkResolver + 'static,
    {
        let logger = logger.new(o!("component" => "RuntimeHost"));

        // Create channel for sending runtime host events
        let (event_sender, event_receiver) = channel(100);

        info!(logger, "Loading WASM runtime"; "data_source" => &config.data_source.name);

        // Load the mappings as a WASM module
        let module = WasmiModule::new(
            &logger,
            WasmiModuleConfig {
                subgraph: config.subgraph_manifest.clone(),
                data_source: config.data_source.clone(),
                runtime: runtime.clone(),
                event_sink: event_sender,
                ethereum_adapter: ethereum_adapter.clone(),
                link_resolver: link_resolver.clone(),
            },
        );

        Self::subscribe_to_events(
            logger,
            runtime,
            config.data_source.clone(),
            module,
            ethereum_adapter,
        );

        RuntimeHost {
            config,
            output: Some(event_receiver),
        }
    }

    /// Subscribe to all smart contract events of `data_source` contained in
    /// `subgraph`.
    fn subscribe_to_events<T, L>(
        logger: Logger,
        runtime: Handle,
        data_source: DataSource,
        module: WasmiModule<T, L>,
        ethereum_adapter: Arc<Mutex<T>>,
    ) where
        T: EthereumAdapter + 'static,
        L: LinkResolver + 'static,
    {
        info!(logger, "Subscribe to events");

        // Obtain the contract address of the first data set
        let address = Address::from_str(data_source.source.address.as_str())
            .expect("Failed to parse contract address");

        // Load the main dataset contract
        let contract = data_source
            .mapping
            .abis
            .iter()
            .find(|abi| abi.name == data_source.source.abi)
            .expect("No ABI entry found for the main contract of the dataset")
            .contract
            .clone();

        // Prepare subscriptions for the events
        let subscription_results: Vec<EthereumEventSubscription> = {
            // Prepare subscriptions for all events
            data_source
                .mapping
                .event_handlers
                .iter()
                .map(|event_handler| {
                    let subscription_id = Uuid::new_v4().simple().to_string();
                    let event = util::ethereum::contract_event_with_signature(
                        &contract,
                        event_handler.event.as_str(),
                    ).expect(
                        format!("Event not found in contract: {}", event_handler.event).as_str(),
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
                .collect()
        };

        // Protect variables for concurrent access
        let protected_module = Arc::new(Mutex::new(module));
        let protected_data_source = Arc::new(data_source);

        // Subscribe to the events now
        for subscription in subscription_results.into_iter() {
            Self::subscribe_to_event(
                logger.clone(),
                runtime.clone(),
                protected_module.clone(),
                ethereum_adapter.clone(),
                protected_data_source.clone(),
                subscription,
            );
        }
    }

    fn subscribe_to_event<T, L>(
        logger: Logger,
        runtime: Handle,
        module: Arc<Mutex<WasmiModule<T, L>>>,
        ethereum_adapter: Arc<Mutex<T>>,
        dataset: Arc<DataSource>,
        subscription: EthereumEventSubscription,
    ) where
        T: EthereumAdapter + 'static,
        L: LinkResolver + 'static,
    {
        info!(logger, "Subscribe to event"; "name" => &subscription.event.name);

        let receiver = ethereum_adapter
            .lock()
            .unwrap()
            .subscribe_to_event(subscription);

        let event_logger = logger.clone();
        let error_logger = logger.clone();
        runtime.spawn(
            receiver
                .for_each(move |event| {
                    info!(event_logger, "Ethereum event received");

                    if event.removed {
                        info!(event_logger, "Event removed";
                              "block" => event.block_hash.to_string());
                    } else {
                        let event_handler = dataset
                            .mapping
                            .event_handlers
                            .iter()
                            .find(|event_handler| {
                                util::ethereum::string_to_h256(event_handler.event.as_str())
                                    == event.event_signature
                            })
                            .expect("Received an Ethereum event not mentioned in the data set")
                            .to_owned();

                        debug!(event_logger, "  Call event handler";
                               "name" => &event_handler.handler);

                        module
                            .lock()
                            .unwrap()
                            .handle_ethereum_event(event_handler.handler.as_str(), event);
                    }

                    Ok(())
                })
                .map_err(move |e| error!(error_logger, "Event subscription failed: {}", e)),
        );
    }
}

impl EventProducer<RuntimeHostEvent> for RuntimeHost {
    fn take_event_stream(&mut self) -> Option<Box<Stream<Item = RuntimeHostEvent, Error = ()>>> {
        self.output
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = RuntimeHostEvent, Error = ()>>)
    }
}

impl RuntimeHostTrait for RuntimeHost {
    fn subgraph_manifest(&self) -> &SubgraphManifest {
        &self.config.subgraph_manifest
    }
}
