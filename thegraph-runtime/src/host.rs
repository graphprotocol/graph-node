use ethereum_types::Address;
use futures::future;
use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver};
use slog::Logger;
use std::fs;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tokio_core::reactor::Handle;
use uuid::Uuid;

use thegraph::components::data_sources::RuntimeHostEvent;
use thegraph::components::ethereum::*;
use thegraph::prelude::{
    RuntimeHost as RuntimeHostTrait, RuntimeHostBuilder as RuntimeHostBuilderTrait, *,
};
use thegraph::util;

use module::{WasmiModule, WasmiModuleConfig};

pub struct RuntimeHostConfig {
    pub data_source_definition: DataSourceDefinition,
}

pub struct RuntimeHostBuilder<T>
where
    T: EthereumAdapter,
{
    logger: Logger,
    runtime: Handle,
    ethereum_watcher: Arc<Mutex<T>>,
}

impl<T> RuntimeHostBuilder<T>
where
    T: EthereumAdapter,
{
    pub fn new(logger: &Logger, runtime: Handle, ethereum_watcher: Arc<Mutex<T>>) -> Self {
        RuntimeHostBuilder {
            logger: logger.new(o!("component" => "RuntimeHostBuilder")),
            runtime,
            ethereum_watcher,
        }
    }
}

impl<T> RuntimeHostBuilderTrait for RuntimeHostBuilder<T>
where
    T: EthereumAdapter,
{
    type Host = RuntimeHost<T>;

    fn build(&mut self, data_source_definition: DataSourceDefinition) -> Self::Host {
        RuntimeHost::new(
            &self.logger,
            self.runtime.clone(),
            self.ethereum_watcher.clone(),
            RuntimeHostConfig {
                data_source_definition,
            },
        )
    }
}

pub struct RuntimeHost<T>
where
    T: EthereumAdapter,
{
    config: RuntimeHostConfig,
    logger: Logger,
    runtime: Handle,
    output: Option<Receiver<RuntimeHostEvent>>,
    ethereum_watcher: Arc<Mutex<T>>,
    module: WasmiModule,
}

impl<T> RuntimeHost<T>
where
    T: EthereumAdapter,
{
    pub fn new(
        logger: &Logger,
        runtime: Handle,
        ethereum_watcher: Arc<Mutex<T>>,
        config: RuntimeHostConfig,
    ) -> Self {
        let logger = logger.new(o!("component" => "RuntimeHost"));

        // Create channel for sending runtime host events
        let (event_sender, event_receiver) = channel(100);

        // Obtain mapping location
        let location = config.data_source_definition.resolve_path(
            &config
                .data_source_definition
                .datasets
                .first()
                .unwrap()
                .mapping
                .source
                .path,
        );

        info!(logger, "Load WASM runtime from"; "file" => location.to_str());

        // Load the mappings as a WASM module
        let module = WasmiModule::new(
            location,
            &logger,
            WasmiModuleConfig {
                data_source_id: String::from("TODO: DATA SOURCE ID"),
                runtime: runtime.clone(),
                event_sink: event_sender,
                ethereum_watcher: ethereum_watcher.clone(),
            },
        );

        let mut host = RuntimeHost {
            config,
            logger,
            runtime,
            output: Some(event_receiver),
            ethereum_watcher,
            module,
        };

        host.subscribe_to_events();
        host
    }

    /// Subscribe to all smart contract events of the first data set
    /// in the data source definition.
    ///
    /// NOTE: We'll add support for multiple datasets soon.
    fn subscribe_to_events(&mut self) {
        // Prepare subscriptions for the events
        let subscription_results: Vec<EthereumEventSubscription> = {
            let dataset = self.config
                .data_source_definition
                .datasets
                .first()
                .expect("Data source must contain at least one data set");

            // Obtain the contract address of the first data set
            let address = Address::from_str(dataset.data.address.as_str())
                .expect("Failed to parse contract address");

            // Load the main dataset contract
            let contract_abi_path = dataset
                .mapping
                .abis
                .iter()
                .find(|abi| abi.name == dataset.data.structure.abi)
                .map(|entry| {
                    self.config
                        .data_source_definition
                        .resolve_path(&entry.source.path)
                })
                .expect("No ABI entry found for the main contract of the dataset");
            let contract_abi_file = fs::File::open(contract_abi_path)
                .expect("Contract ABI file does not exist or could not be opend for reading");
            let contract = Contract::load(contract_abi_file).expect("Failed to parse contract ABI");

            // Prepare subscriptions for all events
            dataset
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

        // Subscribe to the events now
        for subscription in subscription_results.into_iter() {
            self.subscribe_to_event(subscription);
        }
    }

    fn subscribe_to_event(&mut self, subscription: EthereumEventSubscription) {
        debug!(self.logger, "Subscribe to event"; "event" => format!("{:#?}", subscription));

        let receiver = self.ethereum_watcher
            .lock()
            .unwrap()
            .subscribe_to_event(subscription);

        self.runtime.spawn(receiver.for_each(|event| {
            self.module.handle_ethereum_event(event);
        }));
    }
}

impl<T> EventProducer<RuntimeHostEvent> for RuntimeHost<T>
where
    T: EthereumAdapter,
{
    type EventStream = Receiver<RuntimeHostEvent>;

    fn take_event_stream(&mut self) -> Option<Self::EventStream> {
        self.output.take()
    }
}

impl<T> RuntimeHostTrait for RuntimeHost<T>
where
    T: EthereumAdapter,
{
    fn data_source_definition(&self) -> &DataSourceDefinition {
        &self.config.data_source_definition
    }
}
