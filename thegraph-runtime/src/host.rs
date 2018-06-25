use ethereum_types::Address;
use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use slog::Logger;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tokio_core::reactor::Handle;
use uuid::Uuid;

use thegraph::components::data_sources::RuntimeHostEvent;
use thegraph::components::ethereum::*;
use thegraph::prelude::{
    RuntimeHost as RuntimeHostTrait, RuntimeHostBuilder as RuntimeHostBuilderTrait, *,
};

use module::{WasmiModule, WasmiModuleConfig};

pub struct RuntimeHostConfig {
    pub data_source_definition: DataSourceDefinition,
}

pub struct RuntimeHostBuilder<T>
where
    T: EthereumWatcher,
{
    logger: Logger,
    runtime: Handle,
    ethereum_watcher: Arc<Mutex<T>>,
}

impl<T> RuntimeHostBuilder<T>
where
    T: EthereumWatcher,
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
    T: EthereumWatcher,
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
    T: EthereumWatcher,
{
    config: RuntimeHostConfig,
    logger: Logger,
    runtime: Handle,
    event_sender: Sender<RuntimeHostEvent>,
    output: Option<Receiver<RuntimeHostEvent>>,
    ethereum_watcher: Arc<Mutex<T>>,
    module: Option<WasmiModule>,
}

impl<T> RuntimeHost<T>
where
    T: EthereumWatcher,
{
    pub fn new(
        logger: &Logger,
        runtime: Handle,
        ethereum_watcher: Arc<Mutex<T>>,
        config: RuntimeHostConfig,
    ) -> Self {
        let (event_sender, event_receiver) = channel(100);

        RuntimeHost {
            config,
            logger: logger.new(o!("component" => "RuntimeHost")),
            runtime,
            event_sender,
            output: Some(event_receiver),
            ethereum_watcher,
            module: None,
        }
    }

    fn subscribe_to_event(&mut self, subscription: EthereumEventSubscription) {
        debug!(self.logger, "Subscribe to event"; "event" => format!("{:#?}", subscription));

        let receiver = self.ethereum_watcher
            .lock()
            .unwrap()
            .subscribe_to_event(subscription);

        let logger = self.logger.clone();

        self.runtime.spawn(receiver.for_each(move |event| {
            debug!(logger, "Handle Ethereum event: {:?}", event);
            Ok(())
        }));
    }
}

impl<T> EventProducer<RuntimeHostEvent> for RuntimeHost<T>
where
    T: EthereumWatcher,
{
    type EventStream = Receiver<RuntimeHostEvent>;

    fn take_event_stream(&mut self) -> Option<Self::EventStream> {
        self.output.take()
    }
}

impl<T> RuntimeHostTrait for RuntimeHost<T>
where
    T: EthereumWatcher,
{
    fn start(&mut self) {
        info!(self.logger, "Start");

        // Obtain the first data set (NOTE: We'll add support for multiple datasets soon).
        let subscriptions: Vec<EthereumEventSubscription> = {
            let dataset = self.config
                .data_source_definition
                .datasets
                .first()
                .expect("Data source must contain at least one data set");

            // Obtain the contract address of the first data set
            let address = Address::from_str(dataset.data.address.as_str())
                .expect("Failed to parse contract address");

            // Prepare subscriptions for all events
            dataset
                .mapping
                .event_handlers
                .iter()
                .map(|event_handler| {
                    let subscription_id = Uuid::new_v4().simple().to_string();

                    EthereumEventSubscription {
                        subscription_id: subscription_id.clone(),
                        address,
                        event_signature: event_handler.event.clone(),
                        range: BlockNumberRange {
                            from: Some(0),
                            to: None,
                        },
                    }
                })
                .collect()
        };

        // Subscribe to the events
        for subscription in subscriptions.into_iter() {
            self.subscribe_to_event(subscription);
        }

        // Obtain mapping location
        let location = self.config.data_source_definition.resolve_path(
            &self.config
                .data_source_definition
                .datasets
                .first()
                .unwrap()
                .mapping
                .source
                .path,
        );

        info!(self.logger, "Load WASM runtime from"; "file" => location.to_str());

        // Load the mappings as a WASM module
        self.module = Some(WasmiModule::new(
            location,
            &self.logger,
            WasmiModuleConfig {
                data_source_id: String::from("TODO DATA SOURCE ID"),
                runtime: self.runtime.clone(),
                event_sink: self.event_sender.clone(),
            },
        ));
    }

    fn stop(&mut self) {
        info!(self.logger, "Stop");

        // Drop the WASM module
        self.module = None;
    }

    fn data_source_definition(&self) -> &DataSourceDefinition {
        &self.config.data_source_definition
    }
}
