use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use slog::Logger;
use std::sync::{Arc, Mutex};
use tokio_core::reactor::Handle;

use thegraph::components::data_sources::RuntimeHostEvent;
use thegraph::components::ethereum::*;
use thegraph::prelude::{
    RuntimeHost as RuntimeHostTrait, RuntimeHostBuilder as RuntimeHostBuilderTrait, *,
};

use module::WasmiModule;

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
            runtime: runtime,
            event_sender,
            output: Some(event_receiver),
            ethereum_watcher,
            module: None,
        }
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

        // Obtain mapping location
        let location = self.config
            .data_source_definition
            .datasets
            .first()
            .expect("Data source must contain at least one data set")
            .mapping
            .source
            .path
            .to_string();

        // Load the mappings as a WASM module
        self.module = Some(WasmiModule::new(location, self.event_sender.clone()));
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
