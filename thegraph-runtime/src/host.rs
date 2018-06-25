use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use slog::Logger;
use std::sync::{Arc, Mutex};
use tokio_core::reactor::Handle;

use thegraph::components::data_sources::{RuntimeHostEvent, RuntimeHostBuilder as RuntimeHostBuilderTrait};
use thegraph::components::ethereum::*;
use thegraph::prelude::RuntimeHost as RuntimeHostTrait;
use thegraph::util::stream::StreamError;
use thegraph_store_postgres_diesel::{Store as DieselStore};
use thegraph::prelude::BasicStore;

use module::WasmiModule;

pub struct RuntimeHostConfig {
    pub data_source_definition: String,
}

pub struct RuntimeHostBuilder {
    logger: Logger,
    runtime: Handle,
    ethereum_watcher: Arc<Mutex<EthereumWatcher>>,
    store: Arc<Mutex<DieselStore>>,
}

impl RuntimeHostBuilder {
    pub fn new(
        logger: &Logger,
        runtime: Handle,
        ethereum_watcher: Arc<Mutex<EthereumWatcher>>,
        store: Arc<Mutex<DieselStore>>,
    ) -> Self {
        RuntimeHostBuilder {
            logger: logger.new(o!("component" => "RuntimeHostBuilder")),
            runtime,
            ethereum_watcher,
            store,
        }
    }
}

impl RuntimeHostBuilderTrait for RuntimeHostBuilder {
    fn create_host(&mut self, source_location: String) {
        let config = RuntimeHostConfig {
            data_source_definition: source_location,
        };
        let mut data_source_runtime_host =
            RuntimeHost::new(&self.logger, self.runtime, self.ethereum_watcher, config);

        self.runtime.spawn({
            data_source_runtime_host
                .event_stream()
                .unwrap()
                .for_each(move |event| {
                    let mut store = self.store.lock().unwrap();

                    match event {
                        RuntimeHostEvent::EntityCreated(_, k, entity) => {
                            store
                                .set(k, entity)
                                .expect("Failed to set entity in the store");
                        }
                        RuntimeHostEvent::EntityChanged(_, k, entity) => {
                            store
                                .set(k, entity)
                                .expect("Failed to set entity in the store");
                        }
                        RuntimeHostEvent::EntityRemoved(_, k) => {
                            store
                                .delete(k)
                                .expect("Failed to remove entity from the store");
                        }
                    };
                    Ok(())
                })
                .and_then(|_| Ok(()))
        });

        data_source_runtime_host.start();
    }
}

pub struct RuntimeHost<T>
where
    T: EthereumWatcher,
{
    config: RuntimeHostConfig,
    _runtime: Handle,
    logger: Logger,
    event_sink: Arc<Mutex<Option<Sender<RuntimeHostEvent>>>>,
    ethereum_watcher: Arc<Mutex<T>>,
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
        let (sender, _receiver) = channel(100);
        RuntimeHost {
            config,
            _runtime: runtime,
            logger: logger.new(o!("component" => "RuntimeHost")),
            event_sink: Arc::new(Mutex::new(Some(sender))),
            ethereum_watcher,
        }
    }
}

impl<T> RuntimeHostTrait for RuntimeHost<T>
where
    T: EthereumWatcher,
{
    fn start(&mut self) {
        info!(self.logger, "Start");

        let event_sink = self.event_sink
            .lock()
            .unwrap()
            .clone()
            .expect("Runtime started without event sink");

        // Instantiate Wasmi module
        info!(self.logger, "Instantiate wasm module from file");
        let _wasmi_module = WasmiModule::new(self.config.data_source_definition, event_sink);
    }

    fn stop(&mut self) {
        info!(self.logger, "Stop");
    }

    fn event_stream(&mut self) -> Result<Receiver<RuntimeHostEvent>, StreamError> {
        // If possible, create a new channel for streaming runtime host events
        let mut event_sink = self.event_sink.lock().unwrap();
        match *event_sink {
            Some(_) => Err(StreamError::AlreadyCreated),
            None => {
                let (sink, stream) = channel(100);
                *event_sink = Some(sink);
                Ok(stream)
            }
        }
    }
}
