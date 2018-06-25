use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use slog::Logger;
use std::sync::{Arc, Mutex};
use tokio_core::reactor::Handle;

use thegraph::components::data_sources::DataSourceProviderEvent;
use thegraph::components::data_sources::RuntimeHostEvent;
use thegraph::prelude::*;

pub struct RuntimeManager<S, T>
where
    S: Store,
    T: RuntimeHostBuilder,
{
    logger: Logger,
    runtime: Handle,
    store: Arc<Mutex<S>>,
    runtime_host_builder: Arc<Mutex<T>>,
    runtime_hosts: Arc<Mutex<Vec<T::Host>>>,
    input: Sender<DataSourceProviderEvent>,
}

impl<S, T> RuntimeManager<S, T>
where
    S: Store + 'static,
    T: RuntimeHostBuilder + 'static,
{
    /// Creates a new runtime manager.
    pub fn new(logger: &Logger, runtime: Handle, store: Arc<Mutex<S>>, host_builder: T) -> Self {
        let logger = logger.new(o!("component" => "RuntimeManager"));

        // Create channel for receiving data source provider events.
        let (data_source_sender, data_source_receiver) = channel(100);

        let mut manager = RuntimeManager {
            logger,
            runtime,
            store,
            input: data_source_sender,
            runtime_host_builder: Arc::new(Mutex::new(host_builder)),
            runtime_hosts: Arc::new(Mutex::new(vec![])),
        };

        // Handle incoming events from the data source provider.
        manager.handle_data_source_events(data_source_receiver);

        manager
    }

    /// Handle incoming events from data source providers.
    fn handle_data_source_events(&mut self, receiver: Receiver<DataSourceProviderEvent>) {
        let runtime = self.runtime.clone();

        let runtime_hosts = self.runtime_hosts.clone();
        let runtime_host_builder = self.runtime_host_builder.clone();

        let store = self.store.clone();

        self.runtime.spawn(receiver.for_each(move |event| {
            let store = store.clone();

            match event {
                DataSourceProviderEvent::DataSourceAdded(definition) => {
                    // Create a new runtime host for the data source definition
                    let mut new_host = runtime_host_builder.lock().unwrap().build(definition);

                    // Forward events from the runtime host to the store
                    runtime
                        .clone()
                        .spawn(
                            new_host
                                .take_event_stream()
                                .unwrap()
                                .for_each(move |event| {
                                    match event {
                                        RuntimeHostEvent::EntityCreated(
                                            _data_source_id,
                                            store_key,
                                            entity,
                                        ) => {
                                            store
                                                .lock()
                                                .unwrap()
                                                .set(store_key, entity)
                                                .expect("Failed to create entity in the store");
                                        }
                                        RuntimeHostEvent::EntityChanged(
                                            _data_source_id,
                                            store_key,
                                            entity,
                                        ) => {
                                            store
                                                .lock()
                                                .unwrap()
                                                .set(store_key, entity)
                                                .expect("Failed to update entity in the store");
                                        }
                                        RuntimeHostEvent::EntityRemoved(
                                            _data_source_id,
                                            store_key,
                                        ) => {
                                            store
                                                .lock()
                                                .unwrap()
                                                .delete(store_key)
                                                .expect("Failed to delete entity from the store");
                                        }
                                    }

                                    Ok(())
                                }),
                        );

                    // Start the hosted runtime
                    new_host.start();

                    // Add the new host to the list of managed runtime hosts
                    runtime_hosts.lock().unwrap().push(new_host);
                }
                DataSourceProviderEvent::DataSourceRemoved(ref definition) => {
                    let mut hosts = runtime_hosts.lock().unwrap();

                    {
                        // Find the runtime host for the data source
                        let host = hosts
                            .iter_mut()
                            .find(|host| host.data_source_definition() == definition);

                        // If we have a runtime host, stop it now
                        if host.is_some() {
                            host.unwrap().stop();
                        }
                    }

                    // Destroy all runtime hosts for this data source
                    hosts.retain(|host| host.data_source_definition() != definition);
                }
            }

            Ok(())
        }))
    }
}

impl<S, T> EventConsumer<DataSourceProviderEvent> for RuntimeManager<S, T>
where
    S: Store,
    T: RuntimeHostBuilder,
{
    type EventSink = Box<Sink<SinkItem = DataSourceProviderEvent, SinkError = ()>>;

    /// Get the wrapped event sink.
    fn event_sink(&self) -> Self::EventSink {
        let logger = self.logger.clone();
        Box::new(self.input.clone().sink_map_err(move |e| {
            error!(logger, "Component was dropped: {}", e);
        }))
    }
}
