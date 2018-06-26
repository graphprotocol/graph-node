use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use slog::Logger;
use std::sync::{Arc, Mutex};
use tokio_core::reactor::Handle;

use thegraph::components::data_sources::DataSourceProviderEvent;
use thegraph::components::data_sources::RuntimeHostEvent;
use thegraph::prelude::*;

pub struct RuntimeManager {
    logger: Logger,
    input: Sender<DataSourceProviderEvent>,
}

impl RuntimeManager where {
    /// Creates a new runtime manager.
    pub fn new<S, T>(
        logger: &Logger,
        runtime: Handle,
        store: Arc<Mutex<S>>,
        host_builder: T,
    ) -> Self
    where
        S: Store + 'static,
        T: RuntimeHostBuilder + 'static,
    {
        let logger = logger.new(o!("component" => "RuntimeManager"));

        // Create channel for receiving data source provider events.
        let (data_source_sender, data_source_receiver) = channel(100);

        // Handle incoming events from the data source provider.
        Self::handle_data_source_events(
            logger.clone(),
            runtime,
            store,
            host_builder,
            data_source_receiver,
        );

        RuntimeManager {
            logger,
            input: data_source_sender,
        }
    }

    /// Handle incoming events from data source providers.
    fn handle_data_source_events<S, T>(
        logger: Logger,
        runtime: Handle,
        store: Arc<Mutex<S>>,
        mut host_builder: T,
        receiver: Receiver<DataSourceProviderEvent>,
    ) where
        S: Store + 'static,
        T: RuntimeHostBuilder + 'static,
    {
        let mut runtime_hosts = vec![];

        runtime.clone().spawn(receiver.for_each(move |event| {
            match event {
                DataSourceProviderEvent::DataSourceAdded(definition) => {
                    info!(logger, "Data source created, host runtime";
                          "location" => &definition.location);

                    // Create a new runtime host for the data source definition
                    let mut new_host = host_builder.build(definition);

                    // Forward events from the runtime host to the store; this
                    // Tokio task will terminate when the corresponding data source
                    // is removed and the host and its event sender are dropped
                    let store = store.clone();
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

                    // Add the new host to the list of managed runtime hosts
                    runtime_hosts.push(new_host);
                }
                DataSourceProviderEvent::DataSourceRemoved(ref definition) => {
                    // Destroy all runtime hosts for this data source; this will
                    // also terminate the host's event stream
                    runtime_hosts.retain(|host| host.data_source_definition() != definition);
                }
            }

            Ok(())
        }))
    }
}

impl EventConsumer<DataSourceProviderEvent> for RuntimeManager {
    type EventSink = Box<Sink<SinkItem = DataSourceProviderEvent, SinkError = ()>>;

    /// Get the wrapped event sink.
    fn event_sink(&self) -> Self::EventSink {
        let logger = self.logger.clone();
        Box::new(self.input.clone().sink_map_err(move |e| {
            error!(logger, "Component was dropped: {}", e);
        }))
    }
}
