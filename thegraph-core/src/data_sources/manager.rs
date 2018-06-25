use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use slog::Logger;
use tokio_core::reactor::Handle;

use thegraph::components::data_sources::DataSourceProviderEvent;
use thegraph::prelude::*;
use thegraph_runtime::RuntimeHostBuilder;

pub struct RuntimeManager {
    logger: Logger,
    runtime: Handle,
    host_builder: RuntimeHostBuilder,
    input: Sender<DataSourceProviderEvent>,
}

impl RuntimeManager {
    /// Creates a new runtime manager.
    pub fn new(logger: &Logger, runtime: Handle, host_builder: RuntimeHostBuilder) -> Self {
        let logger = logger.new(o!("component" => "RuntimeManager"));

        // Create channel for receiving data source provider events.
        let (data_source_sender, data_source_receiver) = channel(100);

        let mut manager = RuntimeManager {
            logger,
            runtime,
            host_builder,
            input: data_source_sender,
        };

        // Handle incoming events from the data source provider.
        manager.handle_data_source_events(data_source_receiver);

        manager
    }

    /// Handle incoming events from data source providers.
    fn handle_data_source_events(&mut self, receiver: Receiver<DataSourceProviderEvent>) {
        self.runtime.spawn(receiver.for_each(|event| {
            match event {
                DataSourceProviderEvent::DataSourceAdded(definition) => {
                    self.host_builder
                        .create_host(definition.data_source.mapping.location.to_string());
                }
                _ => unimplemented!(),
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
