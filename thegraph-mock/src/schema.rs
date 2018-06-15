use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use slog::Logger;
use tokio_core::reactor::Handle;

use thegraph::components::data_sources::SchemaEvent;
use thegraph::components::schema::SchemaProviderEvent;
use thegraph::components::{EventConsumer, EventProducer};
use thegraph::prelude::*;

/// A mock `SchemaProvider`.
pub struct MockSchemaProvider {
    logger: Logger,
    input: Sender<SchemaEvent>,
    output: Option<Receiver<SchemaProviderEvent>>,
}

impl SchemaProvider for MockSchemaProvider {}

impl EventProducer<SchemaProviderEvent> for MockSchemaProvider {
    type EventStream = Receiver<SchemaProviderEvent>;

    fn take_event_stream(&mut self) -> Option<Self::EventStream> {
        self.output.take()
    }
}

impl EventConsumer<SchemaEvent> for MockSchemaProvider {
    type EventSink = Box<Sink<SinkItem = SchemaEvent, SinkError = ()>>;

    /// Get the wrapped event sink.
    fn event_sink(&self) -> Self::EventSink {
        let logger = self.logger.clone();
        Box::new(self.input.clone().sink_map_err(move |e| {
            error!(logger, "MockSchemaProvider was dropped {}", e);
        }))
    }
}

/// A mock implementor of `SchemaProvider`.
impl MockSchemaProvider {
    /// Spawns the provider and returns it's input and output handles.
    pub fn new(logger: &Logger, runtime: Handle) -> Self {
        let logger = logger.new(o!("component" => "MockSchemaProvider"));
        info!(logger, "Building a `MockSchemaProvider`");

        // Create a channel for receiving events from the data source provider.
        let (data_source_sender, data_source_recv) = channel(100);
        // Create a channel for broadcasting changes to the schema.
        let (schema_sender, schema_recv) = channel(100);

        // Spawn the internal handler for any incoming events from the data source provider.
        runtime.spawn(Self::schema_event_handler(
            logger.clone(),
            data_source_recv,
            schema_sender,
        ));

        MockSchemaProvider {
            logger,
            input: data_source_sender,
            output: Some(schema_recv),
        }
    }

    // A task that handles incoming events from the data source provider, updates
    // the schema and pushes the result to listeners. Any panics here crash the
    // component, do be careful.
    fn schema_event_handler(
        logger: Logger,
        input: Receiver<SchemaEvent>,
        output: Sender<SchemaProviderEvent>,
    ) -> impl Future<Item = (), Error = ()> {
        let sink_err_logger = logger.clone();
        input
        .map(move |event| {
            info!(logger, "Received schema event"; "event" => format!("{:?}", event));
            info!(logger, "Combining schemas");

            // Mock processing the event from the data source provider
            match event {
                SchemaEvent::SchemaAdded(schema) | SchemaEvent::SchemaRemoved(schema) => {
                    SchemaProviderEvent::SchemaChanged(Some(schema))
                }
            }
        })
        // Forward the new combined schema through the schema channel.
        .forward(output.sink_map_err(move |e| {
            error!(sink_err_logger, "Receiver of schema events was dropped {}", e);
        })).map(|_| ())
    }
}
