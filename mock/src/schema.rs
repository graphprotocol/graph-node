use futures::sync::mpsc::{channel, Receiver, Sender};

use graph::prelude::*;

/// A mock `SchemaProvider`.
pub struct MockSchemaProvider {
    logger: Logger,
    input: Sender<SchemaEvent>,
    output: Option<Receiver<SchemaProviderEvent>>,
}

impl SchemaProvider for MockSchemaProvider {}

impl EventProducer<SchemaProviderEvent> for MockSchemaProvider {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<Stream<Item = SchemaProviderEvent, Error = ()> + Send>> {
        self.output
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = SchemaProviderEvent, Error = ()> + Send>)
    }
}

impl EventConsumer<SchemaEvent> for MockSchemaProvider {
    /// Get the wrapped event sink.
    fn event_sink(&self) -> Box<Sink<SinkItem = SchemaEvent, SinkError = ()> + Send> {
        let logger = self.logger.clone();
        Box::new(self.input.clone().sink_map_err(move |e| {
            error!(logger, "MockSchemaProvider was dropped {}", e);
        }))
    }
}

/// A mock implementor of `SchemaProvider`.
impl MockSchemaProvider {
    /// Spawns the provider and returns it's input and output handles.
    pub fn new(logger: &Logger) -> Self {
        let logger = logger.new(o!("component" => "MockSchemaProvider"));
        info!(logger, "Building a `MockSchemaProvider`");

        // Create a channel for receiving events from the subgraph provider.
        let (subgraph_sender, subgraph_recv) = channel(100);
        // Create a channel for broadcasting changes to the schema.
        let (schema_sender, schema_recv) = channel(100);

        // Spawn the internal handler for any incoming events from the subgraph provider.
        tokio::spawn(Self::schema_event_handler(
            logger.clone(),
            subgraph_recv,
            schema_sender,
        ));

        MockSchemaProvider {
            logger,
            input: subgraph_sender,
            output: Some(schema_recv),
        }
    }

    // A task that handles incoming events from the subgraph provider, updates
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

            // Mock processing the event from the subgraph provider
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
