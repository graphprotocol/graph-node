use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use slog;
use std::sync::{Arc, Mutex};
use tokio_core::reactor::Handle;

use prelude::*;
use common::schema::SchemaProviderEvent;
use common::data_sources::SchemaEvent;
use common::util::stream::StreamError;

/// A mock [SchemaProvider](../common/schema/trait.SchemaProvider.html).
pub struct MockSchemaProvider {
    logger: slog::Logger,
    schema_event_sink: Sender<SchemaEvent>,
    event_sink: Arc<Mutex<Option<Sender<SchemaProviderEvent>>>>,
    runtime: Handle,
}

impl MockSchemaProvider {
    /// Creates a new mock [SchemaProvider](../common/schema/trait.SchemaProvider.html).
    pub fn new(logger: &slog::Logger, runtime: Handle) -> Self {
        // Create a channel for receiving events from the data source provider
        let (sink, stream) = channel(100);

        // Create a new schema provider
        let mut provider = MockSchemaProvider {
            logger: logger.new(o!("component" => "MockSchemaProvider")),
            schema_event_sink: sink,
            event_sink: Arc::new(Mutex::new(None)),
            runtime,
        };

        // Spawn a task to handle any incoming events from the data source provider
        provider.handle_schema_events(stream);

        // Return the new schema provider
        provider
    }

    fn handle_schema_events(&mut self, stream: Receiver<SchemaEvent>) {
        let event_sink = self.event_sink.clone();
        let logger = self.logger.clone();

        self.runtime.spawn(stream.for_each(move |event| {
            info!(logger, "Received schema event"; "event" => format!("{:?}", event));
            info!(logger, "Combining schemas");

            // Obtain a lock on the event sink
            let event_sink = event_sink.lock().unwrap();

            // Mock processing the event from the data source provider
            let resulting_event = match event {
                SchemaEvent::SchemaAdded(schema) => SchemaProviderEvent::SchemaChanged(schema),
                SchemaEvent::SchemaRemoved(schema) => SchemaProviderEvent::SchemaChanged(schema),
            };

            // If we have another component listening to our events, forward the new
            // combined schema to them through the event channel
            match *event_sink {
                Some(ref sink) => {
                    info!(logger, "Forwarding the combined schema");
                    sink.clone().send(resulting_event).wait().unwrap();
                }
                None => {
                    warn!(logger, "Not forwarding the combined schema yet");
                }
            }

            // Tokio tasks always return an empty tuple
            Ok(())
        }));
    }
}

impl SchemaProvider for MockSchemaProvider {
    fn event_stream(&mut self) -> Result<Receiver<SchemaProviderEvent>, StreamError> {
        info!(self.logger, "Setting up event stream");

        // If possible, create a new channel for streaming schema provider events
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

    fn schema_event_sink(&mut self) -> Sender<SchemaEvent> {
        self.schema_event_sink.clone()
    }
}
