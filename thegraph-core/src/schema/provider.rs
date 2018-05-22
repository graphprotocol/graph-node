use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use slog;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio_core::reactor::Handle;

use thegraph::components::data_sources::SchemaEvent;
use thegraph::components::schema::{SchemaProvider as SchemaProviderTrait, SchemaProviderEvent};
use thegraph::data::schema::Schema;
use thegraph::util::stream::StreamError;

/// Common schema provider implementation for The Graph.
pub struct SchemaProvider {
    logger: slog::Logger,
    schema_event_sink: Sender<SchemaEvent>,
    event_sink: Arc<Mutex<Option<Sender<SchemaProviderEvent>>>>,
    runtime: Handle,
    input_schemas: Arc<Mutex<HashMap<String, Schema>>>,
    combined_schema: Arc<Mutex<Option<Schema>>>,
}

impl SchemaProvider {
    /// Creates a new schema provider.
    pub fn new(logger: &slog::Logger, runtime: Handle) -> Self {
        // Create a channel for receiving events from the data source provider
        let (sink, stream) = channel(100);

        // Create a new schema provider
        let mut provider = SchemaProvider {
            logger: logger.new(o!("component" => "SchemaProvider")),
            schema_event_sink: sink,
            event_sink: Arc::new(Mutex::new(None)),
            runtime,
            input_schemas: Arc::new(Mutex::new(HashMap::new())),
            combined_schema: Arc::new(Mutex::new(None)),
        };

        // Spawn a task to handle any incoming events from the data source provider
        provider.handle_schema_events(stream);

        // Return the new schema provider
        provider
    }

    fn handle_schema_events(&mut self, stream: Receiver<SchemaEvent>) {
        let event_sink = self.event_sink.clone();
        let logger = self.logger.clone();
        let input_schemas = self.input_schemas.clone();
        let combined_schema = self.combined_schema.clone();

        self.runtime.spawn(stream.for_each(move |event| {
            info!(logger, "Received schema event"; "event" => format!("{:?}", event));
            info!(logger, "Combining schemas");

            {
                // Add or remove the schema from the input schemas
                let mut input_schemas = input_schemas.lock().unwrap();
                match event {
                    SchemaEvent::SchemaAdded(ref schema) => {
                        input_schemas.insert(schema.id.clone(), schema.clone());
                    }
                    SchemaEvent::SchemaRemoved(ref schema) => {
                        input_schemas.remove(&schema.id);
                    }
                };

                // Attempt to combine the input schemas into one schema;
                // NOTE: For the moment, we're simply picking the first schema
                // we can find in the map. Once we support multiple data sources,
                // this would be where we combine them into one and also detect
                // conflicts
                let mut combined_schema = combined_schema.lock().unwrap();
                *combined_schema = match input_schemas.len() {
                    0 => None,
                    _ => Some(input_schemas.values().nth(0).unwrap().clone()),
                };
            }

            // Obtain a lock on the event sink
            let event_sink = event_sink.lock().unwrap();

            // Mock processing the event from the data source provider
            let output_event = {
                let combined_schema = combined_schema.lock().unwrap();
                SchemaProviderEvent::SchemaChanged(combined_schema.clone())
            };

            // If we have another component listening to our events, forward the new
            // combined schema to them through the event channel
            match *event_sink {
                Some(ref sink) => {
                    info!(logger, "Forwarding the combined schema");
                    sink.clone().send(output_event).wait().unwrap();
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

impl SchemaProviderTrait for SchemaProvider {
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

#[cfg(test)]
mod tests {
    use futures::prelude::*;
    use graphql_parser;
    use tokio_core::reactor::Core;

    use slog;
    use thegraph::components::data_sources::SchemaEvent;
    use thegraph::components::schema::SchemaProviderEvent;
    use thegraph::prelude::*;

    use super::SchemaProvider as CoreSchemaProvider;

    #[test]
    fn emits_a_combined_schema_after_one_schema_is_added() {
        let mut core = Core::new().unwrap();

        // Set up the schema provider
        let logger = slog::Logger::root(slog::Discard, o!());
        let mut schema_provider = CoreSchemaProvider::new(&logger, core.handle());
        let schema_sink = schema_provider.schema_event_sink();
        let schema_stream = schema_provider.event_stream().unwrap();

        // Create an input schema event
        let input_doc = graphql_parser::parse_schema("type User { name: String! }").unwrap();
        let input_schema = Schema {
            id: "input-schema".to_string(),
            document: input_doc,
        };
        let input_event = SchemaEvent::SchemaAdded(input_schema.clone());

        // Send the input schema event to the schema provider
        schema_sink.clone().send(input_event).wait().unwrap();

        // Expect one schema provider event to be emitted as a result
        let work = schema_stream.take(1).into_future();
        let output_event = core.run(work)
            .expect("Failed to receive schema provider event from the stream")
            .0
            .expect("Schema provider event must not be None");

        // Extract the output schema from the schema provider event
        let SchemaProviderEvent::SchemaChanged(output_schema) = output_event;
        let output_schema = output_schema.expect("Combined schema must not be None");

        // The output schema (currently) must equal the input schema
        assert_eq!(output_schema.id, input_schema.id);
        assert_eq!(output_schema.document, input_schema.document);
    }
}
