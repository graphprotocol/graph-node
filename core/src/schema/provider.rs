use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use graph::tokio;
use slog::Logger;
use std::collections::HashMap;

use graph::components::schema::{SchemaProvider as SchemaProviderTrait, SchemaProviderEvent};
use graph::components::subgraph::SchemaEvent;
use graph::components::{EventConsumer, EventProducer};
use graph::data::schema::Schema;

use graph_graphql::prelude::*;

/// Common schema provider implementation for The Graph.
pub struct SchemaProvider {
    logger: Logger,
    input: Sender<SchemaEvent>,
    output: Option<Receiver<SchemaProviderEvent>>,
}

impl SchemaProviderTrait for SchemaProvider {}

impl EventProducer<SchemaProviderEvent> for SchemaProvider {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<Stream<Item = SchemaProviderEvent, Error = ()> + Send>> {
        self.output
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = SchemaProviderEvent, Error = ()> + Send>)
    }
}

impl EventConsumer<SchemaEvent> for SchemaProvider {
    /// Get the wrapped event sink.
    fn event_sink(&self) -> Box<Sink<SinkItem = SchemaEvent, SinkError = ()> + Send> {
        let logger = self.logger.clone();
        Box::new(self.input.clone().sink_map_err(move |e| {
            error!(logger, "MockSchemaProvider was dropped {}", e);
        }))
    }
}

impl SchemaProvider {
    /// Spawns the provider and returns it's input and output handles.
    pub fn new(logger: &Logger) -> Self {
        let logger = logger.new(o!("component" => "SchemaProvider"));

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

        // Return the new schema provider
        SchemaProvider {
            logger,
            input: subgraph_sender,
            output: Some(schema_recv),
        }
    }

    fn schema_event_handler(
        logger: Logger,
        input: Receiver<SchemaEvent>,
        output: Sender<SchemaProviderEvent>,
    ) -> impl Future<Item = (), Error = ()> {
        let sink_err_logger = logger.clone();
        let mut input_schemas = HashMap::new();
        let mut combined_schema: Option<Schema> = None;

        input
            .map(move |event| {
                info!(logger, "Received event -> combining schemas");
                // Add or remove the schema from the input schemas.
                match event {
                    SchemaEvent::SchemaAdded(ref schema) => {
                        input_schemas.insert(schema.id.clone(), schema.clone());
                    }
                    SchemaEvent::SchemaRemoved(ref schema) => {
                        input_schemas.remove(&schema.id);
                    }
                };

                // Derive a full-fledged API schema from the first input schema
                //
                // NOTE: For the moment, we're simply picking the first schema
                // we can find in the map. Once we support multiple subgraphs,
                // this would be where we combine them into one and also detect
                // conflicts
                combined_schema = input_schemas.values().next().and_then(|schema| {
                    api_schema(&schema.document)
                        .map(|document| Schema {
                            id: schema.id.clone(),
                            document,
                        })
                        .map_err(|e| {
                            error!(
                                logger,
                                "Failed to derive API schema from input schemas: {}", e
                            );
                        })
                        .ok()
                });

                // Forward the new combined schema to them through the event channel
                info!(logger, "Forwarding the combined schema");
                // Mock processing the event from the subgraph provider
                SchemaProviderEvent::SchemaChanged(combined_schema.clone())
            })
            .forward(output.sink_map_err(move |e| {
                error!(
                    sink_err_logger,
                    "Receiver of schema events was dropped {}", e
                );
            }))
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use graphql_parser;

    use graph::components::schema::SchemaProviderEvent;
    use graph::components::subgraph::SchemaEvent;
    use graph::prelude::*;
    use graph_graphql::schema::ast;
    use slog;

    use super::SchemaProvider as CoreSchemaProvider;

    #[test]
    fn emits_a_combined_schema_after_one_schema_is_added() {
        tokio::run(future::lazy(|| {
            Ok({
                // Set up the schema provider
                let logger = slog::Logger::root(slog::Discard, o!());
                let mut schema_provider = CoreSchemaProvider::new(&logger);
                let schema_sink = schema_provider.event_sink();
                let schema_stream = schema_provider.take_event_stream().unwrap();

                // Create an input schema event
                let input_doc =
                    graphql_parser::parse_schema("type User { name: String! }").unwrap();
                let input_schema = Schema {
                    id: "input-schema".to_string(),
                    document: input_doc,
                };
                let input_event = SchemaEvent::SchemaAdded(input_schema.clone());

                // Send the input schema event to the schema provider
                schema_sink.send(input_event).wait().unwrap();

                // Expect one schema provider event to be emitted as a result
                let work = schema_stream.take(1).into_future();
                let output_event = if let Ok(x) = work.wait() {
                    x.0.expect("Schema provider event must not be None")
                } else {
                    panic!("Failed to receive schema provider event from the stream")
                };

                // Extract the output schema from the schema provider event
                let SchemaProviderEvent::SchemaChanged(output_schema) = output_event;
                let output_schema = output_schema.expect("Combined schema must not be None");

                assert_eq!(output_schema.id, input_schema.id);

                // The output schema must include the input schema types
                assert_eq!(
                    ast::get_named_type(&input_schema.document, &"User".to_string()),
                    ast::get_named_type(&output_schema.document, &"User".to_string())
                );

                // The output schema must include a Query type
                ast::get_named_type(&output_schema.document, &"Query".to_string())
                    .expect("Query type missing in output schema");
            })
        }))
    }
}
