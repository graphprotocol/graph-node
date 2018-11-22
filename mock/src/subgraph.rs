use futures::sync::mpsc::{channel, Receiver, Sender};
use graphql_parser;

use graph::prelude::*;
use graphql_parser::schema::Document;

/// A mock `SubgraphProvider`.
pub struct MockSubgraphProvider {
    logger: slog::Logger,
    event_sink: Sender<SubgraphProviderEvent>,
    schema_event_sink: Sender<SchemaEvent>,
    event_stream: Option<Receiver<SubgraphProviderEvent>>,
    schema_event_stream: Option<Receiver<SchemaEvent>>,
    schemas: Vec<Schema>,
}

impl MockSubgraphProvider {
    /// Creates a new mock `SubgraphProvider`.
    pub fn new(logger: &slog::Logger) -> Self {
        let (event_sink, event_stream) = channel(100);

        let (schema_event_sink, schema_event_stream) = channel(100);

        let id = "176dbd4fdeb8407b899be5d456ababc0".to_string();
        MockSubgraphProvider {
            logger: logger.new(o!("component" => "MockSubgraphProvider")),
            event_sink,
            schema_event_sink,
            event_stream: Some(event_stream),
            schema_event_stream: Some(schema_event_stream),
            schemas: vec![Schema {
                id,
                document: graphql_parser::parse_schema(
                    "type User {
                           id: ID!
                           name: String!
                         }",
                ).unwrap(),
            }],
        }
    }

    /// Generates a bunch of mock subgraph provider events.
    fn generate_mock_events(&mut self) {
        info!(self.logger, "Generate mock events");

        let mock_subgraph = SubgraphManifest {
            id: String::from("mock subgraph"),
            location: String::from("/tmp/example-data-source.yaml"),
            spec_version: String::from("0.1"),
            description: None,
            repository: None,
            schema: Schema {
                id: String::from("exampled id"),
                document: Document {
                    definitions: vec![],
                },
            },
            data_sources: vec![],
        };

        self.event_sink
            .clone()
            .send(SubgraphProviderEvent::SubgraphStart(mock_subgraph))
            .wait()
            .unwrap();
    }

    /// Generates a bunch of mock schema events.
    fn generate_mock_schema_events(&mut self) {
        info!(self.logger, "Generate mock schema events");

        for schema in self.schemas.iter() {
            self.schema_event_sink
                .clone()
                .clone()
                .send(SchemaEvent::SchemaAdded(schema.clone()))
                .wait()
                .unwrap();
        }
    }
}

impl EventProducer<SubgraphProviderEvent> for MockSubgraphProvider {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<Stream<Item = SubgraphProviderEvent, Error = ()> + Send>> {
        self.generate_mock_events();
        self.event_stream
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = SubgraphProviderEvent, Error = ()> + Send>)
    }
}

impl EventProducer<SchemaEvent> for MockSubgraphProvider {
    fn take_event_stream(&mut self) -> Option<Box<Stream<Item = SchemaEvent, Error = ()> + Send>> {
        self.generate_mock_schema_events();
        self.schema_event_stream
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = SchemaEvent, Error = ()> + Send>)
    }
}
