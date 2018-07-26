use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use graphql_parser;
use slog;

use graphql_parser::schema::Document;
use graph::components::data_sources::{DataSourceProviderEvent, SchemaEvent};
use graph::prelude::*;

/// A mock `DataSourceProvider`.
pub struct MockDataSourceProvider {
    logger: slog::Logger,
    event_sink: Sender<DataSourceProviderEvent>,
    schema_event_sink: Sender<SchemaEvent>,
    event_stream: Option<Receiver<DataSourceProviderEvent>>,
    schema_event_stream: Option<Receiver<SchemaEvent>>,
    schemas: Vec<Schema>,
}

impl MockDataSourceProvider {
    /// Creates a new mock `DataSourceProvider`.
    pub fn new(logger: &slog::Logger) -> Self {
        let (event_sink, event_stream) = channel(100);

        let (schema_event_sink, schema_event_stream) = channel(100);

        MockDataSourceProvider {
            logger: logger.new(o!("component" => "MockDataSourceProvider")),
            event_sink,
            schema_event_sink,
            event_stream: Some(event_stream),
            schema_event_stream: Some(schema_event_stream),
            schemas: vec![Schema {
                id: "176dbd4fdeb8407b899be5d456ababc0".to_string(),
                document: graphql_parser::parse_schema(
                    "type User {
                           id: ID!
                           name: String!
                         }",
                ).unwrap(),
            }],
        }
    }

    /// Generates a bunch of mock data source provider events.
    fn generate_mock_events(&mut self) {
        info!(self.logger, "Generate mock events");

        let mock_data_source = DataSourceDefinition {
            id: String::from("mock data source"),
            location: String::from("/tmp/example-data-source.yaml"),
            spec_version: String::from("0.1"),
            schema: Schema {
                id: String::from("exampled id"),
                document: Document {
                    definitions: vec![],
                },
            },
            datasets: vec![],
        };

        self.event_sink
            .clone()
            .send(DataSourceProviderEvent::DataSourceAdded(mock_data_source))
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

impl EventProducer<DataSourceProviderEvent> for MockDataSourceProvider {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<Stream<Item = DataSourceProviderEvent, Error = ()>>> {
        self.generate_mock_events();
        self.event_stream
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = DataSourceProviderEvent, Error = ()>>)
    }
}

impl EventProducer<SchemaEvent> for MockDataSourceProvider {
    fn take_event_stream(&mut self) -> Option<Box<Stream<Item = SchemaEvent, Error = ()>>> {
        self.generate_mock_schema_events();
        self.schema_event_stream
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = SchemaEvent, Error = ()>>)
    }
}
