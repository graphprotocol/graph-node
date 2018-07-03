use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use graphql_parser;
use slog;

use thegraph::components::data_sources::{DataSourceProviderEvent, SchemaEvent};
use thegraph::prelude::*;
use thegraph::util::stream::StreamError;

/// A mock `DataSourceProvider`.
pub struct MockDataSourceProvider {
    logger: slog::Logger,
    event_sink: Option<Sender<DataSourceProviderEvent>>,
    schema_event_sink: Option<Sender<SchemaEvent>>,
    schemas: Vec<Schema>,
}

impl MockDataSourceProvider {
    /// Creates a new mock `DataSourceProvider`.
    pub fn new(logger: &slog::Logger) -> Self {
        MockDataSourceProvider {
            logger: logger.new(o!("component" => "MockDataSourceProvider")),
            event_sink: None,
            schema_event_sink: None,
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
            schema: String::from("example schema"),
            datasets: vec![],
        };

        let sink = self.event_sink.clone().unwrap();
        sink.clone()
            .send(DataSourceProviderEvent::DataSourceAdded(mock_data_source))
            .wait()
            .unwrap();
    }

    /// Generates a bunch of mock schema events.
    fn generate_mock_schema_events(&mut self) {
        info!(self.logger, "Generate mock schema events");

        let sink = self.schema_event_sink.clone().unwrap();

        for schema in self.schemas.iter() {
            sink.clone()
                .send(SchemaEvent::SchemaAdded(schema.clone()))
                .wait()
                .unwrap();
        }
    }
}

impl DataSourceProvider for MockDataSourceProvider {
    fn event_stream(&mut self) -> Result<Receiver<DataSourceProviderEvent>, StreamError> {
        // If possible, create a new channel for streaming data source provider events
        let result = match self.event_sink {
            Some(_) => Err(StreamError::AlreadyCreated),
            None => {
                let (sink, stream) = channel(100);
                self.event_sink = Some(sink);
                Ok(stream)
            }
        };

        self.generate_mock_events();
        result
    }

    fn schema_event_stream(&mut self) -> Result<Receiver<SchemaEvent>, StreamError> {
        // If possible, create a new channel for streaming schema events
        let result = match self.schema_event_sink {
            Some(_) => Err(StreamError::AlreadyCreated),
            None => {
                let (sink, stream) = channel(100);
                self.schema_event_sink = Some(sink);
                Ok(stream)
            }
        };

        self.generate_mock_schema_events();
        result
    }
}
