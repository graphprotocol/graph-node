use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use graphql_parser;
use slog;
use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use tokio_core::reactor::Handle;

use thegraph::components::data_sources::{DataSourceProviderEvent, SchemaEvent};
use thegraph::prelude::{DataSourceProvider as DataSourceProviderTrait, *};
use thegraph::util::stream::StreamError;
use thegraph_core;

pub struct LocalDataSourceProvider {
    logger: slog::Logger,
    event_sink: Option<Sender<DataSourceProviderEvent>>,
    schema_event_sink: Option<Sender<SchemaEvent>>,
    runtime: Handle,
    schema: Schema,
}

impl LocalDataSourceProvider {
    pub fn new(logger: &slog::Logger, runtime: Handle, filename: &str) -> Self {
        // Load the data source definition
        let loader = thegraph_core::DataSourceDefinitionLoader::default();
        let definition = loader
            .load_from_path(Path::new(filename).to_owned())
            .expect("Failed to load data source definition");

        // Parse the schema
        let schema = graphql_parser::parse_schema(definition.schema.as_str())
            .map(|document| Schema {
                id: String::from("local-data-source-schema"),
                document,
            })
            .expect("Failed to parse data source schema");

        // Create the data source provider
        LocalDataSourceProvider {
            logger: logger.new(o!("component" => "LocalDataSourceProvider")),
            event_sink: None,
            schema_event_sink: None,
            runtime,
            schema: schema,
        }
    }
}

impl DataSourceProviderTrait for LocalDataSourceProvider {
    fn event_stream(&mut self) -> Result<Receiver<DataSourceProviderEvent>, StreamError> {
        // If possible, create a new channel for streaming data source provider events
        match self.event_sink {
            Some(_) => Err(StreamError::AlreadyCreated),
            None => {
                let (sink, stream) = channel(100);
                self.event_sink = Some(sink);
                Ok(stream)
            }
        }
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

        // If the stream was set up successfully, push the schema into it
        if result.is_ok() && self.schema_event_sink.is_some() {
            self.runtime.spawn(
                self.schema_event_sink
                    .clone()
                    .unwrap()
                    .send(SchemaEvent::SchemaAdded(self.schema.clone()))
                    .map_err(|e| panic!("Failed to forward the data source schema"))
                    .and_then(|_| Ok(())),
            );
        }

        result
    }
}
