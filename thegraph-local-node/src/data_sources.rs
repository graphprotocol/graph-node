use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use graphql_parser;
use slog;
use tokio_core::reactor::Handle;

use thegraph::components::data_sources::{DataSourceDefinitionLoaderError, DataSourceProviderEvent,
                                         SchemaEvent};
use thegraph::prelude::{DataSourceProvider as DataSourceProviderTrait, *};
use thegraph::util::stream::StreamError;
use thegraph_core;

pub struct LocalDataSourceProvider {
    _logger: slog::Logger,
    event_sink: Option<Sender<DataSourceProviderEvent>>,
    schema_event_sink: Option<Sender<SchemaEvent>>,
    runtime: Handle,
    data_source: DataSourceDefinition,
    schema: Schema,
}

impl LocalDataSourceProvider {
    pub fn new<'a, T: Ipfs>(
        logger: slog::Logger,
        runtime: Handle,
        filename: &str,
        ipfs_client: &'a T,
    ) -> impl Future<Item = Self, Error = DataSourceDefinitionLoaderError> + 'a {
        // Load the data source definition
        let loader = thegraph_core::DataSourceDefinitionLoader::default();
        loader
            .load_from_ipfs(filename, ipfs_client)
            .map(move |data_source| {
                // Parse the schema
                let schema = graphql_parser::parse_schema(data_source.schema.source.as_str())
                    .map(|document| Schema {
                        id: String::from("local-data-source-schema"),
                        document,
                    })
                    .expect("Failed to parse data source schema");

                // Create the data source provider
                LocalDataSourceProvider {
                    _logger: logger.new(o!("component" => "LocalDataSourceProvider")),
                    event_sink: None,
                    schema_event_sink: None,
                    runtime,
                    data_source,
                    schema,
                }
            })
    }
}

impl DataSourceProviderTrait for LocalDataSourceProvider {
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

        // If the stream was set up successfully, push the data source into it
        if result.is_ok() && self.event_sink.is_some() {
            self.runtime.spawn(
                self.event_sink
                    .clone()
                    .unwrap()
                    .send(DataSourceProviderEvent::DataSourceAdded(
                        self.data_source.clone(),
                    ))
                    .map_err(|e| panic!("Failed to forward data source: {}", e))
                    .and_then(|_| Ok(())),
            );
        }

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

        // If the stream was set up successfully, push the schema into it
        if result.is_ok() && self.schema_event_sink.is_some() {
            self.runtime.spawn(
                self.schema_event_sink
                    .clone()
                    .unwrap()
                    .send(SchemaEvent::SchemaAdded(self.schema.clone()))
                    .map_err(|e| panic!("Failed to forward data source schema: {}", e))
                    .and_then(|_| Ok(())),
            );
        }

        result
    }
}
