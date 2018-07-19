use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver};
use slog;
use tokio_core::reactor::Handle;

use thegraph::components::data_sources::{DataSourceProviderEvent, SchemaEvent};
use thegraph::data::data_sources::DataSourceDefinitionResolveError;
use thegraph::prelude::{DataSourceProvider as DataSourceProviderTrait, *};

pub struct DataSourceProvider {
    _logger: slog::Logger,
    event_stream: Option<Receiver<DataSourceProviderEvent>>,
    schema_event_stream: Option<Receiver<SchemaEvent>>,
}

impl DataSourceProvider {
    pub fn new<'a>(
        logger: slog::Logger,
        runtime: Handle,
        link: &str,
        resolver: &'a impl LinkResolver,
    ) -> impl Future<Item = Self, Error = DataSourceDefinitionResolveError> + 'a {
        // Load the data source definition
        DataSourceDefinition::resolve(
            Link {
                link: link.to_owned(),
            },
            resolver,
        ).map(move |data_source| {
            let schema = data_source.schema.clone();

            let (event_sink, event_stream) = channel(100);

            // Push the data source into the stream
            runtime.spawn(
                event_sink
                    .send(DataSourceProviderEvent::DataSourceAdded(data_source))
                    .map_err(|e| panic!("Failed to forward data source: {}", e))
                    .map(|_| ()),
            );

            let (schema_event_sink, schema_event_stream) = channel(100);

            // Push the schema into the stream
            runtime.spawn(
                schema_event_sink
                    .send(SchemaEvent::SchemaAdded(schema))
                    .map_err(|e| panic!("Failed to forward data source schema: {}", e))
                    .map(|_| ()),
            );
            // Create the data source provider
            DataSourceProvider {
                _logger: logger.new(o!("component" => "DataSourceProvider")),
                event_stream: Some(event_stream),
                schema_event_stream: Some(schema_event_stream),
            }
        })
    }
}

impl DataSourceProviderTrait for DataSourceProvider {}

impl EventProducer<DataSourceProviderEvent> for DataSourceProvider {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<Stream<Item = DataSourceProviderEvent, Error = ()>>> {
        self.event_stream
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = DataSourceProviderEvent, Error = ()>>)
    }
}

impl EventProducer<SchemaEvent> for DataSourceProvider {
    fn take_event_stream(&mut self) -> Option<Box<Stream<Item = SchemaEvent, Error = ()>>> {
        self.schema_event_stream
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = SchemaEvent, Error = ()>>)
    }
}
