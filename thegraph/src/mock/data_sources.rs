use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use slog;

use prelude::*;
use common::data_sources::{DataSourceProviderEvent, SchemaEvent};
use common::util::stream::StreamError;

/// A mock [DataSourceProvider](../common/data_sources/trait.DataSourceProvider.html).
pub struct MockDataSourceProvider {
    logger: slog::Logger,
    event_sink: Option<Sender<DataSourceProviderEvent>>,
    schema_event_sink: Option<Sender<SchemaEvent>>,
}

impl MockDataSourceProvider {
    /// Creates a new mock [DataSourceProvider](
    /// ../common/data_sources/trait.DataSourceProvider.html).
    pub fn new(logger: &slog::Logger) -> Self {
        MockDataSourceProvider {
            logger: logger.new(o!("component" => "MockDataSourceProvider")),
            event_sink: None,
            schema_event_sink: None,
        }
    }

    /// Generates a bunch of mock data source provider events.
    fn generate_mock_events(&mut self) {
        info!(self.logger, "Generate mock events");

        let sink = self.event_sink.clone().unwrap();
        sink.clone()
            .send(DataSourceProviderEvent::DataSourceAdded(
                "First data source",
            ))
            .wait()
            .unwrap();
        sink.clone()
            .send(DataSourceProviderEvent::DataSourceAdded(
                "Second data source",
            ))
            .wait()
            .unwrap();
        sink.clone()
            .send(DataSourceProviderEvent::DataSourceRemoved(
                "Second data source",
            ))
            .wait()
            .unwrap();
    }

    /// Generates a bunch of mock schema events.
    fn generate_mock_schema_events(&mut self) {
        info!(self.logger, "Generate mock schema events");

        let sink = self.schema_event_sink.clone().unwrap();
        sink.clone()
            .send(SchemaEvent::SchemaAdded("First schema"))
            .wait()
            .unwrap();
        sink.clone()
            .send(SchemaEvent::SchemaAdded("Second schema"))
            .wait()
            .unwrap();
        sink.clone()
            .send(SchemaEvent::SchemaRemoved("Second schema"))
            .wait()
            .unwrap();
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
