use futures::sync::mpsc::Receiver;
use super::util::stream::StreamError;

/// Events emitted by [DataSourceProvider](trait.DataSourceProvider.html) implementations.
#[derive(Debug)]
pub enum DataSourceProviderEvent {
    /// A data source was added to the provider.
    DataSourceAdded(&'static str),
    /// A data source was removed from the provider.
    DataSourceRemoved(&'static str),
}

impl Clone for DataSourceProviderEvent {
    fn clone(&self) -> Self {
        match self {
            &DataSourceProviderEvent::DataSourceAdded(ref s) => {
                DataSourceProviderEvent::DataSourceAdded(s)
            }
            &DataSourceProviderEvent::DataSourceRemoved(ref s) => {
                DataSourceProviderEvent::DataSourceRemoved(s)
            }
        }
    }
}

/// Schema-only events emitted by a [DataSourceProvider](trait.DataSourceProvider.html).
#[derive(Debug)]
pub enum SchemaEvent {
    /// A data source with a new schema was added.
    SchemaAdded(&'static str),
    /// A data source with an existing schema was removed.
    SchemaRemoved(&'static str),
}

impl Clone for SchemaEvent {
    fn clone(&self) -> Self {
        match self {
            &SchemaEvent::SchemaAdded(ref s) => SchemaEvent::SchemaAdded(s),
            &SchemaEvent::SchemaRemoved(ref s) => SchemaEvent::SchemaRemoved(s),
        }
    }
}

/// Common trait for data source providers.
pub trait DataSourceProvider {
    /// Receiver from which others can read events emitted by the data source provider.
    /// Can only be called once. Any consecutive call will result in a StreamError.
    fn event_stream(&mut self) -> Result<Receiver<DataSourceProviderEvent>, StreamError>;

    /// Receiver from whith others can read schema-only events emitted by the data source provider.
    /// Can only be called once. Any consecutive call will result in a StreamError.
    fn schema_event_stream(&mut self) -> Result<Receiver<SchemaEvent>, StreamError>;
}
