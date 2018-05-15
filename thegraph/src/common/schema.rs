use futures::sync::mpsc::{Receiver, Sender};
use super::data_sources::SchemaEvent;
use super::util::stream::StreamError;

/// Events emitted by [SchemaProvider](trait.SchemaProvider.html) implementations.
#[derive(Clone, Debug)]
pub enum SchemaProviderEvent {
    /// The provided schema has changed.
    SchemaChanged(&'static str),
}

/// Common trait for schema provider implementations.
pub trait SchemaProvider {
    /// Receiver from which others can read events emitted by the schema provider;
    /// Can only be called once. Any consecutive call will result in a StreamError.
    fn event_stream(&mut self) -> Result<Receiver<SchemaProviderEvent>, StreamError>;

    /// Sender to which others should write whenever the data source schemas have
    /// changed from which the provider generates the combined schema.
    fn schema_event_sink(&mut self) -> Sender<SchemaEvent>;
}
