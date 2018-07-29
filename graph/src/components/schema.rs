use super::{EventConsumer, EventProducer};

use super::subgraph::SchemaEvent;
use data::schema::Schema;

/// Events emitted by [SchemaProvider](trait.SchemaProvider.html) implementations.
#[derive(Clone, Debug)]
pub enum SchemaProviderEvent {
    /// The provided schema has changed.
    SchemaChanged(Option<Schema>),
}

/// A `SchemaProvider` is responsible for spawning a task that listens to the
/// changes in the underlining subgraph providers (`EventConsumer`) and
/// consolidates the received data into a single schema which is then
/// broadcasted to higher-level consumers (`EventProducer`), such as query runners
/// and the GraphQL server itself.
///
/// The task should be spawned upon construction of the provider, with the
/// return value providing the required input and output handles.
pub trait SchemaProvider: EventProducer<SchemaProviderEvent> + EventConsumer<SchemaEvent> {}
