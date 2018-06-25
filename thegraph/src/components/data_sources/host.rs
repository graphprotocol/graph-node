use futures::sync::mpsc::Receiver;

use components::store::StoreKey;
use prelude::*;
use util::stream::StreamError;

/// Type alias for data source IDs.
type DataSourceID = String;

/// Events emitted by a runtime host.
#[derive(Debug, Clone)]
pub enum RuntimeHostEvent {
    // An entity should be created.
    EntityCreated(DataSourceID, StoreKey, Entity),
    // An entity should be updated.
    EntityChanged(DataSourceID, StoreKey, Entity),
    // An entity should be removed.
    EntityRemoved(DataSourceID, StoreKey),
}

/// Common trait for runtime host implementations.
pub trait RuntimeHost {
    /// Starts the underlying data source runtime.
    fn start(&mut self);

    /// Stops the underlying data source runtime.
    fn stop(&mut self);

    /// Receiver from which others can read runtime events emitted by the host.
    /// Can only be called once. Any consecutive calls will result in a StreamError.
    fn event_stream(&mut self) -> Result<Receiver<RuntimeHostEvent>, StreamError>;

    /// The data source definition the runtime is for.
    fn data_source_definition(&self) -> &DataSourceDefinition;
}

pub trait RuntimeHostBuilder {
    type Host: RuntimeHost;

    /// Build a new runtime host
    fn build(&mut self, data_source_definition: DataSourceDefinition) -> Self::Host;
}
