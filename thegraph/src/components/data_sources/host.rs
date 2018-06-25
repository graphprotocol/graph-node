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
pub trait RuntimeHost: EventProducer<RuntimeHostEvent> {
    /// Starts the underlying data source runtime.
    fn start(&mut self);

    /// Stops the underlying data source runtime.
    fn stop(&mut self);

    /// The data source definition the runtime is for.
    fn data_source_definition(&self) -> &DataSourceDefinition;
}

pub trait RuntimeHostBuilder {
    type Host: RuntimeHost;

    /// Build a new runtime host
    fn build(&mut self, data_source_definition: DataSourceDefinition) -> Self::Host;
}
