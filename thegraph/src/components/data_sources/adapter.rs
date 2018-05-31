use futures::sync::mpsc::Receiver;

use components::store::StoreKey;
use data::store::Entity;
use util::stream::StreamError;

/// Type alias for data source IDs.
type DataSourceID = String;

/// Events emitted by a runtime adapter.
#[derive(Debug, Clone)]
pub enum RuntimeAdapterEvent {
    // An entity should be added.
    EntityAdded(DataSourceID, StoreKey, Entity),
    // An entity should be updated.
    EntityChanged(DataSourceID, StoreKey, Entity),
    // An entity should be removed.
    EntityRemoved(DataSourceID, StoreKey, Entity),
}

/// Common trait for runtime adapter implementations.
pub trait RuntimeAdapter {
    /// Starts the underlying data source runtime.
    fn start(&mut self);

    /// Stops the underlying data source runtime.
    fn stop(&mut self);

    /// Receiver from which otehrs can read runtime events emitted by the adapter.
    /// Can only be called once. Any consecutive calls will result in a StreamError.
    fn event_stream(&mut self) -> Result<Receiver<RuntimeAdapterEvent>, StreamError>;
}
