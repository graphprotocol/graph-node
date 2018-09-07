use prelude::*;

use components::ethereum::EthereumEvent;
use components::ethereum::EthereumEventFilter;

/// Events emitted by a runtime host.
#[derive(Debug, Clone, PartialEq)]
pub enum RuntimeHostEvent {
    /// An entity should be created or updated.
    EntitySet(StoreKey, Entity, EventSource),
    /// An entity should be removed.
    EntityRemoved(StoreKey, EventSource),
}

/// Common trait for runtime host implementations.
pub trait RuntimeHost: EventProducer<RuntimeHostEvent> + Send {
    /// The subgraph definition the runtime is for.
    fn subgraph_manifest(&self) -> &SubgraphManifest;

    /// An event filter matching all Ethereum events that this runtime host is interested in.
    fn event_filter(&self) -> EthereumEventFilter;

    /// Called when the runtime host should handle an Ethereum event.
    /// Some events provided may not match the event filter (see above).
    /// Runtime hosts should ignore events they are not interested in.
    fn process_event(
        &mut self,
        event: EthereumEvent,
    ) -> Box<Future<Item = (), Error = Error> + Send>;
}

pub trait RuntimeHostBuilder: Send + 'static {
    type Host: RuntimeHost;

    /// Build a new runtime host for a dataset.
    fn build(&mut self, subgraph_manifest: SubgraphManifest, data_source: DataSource)
        -> Self::Host;
}
