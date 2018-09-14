use prelude::*;

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
}

pub trait RuntimeHostBuilder: Clone + Send + 'static {
    type Host: RuntimeHost;

    /// Build a new runtime host for a dataset.
    fn build(&self, subgraph_manifest: SubgraphManifest, data_source: DataSource) -> Self::Host;
}
