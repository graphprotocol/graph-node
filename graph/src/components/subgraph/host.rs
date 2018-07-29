use prelude::*;

/// Type alias for subgraph IDs.
type SubgraphID = String;

/// Events emitted by a runtime host.
#[derive(Debug, Clone, PartialEq)]
pub enum RuntimeHostEvent {
    /// An entity should be create or updated.
    EntitySet(SubgraphID, StoreKey, Entity),
    /// An entity should be removed.
    EntityRemoved(SubgraphID, StoreKey),
}

/// Common trait for runtime host implementations.
pub trait RuntimeHost: EventProducer<RuntimeHostEvent> {
    /// The subgraph definition the runtime is for.
    fn subgraph_manifest(&self) -> &SubgraphManifest;
}

pub trait RuntimeHostBuilder {
    type Host: RuntimeHost;

    /// Build a new runtime host for a dataset.
    fn build(&mut self, subgraph_manifest: SubgraphManifest, data_source: DataSource)
        -> Self::Host;
}
