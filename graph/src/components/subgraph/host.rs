use failure::Error;
use futures::prelude::*;

use prelude::*;

/// Common trait for runtime host implementations.
pub trait RuntimeHost: Send + Sync {
    /// The subgraph definition the runtime is for.
    fn subgraph_manifest(&self) -> &SubgraphManifest;

    /// Returns true if the RuntimeHost has a handler for an EthereumEvent.
    fn matches_event(&self, event: &EthereumEvent) -> bool;

    /// Process an Ethereum event and return a vector of entity operations.
    fn process_event(
        &self,
        event: EthereumEvent,
    ) -> Box<Future<Item = Vec<EntityOperation>, Error = Error> + Send>;
}

pub trait RuntimeHostBuilder: Clone + Send + 'static {
    type Host: RuntimeHost;

    /// Build a new runtime host for a dataset.
    fn build(&self, subgraph_manifest: SubgraphManifest, data_source: DataSource) -> Self::Host;
}
