use failure::Error;
use futures::prelude::*;

use prelude::{EntityOperation, EthereumEvent, RuntimeHostBuilder, SubgraphManifest};

/// Represents a loaded instance of a subgraph.
pub trait SubgraphInstance<T>
where
    T: RuntimeHostBuilder,
{
    /// Creates a subgraph instance from a manifest.
    fn from_manifest(manifest: SubgraphManifest, host_builder: T) -> Self;

    /// Process an Ethereum event and return the resulting entity operations as a future.
    fn process_event(
        &self,
        event: EthereumEvent,
    ) -> Box<Future<Item = Vec<EntityOperation>, Error = Error>>;
}
