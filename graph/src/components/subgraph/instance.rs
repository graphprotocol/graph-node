use failure::Error;
use futures::prelude::*;

use prelude::{EntityChange, EthereumEvent, RuntimeHostBuilder, SubgraphManifest};

/// Represents a loaded instance of a subgraph.
pub trait SubgraphInstance {
    /// Creates a subgraph instance from a manifest.
    fn from_manifest<T>(manifest: SubgraphManifest, host_builder: T) -> Self
    where
        T: RuntimeHostBuilder;

    /// Process an Ethereum event and return the resulting entity changes as a future.
    fn process_event(
        &mut self,
        event: EthereumEvent,
    ) -> Box<Future<Item = Vec<EntityChange>, Error = Error>>;
}
