use failure::Error;
use futures::prelude::*;

use prelude::{EntityOperation, EthereumEvent, RuntimeHostBuilder, SubgraphManifest};
use web3::types::Log;

/// Represents a loaded instance of a subgraph.
pub trait SubgraphInstance<T>
where
    T: RuntimeHostBuilder,
{
    /// Creates a subgraph instance from a manifest.
    fn from_manifest(manifest: SubgraphManifest, host_builder: T) -> Self;

    /// Parses an Ethereum log into an event; fails if it doesn't match the subgraph contracts.
    fn parse_log(&self, log: &Log) -> Result<EthereumEvent, Error>;

    /// Process an Ethereum event and return the resulting entity operations as a future.
    fn process_event(
        &self,
        event: EthereumEvent,
    ) -> Box<Future<Item = Vec<EntityOperation>, Error = Error> + Send>;
}
