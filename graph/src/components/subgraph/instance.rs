use failure::Error;
use futures::prelude::*;
use std::sync::Arc;

use prelude::*;
use web3::types::{Block, Log, Transaction};

/// Represents a loaded instance of a subgraph.
pub trait SubgraphInstance<T>
where
    T: RuntimeHostBuilder,
{
    /// Creates a subgraph instance from a manifest.
    fn from_manifest(manifest: SubgraphManifest, host_builder: T) -> Self;

    /// Returns true if the subgraph has a handler for an Ethereum event.
    fn matches_log(&self, log: &Log) -> bool;

    /// Process an Ethereum event and return the resulting entity operations as a future.
    fn process_log(
        &self,
        block: Arc<EthereumBlock>,
        transaction: Arc<Transaction>,
        log: Log,
        entity_operations: Vec<EntityOperation>,
    ) -> Box<Future<Item = Vec<EntityOperation>, Error = Error> + Send>;
}
