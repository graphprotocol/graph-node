use crate::prelude::*;
use web3::types::{Log, Transaction};

#[derive(Debug, Default)]
pub struct ProcessingState {
    pub entity_operations: Vec<EntityOperation>,
}

/// Represents a loaded instance of a subgraph.
pub trait SubgraphInstance<T>: Sized
where
    T: RuntimeHostBuilder,
{
    /// Creates a subgraph instance from a manifest.
    fn from_manifest(
        logger: &Logger,
        manifest: SubgraphManifest,
        host_builder: T,
    ) -> Result<Self, Error>;

    /// Returns true if the subgraph has a handler for an Ethereum event.
    fn matches_log(&self, log: &Log) -> bool;

    /// Process an Ethereum event and return the resulting entity operations as a future.
    fn process_log(
        &self,
        logger: &Logger,
        block: Arc<EthereumBlock>,
        transaction: Arc<Transaction>,
        log: Log,
        state: ProcessingState,
    ) -> Box<Future<Item = ProcessingState, Error = Error> + Send>;
}
