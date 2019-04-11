use crate::prelude::*;
use web3::types::{Log, Transaction};

#[derive(Debug)]
pub struct DataSourceTemplateInfo {
    pub data_source: String,
    pub template: String,
    pub params: Vec<String>,
}

#[derive(Debug, Default)]
pub struct ProcessingState {
    pub entity_operations: Vec<EntityOperation>,
    pub created_data_sources: Vec<DataSourceTemplateInfo>,
}

/// Represents a loaded instance of a subgraph.
pub trait SubgraphInstance<T>: Sized + Sync
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
