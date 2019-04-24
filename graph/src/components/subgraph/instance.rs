use crate::prelude::*;
use web3::types::Log;

#[derive(Clone, Debug, PartialEq)]
pub struct DataSourceTemplateInfo {
    pub data_source: String,
    pub template: String,
    pub params: Vec<String>,
}

#[derive(Debug, Default)]
pub struct BlockState {
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
        host_builder: &T,
    ) -> Result<Self, Error>;

    /// Returns true if the subgraph has a handler for an Ethereum event.
    fn matches_log(&self, log: &Log) -> bool;

    /// Process and Ethereum trigger and return the resulting entity operations as a future.
    fn process_trigger(
        &self,
        logger: &Logger,
        block: Arc<EthereumBlock>,
        trigger: EthereumTrigger,
        state: BlockState,
    ) -> Box<Future<Item = BlockState, Error = Error> + Send>;

    /// Like `process_trigger` but processes an Ethereum event in a given list of hosts.
    fn process_trigger_in_runtime_hosts<I>(
        logger: &Logger,
        hosts: I,
        block: Arc<EthereumBlock>,
        trigger: EthereumTrigger,
        state: BlockState,
    ) -> Box<Future<Item = BlockState, Error = Error> + Send>
    where
        I: IntoIterator<Item = Arc<T::Host>>;

    /// Adds dynamic data sources to the subgraph.
    fn add_dynamic_data_sources(&mut self, runtime_hosts: Vec<Arc<T::Host>>) -> Result<(), Error>;
}
