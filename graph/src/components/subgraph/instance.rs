use crate::prelude::*;
use web3::types::Log;

#[derive(Clone, Debug)]
pub struct DataSourceTemplateInfo {
    pub data_source: String,
    pub template: DataSourceTemplate,
    pub params: Vec<String>,
}

#[derive(Debug, Default)]
pub struct BlockState {
    pub entity_cache: EntityCache,
    pub created_data_sources: Vec<DataSourceTemplateInfo>,
}

/// Represents a loaded instance of a subgraph.
pub trait SubgraphInstance<H: RuntimeHost> {
    /// Returns true if the subgraph has a handler for an Ethereum event.
    fn matches_log(&self, log: &Log) -> bool;

    /// Process and Ethereum trigger and return the resulting entity operations as a future.
    fn process_trigger(
        &self,
        logger: &Logger,
        block: Arc<LightEthereumBlock>,
        trigger: EthereumTrigger,
        state: BlockState,
    ) -> Box<dyn Future<Item = BlockState, Error = Error> + Send>;

    /// Like `process_trigger` but processes an Ethereum event in a given list of hosts.
    fn process_trigger_in_runtime_hosts(
        logger: &Logger,
        hosts: impl Iterator<Item = Arc<H>>,
        block: Arc<LightEthereumBlock>,
        trigger: EthereumTrigger,
        state: BlockState,
    ) -> Box<dyn Future<Item = BlockState, Error = Error> + Send>;

    /// Adds dynamic data sources to the subgraph.
    fn add_dynamic_data_source(
        &mut self,
        logger: &Logger,
        data_source: DataSource,
        top_level_templates: Vec<DataSourceTemplate>,
        metrics: Arc<HostMetrics>,
    ) -> Result<Arc<H>, Error>;
}
