use async_trait::async_trait;
use web3::types::Log;

use crate::prelude::*;

/// Represents a loaded instance of a subgraph.
#[async_trait]
pub trait SubgraphInstance<H: RuntimeHost> {
    /// Returns true if the subgraph has a handler for an Ethereum event.
    fn matches_log(&self, log: &Log) -> bool;

    /// Process and Ethereum trigger and return the resulting entity operations as a future.
    async fn process_trigger(
        &self,
        logger: &Logger,
        block: &Arc<LightEthereumBlock>,
        trigger: EthereumTrigger,
        state: BlockState,
    ) -> Result<BlockState, Error>;

    /// Like `process_trigger` but processes an Ethereum event in a given list of hosts.
    async fn process_trigger_in_runtime_hosts(
        logger: &Logger,
        hosts: &[Arc<H>],
        block: &Arc<LightEthereumBlock>,
        trigger: EthereumTrigger,
        state: BlockState,
    ) -> Result<BlockState, Error>;

    /// Adds dynamic data sources to the subgraph.
    fn add_dynamic_data_source(
        &mut self,
        logger: &Logger,
        data_source: DataSource,
        top_level_templates: Arc<Vec<DataSourceTemplate>>,
        metrics: Arc<HostMetrics>,
    ) -> Result<Arc<H>, Error>;
}
