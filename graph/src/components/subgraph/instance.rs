use async_trait::async_trait;
use web3::types::Log;

use crate::components::subgraph::{MappingError, SharedProofOfIndexing};
use crate::prelude::*;
use crate::util::lfu_cache::LfuCache;

#[derive(Clone, Debug)]
pub struct DataSourceTemplateInfo {
    pub data_source: String,
    pub template: DataSourceTemplate,
    pub params: Vec<String>,
    pub context: Option<DataSourceContext>,
}

#[derive(Debug)]
pub struct BlockState {
    pub entity_cache: EntityCache,
    pub created_data_sources: Vec<DataSourceTemplateInfo>,
}

impl BlockState {
    pub fn new(store: Arc<dyn Store>, lfu_cache: LfuCache<EntityKey, Option<Entity>>) -> Self {
        BlockState {
            entity_cache: EntityCache::with_current(store, lfu_cache),
            created_data_sources: Vec::new(),
        }
    }
}

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
        proof_of_indexing: SharedProofOfIndexing,
    ) -> Result<BlockState, MappingError>;

    /// Like `process_trigger` but processes an Ethereum event in a given list of hosts.
    async fn process_trigger_in_runtime_hosts(
        logger: &Logger,
        hosts: &[Arc<H>],
        block: &Arc<LightEthereumBlock>,
        trigger: EthereumTrigger,
        state: BlockState,
        proof_of_indexing: SharedProofOfIndexing,
    ) -> Result<BlockState, MappingError>;

    /// Adds dynamic data sources to the subgraph.
    fn add_dynamic_data_source(
        &mut self,
        logger: &Logger,
        data_source: DataSource,
        top_level_templates: Arc<Vec<DataSourceTemplate>>,
        metrics: Arc<HostMetrics>,
    ) -> Result<Option<Arc<H>>, anyhow::Error>;
}
