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
    pub deterministic_errors: Vec<anyhow::Error>,
    pub created_data_sources: im::Vector<DataSourceTemplateInfo>,
}

pub struct BlockStateUpdatesSnapshot {
    pub entity_updates: im::HashMap<EntityKey, Option<Entity>>,
    pub created_data_sources: im::Vector<DataSourceTemplateInfo>,
}

impl BlockState {
    pub fn new(store: Arc<dyn Store>, lfu_cache: LfuCache<EntityKey, Option<Entity>>) -> Self {
        BlockState {
            entity_cache: EntityCache::with_current(store, lfu_cache),
            deterministic_errors: Vec::new(),
            created_data_sources: im::Vector::new(),
        }
    }

    pub fn extend(&mut self, other: BlockState) -> Result<(), anyhow::Error> {
        let BlockState {
            entity_cache,
            deterministic_errors,
            created_data_sources,
        } = self;
        entity_cache
            .extend(other.entity_cache)
            .map_err(anyhow::Error::from)?;
        created_data_sources.extend(other.created_data_sources);
        deterministic_errors.extend(other.deterministic_errors);
        Ok(())
    }

    /// Returns a clone of the pending updates in the block state.
    /// This uses peristent data structures so it is a cheap operation.
    pub fn updates_snapshot(&self) -> BlockStateUpdatesSnapshot {
        BlockStateUpdatesSnapshot {
            entity_updates: self.entity_cache.updates_snapshot(),
            created_data_sources: self.created_data_sources.clone(),
        }
    }

    pub fn restore_updates_snapshot_due_to_error(
        &mut self,
        snapshot: BlockStateUpdatesSnapshot,
        error: anyhow::Error,
    ) {
        self.entity_cache
            .restore_updates_snapshot(snapshot.entity_updates);
        self.created_data_sources = snapshot.created_data_sources;
        self.deterministic_errors.push(error);
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
