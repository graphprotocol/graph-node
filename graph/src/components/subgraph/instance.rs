use async_trait::async_trait;
use web3::types::Log;

use crate::prelude::*;
use crate::util::lfu_cache::LfuCache;
use crate::{
    components::subgraph::{MappingError, SharedProofOfIndexing},
    data::subgraph::schema::SubgraphError,
};

#[derive(Clone, Debug)]
pub struct DataSourceTemplateInfo {
    pub data_source: String,
    pub template: DataSourceTemplate,
    pub params: Vec<String>,
    pub context: Option<DataSourceContext>,
    pub creation_block: u64,
}

#[derive(Debug)]
pub struct BlockState {
    pub entity_cache: EntityCache,
    pub deterministic_errors: Vec<SubgraphError>,
    created_data_sources: Vec<DataSourceTemplateInfo>,

    // Data sources created in the current handler.
    handler_created_data_sources: Vec<DataSourceTemplateInfo>,

    // Marks whether a handler is currently executing.
    in_handler: bool,
}

impl BlockState {
    pub fn new(
        store: Arc<dyn SubgraphStore>,
        lfu_cache: LfuCache<EntityKey, Option<Entity>>,
    ) -> Self {
        BlockState {
            entity_cache: EntityCache::with_current(store, lfu_cache),
            deterministic_errors: Vec::new(),
            created_data_sources: Vec::new(),
            handler_created_data_sources: Vec::new(),
            in_handler: false,
        }
    }

    pub fn extend(&mut self, other: BlockState) {
        assert!(!other.in_handler);

        let BlockState {
            entity_cache,
            deterministic_errors,
            created_data_sources,
            handler_created_data_sources,
            in_handler,
        } = self;

        match in_handler {
            true => handler_created_data_sources.extend(other.created_data_sources),
            false => created_data_sources.extend(other.created_data_sources),
        }
        deterministic_errors.extend(other.deterministic_errors);
        entity_cache.extend(other.entity_cache);
    }

    pub fn has_errors(&self) -> bool {
        !self.deterministic_errors.is_empty()
    }

    pub fn has_created_data_sources(&self) -> bool {
        assert!(!self.in_handler);
        !self.created_data_sources.is_empty()
    }

    pub fn drain_created_data_sources(&mut self) -> Vec<DataSourceTemplateInfo> {
        assert!(!self.in_handler);
        std::mem::replace(&mut self.created_data_sources, Vec::new())
    }

    pub fn enter_handler(&mut self) {
        assert!(!self.in_handler);
        self.in_handler = true;
        self.entity_cache.enter_handler()
    }

    pub fn exit_handler(&mut self) {
        assert!(self.in_handler);
        self.in_handler = false;
        self.created_data_sources
            .extend(self.handler_created_data_sources.drain(..));
        self.entity_cache.exit_handler()
    }

    pub fn exit_handler_and_discard_changes_due_to_error(&mut self, e: SubgraphError) {
        assert!(self.in_handler);
        self.in_handler = false;
        self.handler_created_data_sources.clear();
        self.entity_cache.exit_handler_and_discard_changes();
        self.deterministic_errors.push(e);
    }

    pub fn push_created_data_source(&mut self, ds: DataSourceTemplateInfo) {
        assert!(self.in_handler);
        self.handler_created_data_sources.push(ds);
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

    fn revert_data_sources(&mut self, reverted_block: u64);
}
