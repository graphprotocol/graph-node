use graph::blockchain::TriggersAdapter;
use graph::components::store::WritableStore;
use graph::{
    blockchain::Blockchain,
    components::store::{DeploymentLocator, SubgraphFork},
    data::subgraph::{SubgraphFeature, UnifiedMappingApiVersion},
    prelude::BlockNumber,
};
use std::collections::BTreeSet;
use std::sync::Arc;

pub struct IndexingInputs<C: Blockchain> {
    pub deployment: DeploymentLocator,
    pub features: BTreeSet<SubgraphFeature>,
    pub start_blocks: Vec<BlockNumber>,
    pub stop_block: Option<BlockNumber>,
    pub store: Arc<dyn WritableStore>,
    pub debug_fork: Option<Arc<dyn SubgraphFork>>,
    pub triggers_adapter: Arc<dyn TriggersAdapter<C>>,
    pub chain: Arc<C>,
    pub templates: Arc<Vec<C::DataSourceTemplate>>,
    pub unified_api_version: UnifiedMappingApiVersion,
    pub static_filters: bool,
}
