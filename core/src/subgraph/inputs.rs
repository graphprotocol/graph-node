use graph::{
    blockchain::{Blockchain, TriggersAdapter},
    components::{
        store::{DeploymentLocator, SubgraphFork, WritableStore},
        subgraph::ProofOfIndexingVersion,
    },
    data::subgraph::{SubgraphFeature, UnifiedMappingApiVersion},
    data_source::DataSourceTemplate,
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
    pub templates: Arc<Vec<DataSourceTemplate<C>>>,
    pub unified_api_version: UnifiedMappingApiVersion,
    pub static_filters: bool,
    pub poi_version: ProofOfIndexingVersion,
    pub network: String,

    // Correspondence between data source or template position in the manifest and name.
    pub manifest_idx_and_name: Vec<(u32, String)>,

    /// Whether to instrument trigger processing and log additional,
    /// possibly expensive and noisy, information
    pub instrument: bool,
}
