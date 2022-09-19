use std::collections::BTreeSet;
use std::sync::Arc;

use graph::blockchain::{Blockchain, TriggersAdapter};
use graph::components::store::{DeploymentLocator, SubgraphFork, WritableStore};
use graph::components::subgraph::ProofOfIndexingVersion;
use graph::data::subgraph::{SubgraphFeature, UnifiedMappingApiVersion};
use graph::data_source::DataSourceTemplate;
use graph::prelude::BlockNumber;

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
}
