use graph::{
    blockchain::{block_stream::TriggersAdapterWrapper, Blockchain},
    components::{
        store::{DeploymentLocator, SourceableStore, SubgraphFork, WritableStore},
        subgraph::ProofOfIndexingVersion,
    },
    data::subgraph::{SubgraphFeature, UnifiedMappingApiVersion, SPEC_VERSION_1_3_0},
    data_source::DataSourceTemplate,
    prelude::BlockNumber,
    semver::Version,
};
use std::collections::BTreeSet;
use std::sync::Arc;

pub struct IndexingInputs<C: Blockchain> {
    pub deployment: DeploymentLocator,
    pub features: BTreeSet<SubgraphFeature>,
    pub start_blocks: Vec<BlockNumber>,
    pub end_blocks: BTreeSet<BlockNumber>,
    pub source_subgraph_stores: Vec<Arc<dyn SourceableStore>>,
    pub stop_block: Option<BlockNumber>,
    pub max_end_block: Option<BlockNumber>,
    pub store: Arc<dyn WritableStore>,
    pub debug_fork: Option<Arc<dyn SubgraphFork>>,
    pub triggers_adapter: Arc<TriggersAdapterWrapper<C>>,
    pub chain: Arc<C>,
    pub templates: Arc<Vec<DataSourceTemplate<C>>>,
    pub unified_api_version: UnifiedMappingApiVersion,
    pub static_filters: bool,
    pub poi_version: ProofOfIndexingVersion,
    pub network: String,
    pub spec_version: Version,

    /// Whether to instrument trigger processing and log additional,
    /// possibly expensive and noisy, information
    pub instrument: bool,
}

impl<C: Blockchain> IndexingInputs<C> {
    pub fn with_store(&self, store: Arc<dyn WritableStore>) -> Self {
        let IndexingInputs {
            deployment,
            features,
            start_blocks,
            end_blocks,
            source_subgraph_stores,
            stop_block,
            max_end_block,
            store: _,
            debug_fork,
            triggers_adapter,
            chain,
            templates,
            unified_api_version,
            static_filters,
            poi_version,
            network,
            spec_version,
            instrument,
        } = self;
        IndexingInputs {
            deployment: deployment.clone(),
            features: features.clone(),
            start_blocks: start_blocks.clone(),
            end_blocks: end_blocks.clone(),
            source_subgraph_stores: source_subgraph_stores.clone(),
            stop_block: stop_block.clone(),
            max_end_block: max_end_block.clone(),
            store,
            debug_fork: debug_fork.clone(),
            triggers_adapter: triggers_adapter.clone(),
            chain: chain.clone(),
            templates: templates.clone(),
            unified_api_version: unified_api_version.clone(),
            static_filters: *static_filters,
            poi_version: *poi_version,
            network: network.clone(),
            spec_version: spec_version.clone(),
            instrument: *instrument,
        }
    }

    // Whether to use strict vid order for the subgraph
    // This is true for all subgraphs with spec version 1.3.0 or greater
    pub fn strict_vid_order(&self) -> bool {
        self.spec_version >= SPEC_VERSION_1_3_0
    }
}
