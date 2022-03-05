use crate::subgraph::metrics::SubgraphInstanceMetrics;
use crate::subgraph::SubgraphInstance;
use graph::{
    blockchain::{block_stream::BlockStreamMetrics, Blockchain},
    components::store::DeploymentId,
    prelude::{CancelGuard, Entity, EntityKey, HostMetrics, RuntimeHostBuilder},
    util::lfu_cache::LfuCache,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub type SharedInstanceKeepAliveMap = Arc<RwLock<HashMap<DeploymentId, CancelGuard>>>;

pub struct IndexingState<T: RuntimeHostBuilder<C>, C: Blockchain> {
    pub instance: SubgraphInstance<C, T>,
    pub instances: SharedInstanceKeepAliveMap,
    pub filter: C::TriggerFilter,
    pub entity_lfu_cache: LfuCache<EntityKey, Option<Entity>>,
}

pub struct IndexingContext<T: RuntimeHostBuilder<C>, C: Blockchain> {
    /// Mutable state that may be modified while indexing a subgraph.
    pub state: IndexingState<T, C>,
    /// Sensors to measure the execution of the subgraph instance
    pub subgraph_metrics: Arc<SubgraphInstanceMetrics>,
    /// Sensors to measure the execution of the subgraph's runtime hosts
    pub host_metrics: Arc<HostMetrics>,
    pub block_stream_metrics: Arc<BlockStreamMetrics>,
}
