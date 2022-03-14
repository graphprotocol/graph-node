use crate::subgraph::SubgraphInstance;
use graph::{
    blockchain::Blockchain,
    components::store::DeploymentId,
    prelude::{CancelGuard, Entity, EntityKey, RuntimeHostBuilder},
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
