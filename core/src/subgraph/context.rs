use crate::subgraph::SubgraphInstance;
use graph::{
    blockchain::Blockchain,
    components::store::DeploymentId,
    prelude::{CancelGuard, RuntimeHostBuilder},
};

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub type SharedInstanceKeepAliveMap = Arc<RwLock<HashMap<DeploymentId, CancelGuard>>>;

pub struct IndexingContext<T: RuntimeHostBuilder<C>, C: Blockchain> {
    pub instance: SubgraphInstance<C, T>,
    pub instances: SharedInstanceKeepAliveMap,
    pub filter: C::TriggerFilter,
}
