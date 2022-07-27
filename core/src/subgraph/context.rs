use crate::subgraph::SubgraphInstance;
use graph::{
    blockchain::Blockchain,
    components::store::DeploymentId,
    prelude::{CancelGuard, RuntimeHostBuilder},
};

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::TriggerProcessor;

pub type SharedInstanceKeepAliveMap = Arc<RwLock<HashMap<DeploymentId, CancelGuard>>>;

pub struct IndexingContext<C, T, TP>
where
    T: RuntimeHostBuilder<C>,
    C: Blockchain,
    TP: TriggerProcessor<C, T>,
{
    pub instance: SubgraphInstance<C, T, TP>,
    pub instances: SharedInstanceKeepAliveMap,
    pub filter: C::TriggerFilter,
}
