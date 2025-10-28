use crate::prelude::BlockNumber;
use std::sync::Arc;

use crate::components::store::DeploymentLocator;

/// A `SubgraphInstanceManager` loads and manages subgraph instances.
///
/// When a subgraph is added, the subgraph instance manager creates and starts
/// a subgraph instances for the subgraph. When a subgraph is removed, the
/// subgraph instance manager stops and removes the corresponding instance.
#[async_trait::async_trait]
pub trait SubgraphInstanceManager: Send + Sync + 'static {
    /// Returns `true` if this manager has the necessary capabilities to manage the subgraph.
    fn can_manage(
        &self,
        deployment: &DeploymentLocator,
        raw_manifest: &serde_yaml::Mapping,
    ) -> bool;

    async fn start_subgraph(
        self: Arc<Self>,
        deployment: DeploymentLocator,
        stop_block: Option<BlockNumber>,
    );
    async fn stop_subgraph(&self, deployment: DeploymentLocator);
}
