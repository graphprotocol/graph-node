use async_trait::async_trait;

use crate::prelude::*;

#[derive(Clone, Copy, Debug)]
pub enum SubgraphVersionSwitchingMode {
    Instant,
    Synced,
}

impl SubgraphVersionSwitchingMode {
    pub fn parse(mode: &str) -> Self {
        match mode.to_ascii_lowercase().as_str() {
            "instant" => SubgraphVersionSwitchingMode::Instant,
            "synced" => SubgraphVersionSwitchingMode::Synced,
            _ => panic!("invalid version switching mode: {:?}", mode),
        }
    }
}

/// Common trait for subgraph registrars.
#[async_trait]
pub trait SubgraphRegistrar: Send + Sync + 'static {
    async fn create_subgraph(
        &self,
        name: SubgraphName,
    ) -> Result<CreateSubgraphResult, SubgraphRegistrarError>;

    async fn create_subgraph_version(
        &self,
        name: SubgraphName,
        hash: DeploymentHash,
        assignment_node_id: NodeId,
    ) -> Result<(), SubgraphRegistrarError>;

    async fn remove_subgraph(&self, name: SubgraphName) -> Result<(), SubgraphRegistrarError>;

    async fn reassign_subgraph(
        &self,
        hash: &DeploymentHash,
        node_id: &NodeId,
    ) -> Result<(), SubgraphRegistrarError>;
}
