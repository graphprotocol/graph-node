use crate::prelude::{DeploymentHash, Entity};
use anyhow::Result;

// TODO: add docs
pub trait SubgraphForker: Send + Sync + 'static {
    type Fork;
    fn fork(&self, subgraph_id: &DeploymentHash) -> Result<Self::Fork>;
}

// TODO: add docs
pub trait SubgraphFork {
    fn fetch(self, entity_type: String, id: String) -> Result<Option<Entity>>;
}
