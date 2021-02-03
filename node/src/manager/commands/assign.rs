use std::sync::Arc;

use graph::prelude::{anyhow, Error, NodeId, SubgraphDeploymentId, SubgraphStore as _};
use graph_store_postgres::SubgraphStore;

pub fn unassign(store: Arc<SubgraphStore>, id: String) -> Result<(), Error> {
    let id =
        SubgraphDeploymentId::new(id).map_err(|id| anyhow!("illegal deployment id `{}`", id))?;

    println!("unassigning {}", id.as_str());
    store.unassign_subgraph(&id)?;

    Ok(())
}

pub fn reassign(store: Arc<SubgraphStore>, id: String, node: String) -> Result<(), Error> {
    let id =
        SubgraphDeploymentId::new(id).map_err(|id| anyhow!("illegal deployment id `{}`", id))?;
    let node = NodeId::new(node.clone()).map_err(|()| anyhow!("illegal node id `{}`", node))?;

    println!("reassigning {} to {}", id.as_str(), node.as_str());
    store.reassign_subgraph(&id, &node)?;

    Ok(())
}
