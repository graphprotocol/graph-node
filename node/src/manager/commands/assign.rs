use std::sync::Arc;

use graph::prelude::{anyhow::anyhow, Error, NodeId, SubgraphStore as _};
use graph_store_postgres::SubgraphStore;

use crate::manager::deployment::locate;

pub fn unassign(
    store: Arc<SubgraphStore>,
    hash: String,
    shard: Option<String>,
) -> Result<(), Error> {
    let deployment = locate(store.as_ref(), hash, shard)?;

    println!("unassigning {}", deployment);
    store.writable(&deployment)?.unassign_subgraph()?;

    Ok(())
}

pub fn reassign(
    store: Arc<SubgraphStore>,
    hash: String,
    node: String,
    shard: Option<String>,
) -> Result<(), Error> {
    let node = NodeId::new(node.clone()).map_err(|()| anyhow!("illegal node id `{}`", node))?;
    let deployment = locate(store.as_ref(), hash, shard)?;

    println!("reassigning {} to {}", deployment, node.as_str());
    store.reassign_subgraph(&deployment, &node)?;

    Ok(())
}
