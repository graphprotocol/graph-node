use std::sync::Arc;

use graph::{
    components::store::DeploymentLocator,
    prelude::{
        anyhow::{anyhow, bail},
        Error, NodeId, SubgraphStore as _,
    },
};
use graph_store_postgres::SubgraphStore;

fn locate(store: &SubgraphStore, hash: String) -> Result<DeploymentLocator, Error> {
    let locators = store.locators(&hash)?;

    match locators.len() {
        0 => {
            bail!("no matching assignment");
        }
        1 => Ok(locators[0].clone()),
        _ => {
            bail!(
                "deployment hash `{}` is ambiguous: {} locations found",
                hash,
                locators.len()
            );
        }
    }
}

pub fn unassign(store: Arc<SubgraphStore>, hash: String) -> Result<(), Error> {
    let deployment = locate(store.as_ref(), hash)?;

    println!("unassigning {}", deployment);
    store.writable(&deployment)?.unassign_subgraph()?;

    Ok(())
}

pub fn reassign(store: Arc<SubgraphStore>, hash: String, node: String) -> Result<(), Error> {
    let node = NodeId::new(node.clone()).map_err(|()| anyhow!("illegal node id `{}`", node))?;
    let deployment = locate(store.as_ref(), hash)?;

    println!("reassigning {} to {}", deployment, node.as_str());
    store.reassign_subgraph(&deployment, &node)?;

    Ok(())
}
