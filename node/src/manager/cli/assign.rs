use graph::prelude::{anyhow::anyhow, Error, NodeId, StoreEvent};
use graph_store_postgres::{
    command_support::catalog, connection_pool::ConnectionPool, NotificationSender,
};

use crate::manager::{core, deployment::DeploymentSearch};

pub fn unassign(
    primary: ConnectionPool,
    sender: &NotificationSender,
    search: &DeploymentSearch,
) -> Result<(), Error> {
    let locator = search.locate_unique(&primary)?;

    println!("unassigning {locator}");

    core::assign::unassign(primary, sender, search)
}

pub fn reassign(
    primary: ConnectionPool,
    sender: &NotificationSender,
    search: &DeploymentSearch,
    node: String,
) -> Result<(), Error> {
    let node_id = NodeId::new(node.clone()).map_err(|()| anyhow!("illegal node id `{}`", node))?;
    let locator = search.locate_unique(&primary)?;

    let changes = match core::assign::reassign(primary, sender, search, node)? {
        Some(cur) => {
            if cur == node_id {
                println!("deployment {locator} is already assigned to {cur}");
            } else {
                println!("reassigning {locator} to {node} (was {cur})");
            }
        }
        None => {
            println!("assigning {locator} to {node}");
        }
    };

    // It's easy to make a typo in the name of the node; if this operation
    // assigns to a node that wasn't used before, warn the user that they
    // might have mistyped the node name
    let mirror = catalog::Mirror::primary_only(primary);
    let count = mirror.assignments(&node_id)?.len();
    if count == 1 {
        println!("warning: this is the only deployment assigned to {node}");
        println!("         are you sure it is spelled correctly?");
    }
    Ok(())
}
