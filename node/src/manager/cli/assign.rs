use graph::prelude::Error;
use graph_store_postgres::{
    command_support::catalog, connection_pool::ConnectionPool, NotificationSender,
};

use crate::manager::{
    core::{self, assign::ReassignResult},
    deployment::DeploymentSearch,
};

pub fn unassign(
    primary: ConnectionPool,
    sender: &NotificationSender,
    search: &DeploymentSearch,
) -> Result<(), Error> {
    let locator = core::assign::unassign(primary, sender, search)?;

    println!("unassigning {locator}");

    Ok(())
}

pub fn reassign(
    primary: ConnectionPool,
    sender: &NotificationSender,
    search: &DeploymentSearch,
    node: String,
) -> Result<(), Error> {
    let ReassignResult {
        node_id,
        assigned_node,
        locator,
    } = core::assign::reassign(primary.clone(), sender, search, node.clone())?;

    match assigned_node {
        Some(cur) => {
            if cur == node_id {
                println!("deployment {locator} is already assigned to {cur}");
            } else {
                println!("reassigning {locator} to {node_id} (was {cur})");
            }
        }
        None => {
            println!("assigning {locator} to {node_id}");
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
