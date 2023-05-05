use graph::prelude::{anyhow::anyhow, Error, NodeId, StoreEvent};
use graph_store_postgres::{
    command_support::catalog, connection_pool::ConnectionPool, NotificationSender,
};

use crate::manager::deployment::DeploymentSearch;

pub fn unassign(
    primary: ConnectionPool,
    sender: &NotificationSender,
    search: &DeploymentSearch,
) -> Result<(), Error> {
    let locator = search.locate_unique(&primary)?;

    let conn = primary.get()?;
    let conn = catalog::Connection::new(conn);

    let site = conn
        .locate_site(locator.clone())?
        .ok_or_else(|| anyhow!("failed to locate site for {locator}"))?;

    let changes = conn.unassign_subgraph(&site)?;
    conn.send_store_event(sender, &StoreEvent::new(changes))?;

    Ok(())
}

pub fn reassign(
    primary: ConnectionPool,
    sender: &NotificationSender,
    search: &DeploymentSearch,
    node: String,
) -> Result<Option<NodeId>, Error> {
    let node = NodeId::new(node.clone()).map_err(|()| anyhow!("illegal node id `{}`", node))?;
    let locator = search.locate_unique(&primary)?;

    let conn = primary.get()?;
    let conn = catalog::Connection::new(conn);

    let site = conn
        .locate_site(locator.clone())?
        .ok_or_else(|| anyhow!("failed to locate site for {locator}"))?;
    let changes = match conn.assigned_node(&site)? {
        Some(cur) => {
            if cur == node {
                vec![]
            } else {
                conn.reassign_subgraph(&site, &node)?
            }
        }
        None => conn.assign_subgraph(&site, &node)?,
    };
    conn.send_store_event(sender, &StoreEvent::new(changes))?;

    // Return the assigned node for further processing
    Ok(conn.assigned_node(&site)?)
}
