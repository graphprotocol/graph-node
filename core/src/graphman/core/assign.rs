use graph::{
    components::store::DeploymentLocator,
    prelude::{anyhow::anyhow, Error, NodeId, StoreEvent},
};
use graph_store_postgres::{
    command_support::catalog, connection_pool::ConnectionPool, NotificationSender,
};

use crate::graphman::deployment::DeploymentSearch;

pub fn unassign(
    primary: ConnectionPool,
    sender: &NotificationSender,
    search: &DeploymentSearch,
) -> Result<DeploymentLocator, Error> {
    let locator = search.locate_unique(&primary)?;

    let conn = primary.get()?;
    let conn = catalog::Connection::new(conn);

    let site = conn
        .locate_site(locator.clone())?
        .ok_or_else(|| anyhow!("failed to locate site for {locator}"))?;

    let changes = conn.unassign_subgraph(&site)?;
    conn.send_store_event(sender, &StoreEvent::new(changes))?;

    Ok(locator)
}

pub struct ReassignResult {
    pub locator: DeploymentLocator,
    pub node_id: NodeId,
    pub assigned_node: Option<NodeId>,
}

pub fn reassign(
    primary: ConnectionPool,
    sender: &NotificationSender,
    search: &DeploymentSearch,
    node: String,
) -> Result<ReassignResult, Error> {
    let node_id = NodeId::new(node.clone()).map_err(|()| anyhow!("illegal node id `{}`", node))?;
    let locator = search.locate_unique(&primary)?;

    let conn = primary.get()?;
    let conn = catalog::Connection::new(conn);

    let site = conn
        .locate_site(locator.clone())?
        .ok_or_else(|| anyhow!("failed to locate site for {locator}"))?;
    let changes = match conn.assigned_node(&site)? {
        Some(cur) => {
            if cur == node_id {
                vec![]
            } else {
                conn.reassign_subgraph(&site, &node_id)?
            }
        }
        None => conn.assign_subgraph(&site, &node_id)?,
    };
    conn.send_store_event(sender, &StoreEvent::new(changes))?;

    // Return the assigned node for further processing
    Ok(ReassignResult {
        node_id,
        assigned_node: conn.assigned_node(&site)?,
        locator,
    })
}
