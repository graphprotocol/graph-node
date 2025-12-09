use graph::components::store::DeploymentLocator;
use graph::prelude::{anyhow::anyhow, Error, NodeId, StoreEvent};
use graph_store_postgres::{command_support::catalog, ConnectionPool, NotificationSender};
use std::time::Duration;
use tokio;

use crate::manager::deployment::DeploymentSearch;

pub async fn unassign(
    primary: ConnectionPool,
    sender: &NotificationSender,
    search: &DeploymentSearch,
) -> Result<(), Error> {
    let locator = search.locate_unique(&primary).await?;

    let pconn = primary.get_permitted().await?;
    let mut conn = catalog::Connection::new(pconn);

    let site = conn
        .locate_site(locator.clone())
        .await?
        .ok_or_else(|| anyhow!("failed to locate site for {locator}"))?;

    println!("unassigning {locator}");
    let changes = conn.unassign_subgraph(&site).await?;
    conn.send_store_event(sender, &StoreEvent::new(changes))
        .await?;

    Ok(())
}

pub async fn reassign(
    primary: ConnectionPool,
    sender: &NotificationSender,
    search: &DeploymentSearch,
    node: String,
) -> Result<(), Error> {
    let node = NodeId::new(node.clone()).map_err(|()| anyhow!("illegal node id `{}`", node))?;
    let locator = search.locate_unique(&primary).await?;

    let pconn = primary.get_permitted().await?;
    let mut conn = catalog::Connection::new(pconn);

    let site = conn
        .locate_site(locator.clone())
        .await?
        .ok_or_else(|| anyhow!("failed to locate site for {locator}"))?;
    let changes = match conn.assigned_node(&site).await? {
        Some(cur) => {
            if cur == node {
                println!("deployment {locator} is already assigned to {cur}");
                vec![]
            } else {
                println!("reassigning {locator} to {node} (was {cur})");
                conn.reassign_subgraph(&site, &node).await?
            }
        }
        None => {
            println!("assigning {locator} to {node}");
            conn.assign_subgraph(&site, &node).await?
        }
    };
    conn.send_store_event(sender, &StoreEvent::new(changes))
        .await?;

    // It's easy to make a typo in the name of the node; if this operation
    // assigns to a node that wasn't used before, warn the user that they
    // might have mistyped the node name
    let mirror = catalog::Mirror::primary_only(primary);
    let count = mirror.assignments(&node).await?.len();
    if count == 1 {
        println!("warning: this is the only deployment assigned to {node}");
        println!("         are you sure it is spelled correctly?");
    }
    Ok(())
}

pub async fn pause_or_resume(
    primary: ConnectionPool,
    sender: &NotificationSender,
    locator: &DeploymentLocator,
    should_pause: bool,
) -> Result<(), Error> {
    let pconn = primary.get_permitted().await?;
    let mut conn = catalog::Connection::new(pconn);

    let site = conn
        .locate_site(locator.clone())
        .await?
        .ok_or_else(|| anyhow!("failed to locate site for {locator}"))?;

    let change = match conn.assignment_status(&site).await? {
        Some((_, is_paused)) => {
            if should_pause {
                if is_paused {
                    println!("deployment {locator} is already paused");
                    return Ok(());
                }
                println!("pausing {locator}");
                conn.pause_subgraph(&site).await?
            } else {
                println!("resuming {locator}");
                conn.resume_subgraph(&site).await?
            }
        }
        None => {
            println!("deployment {locator} not found");
            return Ok(());
        }
    };
    println!("Operation completed");
    conn.send_store_event(sender, &StoreEvent::new(change))
        .await?;

    Ok(())
}

pub async fn restart(
    primary: ConnectionPool,
    sender: &NotificationSender,
    locator: &DeploymentLocator,
    sleep: Duration,
) -> Result<(), Error> {
    pause_or_resume(primary.clone(), sender, locator, true).await?;
    println!(
        "Waiting {}s to make sure pausing was processed",
        sleep.as_secs()
    );
    tokio::time::sleep(sleep).await;
    pause_or_resume(primary, sender, locator, false).await?;
    Ok(())
}
