use std::sync::Arc;

use anyhow::anyhow;
use graph::components::store::DeploymentLocator;
use graph::components::store::StoreEvent;
use graph::prelude::EntityChange;
use graph::prelude::NodeId;
use graph_store_postgres::command_support::catalog;
use graph_store_postgres::command_support::catalog::Site;
use graph_store_postgres::connection_pool::ConnectionPool;
use graph_store_postgres::NotificationSender;
use thiserror::Error;

use crate::deployment::DeploymentSelector;
use crate::deployment::DeploymentVersionSelector;
use crate::GraphmanError;

pub struct ActiveDeployment {
    locator: DeploymentLocator,
    site: Site,
}

#[derive(Debug, Error)]
pub enum ReassignDeploymentError {
    #[error("deployment '{0}' is already assigned to '{1}'")]
    AlreadyAssigned(String, String),

    #[error(transparent)]
    Common(#[from] GraphmanError),
}

pub fn load_deployment(
    primary_pool: ConnectionPool,
    deployment: &DeploymentSelector,
) -> Result<ActiveDeployment, ReassignDeploymentError> {
    let mut primary_conn = primary_pool.get().map_err(GraphmanError::from)?;

    let locator = crate::deployment::load_deployment(
        &mut primary_conn,
        deployment,
        &DeploymentVersionSelector::All,
    )?
    .locator();

    let mut catalog_conn = catalog::Connection::new(primary_conn);

    let site = catalog_conn
        .locate_site(locator.clone())
        .map_err(GraphmanError::from)?
        .ok_or_else(|| {
            GraphmanError::Store(anyhow!("deployment site not found for '{locator}'"))
        })?;

    Ok(ActiveDeployment { locator, site })
}

pub fn reassign_deployment(
    primary_pool: ConnectionPool,
    notification_sender: Arc<NotificationSender>,
    deployment: ActiveDeployment,
    node: &NodeId,
) -> Result<(), ReassignDeploymentError> {
    let primary_conn = primary_pool.get().map_err(GraphmanError::from)?;
    let mut catalog_conn = catalog::Connection::new(primary_conn);

    let changes: Vec<EntityChange> = match catalog_conn
        .assigned_node(&deployment.site)
        .map_err(GraphmanError::from)?
    {
        Some(curr) => {
            if &curr == node {
                vec![]
            } else {
                catalog_conn
                    .reassign_subgraph(&deployment.site, &node)
                    .map_err(GraphmanError::from)?
            }
        }
        None => catalog_conn
            .assign_subgraph(&deployment.site, &node)
            .map_err(GraphmanError::from)?,
    };

    if changes.is_empty() {
        return Err(ReassignDeploymentError::AlreadyAssigned(
            deployment.locator.hash.to_string(),
            node.to_string(),
        ));
    }

    catalog_conn
        .send_store_event(&notification_sender, &StoreEvent::new(changes))
        .map_err(GraphmanError::from)?;

    Ok(())
}
