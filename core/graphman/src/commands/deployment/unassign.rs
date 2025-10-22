use std::sync::Arc;

use anyhow::anyhow;
use graph::components::store::DeploymentLocator;
use graph::components::store::StoreEvent;
use graph_store_postgres::command_support::catalog;
use graph_store_postgres::command_support::catalog::Site;
use graph_store_postgres::ConnectionPool;
use graph_store_postgres::NotificationSender;
use thiserror::Error;

use crate::deployment::DeploymentSelector;
use crate::deployment::DeploymentVersionSelector;
use crate::GraphmanError;

pub struct AssignedDeployment {
    locator: DeploymentLocator,
    site: Site,
}

impl AssignedDeployment {
    pub fn locator(&self) -> &DeploymentLocator {
        &self.locator
    }
}

#[derive(Debug, Error)]
pub enum UnassignDeploymentError {
    #[error("deployment '{0}' is already unassigned")]
    AlreadyUnassigned(String),

    #[error(transparent)]
    Common(#[from] GraphmanError),
}

pub async fn load_assigned_deployment(
    primary_pool: ConnectionPool,
    deployment: &DeploymentSelector,
) -> Result<AssignedDeployment, UnassignDeploymentError> {
    let mut primary_conn = primary_pool
        .get_async()
        .await
        .map_err(GraphmanError::from)?;

    let locator = crate::deployment::load_deployment_locator(
        &mut primary_conn,
        deployment,
        &DeploymentVersionSelector::All,
    )?;

    let mut catalog_conn = catalog::Connection::new(primary_conn);

    let site = catalog_conn
        .locate_site(locator.clone())
        .await
        .map_err(GraphmanError::from)?
        .ok_or_else(|| {
            GraphmanError::Store(anyhow!("deployment site not found for '{locator}'"))
        })?;

    match catalog_conn
        .assigned_node(&site)
        .await
        .map_err(GraphmanError::from)?
    {
        Some(_) => Ok(AssignedDeployment { locator, site }),
        None => Err(UnassignDeploymentError::AlreadyUnassigned(
            locator.to_string(),
        )),
    }
}

pub async fn unassign_deployment(
    primary_pool: ConnectionPool,
    notification_sender: Arc<NotificationSender>,
    deployment: AssignedDeployment,
) -> Result<(), GraphmanError> {
    let primary_conn = primary_pool.get_async().await?;
    let mut catalog_conn = catalog::Connection::new(primary_conn);

    let changes = catalog_conn.unassign_subgraph(&deployment.site).await?;
    catalog_conn
        .send_store_event(&notification_sender, &StoreEvent::new(changes))
        .await?;

    Ok(())
}
