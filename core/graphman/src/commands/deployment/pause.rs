use std::sync::Arc;

use anyhow::anyhow;
use graph::components::store::DeploymentLocator;
use graph::components::store::StoreEvent;
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
pub enum PauseDeploymentError {
    #[error("deployment '{0}' is already paused")]
    AlreadyPaused(String),

    #[error(transparent)]
    Common(#[from] GraphmanError),
}

impl ActiveDeployment {
    pub fn locator(&self) -> &DeploymentLocator {
        &self.locator
    }
}

pub fn load_active_deployment(
    primary_pool: ConnectionPool,
    deployment: &DeploymentSelector,
) -> Result<ActiveDeployment, PauseDeploymentError> {
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

    let (_, is_paused) = catalog_conn
        .assignment_status(&site)
        .map_err(GraphmanError::from)?
        .ok_or_else(|| {
            GraphmanError::Store(anyhow!("assignment status not found for '{locator}'"))
        })?;

    if is_paused {
        return Err(PauseDeploymentError::AlreadyPaused(locator.to_string()));
    }

    Ok(ActiveDeployment { locator, site })
}

pub fn pause_active_deployment(
    primary_pool: ConnectionPool,
    notification_sender: Arc<NotificationSender>,
    active_deployment: ActiveDeployment,
) -> Result<(), GraphmanError> {
    let primary_conn = primary_pool.get()?;
    let mut catalog_conn = catalog::Connection::new(primary_conn);

    let changes = catalog_conn.pause_subgraph(&active_deployment.site)?;
    catalog_conn.send_store_event(&notification_sender, &StoreEvent::new(changes))?;

    Ok(())
}
