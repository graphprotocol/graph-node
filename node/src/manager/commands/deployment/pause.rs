use std::sync::Arc;

use anyhow::Result;
use graph_store_postgres::ConnectionPool;
use graph_store_postgres::NotificationSender;
use graphman::commands::deployment::pause::{
    load_active_deployment, pause_active_deployment, PauseDeploymentError,
};
use graphman::deployment::DeploymentSelector;

pub async fn run(
    primary_pool: ConnectionPool,
    notification_sender: Arc<NotificationSender>,
    deployment: DeploymentSelector,
) -> Result<()> {
    let active_deployment = load_active_deployment(primary_pool.clone(), &deployment).await;

    match active_deployment {
        Ok(active_deployment) => {
            println!("Pausing deployment {} ...", active_deployment.locator());
            pause_active_deployment(primary_pool, notification_sender, active_deployment).await?;
        }
        Err(PauseDeploymentError::AlreadyPaused(locator)) => {
            println!("Deployment {} is already paused", locator);
            return Ok(());
        }
        Err(PauseDeploymentError::Common(e)) => {
            println!("Failed to load active deployment: {}", e);
            return Err(e.into());
        }
    }

    Ok(())
}
