use std::sync::Arc;

use anyhow::Result;
use graph_store_postgres::ConnectionPool;
use graph_store_postgres::NotificationSender;
use graphman::commands::deployment::unassign::load_assigned_deployment;
use graphman::commands::deployment::unassign::unassign_deployment;
use graphman::deployment::DeploymentSelector;

pub async fn run(
    primary_pool: ConnectionPool,
    notification_sender: Arc<NotificationSender>,
    deployment: DeploymentSelector,
) -> Result<()> {
    let assigned_deployment = load_assigned_deployment(primary_pool.clone(), &deployment).await?;

    println!("Unassigning deployment {}", assigned_deployment.locator());

    unassign_deployment(primary_pool, notification_sender, assigned_deployment).await?;

    Ok(())
}
