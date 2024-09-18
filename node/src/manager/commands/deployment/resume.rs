use std::sync::Arc;

use anyhow::Result;
use graph_store_postgres::connection_pool::ConnectionPool;
use graph_store_postgres::NotificationSender;
use graphman::commands::deployment::resume::load_paused_deployment;
use graphman::commands::deployment::resume::resume_paused_deployment;
use graphman::deployment::DeploymentSelector;

pub fn run(
    primary_pool: ConnectionPool,
    notification_sender: Arc<NotificationSender>,
    deployment: DeploymentSelector,
) -> Result<()> {
    let paused_deployment = load_paused_deployment(primary_pool.clone(), &deployment)?;

    println!("Resuming deployment {} ...", paused_deployment.locator());

    resume_paused_deployment(primary_pool, notification_sender, paused_deployment)?;

    Ok(())
}
