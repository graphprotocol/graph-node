use std::sync::Arc;

use anyhow::Result;
use graph_store_postgres::connection_pool::ConnectionPool;
use graph_store_postgres::NotificationSender;
use graphman::commands::deployment::pause::load_active_deployment;
use graphman::commands::deployment::pause::pause_active_deployment;
use graphman::deployment::DeploymentSelector;

pub fn run(
    primary_pool: ConnectionPool,
    notification_sender: Arc<NotificationSender>,
    deployment: DeploymentSelector,
) -> Result<()> {
    let active_deployment = load_active_deployment(primary_pool.clone(), &deployment)?;

    println!("Pausing deployment {} ...", active_deployment.locator());

    pause_active_deployment(primary_pool, notification_sender, active_deployment)?;

    Ok(())
}
