use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use anyhow::Result;
use graph_store_postgres::ConnectionPool;
use graph_store_postgres::NotificationSender;
use graphman::deployment::DeploymentSelector;

pub async fn run(
    primary_pool: ConnectionPool,
    notification_sender: Arc<NotificationSender>,
    deployment: DeploymentSelector,
    delay: Duration,
) -> Result<()> {
    super::pause::run(
        primary_pool.clone(),
        notification_sender.clone(),
        deployment.clone(),
    )
    .await?;

    println!(
        "Waiting {}s to make sure pausing was processed ...",
        delay.as_secs()
    );

    sleep(delay);

    super::resume::run(primary_pool, notification_sender, deployment.clone()).await?;

    Ok(())
}
