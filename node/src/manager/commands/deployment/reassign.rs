use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use graph::prelude::NodeId;
use graph_store_postgres::ConnectionPool;
use graph_store_postgres::NotificationSender;
use graphman::commands::deployment::reassign::{
    load_deployment, reassign_deployment, ReassignResult,
};
use graphman::deployment::DeploymentSelector;

pub fn run(
    primary_pool: ConnectionPool,
    notification_sender: Arc<NotificationSender>,
    deployment_selector: DeploymentSelector,
    node: &NodeId,
    delay: Duration,
) -> Result<()> {
    super::pause::run(
        primary_pool.clone(),
        notification_sender.clone(),
        deployment_selector.clone(),
    )?;

    println!(
        "Waiting {}s to make sure pausing was processed ...",
        delay.as_secs()
    );

    let deployment = load_deployment(primary_pool.clone(), &deployment_selector)?;
    let curr_node = deployment.assigned_node(primary_pool.clone())?;
    let reassign_msg = match &curr_node {
        Some(curr_node) => format!(
            "Reassigning deployment {} (was {})",
            deployment.locator(),
            curr_node
        ),
        None => format!("Reassigning deployment {}", deployment.locator()),
    };
    println!("{}", reassign_msg);

    let reassign_result = reassign_deployment(
        primary_pool.clone(),
        notification_sender.clone(),
        &deployment,
        node,
        curr_node,
    )?;

    match reassign_result {
        ReassignResult::Ok => {
            println!(
                "Deployment {} assigned to node {}",
                deployment.locator(),
                node
            );
        }
        ReassignResult::CompletedWithWarnings(warnings) => {
            for msg in warnings {
                println!("{}", msg);
            }
        }
    }

    super::resume::run(
        primary_pool,
        notification_sender,
        deployment_selector.clone(),
    )?;

    Ok(())
}
