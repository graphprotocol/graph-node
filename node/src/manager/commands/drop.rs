use crate::manager::deployment::DeploymentSearch;
use graph::anyhow::{self, bail};
use graph_store_postgres::{connection_pool::ConnectionPool, NotificationSender, SubgraphStore};
use std::sync::Arc;

pub async fn run(
    primary_pool: ConnectionPool,
    subgraph_store: Arc<SubgraphStore>,
    sender: Arc<NotificationSender>,
    deployment: DeploymentSearch,
    current: bool,
    pending: bool,
    used: bool,
) -> anyhow::Result<()> {
    // graphman info -> find subgraph
    let subgraph_names = crate::manager::commands::info::find(
        primary_pool.clone(),
        deployment.clone(),
        current,
        pending,
        used,
    )?;
    if subgraph_names.is_empty() {
        bail!("Found no deployment for identifier: {deployment}")
    } else {
        println!("Found {} subgraph(s) to remove:", subgraph_names.len());
        for (idx, deployment) in subgraph_names.iter().enumerate() {
            println!(
                "  {}: name={}, deployment={}",
                idx + 1,
                deployment.name,
                deployment.deployment
            )
        }
    }
    // graphman unassign -> so it stops syncing if active
    crate::manager::commands::assign::unassign(primary_pool, &sender, &deployment).await?;

    // graphman remove -> to unregister the subgraph's name
    for deployment in &subgraph_names {
        crate::manager::commands::remove::run(subgraph_store.clone(), &deployment.name)?;
    }

    // graphman unused record ->  to register the deployment as unused
    crate::manager::commands::unused_deployments::record(subgraph_store.clone())?;

    // graphman unused remove -> to remove the deployment's data
    for deployment in &subgraph_names {
        crate::manager::commands::unused_deployments::remove(
            subgraph_store.clone(),
            1_000_000,
            Some(&deployment.name),
            None,
        )?;
    }
    Ok(())
}
