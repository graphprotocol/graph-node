use crate::manager::{
    deployment::{Deployment, DeploymentSearch},
    display::List,
    prompt::prompt_for_confirmation,
};
use graph::anyhow::{self, bail};
use graph_store_postgres::{connection_pool::ConnectionPool, NotificationSender, SubgraphStore};
use std::sync::Arc;

pub async fn run(
    primary_pool: ConnectionPool,
    subgraph_store: Arc<SubgraphStore>,
    sender: Arc<NotificationSender>,
    search_term: DeploymentSearch,
    current: bool,
    pending: bool,
    used: bool,
) -> anyhow::Result<()> {
    // graphman info -> find subgraph
    let deployments = crate::manager::commands::info::find(
        primary_pool.clone(),
        search_term.clone(),
        current,
        pending,
        used,
    )?;
    if deployments.is_empty() {
        bail!("Found no deployment for search_term: {search_term}")
    } else {
        print_deployments(&deployments);
        if !prompt_for_confirmation("\nContinue?")? {
            bail!("Execution aborted by user")
        }
    }
    // call graphman unassign -> so it stops syncing if active
    crate::manager::commands::assign::unassign(primary_pool, &sender, &search_term).await?;

    // graphman remove -> to unregister the subgraph's name
    for deployment in &deployments {
        crate::manager::commands::remove::run(subgraph_store.clone(), &deployment.name)?;
    }

    // graphman unused record ->  to register the deployment as unused
    crate::manager::commands::unused_deployments::record(subgraph_store.clone())?;

    // graphman unused remove -> to remove the deployment's data
    for deployment in &deployments {
        crate::manager::commands::unused_deployments::remove(
            subgraph_store.clone(),
            1_000_000,
            Some(&deployment.deployment),
            None,
        )?;
    }
    Ok(())
}

fn print_deployments(deployments: &[Deployment]) {
    let mut list = List::new(vec!["name", "deployment"]);
    println!("Found {} deployment(s) to remove:", deployments.len());
    for deployment in deployments {
        list.append(vec![
            deployment.name.to_string(),
            deployment.deployment.to_string(),
        ]);
    }
    list.render();
}
