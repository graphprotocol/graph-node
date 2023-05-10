use crate::graphman::deployment::{Deployment, DeploymentSearch};
use crate::graphman::utils::display::List;
use crate::graphman::utils::prompt::prompt_for_confirmation;

use graph::anyhow::{self, bail};
use graph_store_postgres::{connection_pool::ConnectionPool, NotificationSender, SubgraphStore};
use std::sync::Arc;

/// Finds, unassigns, record and remove matching deployments.
///
/// Asks for confirmation before removing any data.
/// This is a convenience fuction that to call a series of other graphman commands.
pub async fn run<T>(
    primary_pool: ConnectionPool,
    subgraph_store: Arc<SubgraphStore>,
    sender: Arc<NotificationSender>,
    search_term: DeploymentSearch,
    current: bool,
    pending: bool,
    used: bool,
    skip_confirmation: bool,
    enable_logging: bool,
    on_deployment: T,
) -> anyhow::Result<()>
where
    T: Fn(&Deployment) -> Result<(), anyhow::Error>,
{
    // call `graphman info` to find matching deployments
    let deployments = search_term.find(primary_pool.clone(), current, pending, used)?;
    if deployments.is_empty() {
        bail!("Found no deployment for search_term: {search_term}")
    } else {
        if enable_logging {
            print_deployments(&deployments);
        }

        if !skip_confirmation && !prompt_for_confirmation("\nContinue?")? && enable_logging {
            println!("Execution aborted by user");
            return Ok(());
        }
    }
    // call `graphman unassign` to stop any active deployments
    super::assign::unassign(primary_pool, &sender, &search_term)?;

    // call `graphman remove` to unregister the subgraph's name
    for deployment in &deployments {
        super::remove::run(subgraph_store.clone(), &deployment.name, enable_logging)?;
    }

    // call `graphman unused record` to register those deployments unused
    super::unused_deployments::record(subgraph_store.clone(), enable_logging)?;

    // call `graphman unused remove` to remove each deployment's data
    for deployment in &deployments {
        on_deployment(deployment)?;
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
