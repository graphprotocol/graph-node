use std::collections::HashMap;
use std::sync::Arc;

use anyhow::bail;
use anyhow::Result;
use graph_store_postgres::connection_pool::ConnectionPool;
use graph_store_postgres::Store;
use graphman::commands::deployment::info::load_deployment_statuses;
use graphman::commands::deployment::info::load_deployments;
use graphman::commands::deployment::info::DeploymentStatus;
use graphman::deployment::Deployment;
use graphman::deployment::DeploymentSelector;
use graphman::deployment::DeploymentVersionSelector;

use crate::manager::display::List;

pub struct Context {
    pub primary_pool: ConnectionPool,
    pub store: Arc<Store>,
}

pub struct Args {
    pub deployment: Option<DeploymentSelector>,
    pub current: bool,
    pub pending: bool,
    pub status: bool,
    pub used: bool,
    pub all: bool,
}

pub fn run(ctx: Context, args: Args) -> Result<()> {
    let Context {
        primary_pool,
        store,
    } = ctx;

    let Args {
        deployment,
        current,
        pending,
        status,
        used,
        all,
    } = args;

    let deployment = match deployment {
        Some(deployment) => deployment,
        None if all => DeploymentSelector::All,
        None => {
            bail!("Please specify a deployment or use --all to list all deployments");
        }
    };

    let version = make_deployment_version_selector(current, pending, used);
    let deployments = load_deployments(primary_pool.clone(), &deployment, &version)?;

    if deployments.is_empty() {
        println!("No matches");
        return Ok(());
    }

    let statuses = if status {
        Some(load_deployment_statuses(store, &deployments)?)
    } else {
        None
    };

    print_info(deployments, statuses);

    Ok(())
}

fn make_deployment_version_selector(
    current: bool,
    pending: bool,
    used: bool,
) -> DeploymentVersionSelector {
    use DeploymentVersionSelector::*;

    match (current || used, pending || used) {
        (false, false) => All,
        (true, false) => Current,
        (false, true) => Pending,
        (true, true) => Used,
    }
}

fn print_info(deployments: Vec<Deployment>, statuses: Option<HashMap<i32, DeploymentStatus>>) {
    let mut headers = vec![
        "Name",
        "Status",
        "Hash",
        "Namespace",
        "Shard",
        "Active",
        "Chain",
        "Node ID",
    ];

    if statuses.is_some() {
        headers.extend(vec![
            "Paused",
            "Synced",
            "Health",
            "Earliest Block",
            "Latest Block",
            "Chain Head Block",
        ]);
    }

    let mut list = List::new(headers);

    const NONE: &str = "---";

    fn optional(s: Option<impl ToString>) -> String {
        s.map(|x| x.to_string()).unwrap_or(NONE.to_owned())
    }

    for deployment in deployments {
        let mut row = vec![
            deployment.name,
            deployment.version_status,
            deployment.hash,
            deployment.namespace,
            deployment.shard,
            deployment.is_active.to_string(),
            deployment.chain,
            optional(deployment.node_id),
        ];

        let status = statuses.as_ref().map(|x| x.get(&deployment.id));

        match status {
            Some(Some(status)) => {
                row.extend(vec![
                    optional(status.is_paused),
                    status.is_synced.to_string(),
                    status.health.as_str().to_string(),
                    status.earliest_block_number.to_string(),
                    optional(status.latest_block.as_ref().map(|x| x.number)),
                    optional(status.chain_head_block.as_ref().map(|x| x.number)),
                ]);
            }
            Some(None) => {
                row.extend(vec![
                    NONE.to_owned(),
                    NONE.to_owned(),
                    NONE.to_owned(),
                    NONE.to_owned(),
                    NONE.to_owned(),
                    NONE.to_owned(),
                ]);
            }
            None => {}
        }

        list.append(row);
    }

    list.render();
}
