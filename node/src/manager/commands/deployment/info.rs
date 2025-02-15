use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io;
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

use crate::manager::display::Columns;
use crate::manager::display::Row;

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
    pub brief: bool,
    pub no_name: bool,
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
        brief,
        no_name,
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

    render(brief, no_name, deployments, statuses);
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

const NONE: &str = "---";

fn optional(s: Option<impl ToString>) -> String {
    s.map(|x| x.to_string()).unwrap_or(NONE.to_owned())
}

fn render(
    brief: bool,
    no_name: bool,
    deployments: Vec<Deployment>,
    statuses: Option<HashMap<i32, DeploymentStatus>>,
) {
    fn name_and_status(deployment: &Deployment) -> String {
        format!("{} ({})", deployment.name, deployment.version_status)
    }

    fn number(n: Option<i32>) -> String {
        n.map(|x| format!("{x}")).unwrap_or(NONE.to_owned())
    }

    let mut table = Columns::default();

    let mut combined: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for deployment in deployments {
        let status = statuses.as_ref().and_then(|x| x.get(&deployment.id));
        combined
            .entry(deployment.id)
            .or_default()
            .push((deployment, status));
    }

    let mut first = true;
    for (_, deployments) in combined {
        let deployment = &deployments[0].0;
        if first {
            first = false;
        } else {
            table.push_row(Row::separator());
        }
        table.push_row([
            "Namespace",
            &format!("{} [{}]", deployment.namespace, deployment.shard),
        ]);
        table.push_row(["Hash", &deployment.hash]);
        if !no_name && (!brief || deployment.is_active) {
            if deployments.len() > 1 {
                table.push_row(["Versions", &name_and_status(deployment)]);
                for (d, _) in &deployments[1..] {
                    table.push_row(["", &name_and_status(d)]);
                }
            } else {
                table.push_row(["Version", &name_and_status(deployment)]);
            }
            table.push_row(["Chain", &deployment.chain]);
        }
        table.push_row(["Node ID", &optional(deployment.node_id.as_ref())]);
        table.push_row(["Active", &deployment.is_active.to_string()]);
        if let Some((_, status)) = deployments.get(0) {
            if let Some(status) = status {
                table.push_row(["Paused", &optional(status.is_paused)]);
                table.push_row(["Synced", &status.is_synced.to_string()]);
                table.push_row(["Health", status.health.as_str()]);

                let earliest = status.earliest_block_number;
                let latest = status.latest_block.as_ref().map(|x| x.number);
                let chain_head = status.chain_head_block.as_ref().map(|x| x.number);
                let behind = match (latest, chain_head) {
                    (Some(latest), Some(chain_head)) => Some(chain_head - latest),
                    _ => None,
                };

                table.push_row(["Earliest Block", &earliest.to_string()]);
                table.push_row(["Latest Block", &number(latest)]);
                table.push_row(["Chain Head Block", &number(chain_head)]);
                if let Some(behind) = behind {
                    table.push_row(["   Blocks behind", &behind.to_string()]);
                }
            }
        }
    }

    table.render(&mut io::stdout()).ok();
}
