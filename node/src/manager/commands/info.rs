use graph::prelude::anyhow;
use graph_store_postgres::connection_pool::ConnectionPool;

use crate::manager::deployment::Deployment;

pub fn run(
    pool: ConnectionPool,
    name: String,
    current: bool,
    pending: bool,
    used: bool,
) -> Result<(), anyhow::Error> {
    let conn = pool.get()?;
    let current = current || used;
    let pending = pending || used;

    let deployments = Deployment::lookup(&conn, name)?;
    // Filter by status; if neither `current` or `pending` are set, list
    // all deployments
    let deployments: Vec<_> = deployments
        .into_iter()
        .filter(|deployment| match (current, pending) {
            (true, false) => deployment.status == "current",
            (false, true) => deployment.status == "pending",
            (true, true) => deployment.status == "current" || deployment.status == "pending",
            (false, false) => true,
        })
        .collect();

    if deployments.is_empty() {
        println!("No matches");
    } else {
        Deployment::print_table(deployments);
    }
    Ok(())
}
