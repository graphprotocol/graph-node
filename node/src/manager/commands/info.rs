use std::sync::Arc;

use graph::{components::store::StatusStore, data::subgraph::status, prelude::anyhow};
use graph_store_postgres::{connection_pool::ConnectionPool, Store};

use crate::manager::deployment::Deployment;

fn find(
    pool: ConnectionPool,
    name: String,
    current: bool,
    pending: bool,
    used: bool,
) -> Result<Vec<Deployment>, anyhow::Error> {
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
    Ok(deployments)
}

pub fn run(
    pool: ConnectionPool,
    store: Option<Arc<Store>>,
    name: String,
    current: bool,
    pending: bool,
    used: bool,
) -> Result<(), anyhow::Error> {
    let deployments = find(pool, name, current, pending, used)?;
    let hashes: Vec<_> = deployments.iter().map(|d| d.deployment.clone()).collect();
    let statuses = match store {
        Some(store) => store.status(status::Filter::Deployments(hashes))?,
        None => vec![],
    };

    if deployments.is_empty() {
        println!("No matches");
    } else {
        Deployment::print_table(deployments, statuses);
    }
    Ok(())
}
