use std::sync::Arc;

use graph::prelude::anyhow;
use graph_store_postgres::{connection_pool::ConnectionPool, Store};

use graph_core::graphman::{
    core::{self, info::InfoResult},
    deployment::{Deployment, DeploymentSearch},
};

pub fn run(
    pool: ConnectionPool,
    store: Option<Arc<Store>>,
    search: DeploymentSearch,
    current: bool,
    pending: bool,
    used: bool,
) -> Result<(), anyhow::Error> {
    let InfoResult {
        deployments,
        statuses,
    } = core::info::run(pool, store, search, current, pending, used)?;

    if deployments.is_empty() {
        println!("No matches");
    } else {
        Deployment::print_table(deployments, statuses);
    }

    Ok(())
}
