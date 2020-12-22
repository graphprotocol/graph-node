use graph::prelude::anyhow;
use graph_store_postgres::connection_pool::ConnectionPool;

use crate::manager::deployment::Deployment;

pub fn run(pool: ConnectionPool, name: String) -> Result<(), anyhow::Error> {
    let conn = pool.get()?;

    let deployments = Deployment::lookup(&conn, name)?;
    Deployment::print_table(deployments);
    Ok(())
}
