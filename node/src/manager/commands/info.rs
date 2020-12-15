use graph::prelude::anyhow;
use graph_store_postgres::connection_pool::ConnectionPool;

use crate::manager::deployment::Deployment;

pub fn run(pool: ConnectionPool, name: String) -> Result<(), anyhow::Error> {
    let conn = pool.get()?;

    let mut first = true;
    for deployment in Deployment::lookup(&conn, name)? {
        if !first {
            println!("-------------+-----------------------------------------------");
        }
        first = false;

        println!(
            "{:12} | {}",
            "name",
            deployment.name.unwrap_or("---".to_string())
        );
        println!("{:12} | {}", "id", deployment.id);
        println!("{:12} | {}", "nsp", deployment.namespace);
        println!("{:12} | {}", "shard", deployment.shard);
    }
    Ok(())
}
