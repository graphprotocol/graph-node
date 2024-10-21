use anyhow::anyhow;
use graph::prelude::SubgraphName;
use graph_store_postgres::command_support::catalog;
use graphman::GraphmanError;

use crate::resolvers::context::GraphmanContext;

pub fn run(ctx: &GraphmanContext, name: &String) -> Result<(), GraphmanError> {
    let primary_pool = ctx.primary_pool.get().map_err(GraphmanError::from)?;
    let mut catalog_conn = catalog::Connection::new(primary_pool);

    let name = match SubgraphName::new(name) {
        Ok(name) => name,
        Err(_) => {
            return Err(GraphmanError::Store(anyhow!(
                "Subgraph name must contain only a-z, A-Z, 0-9, '-' and '_'"
            )));
        }
    };

    catalog_conn.create_subgraph(&name);

    Ok(())
}
