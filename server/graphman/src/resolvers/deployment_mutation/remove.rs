use anyhow::anyhow;
use async_graphql::Result;
use graph::prelude::{StoreEvent, SubgraphName};
use graph_store_postgres::command_support::catalog;

use crate::resolvers::context::GraphmanContext;
use graphman::GraphmanError;

pub fn run(ctx: &GraphmanContext, name: &String) -> Result<()> {
    let primary_pool = ctx.primary_pool.get().map_err(GraphmanError::from)?;
    let mut catalog_conn = catalog::Connection::new(primary_pool);

    let name = match SubgraphName::new(name) {
        Ok(name) => name,
        Err(_) => {
            return Err(GraphmanError::Store(anyhow!(
                "Subgraph name must contain only a-z, A-Z, 0-9, '-' and '_'"
            ))
            .into())
        }
    };

    let changes = catalog_conn.remove_subgraph(name)?;
    catalog_conn.send_store_event(&ctx.notification_sender, &StoreEvent::new(changes))?;

    Ok(())
}
