use std::fs;
use std::sync::Arc;

use graph::prelude::anyhow::Result;

use graph_store_postgres::{ConnectionPool, SubgraphStore};

use crate::manager::deployment::DeploymentSearch;

pub async fn run(
    subgraph_store: Arc<SubgraphStore>,
    primary_pool: ConnectionPool,
    deployment: DeploymentSearch,
    directory: String,
) -> Result<()> {
    fs::create_dir_all(&directory)?;
    let directory = fs::canonicalize(&directory)?;

    let loc = deployment.locate_unique(&primary_pool).await?;

    subgraph_store.dump(&loc, directory).await?;
    Ok(())
}
