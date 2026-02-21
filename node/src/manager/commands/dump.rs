use std::fs;
use std::sync::Arc;

use graph::{bail, prelude::anyhow::Result};

use graph_store_postgres::{ConnectionPool, SubgraphStore};

use crate::manager::deployment::DeploymentSearch;

pub async fn run(
    subgraph_store: Arc<SubgraphStore>,
    primary_pool: ConnectionPool,
    deployment: DeploymentSearch,
    directory: String,
) -> Result<()> {
    let directory = fs::canonicalize(&directory)?;
    let stat = fs::metadata(&directory)?;

    if !stat.is_dir() {
        bail!(
            "The path `{}` is not a directory",
            directory.to_string_lossy()
        );
    }

    let loc = deployment.locate_unique(&primary_pool).await?;

    subgraph_store.dump(&loc, directory).await?;
    Ok(())
}
