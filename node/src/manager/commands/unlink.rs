use crate::manager::deployment::DeploymentSearch;
use graph::anyhow::bail;
use graph::prelude::{DeploymentHash, Error, SubgraphStore as _};
use graph_store_postgres::SubgraphStore;
use std::sync::Arc;

pub async fn run(store: Arc<SubgraphStore>, search: &DeploymentSearch) -> Result<(), Error> {
    let deployment = match search {
        DeploymentSearch::Hash { hash, shard: _ } => {
            DeploymentHash::new(hash).expect(format!("Cannot convert hash {}", hash).as_str())
        }
        _ => bail!("The `subgraph` argument must be a valid IPFS hash"),
    };
    println!("Unlinking deployment {deployment}");
    store.unlink_deployment(&deployment)?;
    Ok(())
}
