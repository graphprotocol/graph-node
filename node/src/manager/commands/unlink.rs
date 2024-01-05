use std::hash::Hash;
use std::sync::Arc;
use graph::anyhow::bail;
use graph::itertools::Itertools;
use graph::prelude::{anyhow::anyhow, DeploymentHash, Error, NodeId, StoreEvent, SubgraphStore as _};
use graph_store_postgres::{command_support::catalog, connection_pool::ConnectionPool, NotificationSender, SubgraphStore};
use crate::manager::deployment::DeploymentSearch;

pub async fn run(
    store: Arc<SubgraphStore>,
    pool: ConnectionPool,
    search: &DeploymentSearch,
) -> Result<(), Error> {
    // This function accepts only hash
    // TODO: change match to DeploymentSearch::Hash { hash, _ } to simplify
    match search {
        DeploymentSearch::Hash {
            hash,
            shard: Some(shard),
        } => println!("{}:{}", hash, shard),
        DeploymentSearch::Hash { hash, shard: None } => println!("{}", hash),
        _ => bail!("The `subgraph` argument must be a valid IPFS hash"),
    };

    let deployments: Vec<(&DeploymentHash, String)> = search.lookup(&pool)?.iter().map(|d| (d.deployment.into(), d.status.clone())).collect_vec();
    println!("Found in deployments {deployments}");
    store.unlink_deployment(deployments)?;
    Ok(())
}
