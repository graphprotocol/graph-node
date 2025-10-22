use std::sync::Arc;

use anyhow::Result;
use graph::prelude::{
    BlockPtr, DeploymentHash, NodeId, SubgraphRegistrarError, SubgraphStore as SubgraphStoreTrait,
};
use graph::slog::{error, info, Logger};
use graph::tokio::sync::mpsc::Receiver;
use graph::{
    components::store::DeploymentLocator,
    prelude::{SubgraphName, SubgraphRegistrar},
};
use graph_store_postgres::SubgraphStore;

/// Cleanup a subgraph
/// This is used to remove a subgraph before redeploying it when using the watch flag
async fn cleanup_dev_subgraph(
    logger: &Logger,
    subgraph_store: &SubgraphStore,
    name: &SubgraphName,
    locator: &DeploymentLocator,
) -> Result<()> {
    info!(logger, "Removing subgraph"; "name" => name.to_string(), "id" => locator.id.to_string(), "hash" => locator.hash.to_string());
    subgraph_store.remove_subgraph(name.clone()).await?;
    subgraph_store.unassign_subgraph(locator).await?;
    subgraph_store.remove_deployment(locator.id.into()).await?;
    info!(logger, "Subgraph removed"; "name" => name.to_string(), "id" => locator.id.to_string(), "hash" => locator.hash.to_string());
    Ok(())
}

async fn deploy_subgraph(
    logger: &Logger,
    subgraph_registrar: Arc<impl SubgraphRegistrar>,
    name: SubgraphName,
    subgraph_id: DeploymentHash,
    node_id: NodeId,
    debug_fork: Option<DeploymentHash>,
    start_block: Option<BlockPtr>,
) -> Result<DeploymentLocator, SubgraphRegistrarError> {
    info!(logger, "Re-deploying subgraph"; "name" => name.to_string(), "id" => subgraph_id.to_string());
    subgraph_registrar.create_subgraph(name.clone()).await?;
    subgraph_registrar
        .create_subgraph_version(
            name.clone(),
            subgraph_id.clone(),
            node_id,
            debug_fork,
            start_block,
            None,
            None,
            true,
        )
        .await
        .and_then(|locator| {
            info!(logger, "Subgraph deployed"; "name" => name.to_string(), "id" => subgraph_id.to_string(), "locator" => locator.to_string());
            Ok(locator)
        })
}

async fn drop_and_recreate_subgraph(
    logger: &Logger,
    subgraph_store: Arc<SubgraphStore>,
    subgraph_registrar: Arc<impl SubgraphRegistrar>,
    name: SubgraphName,
    subgraph_id: DeploymentHash,
    node_id: NodeId,
    hash: DeploymentHash,
) -> Result<DeploymentLocator> {
    let locator = subgraph_store.active_locator(&hash).await?;
    if let Some(locator) = locator.clone() {
        cleanup_dev_subgraph(logger, &subgraph_store, &name, &locator).await?;
    }

    deploy_subgraph(
        logger,
        subgraph_registrar,
        name,
        subgraph_id,
        node_id,
        None,
        None,
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to deploy subgraph: {}", e))
}

/// Watch for subgraph updates, drop and recreate them
/// This is used to listen to file changes in the subgraph directory
/// And drop and recreate the subgraph when it changes
pub async fn watch_subgraph_updates(
    logger: &Logger,
    subgraph_store: Arc<SubgraphStore>,
    subgraph_registrar: Arc<impl SubgraphRegistrar>,
    node_id: NodeId,
    mut rx: Receiver<(DeploymentHash, SubgraphName)>,
) {
    while let Some((hash, name)) = rx.recv().await {
        let res = drop_and_recreate_subgraph(
            logger,
            subgraph_store.clone(),
            subgraph_registrar.clone(),
            name.clone(),
            hash.clone(),
            node_id.clone(),
            hash.clone(),
        )
        .await;

        if let Err(e) = res {
            error!(logger, "Failed to drop and recreate subgraph";
                "name" => name.to_string(),
                "hash" => hash.to_string(),
                "error" => e.to_string()
            );
            std::process::exit(1);
        }
    }

    error!(logger, "Subgraph watcher terminated unexpectedly"; "action" => "exiting");
    std::process::exit(1);
}
