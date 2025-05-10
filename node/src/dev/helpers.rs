use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use graph::components::link_resolver::FileLinkResolver;
use graph::prelude::{
    BlockPtr, DeploymentHash, NodeId, SubgraphRegistrar as SubgraphRegistrarTrait,
    SubgraphRegistrarError, SubgraphStore as SubgraphStoreTrait,
};
use graph::slog::{error, info, Logger};
use graph::tokio::sync::mpsc::Receiver;
use graph::{
    components::store::DeploymentLocator,
    prelude::{SubgraphAssignmentProvider as SubgraphAssignmentProviderTrait, SubgraphName},
};
use graph_core::{SubgraphAssignmentProvider, SubgraphInstanceManager, SubgraphRegistrar};
use graph_store_postgres::{SubgraphStore, SubscriptionManager};

type SubgraphRegistrarType = SubgraphRegistrar<
    SubgraphAssignmentProvider<SubgraphInstanceManager<SubgraphStore>>,
    SubgraphStore,
    SubscriptionManager,
>;

pub struct DevSubgraph {
    pub hash: DeploymentHash,
    pub name: SubgraphName,
    pub build_dir: PathBuf,
}

pub struct DevModeContext {
    pub watch: bool,
    pub file_link_resolver: Arc<FileLinkResolver>,
    pub updates_rx: Receiver<DevSubgraph>,
}

/// Cleanup a subgraph
/// This is used to remove a subgraph before redeploying it when using the watch flag
async fn cleanup_dev_subgraph(
    logger: &Logger,
    subgraph_store: &SubgraphStore,
    subgraph_registrar: &Arc<SubgraphRegistrarType>,
    subgraph: &DevSubgraph,
) -> Result<()> {
    let hash = subgraph.hash.clone();
    let locator = subgraph_store.active_locator(&hash)?;

    if let Some(locator) = locator {
        info!(logger, "Removing subgraph"; "name" => subgraph.name.to_string(), "id" => locator.id.to_string(), "hash" => hash.to_string());

        subgraph_registrar.provider.stop(locator.clone()).await?;
        subgraph_store.remove_subgraph(subgraph.name.clone())?;
        subgraph_store.unassign_subgraph(&locator)?;
        subgraph_store.remove_deployment(locator.id.into())?;

        info!(logger, "Subgraph removed"; "name" => subgraph.name.to_string(), "id" => locator.id.to_string(), "hash" => hash.to_string());
    }

    Ok(())
}

async fn deploy_subgraph(
    logger: &Logger,
    subgraph_registrar: &Arc<SubgraphRegistrarType>,
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
            None,
        )
        .await
        .and_then(|locator| {
            info!(logger, "Subgraph deployed"; "name" => name.to_string(), "id" => subgraph_id.to_string(), "locator" => locator.to_string());
            Ok(locator)
        })
}

pub async fn drop_and_recreate_subgraph(
    logger: &Logger,
    subgraph_store: Arc<SubgraphStore>,
    subgraph_registrar: Arc<SubgraphRegistrarType>,
    node_id: NodeId,
    dev_subgraph: DevSubgraph,
) -> Result<DeploymentLocator> {
    let name = dev_subgraph.name.clone();
    let hash = dev_subgraph.hash.clone();

    let link_resolver = Arc::new(FileLinkResolver::with_base_dir(
        dev_subgraph.build_dir.clone(),
    ));

    cleanup_dev_subgraph(logger, &subgraph_store, &subgraph_registrar, &dev_subgraph).await?;

    let locator = deploy_subgraph(
        logger,
        &subgraph_registrar,
        name.clone(),
        hash.clone(),
        node_id,
        None,
        None,
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to deploy subgraph: {}", e))?;

    info!(logger, "Starting subgraph"; "name" => name.to_string(), "id" => hash.to_string(), "locator" => locator.to_string());
    subgraph_registrar
        .provider
        .start(locator.clone(), None, Some(link_resolver))
        .await?;
    info!(logger, "Subgraph started"; "name" => name.to_string(), "id" => hash.to_string(), "locator" => locator.to_string());

    Ok(locator)
}

/// Watch for subgraph updates, drop and recreate them
/// This is used to listen to file changes in the subgraph directory
/// And drop and recreate the subgraph when it changes
pub async fn watch_subgraph_updates(
    logger: &Logger,
    subgraph_store: Arc<SubgraphStore>,
    subgraph_registrar: Arc<SubgraphRegistrarType>,
    node_id: NodeId,
    mut rx: Receiver<DevSubgraph>,
) {
    while let Some(dev_subgraph) = rx.recv().await {
        let name = dev_subgraph.name.clone();
        let hash = dev_subgraph.hash.clone();
        let build_dir = dev_subgraph.build_dir.clone();

        let res = drop_and_recreate_subgraph(
            logger,
            subgraph_store.clone(),
            subgraph_registrar.clone(),
            node_id.clone(),
            dev_subgraph,
        )
        .await;

        if let Err(e) = res {
            error!(logger, "Failed to drop and recreate subgraph";
                "name" => name.to_string(),
                "hash" => hash.to_string(),
                "build_dir" => format!("{}", build_dir.display()),
                "error" => e.to_string()
            );
        }
    }

    error!(logger, "Subgraph watcher terminated unexpectedly"; "action" => "exiting");
    std::process::exit(1);
}
