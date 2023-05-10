use graph::anyhow::{self};
use graph_core::graphman::{core, deployment::DeploymentSearch};
use graph_store_postgres::{connection_pool::ConnectionPool, NotificationSender, SubgraphStore};
use std::sync::Arc;

/// Finds, unassigns, record and remove matching deployments.
///
/// Asks for confirmation before removing any data.
/// This is a convenience fuction that to call a series of other graphman commands.
pub async fn run(
    primary_pool: ConnectionPool,
    subgraph_store: Arc<SubgraphStore>,
    sender: Arc<NotificationSender>,
    search_term: DeploymentSearch,
    current: bool,
    pending: bool,
    used: bool,
    skip_confirmation: bool,
) -> anyhow::Result<()> {
    core::drop::run(
        primary_pool,
        subgraph_store.clone(),
        sender,
        search_term,
        current,
        pending,
        used,
        skip_confirmation,
        true,
        |deployment| {
            crate::manager::commands::unused_deployments::remove(
                subgraph_store.clone(),
                1_000_000,
                Some(&deployment.deployment),
                None,
            )
        },
    )
    .await
}
