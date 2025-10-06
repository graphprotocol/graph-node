use std::sync::Arc;
use std::time::Duration;

use async_graphql::Result;
use graph_store_postgres::graphman::GraphmanStore;
use graphman::deployment::DeploymentSelector;
use graphman::GraphmanExecutionTracker;
use graphman_store::CommandKind;
use graphman_store::GraphmanStore as _;

use crate::entities::ExecutionId;
use crate::resolvers::context::GraphmanContext;

pub async fn run_in_background(
    ctx: GraphmanContext,
    store: Arc<GraphmanStore>,
    deployment: DeploymentSelector,
    delay_seconds: u64,
) -> Result<ExecutionId> {
    let id = store.new_execution(CommandKind::RestartDeployment).await?;

    graph::spawn(async move {
        let tracker = GraphmanExecutionTracker::new(store, id);
        let result = run(&ctx, &deployment, delay_seconds).await;

        match result {
            Ok(()) => {
                tracker.track_success().await.unwrap();
            }
            Err(err) => {
                tracker.track_failure(format!("{err:#?}")).await.unwrap();
            }
        };
    });

    Ok(id.into())
}

async fn run(
    ctx: &GraphmanContext,
    deployment: &DeploymentSelector,
    delay_seconds: u64,
) -> Result<()> {
    super::pause::run(ctx, deployment).await?;

    tokio::time::sleep(Duration::from_secs(delay_seconds)).await;

    super::resume::run(ctx, deployment).await?;

    Ok(())
}
