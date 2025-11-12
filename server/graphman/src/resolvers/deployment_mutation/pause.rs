use async_graphql::Result;
use graphman::commands::deployment::pause::{
    load_active_deployment, pause_active_deployment, PauseDeploymentError,
};
use graphman::deployment::DeploymentSelector;

use crate::resolvers::context::GraphmanContext;

pub async fn run(ctx: &GraphmanContext, deployment: &DeploymentSelector) -> Result<()> {
    let active_deployment = load_active_deployment(ctx.primary_pool.clone(), deployment).await;

    match active_deployment {
        Ok(active_deployment) => {
            pause_active_deployment(
                ctx.primary_pool.clone(),
                ctx.notification_sender.clone(),
                active_deployment,
            )
            .await?;
        }
        Err(PauseDeploymentError::AlreadyPaused(_)) => {
            return Ok(());
        }
        Err(PauseDeploymentError::Common(e)) => {
            return Err(e.into());
        }
    }

    Ok(())
}
