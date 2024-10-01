use async_graphql::Result;
use graphman::commands::deployment::resume::load_paused_deployment;
use graphman::commands::deployment::resume::resume_paused_deployment;
use graphman::deployment::DeploymentSelector;

use crate::resolvers::context::GraphmanContext;

pub fn run(ctx: &GraphmanContext, deployment: &DeploymentSelector) -> Result<()> {
    let paused_deployment = load_paused_deployment(ctx.primary_pool.clone(), deployment)?;

    resume_paused_deployment(
        ctx.primary_pool.clone(),
        ctx.notification_sender.clone(),
        paused_deployment,
    )?;

    Ok(())
}
