use async_graphql::Result;
use graphman::commands::deployment::pause::load_active_deployment;
use graphman::commands::deployment::pause::pause_active_deployment;
use graphman::deployment::DeploymentSelector;

use crate::resolvers::context::GraphmanContext;

pub fn run(ctx: &GraphmanContext, deployment: &DeploymentSelector) -> Result<()> {
    let active_deployment = load_active_deployment(ctx.primary_pool.clone(), deployment)?;

    pause_active_deployment(
        ctx.primary_pool.clone(),
        ctx.notification_sender.clone(),
        active_deployment,
    )?;

    Ok(())
}
