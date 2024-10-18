use async_graphql::Result;
use graphman::commands::deployment::unassign::load_assigned_deployment;
use graphman::commands::deployment::unassign::unassign_deployment;
use graphman::deployment::DeploymentSelector;

use crate::resolvers::context::GraphmanContext;

pub fn run(ctx: &GraphmanContext, deployment: &DeploymentSelector) -> Result<()> {
    let deployment = load_assigned_deployment(ctx.primary_pool.clone(), deployment)?;
    unassign_deployment(
        ctx.primary_pool.clone(),
        ctx.notification_sender.clone(),
        deployment,
    )?;

    Ok(())
}
