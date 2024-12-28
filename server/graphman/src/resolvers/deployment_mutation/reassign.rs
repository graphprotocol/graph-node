use anyhow::Ok;
use async_graphql::Result;
use graph::prelude::NodeId;
use graphman::commands::deployment::reassign::load_deployment;
use graphman::commands::deployment::reassign::reassign_deployment;
use graphman::commands::deployment::reassign::ReassignResult;
use graphman::deployment::DeploymentSelector;

use crate::resolvers::context::GraphmanContext;

pub fn run(
    ctx: &GraphmanContext,
    deployment: &DeploymentSelector,
    node: &NodeId,
) -> Result<ReassignResult, anyhow::Error> {
    let deployment = load_deployment(ctx.primary_pool.clone(), deployment)?;
    let reassign_result = reassign_deployment(
        ctx.primary_pool.clone(),
        ctx.notification_sender.clone(),
        &deployment,
        &node,
    )?;
    Ok(reassign_result)
}
