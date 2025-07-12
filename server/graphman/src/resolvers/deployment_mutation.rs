use std::sync::Arc;

use anyhow::anyhow;
use async_graphql::Context;
use async_graphql::Object;
use async_graphql::Result;
use async_graphql::Union;
use graph::prelude::NodeId;
use graph_store_postgres::graphman::GraphmanStore;
use graphman::commands::deployment::reassign::ReassignResult;

use crate::entities::CompletedWithWarnings;
use crate::entities::DeploymentSelector;
use crate::entities::EmptyResponse;
use crate::entities::ExecutionId;
use crate::resolvers::context::GraphmanContext;

mod create;
mod pause;
mod reassign;
mod remove;
mod restart;
mod resume;
mod unassign;

pub struct DeploymentMutation;

#[derive(Clone, Debug, Union)]
pub enum ReassignResponse {
    Ok(EmptyResponse),
    CompletedWithWarnings(CompletedWithWarnings),
}

/// Mutations related to one or multiple deployments.
#[Object]
impl DeploymentMutation {
    /// Pauses a deployment that is not already paused.
    pub async fn pause(
        &self,
        ctx: &Context<'_>,
        deployment: DeploymentSelector,
    ) -> Result<EmptyResponse> {
        let ctx = GraphmanContext::new(ctx)?;
        let deployment = deployment.try_into()?;

        pause::run(&ctx, &deployment)?;

        Ok(EmptyResponse::new())
    }

    /// Resumes a deployment that has been previously paused.
    pub async fn resume(
        &self,
        ctx: &Context<'_>,
        deployment: DeploymentSelector,
    ) -> Result<EmptyResponse> {
        let ctx = GraphmanContext::new(ctx)?;
        let deployment = deployment.try_into()?;

        resume::run(&ctx, &deployment)?;

        Ok(EmptyResponse::new())
    }

    /// Pauses a deployment and resumes it after a delay.
    pub async fn restart(
        &self,
        ctx: &Context<'_>,
        deployment: DeploymentSelector,
        #[graphql(
            default = 20,
            desc = "The number of seconds to wait before resuming the deployment.
                    When not specified, it defaults to 20 seconds."
        )]
        delay_seconds: u64,
    ) -> Result<ExecutionId> {
        let store = ctx.data::<Arc<GraphmanStore>>()?.to_owned();
        let ctx = GraphmanContext::new(ctx)?;
        let deployment = deployment.try_into()?;

        restart::run_in_background(ctx, store, deployment, delay_seconds).await
    }

    /// Create a subgraph
    pub async fn create(&self, ctx: &Context<'_>, name: String) -> Result<EmptyResponse> {
        let ctx = GraphmanContext::new(ctx)?;
        create::run(&ctx, &name)?;
        Ok(EmptyResponse::new())
    }

    /// Remove a subgraph
    pub async fn remove(&self, ctx: &Context<'_>, name: String) -> Result<EmptyResponse> {
        let ctx = GraphmanContext::new(ctx)?;
        remove::run(&ctx, &name)?;
        Ok(EmptyResponse::new())
    }

    /// Unassign a deployment
    pub async fn unassign(
        &self,
        ctx: &Context<'_>,
        deployment: DeploymentSelector,
    ) -> Result<EmptyResponse> {
        let ctx = GraphmanContext::new(ctx)?;
        let deployment = deployment.try_into()?;

        unassign::run(&ctx, &deployment)?;

        Ok(EmptyResponse::new())
    }

    /// Assign or reassign a deployment
    pub async fn reassign(
        &self,
        ctx: &Context<'_>,
        deployment: DeploymentSelector,
        node: String,
    ) -> Result<ReassignResponse> {
        let ctx = GraphmanContext::new(ctx)?;
        let deployment = deployment.try_into()?;
        let node = NodeId::new(node.clone()).map_err(|()| anyhow!("illegal node id `{}`", node))?;
        let reassign_result = reassign::run(&ctx, &deployment, &node)?;
        match reassign_result {
            ReassignResult::CompletedWithWarnings(warnings) => Ok(
                ReassignResponse::CompletedWithWarnings(CompletedWithWarnings::new(warnings)),
            ),
            ReassignResult::Ok => Ok(ReassignResponse::Ok(EmptyResponse::new())),
        }
    }
}
