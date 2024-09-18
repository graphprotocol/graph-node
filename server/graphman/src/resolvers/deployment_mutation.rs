use std::sync::Arc;

use async_graphql::Context;
use async_graphql::Object;
use async_graphql::Result;
use graph_store_postgres::graphman::GraphmanStore;

use crate::entities::DeploymentSelector;
use crate::entities::EmptyResponse;
use crate::entities::ExecutionId;
use crate::resolvers::context::GraphmanContext;

mod pause;
mod restart;
mod resume;

pub struct DeploymentMutation;

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
}
