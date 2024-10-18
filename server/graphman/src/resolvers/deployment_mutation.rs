use std::sync::Arc;

use anyhow::anyhow;
use async_graphql::Context;
use async_graphql::Object;
use async_graphql::Result;
use graph::prelude::NodeId;
use graph_store_postgres::command_support::catalog;
use graph_store_postgres::graphman::GraphmanStore;

use crate::entities::DeploymentSelector;
use crate::entities::EmptyResponse;
use crate::entities::ExecutionId;
use crate::resolvers::context::GraphmanContext;

mod pause;
mod reassign;
mod restart;
mod resume;
mod unassign;
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

        Ok(EmptyResponse::new(None))
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

        Ok(EmptyResponse::new(None))
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

    /// Unassign a deployment
    pub async fn unassign(
        &self,
        ctx: &Context<'_>,
        deployment: DeploymentSelector,
    ) -> Result<EmptyResponse> {
        let ctx = GraphmanContext::new(ctx)?;
        let deployment = deployment.try_into()?;

        unassign::run(&ctx, &deployment)?;

        Ok(EmptyResponse::new(None))
    }

    /// Assign or reassign a deployment
    pub async fn reassign(
        &self,
        ctx: &Context<'_>,
        deployment: DeploymentSelector,
        node: String,
    ) -> Result<EmptyResponse> {
        let ctx = GraphmanContext::new(ctx)?;
        let deployment = deployment.try_into()?;
        let node = NodeId::new(node.clone()).map_err(|()| anyhow!("illegal node id `{}`", node))?;
        reassign::run(&ctx, &deployment, &node)?;

        let mirror = catalog::Mirror::primary_only(ctx.primary_pool);
        let count = mirror.assignments(&node)?.len();
        if count == 1 {
            Ok(EmptyResponse::new(Some(format!("warning: this is the only deployment assigned to '{}'. Are you sure it is spelled correctly?",node.as_str()))))
        } else {
            Ok(EmptyResponse::new(None))
        }
    }
}
