use async_graphql::{Context, Object, Result};
use graph_core::graphman::commands::deployment::pause::PauseDeploymentCommand;
use graph_core::graphman::commands::deployment::resume::ResumeDeploymentCommand;
use graph_core::graphman_primitives::GraphmanCommand;

use crate::entities::DeploymentSelector;
use crate::resolvers::context::make_graphman_context;

/// Mutations related to one or multiple deployments.
pub struct DeploymentMutation;

#[Object]
impl DeploymentMutation {
    /// Pauses a deployment that is not already paused.
    pub async fn pause(&self, ctx: &Context<'_>, deployment: DeploymentSelector) -> Result<bool> {
        let command = PauseDeploymentCommand {
            deployment: deployment.into(),
        };

        let ctx = make_graphman_context(ctx)?;

        Ok(command.execute(ctx).await?)
    }

    /// Resumes a deployment that has been previously paused.
    pub async fn resume(&self, ctx: &Context<'_>, deployment: DeploymentSelector) -> Result<bool> {
        let command = ResumeDeploymentCommand {
            deployment: deployment.into(),
        };

        let ctx = make_graphman_context(ctx)?;

        Ok(command.execute(ctx).await?)
    }
}
