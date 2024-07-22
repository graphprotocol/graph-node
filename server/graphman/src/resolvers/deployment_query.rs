use async_graphql::{Context, Object, Result};
use graph_core::graphman::commands::deployment::info::DeploymentInfoCommand;
use graph_core::graphman::commands::deployment::info::DeploymentStatusSelector;
use graph_core::graphman_primitives::GraphmanCommand;

use crate::entities::deployment_info_command::DeploymentInfo;
use crate::entities::DeploymentFilters;
use crate::entities::DeploymentSelector;
use crate::resolvers::context::make_graphman_context;

/// Queries related to one or multiple deployments.
pub struct DeploymentQuery;

#[Object]
impl DeploymentQuery {
    /// Returns the available information about one, multiple, or all deployments.
    pub async fn info(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "A selector for one or multiple deployments.
                          When not provided, it matches all deployments.")]
        deployment: Option<DeploymentSelector>,
        #[graphql(desc = "Applies additional filters to the selected deployments.
                          When not provided, no additional filters are applied.")]
        filters: Option<DeploymentFilters>,
    ) -> Result<Vec<DeploymentInfo>> {
        let status = if ctx.look_ahead().field("status").exists() {
            DeploymentStatusSelector::Load
        } else {
            DeploymentStatusSelector::Skip
        };

        let command = DeploymentInfoCommand {
            deployment: deployment.map(Into::into).unwrap_or_default(),
            filters: filters.map(Into::into).unwrap_or_default(),
            status,
        };

        let ctx = make_graphman_context(ctx)?;

        let output = command
            .execute(ctx)
            .await?
            .into_iter()
            .map(Into::into)
            .collect();

        Ok(output)
    }
}
