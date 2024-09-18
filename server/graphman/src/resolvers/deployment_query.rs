use async_graphql::Context;
use async_graphql::Object;
use async_graphql::Result;

use crate::entities::DeploymentInfo;
use crate::entities::DeploymentSelector;
use crate::entities::DeploymentVersionSelector;

mod info;

pub struct DeploymentQuery;

/// Queries related to one or multiple deployments.
#[Object]
impl DeploymentQuery {
    /// Returns the available information about one, multiple, or all deployments.
    pub async fn info(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "A selector for one or multiple deployments.
                          When not provided, it matches all deployments.")]
        deployment: Option<DeploymentSelector>,
        #[graphql(desc = "Applies version filter to the selected deployments.
                          When not provided, no additional version filter is applied.")]
        version: Option<DeploymentVersionSelector>,
    ) -> Result<Vec<DeploymentInfo>> {
        info::run(ctx, deployment, version)
    }
}
