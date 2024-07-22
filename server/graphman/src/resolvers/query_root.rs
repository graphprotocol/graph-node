use async_graphql::Object;

use crate::resolvers::DeploymentQuery;

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Queries related to one or multiple deployments.
    pub async fn deployment(&self) -> DeploymentQuery {
        DeploymentQuery {}
    }
}
