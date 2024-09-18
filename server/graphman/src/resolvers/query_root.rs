use async_graphql::Object;

use crate::resolvers::DeploymentQuery;
use crate::resolvers::ExecutionQuery;

/// Note: Converted to GraphQL schema as `query`.
pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Queries related to one or multiple deployments.
    pub async fn deployment(&self) -> DeploymentQuery {
        DeploymentQuery {}
    }

    /// Queries related to command executions.
    pub async fn execution(&self) -> ExecutionQuery {
        ExecutionQuery {}
    }
}
