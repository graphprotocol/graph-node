use async_graphql::Object;

use crate::resolvers::DeploymentMutation;

pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// Mutations related to one or multiple deployments.
    pub async fn deployment(&self) -> DeploymentMutation {
        DeploymentMutation {}
    }
}
