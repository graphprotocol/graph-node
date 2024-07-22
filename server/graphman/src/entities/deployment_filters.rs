use async_graphql::{Enum, InputObject};
use graph_core::graphman;

#[derive(Clone, Debug, InputObject)]
pub struct DeploymentFilters {
    pub included_versions: Option<DeploymentVersionFilter>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(remote = "graphman::deployment_search::DeploymentVersionFilter")]
pub enum DeploymentVersionFilter {
    Current,
    Pending,
    Used,
    All,
}

impl From<DeploymentFilters> for graphman::deployment_search::DeploymentFilters {
    fn from(filters: DeploymentFilters) -> Self {
        let DeploymentFilters { included_versions } = filters;

        Self {
            included_versions: included_versions.map(Into::into),
        }
    }
}
