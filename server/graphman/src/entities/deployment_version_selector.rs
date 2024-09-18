use async_graphql::Enum;

/// Used to filter deployments by version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
pub enum DeploymentVersionSelector {
    Current,
    Pending,
    Used,
}

impl From<DeploymentVersionSelector> for graphman::deployment::DeploymentVersionSelector {
    fn from(version: DeploymentVersionSelector) -> Self {
        match version {
            DeploymentVersionSelector::Current => Self::Current,
            DeploymentVersionSelector::Pending => Self::Pending,
            DeploymentVersionSelector::Used => Self::Used,
        }
    }
}
