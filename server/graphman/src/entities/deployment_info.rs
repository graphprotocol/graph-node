use async_graphql::SimpleObject;

use crate::entities::DeploymentStatus;

#[derive(Clone, Debug, SimpleObject)]
pub struct DeploymentInfo {
    pub hash: String,
    pub namespace: String,
    pub name: String,
    pub node_id: Option<String>,
    pub shard: String,
    pub chain: String,
    pub version_status: String,
    pub is_active: bool,
    pub status: Option<DeploymentStatus>,
}

impl From<graphman::deployment::Deployment> for DeploymentInfo {
    fn from(deployment: graphman::deployment::Deployment) -> Self {
        let graphman::deployment::Deployment {
            id: _,
            hash,
            namespace,
            name,
            node_id,
            shard,
            chain,
            version_status,
            is_active,
        } = deployment;

        Self {
            hash,
            namespace,
            name,
            node_id,
            shard,
            chain,
            version_status,
            is_active,
            status: None,
        }
    }
}
