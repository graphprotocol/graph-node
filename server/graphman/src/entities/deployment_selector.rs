use async_graphql::{InputObject, OneofObject};
use graph_core::graphman;

#[derive(Clone, Debug, OneofObject)]
pub enum DeploymentSelector {
    Subgraph(DeploymentSubgraphSelector),
    Ipfs(DeploymentIpfsSelector),
    Namespace(DeploymentNamespaceSelector),
}

#[derive(Clone, Debug, InputObject)]
pub struct DeploymentSubgraphSelector {
    name: String,
}

#[derive(Clone, Debug, InputObject)]
pub struct DeploymentIpfsSelector {
    hash: String,
    shard: Option<String>,
}

#[derive(Clone, Debug, InputObject)]
pub struct DeploymentNamespaceSelector {
    namespace: String,
}

impl From<DeploymentSelector> for graphman::deployment_search::DeploymentSelector {
    fn from(selector: DeploymentSelector) -> Self {
        use DeploymentSelector::*;

        match selector {
            Subgraph(DeploymentSubgraphSelector { name }) => Self::Subgraph { name },
            Ipfs(DeploymentIpfsSelector { hash, shard }) => Self::Ipfs { hash, shard },
            Namespace(DeploymentNamespaceSelector { namespace }) => Self::Namespace(namespace),
        }
    }
}
