use anyhow::anyhow;
use anyhow::Result;
use async_graphql::InputObject;

/// Available criteria for selecting one or more deployments.
/// No more than one criterion can be selected at a time.
#[derive(Clone, Debug, InputObject)]
pub struct DeploymentSelector {
    /// Selects deployments by subgraph name.
    ///
    /// It is not necessary to enter the full name, a name prefix or suffix may be sufficient.
    pub name: Option<String>,

    /// Selects deployments by IPFS hash. The format is `Qm...`.
    pub hash: Option<String>,

    /// Since the same IPFS hash can be deployed in multiple shards,
    /// it is possible to specify the shard.
    ///
    /// It only works if the IPFS hash is also provided.
    pub shard: Option<String>,

    /// Selects a deployment by its database namespace. The format is `sgdNNN`.
    pub schema: Option<String>,
}

impl TryFrom<DeploymentSelector> for graphman::deployment::DeploymentSelector {
    type Error = anyhow::Error;

    fn try_from(deployment: DeploymentSelector) -> Result<Self> {
        let DeploymentSelector {
            name,
            hash,
            shard,
            schema,
        } = deployment;

        match (name, hash, shard, schema) {
            (Some(name), None, None, None) => Ok(Self::Name(name)),
            (None, Some(hash), shard, None) => Ok(Self::Subgraph { hash, shard }),
            (None, None, None, Some(name)) => Ok(Self::Schema(name)),
            (None, None, None, None) => Err(anyhow!("selector can not be empty")),
            _ => Err(anyhow!("multiple selectors can not be applied at once")),
        }
    }
}
