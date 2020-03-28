use async_trait::async_trait;

use crate::prelude::*;

/// Common trait for subgraph providers.
#[async_trait]
pub trait SubgraphAssignmentProvider:
    EventProducer<SubgraphAssignmentProviderEvent> + Send + Sync + 'static
{
    async fn start(&self, id: &SubgraphDeploymentId)
        -> Result<(), SubgraphAssignmentProviderError>;
    async fn stop(&self, id: SubgraphDeploymentId) -> Result<(), SubgraphAssignmentProviderError>;
}
