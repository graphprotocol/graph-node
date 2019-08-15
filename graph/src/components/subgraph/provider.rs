use crate::prelude::*;

/// Common trait for subgraph providers.
pub trait SubgraphAssignmentProvider:
    EventProducer<SubgraphAssignmentProviderEvent> + Send + Sync + 'static
{
    fn start(
        &self,
        id: SubgraphDeploymentId,
    ) -> Box<dyn Future<Item = (), Error = SubgraphAssignmentProviderError> + Send + 'static>;

    fn stop(
        &self,
        id: SubgraphDeploymentId,
    ) -> Box<dyn Future<Item = (), Error = SubgraphAssignmentProviderError> + Send + 'static>;
}
