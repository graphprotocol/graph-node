use prelude::*;

/// Common trait for subgraph providers.
pub trait SubgraphDeploymentProvider:
    EventProducer<SubgraphDeploymentProviderEvent> + Send + Sync + 'static
{
    fn start(
        &self,
        id: SubgraphId,
    ) -> Box<Future<Item = (), Error = SubgraphDeploymentProviderError> + Send + 'static>;

    fn stop(
        &self,
        id: SubgraphId,
    ) -> Box<Future<Item = (), Error = SubgraphDeploymentProviderError> + Send + 'static>;
}
