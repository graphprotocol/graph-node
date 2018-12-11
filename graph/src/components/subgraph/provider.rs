use prelude::*;

/// Events emitted by [SubgraphProvider](trait.SubgraphProvider.html) implementations.
#[derive(Debug, PartialEq)]
pub enum SubgraphProviderEvent {
    /// A subgraph with the given manifest should start processing.
    SubgraphStart(SubgraphManifest),
    /// The subgraph with the given ID should stop processing.
    SubgraphStop(SubgraphId),
}

/// Common trait for subgraph providers.
pub trait SubgraphProvider: EventProducer<SubgraphProviderEvent> + Send + Sync + 'static {
    fn start(
        &self,
        id: SubgraphId,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static>;

    fn stop(
        &self,
        id: SubgraphId,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static>;
}

/// Common trait for named subgraph providers.
pub trait SubgraphProviderWithNames: Send + Sync + 'static {
    fn deploy(
        &self,
        name: SubgraphDeploymentName,
        id: SubgraphId,
        node_id: NodeId,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static>;

    fn remove(
        &self,
        name: SubgraphDeploymentName,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static>;

    fn list(&self) -> Result<Vec<(SubgraphDeploymentName, SubgraphId)>, Error>;
}
