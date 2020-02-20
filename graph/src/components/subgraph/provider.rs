use crate::prelude::*;

/// Common trait for subgraph providers.
pub trait SubgraphAssignmentProvider:
    EventProducer<SubgraphAssignmentProviderEvent> + Send + Sync + 'static
{
    fn start<'a>(
        &'a self,
        id: &'a SubgraphDeploymentId,
    ) -> Pin<
        Box<
            dyn futures03::Future<Output = Result<(), SubgraphAssignmentProviderError>> + Send + 'a,
        >,
    >;

    fn stop(
        &self,
        id: SubgraphDeploymentId,
    ) -> Box<dyn Future<Item = (), Error = SubgraphAssignmentProviderError> + Send + 'static>;
}
