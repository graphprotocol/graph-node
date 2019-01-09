use prelude::*;

/// Common trait for named subgraph providers.
pub trait SubgraphRegistrar: Send + Sync + 'static {
    fn create_subgraph(
        &self,
        name: SubgraphName,
    ) -> Box<Future<Item = CreateSubgraphResult, Error = SubgraphRegistrarError> + Send + 'static>;

    fn create_subgraph_version(
        &self,
        name: SubgraphName,
        hash: SubgraphId,
        deployment_node_id: NodeId,
    ) -> Box<Future<Item = (), Error = SubgraphRegistrarError> + Send + 'static>;

    fn remove_subgraph(
        &self,
        name: SubgraphName,
    ) -> Box<Future<Item = (), Error = SubgraphRegistrarError> + Send + 'static>;

    fn list_subgraphs(
        &self,
    ) -> Box<Future<Item = Vec<SubgraphName>, Error = SubgraphRegistrarError> + Send + 'static>;
}
