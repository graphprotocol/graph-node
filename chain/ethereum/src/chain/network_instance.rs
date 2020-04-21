use async_trait::async_trait;
use futures03::future::AbortHandle;

use graph::prelude::{
  futures03, ChainStore, Error, EthereumCallCache, MetricsRegistry, NetworkInstance,
  NetworkInstanceId, Store, SubgraphDeploymentStore, SubgraphManifest,
};

use super::Chain;

#[async_trait]
impl<MR, S> NetworkInstance for Chain<MR, S>
where
  MR: MetricsRegistry,
  S: Store + ChainStore + SubgraphDeploymentStore + EthereumCallCache,
{
  fn id(&self) -> &NetworkInstanceId {
    &self.id
  }

  fn url(&self) -> &str {
    self.url.as_str()
  }

  async fn start_subgraph(&self, subgraph: SubgraphManifest) -> Result<AbortHandle, Error> {
    self.subgraph_indexer.start(subgraph).await
  }
}
