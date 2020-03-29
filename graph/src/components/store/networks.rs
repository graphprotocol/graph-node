use std::sync::Arc;

use async_trait::async_trait;

use crate::prelude::{
  ChainStore, Error, EthereumCallCache, NetworkInstanceId, Store, SubgraphDeploymentStore,
};

/// Common trait for a component that can provide network stores.
#[async_trait]
pub trait NetworkStoreFactory {
  type BlockchainStore: Store + ChainStore + SubgraphDeploymentStore + EthereumCallCache + Sized;

  async fn blockchain_store(
    &mut self,
    id: &NetworkInstanceId,
  ) -> Result<Arc<Self::BlockchainStore>, Error>;
}
