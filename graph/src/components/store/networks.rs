use std::sync::Arc;

use async_trait::async_trait;

use crate::prelude::{BlockPointer, ChainStore, Error, NetworkInstanceId, Store};

/// Combination of store traits required for blockchain stores.
pub trait BlockchainStore: Store + ChainStore {}

#[derive(Debug, PartialEq)]
pub struct BlockchainStoreId {
  pub network_instance: NetworkInstanceId,
  pub genesis_block: BlockPointer,
}

/// Common trait for a component that can provide network stores.
#[async_trait]
pub trait NetworkStoreFactory {
  async fn blockchain_store(
    &self,
    id: &BlockchainStoreId,
  ) -> Result<Arc<Box<dyn BlockchainStore>>, Error>;
}
