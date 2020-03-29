use std::sync::Arc;

use async_trait::async_trait;

use graph::prelude::{
  BlockchainStore, BlockchainStoreId, Error, Logger,
  NetworkStoreFactory as NetworkStoreFactoryTrait,
};

use crate::Store;

pub struct NetworkStoreFactoryOptions {
  logger: Logger,
}

pub struct NetworkStoreFactory {}

impl NetworkStoreFactory {
  pub fn new(options: NetworkStoreFactoryOptions) -> Self {
    unimplemented!();
  }
}

#[async_trait]
impl NetworkStoreFactoryTrait for NetworkStoreFactory {
  async fn blockchain_store(
    &self,
    id: &BlockchainStoreId,
  ) -> Result<Arc<Box<dyn BlockchainStore>>, Error> {
    unimplemented!();
  }
}
