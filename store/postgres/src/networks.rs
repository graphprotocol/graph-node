use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, Pool};

use graph::prelude::{
  Error, Logger, MetricsRegistry, NetworkInstanceId,
  NetworkStoreFactory as NetworkStoreFactoryTrait,
};

use crate::{Store, StoreConfig};

pub struct NetworkStoreFactoryOptions {
  pub logger: Logger,
  pub url: String,
  pub conn_pool: Pool<ConnectionManager<PgConnection>>,
  pub metrics_registry: Arc<dyn MetricsRegistry>,
}

pub struct NetworkStoreFactory {
  logger: Logger,
  url: String,
  conn_pool: Pool<ConnectionManager<PgConnection>>,
  metrics_registry: Arc<dyn MetricsRegistry>,
  stores: HashMap<NetworkInstanceId, Arc<Store>>,
}

impl NetworkStoreFactory {
  pub fn new(options: NetworkStoreFactoryOptions) -> Self {
    let NetworkStoreFactoryOptions {
      logger,
      url,
      conn_pool,
      metrics_registry,
    } = options;

    Self {
      logger,
      url,
      conn_pool,
      metrics_registry,
      stores: Default::default(),
    }
  }
}

#[async_trait]
impl NetworkStoreFactoryTrait for NetworkStoreFactory {
  type BlockchainStore = Store;

  async fn blockchain_store(
    &mut self,
    id: &NetworkInstanceId,
  ) -> Result<Arc<Self::BlockchainStore>, Error> {
    let url = self.url.clone();
    let conn_pool = self.conn_pool.clone();
    let logger = self.logger.clone();
    let metrics_registry = self.metrics_registry.clone();

    let store = self.stores.entry(id.clone()).or_insert_with(|| {
      Arc::new(Store::new(
        StoreConfig {
          postgres_url: url,
          network_name: id.name.clone(),
        },
        &logger,
        conn_pool,
        metrics_registry,
      ))
    });

    Ok(store.clone())
  }
}
