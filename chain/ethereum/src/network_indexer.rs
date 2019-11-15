use std::sync::Arc;

use futures::future;
use futures::prelude::Future;

use graph::prelude::{
  ComponentLoggerConfig, ElasticComponentLoggerConfig, EthereumAdapter, Logger, LoggerFactory,
  NetworkIndexer as NetworkIndexerTrait, Store,
};

pub struct NetworkIndexer {
  network_name: String,
  logger: Logger,
  store: Arc<dyn Store>,
  adapter: Arc<dyn EthereumAdapter>,
}

impl NetworkIndexer {
  pub fn new(
    network_name: String,
    store: Arc<dyn Store>,
    adapter: Arc<dyn EthereumAdapter>,
    logger_factory: &LoggerFactory,
  ) -> Self {
    let logger = logger_factory.component_logger(
      "NetworkIndexer",
      Some(ComponentLoggerConfig {
        elastic: Some(ElasticComponentLoggerConfig {
          index: String::from("ethereum-network-indexer"),
        }),
      }),
    );

    Self {
      network_name,
      logger,
      store,
      adapter,
    }
  }
}

impl NetworkIndexerTrait for NetworkIndexer {
  fn into_polling_stream(self) -> Box<dyn Future<Item = (), Error = ()> + Send> {
    Box::new(future::ok(true).map(|_| ()).map_err(|_: ()| ()))
  }
}
