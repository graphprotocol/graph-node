use graph::prelude::blockchain::*;
use graph::prelude::*;

use crate::network_indexer::ensure_subgraph_exists;
use crate::{EthereumAdapter, *};

const INDEXER_VERSION: i32 = 0;

pub struct EthereumIndexerOptions {
    pub subgraph_name: String,
    pub logger: Logger,
    pub clients: Vec<Arc<EthereumAdapter<Transport>>>,
    pub metrics_registry: Arc<dyn MetricsRegistry>,
    pub options: NetworkIndexerOptions,
}

pub struct EthereumIndexer {}

impl EthereumIndexer {
    pub fn create(options: EthereumIndexerOptions) -> impl Future<Item = Self, Error = Error> {
        // Create a subgraph name and ID
        let id_str = format!(
            "{}_v{}",
            options.subgraph_name.replace("/", "_"),
            INDEXER_VERSION
        );
        let subgraph_id = SubgraphDeploymentId::new(id_str).expect("valid network subgraph ID");
        let subgraph_name =
            SubgraphName::new(options.subgraph_name).expect("valid network subgraph name");

        let logger = options.logger.new(o!(
          "subgraph_name" => subgraph_name.to_string(),
          "subgraph_id" => subgraph_id.to_string(),
        ));

        // Ensure subgraph, the wire up the tracer and indexer
        ensure_subgraph_exists(
            subgraph_name,
            subgraph_id.clone(),
            logger.clone(),
            options.options.store.clone(),
            options.options.start_block,
        )
        .and_then(move |_| {
            future::ok(NetworkIndexer::new(
                subgraph_id.clone(),
                &logger,
                // Just pick the first Ethereum client for now
                options.clients[0].clone(),
                options.options.store.clone(),
                options.metrics_registry.clone(),
            ))
        })
    }
}

impl NetworkIndexer for EthereumIndexer {}

impl EventProducer<NetworkIndexerEvent> for EthereumIndexer {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<dyn Stream<Item = NetworkIndexerEvent, Error = ()> + Send>> {
        unimplemented!();
    }
}
