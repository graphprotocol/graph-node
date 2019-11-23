use graph::components::forward;
use graph::prelude::*;

mod block_writer;
mod common;
mod network_indexer;
mod network_tracer;
mod subgraph;
mod to_entity;

pub use self::block_writer::*;
pub use self::common::*;
pub use self::network_indexer::*;
pub use self::network_tracer::*;
pub use self::subgraph::*;
pub use self::to_entity::*;

pub use self::network_tracer::NetworkTracerEvent;

const NETWORK_INDEXER_VERSION: u32 = 0;

pub fn create<S>(
    subgraph_name: String,
    store: Arc<S>,
    adapter: Arc<dyn EthereumAdapter>,
    logger: &Logger,
    metrics_registry: Arc<dyn MetricsRegistry>,
) -> impl Future<Item = (), Error = ()>
where
    S: Store + ChainStore,
{
    // Create a subgraph name and ID
    let id_str = format!(
        "{}_v{}",
        subgraph_name.replace("/", "_"),
        NETWORK_INDEXER_VERSION
    );
    let subgraph_id = SubgraphDeploymentId::new(id_str).expect("valid network subgraph ID");
    let subgraph_name = SubgraphName::new(subgraph_name).expect("valid network subgraph name");

    let logger = logger.new(o!(
      "subgraph_name" => subgraph_name.to_string(),
      "subgraph_id" => subgraph_id.to_string(),
    ));

    // Ensure subgraph, the wire up the tracer and indexer
    subgraph::ensure_subgraph_exists(
        subgraph_name,
        subgraph_id.clone(),
        logger.clone(),
        store.clone(),
        None,
    )
    .and_then(move |_| {
        let stopwatch = StopwatchMetrics::new(
            logger.clone(),
            subgraph_id.clone(),
            metrics_registry.clone(),
        );

        // Create the network tracer
        let mut tracer = NetworkTracer::new(
            subgraph_id.clone(),
            &logger,
            adapter.clone(),
            store.clone(),
            metrics_registry.clone(),
        );

        // Create the network indexer
        let indexer = NetworkIndexer::new(
            subgraph_id.clone(),
            &logger,
            store.clone(),
            stopwatch.clone(),
            metrics_registry.clone(),
        );

        forward(&mut tracer, &indexer).unwrap()
    })
}
