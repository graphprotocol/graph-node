use std::sync::{Arc, Mutex};
use std::time::Instant;

use graph::prelude::*;

use super::*;

/// Metrics for analyzing the block writer performance.
struct BlockWriterMetrics {
    /// Stopwatch for measuring the overall time spent writing.
    stopwatch: StopwatchMetrics,

    /// Metric for aggregating over all transaction calls.
    transaction: Aggregate,
}

impl BlockWriterMetrics {
    /// Creates new block writer metrics for a given subgraph.
    pub fn new(
        subgraph_id: &SubgraphDeploymentId,
        stopwatch: StopwatchMetrics,
        registry: Arc<dyn MetricsRegistry>,
    ) -> Self {
        let transaction = Aggregate::new(
            format!("{}_transaction", subgraph_id.to_string()),
            "Transactions to the store",
            registry.clone(),
        );

        Self {
            stopwatch,
            transaction,
        }
    }
}

/// Component that writes Ethereum blocks to the network subgraph store.
pub struct BlockWriter {
    /// The network subgraph ID (e.g. `ethereum_mainnet_v0`).
    subgraph_id: SubgraphDeploymentId,

    /// Logger.
    logger: Logger,

    /// Store that manages the network subgraph.
    store: Arc<dyn Store>,

    /// Metrics for analyzing the block writer performance.
    metrics: Arc<Mutex<BlockWriterMetrics>>,
}

impl BlockWriter {
    /// Creates a new block writer for the given subgraph ID.
    pub fn new(
        subgraph_id: SubgraphDeploymentId,
        logger: &Logger,
        store: Arc<dyn Store>,
        stopwatch: StopwatchMetrics,
        metrics_registry: Arc<dyn MetricsRegistry>,
    ) -> Self {
        let logger = logger.new(o!("component" => "BlockWriter"));
        let metrics = Arc::new(Mutex::new(BlockWriterMetrics::new(
            &subgraph_id,
            stopwatch,
            metrics_registry,
        )));
        Self {
            store,
            subgraph_id,
            logger,
            metrics,
        }
    }

    /// Writes a block to the store and updates the network subgraph block pointer.
    pub fn write(&self, block: BlockWithUncles) -> impl Future<Item = (), Error = Error> {
        let logger = self.logger.new(o!(
            "block" => format_block(&block),
        ));

        // Write using a write context that we can thread through futures.
        let context = WriteContext {
            logger,
            subgraph_id: self.subgraph_id.clone(),
            store: self.store.clone(),
            cache: EntityCache::new(),
            metrics: self.metrics.clone(),
        };
        context.write(block)
    }
}

/// Internal context for writing a block.
struct WriteContext {
    logger: Logger,
    subgraph_id: SubgraphDeploymentId,
    store: Arc<dyn Store>,
    cache: EntityCache,
    metrics: Arc<Mutex<BlockWriterMetrics>>,
}

/// Internal result type used to thread WriteContext through the chain of futures
/// when writing blocks to the store.
type WriteContextResult = Box<dyn Future<Item = WriteContext, Error = Error> + Send>;

impl WriteContext {
    /// Updates an entity to a new value (potentially merging it with existing data).
    fn set_entity(mut self, value: impl TryIntoEntity + ToEntityKey) -> WriteContextResult {
        self.cache.set(
            value.to_entity_key(self.subgraph_id.clone()),
            match value.try_into_entity() {
                Ok(entity) => entity,
                Err(e) => return Box::new(future::err(e.into())),
            },
        );
        Box::new(future::ok(self))
    }

    /// Writes a block to the store.
    fn write(self, block: BlockWithUncles) -> impl Future<Item = (), Error = Error> {
        debug!(self.logger, "Write block");

        let block = Arc::new(block);
        let block_for_uncles = block.clone();
        let block_for_store = block.clone();

        Box::new(
            // Add the block entity
            self.set_entity(block.as_ref())
                // Add uncle block entities
                .and_then(move |context| {
                    futures::stream::iter_ok::<_, Error>(block_for_uncles.uncles.clone())
                        .fold(context, |context, ommer| context.set_entity(ommer.unwrap()))
                })
                // Transact everything into the store
                .and_then(move |context| {
                    let cache = context.cache;
                    let metrics = context.metrics;
                    let store = context.store;
                    let subgraph_id = context.subgraph_id;

                    let stopwatch = { metrics.lock().unwrap().stopwatch.clone() };

                    // Collect all entity modifications to be made
                    let modifications = match cache.as_modifications(store.as_ref()) {
                        Ok(mods) => mods,
                        Err(e) => return future::err(e.into()),
                    };

                    // Transact entity modifications into the store
                    let started = Instant::now();
                    future::result(
                        store
                            .transact_block_operations(
                                subgraph_id.clone(),
                                EthereumBlockPointer::from(&block_for_store.block),
                                modifications,
                                stopwatch,
                            )
                            .map_err(|e| e.into())
                            .map(move |_| {
                                let mut metrics = metrics.lock().unwrap();
                                metrics.transaction.update_duration(started.elapsed());
                            }),
                    )
                }),
        )
    }
}
