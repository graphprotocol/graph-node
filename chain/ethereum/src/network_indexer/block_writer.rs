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
            "deployment_transaction",
            subgraph_id.as_str(),
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
    metrics: Arc<BlockWriterMetrics>,
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
        let metrics = Arc::new(BlockWriterMetrics::new(
            &subgraph_id,
            stopwatch,
            metrics_registry,
        ));
        Self {
            store,
            subgraph_id,
            logger,
            metrics,
        }
    }

    /// Writes a block to the store and updates the network subgraph block pointer.
    pub fn write(
        &self,
        block: BlockWithOmmers,
    ) -> impl Future<Item = EthereumBlockPointer, Error = Error> {
        let logger = self.logger.new(o!(
            "block" => format!("{}", block),
        ));

        // Write using a write context that we can thread through futures.
        let context = WriteContext {
            logger,
            subgraph_id: self.subgraph_id.clone(),
            store: self.store.clone(),
            cache: EntityCache::new(self.store.clone()),
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
    metrics: Arc<BlockWriterMetrics>,
}

/// Internal result type used to thread WriteContext through the chain of futures
/// when writing blocks to the store.
type WriteContextResult = Box<dyn Future<Item = WriteContext, Error = Error> + Send>;

impl WriteContext {
    /// Updates an entity to a new value (potentially merging it with existing data).
    fn set_entity(mut self, value: impl TryIntoEntity + ToEntityKey) -> WriteContextResult {
        self.cache
            .set(
                value.to_entity_key(self.subgraph_id.clone()),
                match value.try_into_entity() {
                    Ok(entity) => entity,
                    Err(e) => return Box::new(future::err(e.into())),
                },
            )
            // An error here is only possible if entities ever get removed, which is not the case.
            .unwrap();
        Box::new(future::ok(self))
    }

    /// Writes a block to the store.
    fn write(
        self,
        block: BlockWithOmmers,
    ) -> impl Future<Item = EthereumBlockPointer, Error = Error> {
        debug!(self.logger, "Write block");

        let block = Arc::new(block);
        let block_for_ommers = block.clone();
        let block_for_store = block.clone();

        Box::new(
            // Add the block entity
            self.set_entity(block.as_ref())
                // Add uncle block entities
                .and_then(move |context| {
                    futures::stream::iter_ok::<_, Error>(block_for_ommers.ommers.clone())
                        .fold(context, move |context, ommer| context.set_entity(ommer))
                })
                // Transact everything into the store
                .and_then(move |context| {
                    let cache = context.cache;
                    let metrics = context.metrics;
                    let store = context.store;
                    let subgraph_id = context.subgraph_id;

                    let stopwatch = metrics.stopwatch.clone();

                    // Collect all entity modifications to be made
                    let modifications = match cache.as_modifications(store.as_ref()) {
                        Ok(mods) => mods,
                        Err(e) => return future::err(e.into()),
                    }
                    .modifications;

                    let block_ptr = EthereumBlockPointer::from(&block_for_store.block);

                    // Transact entity modifications into the store
                    let started = Instant::now();
                    future::result(
                        store
                            .transact_block_operations(
                                subgraph_id.clone(),
                                block_ptr.clone(),
                                modifications,
                                stopwatch,
                                Vec::new(),
                            )
                            .map_err(|e| e.into())
                            .map(move |_| {
                                metrics.transaction.update_duration(started.elapsed());
                                block_ptr
                            }),
                    )
                }),
        )
    }
}
