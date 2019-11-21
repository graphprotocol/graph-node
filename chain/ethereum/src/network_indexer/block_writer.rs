use std::sync::{Arc, Mutex};
use std::time::Instant;

use graph::prelude::*;

use super::common::*;
use super::to_entity::*;

struct BlockWriterMetrics {
    transaction: Aggregate,
}

impl BlockWriterMetrics {
    pub fn new(subgraph_id: &SubgraphDeploymentId, registry: Arc<dyn MetricsRegistry>) -> Self {
        let transaction = Aggregate::new(
            format!("{}_transaction", subgraph_id.to_string()),
            "Transactions to the store",
            registry.clone(),
        );

        Self { transaction }
    }
}

pub(crate) struct BlockWriter<S> {
    subgraph_id: SubgraphDeploymentId,
    logger: Logger,
    store: Arc<S>,
    metrics: Arc<Mutex<BlockWriterMetrics>>,
}

impl<S> BlockWriter<S>
where
    S: Store + ChainStore,
{
    pub fn new(
        subgraph_id: SubgraphDeploymentId,
        logger: &Logger,
        store: Arc<S>,
        metrics_registry: Arc<dyn MetricsRegistry>,
    ) -> Self {
        let logger = logger.new(o!("component" => "BlockWriter"));
        let metrics = Arc::new(Mutex::new(BlockWriterMetrics::new(
            &subgraph_id,
            metrics_registry,
        )));
        Self {
            store,
            subgraph_id,
            logger,
            metrics,
        }
    }

    pub fn write(&self, block: BlockWithUncles) -> impl Future<Item = (), Error = Error> {
        let hash = block.inner().hash.clone().unwrap();
        let number = block.inner().number.clone().unwrap();

        let logger = self.logger.new(o!(
            "block_hash" => format!("{:?}", hash),
            "block_number" => format!("{}", number),
        ));

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

struct WriteContext<S> {
    logger: Logger,
    subgraph_id: SubgraphDeploymentId,
    store: Arc<S>,
    cache: EntityCache,
    metrics: Arc<Mutex<BlockWriterMetrics>>,
}

/// Internal result type used to thread WriteContext through
/// the chain of futures when writing network data.
type WriteContextResult<S> = Box<dyn Future<Item = WriteContext<S>, Error = Error> + Send>;

impl<S> WriteContext<S>
where
    S: Store + ChainStore,
{
    fn set_entity(mut self, value: impl ToEntity + ToEntityKey) -> WriteContextResult<S> {
        self.cache.set(
            value.to_entity_key(self.subgraph_id.clone()),
            match value.to_entity() {
                Ok(entity) => entity,
                Err(e) => return Box::new(future::err(e.into())),
            },
        );
        Box::new(future::ok(self))
    }

    fn write(self, block: BlockWithUncles) -> impl Future<Item = (), Error = Error> {
        debug!(self.logger, "Write block");

        let block = Arc::new(block);
        let block_for_uncles = block.clone();
        let block_for_store = block.clone();

        Box::new(
            // Add the block and uncle block entities
            self.set_entity(block.as_ref())
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

                    // Transact entity operations into the store
                    let modifications = match cache.as_modifications(store.as_ref()) {
                        Ok(mods) => mods,
                        Err(e) => return future::err(e.into()),
                    };

                    let started = Instant::now();
                    future::result(
                        store
                            .transact_block_operations(
                                subgraph_id.clone(),
                                EthereumBlockPointer::from(&block_for_store.block),
                                modifications,
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
