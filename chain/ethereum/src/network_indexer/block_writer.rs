use std::sync::{Arc, Mutex};
use std::time::Instant;

use graph::prelude::*;

use super::common::*;
use super::to_entity::*;

/// Internal result type used to thread (BlockWriter, EntityCache) through
/// the chain of futures when writing network data.
type BlockWriteResult<S> =
    Box<dyn Future<Item = (BlockWriter<S>, EntityCache), Error = Error> + Send>;

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

/// Options for configuring a block writer.
pub struct BlockWriterOptions<'a, S> {
    pub logger: &'a Logger,
    pub subgraph_id: SubgraphDeploymentId,
    pub store: Arc<S>,
    pub metrics_registry: Arc<dyn MetricsRegistry>,
}

pub(crate) struct BlockWriter<S> {
    logger: Logger,
    subgraph_id: SubgraphDeploymentId,
    store: Arc<S>,
    metrics: Arc<Mutex<BlockWriterMetrics>>,
}

impl<S> BlockWriter<S>
where
    S: Store + ChainStore,
{
    pub fn new<'a>(options: BlockWriterOptions<'a, S>) -> Self {
        let BlockWriterOptions {
            logger,
            subgraph_id,
            store,
            metrics_registry,
        } = options;

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

    pub fn write(self, block: BlockWithUncles) -> impl Future<Item = Self, Error = Error> {
        let hash = block.inner().hash.clone().unwrap();
        let number = block.inner().number.clone().unwrap();

        let logger = self.logger.new(o!(
            "block_hash" => format!("{:?}", hash),
            "block_number" => format!("{}", number),
        ));

        debug!(logger, "Write block");

        let block = Arc::new(block);
        let block_for_uncles = block.clone();
        let block_for_store = block.clone();

        Box::new(
            // Add the block and uncle block entities
            self.set_entity(EntityCache::new(), block.as_ref())
                .and_then(move |(writer, cache)| {
                    futures::stream::iter_ok::<_, Error>(block_for_uncles.uncles.clone())
                        .fold((writer, cache), |(writer, cache), ommer| {
                            writer.set_entity(cache, ommer.unwrap())
                        })
                })
                // Transact everything into the store
                .and_then(move |(writer, cache)| {
                    // Transact entity operations into the store
                    let modifications = match cache.as_modifications(writer.store.as_ref()) {
                        Ok(mods) => mods,
                        Err(e) => return future::err(e.into()),
                    };

                    let started = Instant::now();
                    future::result(
                        writer
                            .store
                            .transact_block_operations(
                                writer.subgraph_id.clone(),
                                EthereumBlockPointer::from(&block_for_store.block),
                                modifications,
                            )
                            .map_err(|e| e.into())
                            .map(move |_| {
                                {
                                    let mut metrics = writer.metrics.lock().unwrap();
                                    metrics.transaction.update_duration(started.elapsed());
                                }
                                writer
                            }),
                    )
                }),
        )
    }

    fn set_entity(
        self,
        mut cache: EntityCache,
        value: impl ToEntity + ToEntityKey,
    ) -> BlockWriteResult<S> {
        cache.set(
            value.to_entity_key(self.subgraph_id.clone()),
            match value.to_entity() {
                Ok(entity) => entity,
                Err(e) => return Box::new(future::err(e.into())),
            },
        );
        Box::new(future::ok((self, cache)))
    }
}
