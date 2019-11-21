use futures::prelude::Future;
use futures::{
    future,
    future::{loop_fn, Loop},
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::timer::Delay;

use graph::prelude::{NetworkIndexer as NetworkIndexerTrait, *};

mod block_writer;
mod common;
mod subgraph;
mod to_entity;

use block_writer::*;
use common::*;

const NETWORK_INDEXER_VERSION: u32 = 0;

struct NetworkIndexerMetrics {
    pub stopwatch: StopwatchMetrics,
    pub chain_head: Box<Gauge>,
    pub subgraph_head: Box<Gauge>,
    pub poll_chain_head: Aggregate,
    pub poll_subgraph_head: Aggregate,
    pub block_hash: Aggregate,
    pub full_block: Aggregate,
    pub ommers: Aggregate,
    pub index_range: Aggregate,
    pub write_block: Aggregate,
}

impl NetworkIndexerMetrics {
    pub fn new(
        logger: Logger,
        subgraph_id: &SubgraphDeploymentId,
        registry: Arc<dyn MetricsRegistry>,
    ) -> Self {
        let stopwatch =
            StopwatchMetrics::new(logger.clone(), subgraph_id.clone(), registry.clone());

        let chain_head = registry
            .new_gauge(
                format!("{}_chain_head", subgraph_id.to_string()),
                "The latest block on the network".into(),
                HashMap::new(),
            )
            .expect(
                format!(
                    "failed to register `{}_chain_head` metric",
                    subgraph_id.to_string()
                )
                .as_str(),
            );

        let subgraph_head = registry
            .new_gauge(
                format!("{}_subgraph_head", subgraph_id.to_string()),
                "The latest indexed block".into(),
                HashMap::new(),
            )
            .expect(
                format!(
                    "failed to register `{}_subgraph_head` metric",
                    subgraph_id.to_string()
                )
                .as_str(),
            );

        let poll_chain_head = Aggregate::new(
            format!("{}_poll_chain_head", subgraph_id.to_string()),
            "Polling the latest block from the network",
            registry.clone(),
        );

        let poll_subgraph_head = Aggregate::new(
            format!("{}_poll_subgraph_head", subgraph_id.to_string()),
            "Identifying the head block of the subgraph",
            registry.clone(),
        );

        let block_hash = Aggregate::new(
            format!("{}_block_hash", subgraph_id.to_string()),
            "Resolve block number into hash",
            registry.clone(),
        );

        let full_block = Aggregate::new(
            format!("{}_full_block", subgraph_id.to_string()),
            "Download full block",
            registry.clone(),
        );

        let ommers = Aggregate::new(
            format!("{}_ommers", subgraph_id.to_string()),
            "Download ommers",
            registry.clone(),
        );

        let write_block = Aggregate::new(
            format!("{}_write_block", subgraph_id.to_string()),
            "Writing blocks to the store",
            registry.clone(),
        );

        let index_range = Aggregate::new(
            format!("{}_index_range", subgraph_id.to_string()),
            "Fetch and index a range of blocks",
            registry.clone(),
        );

        Self {
            stopwatch,
            chain_head,
            subgraph_head,
            poll_chain_head,
            poll_subgraph_head,
            block_hash,
            full_block,
            ommers,
            index_range,
            write_block,
        }
    }
}

pub struct NetworkIndexer<S> {
    subgraph_name: SubgraphName,
    subgraph_id: SubgraphDeploymentId,
    logger: Logger,
    store: Arc<S>,
    adapter: Arc<dyn EthereumAdapter>,
    block_writer: Option<BlockWriter<S>>,

    metrics: Arc<Mutex<NetworkIndexerMetrics>>,
}

impl<S> NetworkIndexer<S>
where
    S: Store + ChainStore,
{
    pub fn new(
        subgraph_name: String,
        store: Arc<S>,
        adapter: Arc<dyn EthereumAdapter>,
        logger_factory: &LoggerFactory,
        metrics_registry: Arc<dyn MetricsRegistry>,
    ) -> Self {
        // Create a subgraph name and ID (base58 encoded version of the name)
        let id_str = format!(
            "{}_v{}",
            subgraph_name.replace("/", "_"),
            NETWORK_INDEXER_VERSION
        );
        let subgraph_id = SubgraphDeploymentId::new(id_str).expect("valid network subgraph ID");
        let subgraph_name = SubgraphName::new(subgraph_name).expect("valid network subgraph name");

        let logger = logger_factory.component_logger(
            "NetworkIndexer",
            Some(ComponentLoggerConfig {
                elastic: Some(ElasticComponentLoggerConfig {
                    index: String::from("ethereum-network-indexer"),
                }),
            }),
        );

        let logger = logger.new(o!(
          "subgraph_name" => subgraph_name.to_string(),
          "subgraph_id" => subgraph_id.to_string(),
        ));

        let metrics = Arc::new(Mutex::new(NetworkIndexerMetrics::new(
            logger.clone(),
            &subgraph_id,
            metrics_registry.clone(),
        )));

        let block_writer = Some(BlockWriter::new(BlockWriterOptions {
            logger: &logger,
            subgraph_id: subgraph_id.clone(),
            store: store.clone(),
            metrics_registry,
        }));

        Self {
            subgraph_name,
            subgraph_id,
            logger,
            store,
            adapter,
            block_writer,
            metrics,
        }
    }

    fn ensure_subgraph_exists(self) -> impl Future<Item = Self, Error = Error> {
        debug!(self.logger, "Ensure that the network subgraph exists");

        let logger_for_created = self.logger.clone();

        // Do nothing if the deployment already exists
        subgraph::check_subgraph_exists(self.store.clone(), self.subgraph_id.clone())
            .from_err()
            .and_then(move |subgraph_exists| {
                if subgraph_exists {
                    debug!(self.logger, "Network subgraph deployment already exists");
                    Box::new(future::ok(self)) as Box<dyn Future<Item = _, Error = _> + Send>
                } else {
                    debug!(
                        self.logger,
                        "Network subgraph deployment needs to be created"
                    );
                    Box::new(
                        subgraph::create_subgraph(
                            self.store.clone(),
                            self.subgraph_name.clone(),
                            self.subgraph_id.clone(),
                        )
                        .inspect(move |_| {
                            debug!(logger_for_created, "Created Ethereum network subgraph");
                        })
                        .map(|_| self),
                    )
                }
            })
    }

    fn fetch_block_and_uncles(
        logger: Logger,
        adapter: Arc<dyn EthereumAdapter>,
        metrics: Arc<Mutex<NetworkIndexerMetrics>>,
        block_number: u64,
    ) -> impl Future<Item = BlockWithUncles, Error = Error> {
        let logger_for_block = logger.clone();
        let adapter_for_block = adapter.clone();

        let logger_for_full_block = logger.clone();
        let adapter_for_full_block = adapter.clone();

        let logger_for_uncles = logger.clone();
        let adapter_for_uncles = adapter.clone();

        let metrics_for_block_hash = metrics.clone();
        let metrics_for_full_block = metrics.clone();
        let metrics_for_ommers = metrics.clone();

        adapter
            .block_hash_by_block_number(&logger, block_number)
            .measure(move |_, duration| {
                metrics_for_block_hash
                    .lock()
                    .unwrap()
                    .block_hash
                    .update_duration(duration);
            })
            .and_then(move |hash| {
                let hash = hash.expect("no block hash returned for block number");
                adapter_for_block.block_by_hash(&logger_for_block, hash)
            })
            .from_err()
            .and_then(move |block| {
                let block = block.expect("no block returned for hash");
                adapter_for_full_block
                    .load_full_block(&logger_for_full_block, block)
                    .measure(move |_, duration| {
                        metrics_for_full_block
                            .lock()
                            .unwrap()
                            .full_block
                            .update_duration(duration);
                    })
                    .from_err()
            })
            .and_then(move |block| {
                adapter_for_uncles
                    .uncles(&logger_for_uncles, &block.block)
                    .measure(move |_, duration| {
                        metrics_for_ommers
                            .lock()
                            .unwrap()
                            .ommers
                            .update_duration(duration);
                    })
                    .and_then(move |uncles| future::ok(BlockWithUncles { block, uncles }))
            })
    }

    fn index_next_blocks(self) -> impl Future<Item = Self, Error = Error> {
        let logger_for_head_comparison = self.logger.clone();

        let subgraph_id_for_subgraph_head = self.subgraph_id.clone();
        let store_for_subgraph_head = self.store.clone();

        let measure_chain_head = {
            self.metrics
                .lock()
                .unwrap()
                .stopwatch
                .start_section("chain_head")
        };

        let metrics_for_chain_head = self.metrics.clone();
        let metrics_for_subgraph_head = self.metrics.clone();

        // Poll the latest chain head from the network
        self.adapter
            .clone()
            .latest_block(&self.logger)
            .from_err()
            // Log chain head block and fail immediately if it is invalid (i.e.
            // is missing a block number and/or hash)
            .and_then(move |chain_head| {
                if chain_head.number.is_none() || chain_head.hash.is_none() {
                    future::err(format_err!(
                        "chain head block is missing a block number and hash"
                    ))
                } else {
                    future::ok(chain_head)
                }
            })
            .measure(move |chain_head, duration| {
                measure_chain_head.end();
                let mut metrics = metrics_for_chain_head.lock().unwrap();
                metrics
                    .chain_head
                    .set(chain_head.number.unwrap().as_u64() as f64);
                metrics.poll_chain_head.update_duration(duration)
            })
            // Identify the block the Ethereum network subgraph is on right now
            .and_then(move |chain_head| {
                let measure_subgraph_head = {
                    metrics_for_subgraph_head
                        .lock()
                        .unwrap()
                        .stopwatch
                        .start_section("subgraph_head")
                };
                let subgraph_head_started = Instant::now();
                store_for_subgraph_head
                    .clone()
                    .block_ptr(subgraph_id_for_subgraph_head.clone())
                    .map(move |subgraph_head| {
                        measure_subgraph_head.end();
                        metrics_for_subgraph_head
                            .lock()
                            .unwrap()
                            .poll_subgraph_head
                            .update_duration(Instant::now() - subgraph_head_started);
                        (chain_head, subgraph_head)
                    })
            })
            // Log the this block
            .and_then(move |(chain_head, subgraph_head)| {
                debug!(
                    logger_for_head_comparison,
                    "Checking chain and subgraph head blocks";
                    "subgraph" => format!(
                        "({}, {})",
                        subgraph_head.map_or("none".into(), |ptr| format!("{}", ptr.number)),
                        subgraph_head.map_or("none".into(), |ptr| format!("{:?}", ptr.hash))
                    ),
                    "chain" => format!(
                        "({}, {:?})",
                        chain_head.number.unwrap(),
                        chain_head.hash.unwrap()
                    ),
                );

                future::ok((chain_head, subgraph_head))
            })
            .and_then(move |(chain_head, latest_block)| {
                // If we're already on the chain head, try again in 0.5s
                if Some((&chain_head).into()) == latest_block {
                    return Box::new(
                        Delay::new(Instant::now() + Duration::from_millis(500))
                            .from_err()
                            .and_then(|_| future::ok(self)),
                    ) as Box<dyn Future<Item = _, Error = _> + Send>;
                }

                // This is safe to do now; if the chain head block had no number
                // we would've failed earlier already
                let chain_head_number = chain_head.number.unwrap().as_u64();

                // Calculate the number of blocks to ingest;
                // fetch no more than 10000 blocks at a time
                let remaining_blocks =
                    chain_head_number - latest_block.map_or(0u64, |ptr| ptr.number);
                let blocks_to_ingest = remaining_blocks.min(10000);
                let block_range = latest_block.map_or(0u64, |ptr| ptr.number + 1)
                    ..(latest_block.map_or(0u64, |ptr| ptr.number + 1) + blocks_to_ingest);

                debug!(
                    self.logger,
                    "Fetching {} of {} remaining blocks ({:?})",
                    blocks_to_ingest,
                    remaining_blocks,
                    block_range,
                );

                let logger_for_fetching = self.logger.clone();
                let adapter_for_fetching = self.adapter.clone();
                let metrics_for_fetching = self.metrics.clone();

                let metrics_for_range = self.metrics.clone();

                let measure_range = {
                    self.metrics
                        .lock()
                        .unwrap()
                        .stopwatch
                        .start_section("index_range")
                };

                Box::new(
                    futures::stream::iter_ok::<_, Error>(block_range.map(move |block_number| {
                        Self::fetch_block_and_uncles(
                            logger_for_fetching.clone(),
                            adapter_for_fetching.clone(),
                            metrics_for_fetching.clone(),
                            block_number,
                        )
                    }))
                    .buffered(500)
                    .fold(self, move |indexer, block| {
                        indexer.index_block(block).map(|indexer| indexer)
                    })
                    .measure(move |_, duration| {
                        measure_range.end();
                        metrics_for_range
                            .lock()
                            .unwrap()
                            .index_range
                            .update_duration(duration);
                    }),
                )
            })
    }

    fn index_block(mut self, block: BlockWithUncles) -> impl Future<Item = Self, Error = Error> {
        let number = block.inner().number.unwrap();
        let metrics = self.metrics.clone();
        let measure_write_block = {
            metrics
                .lock()
                .unwrap()
                .stopwatch
                .start_section("write_block")
        };

        self.block_writer
            .take()
            .expect("cannot write two blocks at the same time")
            .write(block)
            .measure(move |_, duration| {
                measure_write_block.end();

                let mut metrics = metrics.lock().unwrap();
                metrics.subgraph_head.set(number.as_u64() as f64);
                metrics.write_block.update_duration(duration);
            })
            .map(move |block_writer| {
                self.block_writer = Some(block_writer);
                self
            })
    }
}

impl<S> NetworkIndexerTrait for NetworkIndexer<S>
where
    S: Store + ChainStore,
{
    fn into_polling_stream(self) -> Box<dyn Future<Item = (), Error = ()> + Send> {
        info!(self.logger, "Start network indexer");

        let logger_for_err = self.logger.clone();

        Box::new(
            self.ensure_subgraph_exists()
                .and_then(|indexer| {
                    loop_fn(indexer, |indexer| {
                        indexer
                            .index_next_blocks()
                            .map(|indexer| Loop::Continue(indexer))
                    })
                })
                .map_err(move |e| {
                    error!(
                      logger_for_err,
                      "Failed to index Ethereum network";
                      "error" => format!("{}", e)
                    )
                }),
        )
    }
}
