use futures::future::{loop_fn, Loop};
use std::sync::Arc;
use std::time::{Duration, Instant};

use graph::prelude::*;
use graph::tokio::timer::Delay;

pub struct NetworkIngestor<S, E>
where
    S: NetworkStore,
    E: EthereumAdapter,
{
    store: Arc<S>,
    eth_adapter: Arc<E>,
    network_name: String,
    logger: Logger,
}

impl<S, E> NetworkIngestor<S, E>
where
    S: NetworkStore,
    E: EthereumAdapter,
{
    pub fn new(
        store: Arc<S>,
        eth_adapter: Arc<E>,
        network_name: String,
        logger: Logger,
        elastic_config: Option<ElasticLoggingConfig>,
    ) -> Self {
        let term_logger = logger.new(o!(
            "component" => "NetworkIngestor",
            "network_name" => network_name.clone()
        ));
        let logger = elastic_config
            .clone()
            .map_or(term_logger.clone(), |elastic_config| {
                split_logger(
                    term_logger.clone(),
                    elastic_logger(
                        ElasticDrainConfig {
                            general: elastic_config,
                            index: String::from("ethereum-network-ingestor-logs"),
                            document_type: String::from("log"),
                            custom_id_key: String::from("network"),
                            custom_id_value: network_name.clone(),
                            flush_interval: Duration::from_secs(5),
                        },
                        term_logger,
                    ),
                )
            });

        Self {
            store,
            eth_adapter,
            network_name,
            logger,
        }
    }

    pub fn into_polling_stream(self) -> impl Future<Item = (), Error = ()> {
        let logger = self.logger.clone();
        let logger_for_schema_error = self.logger.clone();

        info!(logger, "Starting Ethereum network ingestion");

        // Ensure that the database schema and tables for the network exist
        self.ensure_schema()
            .map_err(move |e| {
                crit!(
                    logger_for_schema_error,
                    "Failed to ensure Ethereum network schema";
                    "error" => format!("{}", e),
                );
            })
            .and_then(|_| {
                loop_fn(self, |ingestor| {
                    println!("Ingesting next block");
                    ingestor
                        .ingest_next_block()
                        .map(|ingestor| Loop::Continue(ingestor))
                })
                .map(|_: Self| ())
                .map_err(move |e| {
                    crit!(
                        logger,
                        "Failed to index Ethereum block explorer data";
                        "error" => format!("{}", e)
                    )
                })
            })
    }

    fn ensure_schema(&self) -> impl Future<Item = (), Error = Error> {
        future::result(self.store.ensure_network_schema(self.network_name.clone()))
    }

    fn ingest_next_block(self) -> impl Future<Item = Self, Error = Error> {
        let store_for_head_ptr = self.store.clone();
        let network_name_for_head_ptr = self.network_name.clone();

        let store_for_write = self.store.clone();
        let network_name_for_write = self.network_name.clone();

        let eth_adapter_for_hash = self.eth_adapter.clone();
        let logger_for_hash = self.logger.clone();

        let eth_adapter_for_block = self.eth_adapter.clone();
        let logger_for_block = self.logger.clone();

        let eth_adapter_for_full_block = self.eth_adapter.clone();
        let logger_for_full_block = self.logger.clone();

        self.eth_adapter
            .clone()
            .latest_block(&self.logger)
            .from_err()
            .and_then(move |latest_block| {
                println!("Have a latest block");

                store_for_head_ptr
                    .clone()
                    .head_block_ptr(network_name_for_head_ptr)
                    .map(|network_head_ptr| (latest_block, network_head_ptr))
            })
            .and_then(move |(latest_block, network_head_ptr)| {
                println!(
                    "Have a network head pointer: chain = {:?}, network = {:?}",
                    latest_block.number, network_head_ptr
                );

                if network_head_ptr.is_some()
                    && latest_block.number.unwrap().low_u64()
                        <= network_head_ptr.map_or(0u64, |ptr| ptr.number)
                {
                    // Try again in 1s
                    Box::new(
                        Delay::new(Instant::now() + Duration::from_secs(1))
                            .from_err()
                            .and_then(|_| future::ok(self)),
                    ) as Box<dyn Future<Item = _, Error = _> + Send>
                } else {
                    Box::new(
                        eth_adapter_for_hash
                            .block_hash_by_block_number(
                                &logger_for_hash,
                                network_head_ptr.map_or(0u64, |ptr| ptr.number + 1),
                            )
                            .and_then(move |hash| match hash {
                                Some(hash) => Box::new(
                                    eth_adapter_for_block
                                        .block_by_hash(&logger_for_block, hash)
                                        .from_err()
                                        .and_then(move |block| match block {
                                            Some(block) => eth_adapter_for_full_block
                                                .load_full_block(&logger_for_full_block, block),
                                            None => unimplemented!(), // TODO: try again
                                        })
                                        .from_err()
                                        .and_then(move |block| {
                                            warn!(
                                                logger_for_block,
                                                "Ingesting block {} ({})",
                                                block.block.hash.unwrap(),
                                                block.block.number.unwrap()
                                            );

                                            store_for_write
                                                .write_block(network_name_for_write, block)
                                                .and_then(|_| future::ok(self))
                                        }),
                                ),
                                None => Box::new(future::ok(self))
                                    as Box<dyn Future<Item = _, Error = _> + Send>,
                            }),
                    )
                }
            })
    }
}
