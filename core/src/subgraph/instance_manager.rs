use failure::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use std::collections::HashMap;
use std::sync::RwLock;

use graph::components::subgraph::SubgraphProviderEvent;
use graph::prelude::{SubgraphInstance as SubgraphInstanceTrait, *};

use super::SubgraphInstance;
use elastic_logger;
use split_logger;
use ElasticDrainConfig;
use ElasticLoggingConfig;

type InstanceShutdownMap = Arc<RwLock<HashMap<SubgraphId, CancelGuard>>>;

pub struct SubgraphInstanceManager {
    logger: Logger,
    input: Sender<SubgraphProviderEvent>,
}

impl SubgraphInstanceManager {
    /// Creates a new runtime manager.
    pub fn new<B, S, T>(
        logger: &Logger,
        store: Arc<S>,
        host_builder: T,
        block_stream_builder: B,
        elastic_config: Option<ElasticLoggingConfig>,
    ) -> Self
    where
        S: Store + ChainStore,
        T: RuntimeHostBuilder,
        B: BlockStreamBuilder + 'static,
    {
        let logger = logger.new(o!("component" => "SubgraphInstanceManager"));

        // Create channel for receiving subgraph provider events.
        let (subgraph_sender, subgraph_receiver) = channel(100);

        // Handle incoming events from the subgraph provider.
        Self::handle_subgraph_events(
            logger.clone(),
            subgraph_receiver,
            store,
            host_builder,
            block_stream_builder,
            elastic_config,
        );

        SubgraphInstanceManager {
            logger,
            input: subgraph_sender,
        }
    }

    /// Handle incoming events from subgraph providers.
    fn handle_subgraph_events<B, S, T>(
        logger: Logger,
        receiver: Receiver<SubgraphProviderEvent>,
        store: Arc<S>,
        host_builder: T,
        block_stream_builder: B,
        elastic_config: Option<ElasticLoggingConfig>,
    ) where
        S: Store + ChainStore,
        T: RuntimeHostBuilder,
        B: BlockStreamBuilder + 'static,
    {
        // Subgraph instance shutdown senders
        let instances: InstanceShutdownMap = Default::default();

        tokio::spawn(receiver.for_each(move |event| {
            use self::SubgraphProviderEvent::*;

            match event {
                SubgraphStart(manifest) => {
                    let term_logger = logger.new(o!("subgraph_id" => manifest.id.clone()));
                    let logger = elastic_config
                        .clone()
                        .map(|elastic_config| {
                            split_logger(
                                term_logger.clone(),
                                elastic_logger(ElasticDrainConfig {
                                    general: elastic_config,
                                    index: String::from("subgraph-logs"),
                                    document_type: String::from("log"),
                                    subgraph_id: String::from(manifest.id.clone()),
                                }),
                            )
                        }).unwrap_or(term_logger);

                    info!(logger, "Start subgraph");

                    Self::start_subgraph(
                        logger,
                        instances.clone(),
                        host_builder.clone(),
                        block_stream_builder.clone(),
                        store.clone(),
                        manifest,
                    )
                }
                SubgraphStop(id) => {
                    info!(logger, "Stopping subgraph"; "subgraph_id" => &id);
                    Self::stop_subgraph(instances.clone(), id);
                }
            };

            Ok(())
        }));
    }

    fn start_subgraph<B, T, S>(
        logger: Logger,
        instances: InstanceShutdownMap,
        host_builder: T,
        block_stream_builder: B,
        store: Arc<S>,
        manifest: SubgraphManifest,
    ) where
        T: RuntimeHostBuilder,
        B: BlockStreamBuilder,
        S: Store + ChainStore,
    {
        let id = manifest.id.clone();
        let id_for_block = manifest.id.clone();
        let id_for_err = manifest.id.clone();

        // Request a block stream for this subgraph
        let block_stream_canceler = CancelGuard::new();
        let block_stream = block_stream_builder
            .from_subgraph(&manifest, logger.clone())
            .from_err()
            .cancelable(&block_stream_canceler, || CancelableError::Cancel);

        // Load the subgraph
        let instance = Arc::new(SubgraphInstance::from_manifest(
            &logger,
            manifest,
            host_builder,
        ));

        // Prepare loggers for different parts of the async processing
        let block_logger = logger.clone();
        let error_logger = logger.clone();

        // Forward block stream events to the subgraph for processing
        tokio::spawn(
            block_stream
                .for_each(move |block| {
                    let id = id_for_block.clone();
                    let instance = instance.clone();
                    let store = store.clone();
                    let logger = block_logger.new(o!(
                        "block_number" => format!("{:?}", block.block.number.unwrap()),
                        "block_hash" => format!("{:?}", block.block.hash.unwrap())
                    ));

                    info!(logger, "Processing events from block");

                    // Extract logs relevant to the subgraph
                    let logs: Vec<_> = block
                        .transaction_receipts
                        .iter()
                        .flat_map(|receipt| {
                            receipt.logs.iter().filter(|log| instance.matches_log(&log))
                        }).cloned()
                        .collect();

                    if logs.len() == 0 {
                        info!(logger, "No events found for this subgraph");
                    } else if logs.len() == 1 {
                        info!(logger, "1 event found for this subgraph");
                    } else {
                        info!(logger, "{} events found for this subgraph", logs.len());
                    }

                    // Process events one after the other, passing in entity operations
                    // collected previously to every new event being processed
                    let block_for_process = Arc::new(block);
                    let block_for_transact = block_for_process.clone();
                    let logger_for_process = logger;
                    let logger_for_transact = logger_for_process.clone();
                    stream::iter_ok::<_, CancelableError<Error>>(logs)
                        .fold(vec![], move |entity_operations, log| {
                            let logger = logger_for_process.clone();
                            let instance = instance.clone();
                            let block = block_for_process.clone();

                            let transaction = block
                                .transaction_for_log(&log)
                                .map(Arc::new)
                                .ok_or_else(|| format_err!("Found no transaction for event"));

                            future::result(transaction).and_then(move |transaction| {
                                instance
                                    .process_log(
                                        &logger,
                                        block,
                                        transaction,
                                        log,
                                        entity_operations,
                                    ).map_err(|e| format_err!("Failed to process event: {}", e))
                            })
                        }).and_then(move |entity_operations| {
                            let block = block_for_transact.clone();
                            let logger = logger_for_transact.clone();

                            let block_ptr_now = EthereumBlockPointer::to_parent(&block);
                            let block_ptr_after = EthereumBlockPointer::from(&*block);

                            info!(logger, "Applying {} entity operation(s)", entity_operations.len());

                            // Transact entity operations into the store and update the
                            // subgraph's block stream pointer
                            future::result(store.transact_block_operations(
                                id.clone(),
                                block_ptr_now,
                                block_ptr_after,
                                entity_operations,
                            )).map_err(|e| {
                                format_err!("Error while processing block stream for a subgraph: {}", e)
                            }).from_err()
                        })
                }).map_err(move |e| {
                    match e {
                        CancelableError::Cancel => {
                            info!(error_logger, "Subgraph block stream shut down cleanly"; "id" => id_for_err);
                        }
                        CancelableError::Error(e) => {
                            error!(error_logger, "Subgraph instance failed to run: {}", e; "id" => id_for_err);
                        }
                    }
                }),
        );

        // Keep the cancel guard for shutting down the subgraph instance later
        instances.write().unwrap().insert(id, block_stream_canceler);
    }

    fn stop_subgraph(instances: InstanceShutdownMap, id: SubgraphId) {
        // Drop the cancel guard to shut down the subgraph now
        let mut instances = instances.write().unwrap();
        instances.remove(&id);
    }
}

impl EventConsumer<SubgraphProviderEvent> for SubgraphInstanceManager {
    /// Get the wrapped event sink.
    fn event_sink(&self) -> Box<Sink<SinkItem = SubgraphProviderEvent, SinkError = ()> + Send> {
        let logger = self.logger.clone();
        Box::new(self.input.clone().sink_map_err(move |e| {
            error!(logger, "Component was dropped: {}", e);
        }))
    }
}
