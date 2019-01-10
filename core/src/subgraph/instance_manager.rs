use futures::sync::mpsc::{channel, Receiver, Sender};
use graph::data::subgraph::schema::SubgraphDeploymentEntity;
use graph::prelude::{SubgraphInstance as SubgraphInstanceTrait, *};
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;

use super::SubgraphInstance;
use elastic_logger;
use split_logger;
use ElasticDrainConfig;
use ElasticLoggingConfig;

type InstanceShutdownMap = Arc<RwLock<HashMap<SubgraphDeploymentId, CancelGuard>>>;

pub struct SubgraphInstanceManager {
    logger: Logger,
    input: Sender<SubgraphAssignmentProviderEvent>,
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
        receiver: Receiver<SubgraphAssignmentProviderEvent>,
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
            use self::SubgraphAssignmentProviderEvent::*;

            match event {
                SubgraphStart(manifest) => {
                    // Write subgraph logs to the terminal and, if enabled, Elasticsearch
                    let term_logger = logger.new(o!("subgraph_id" => manifest.id.to_string()));
                    let logger = elastic_config
                        .clone()
                        .map(|elastic_config| {
                            split_logger(
                                term_logger.clone(),
                                elastic_logger(
                                    ElasticDrainConfig {
                                        general: elastic_config,
                                        index: String::from("subgraph-logs"),
                                        document_type: String::from("log"),
                                        subgraph_id: manifest.id.clone(),
                                        flush_interval: Duration::from_secs(5),
                                    },
                                    term_logger.clone(),
                                ),
                            )
                        })
                        .unwrap_or(term_logger);

                    info!(logger, "Start subgraph");

                    Self::start_subgraph(
                        logger.clone(),
                        instances.clone(),
                        host_builder.clone(),
                        block_stream_builder.clone(),
                        store.clone(),
                        manifest,
                    )
                    .map_err(|err| error!(logger, "Failed to start subgraph: {}", err))
                    .ok();
                }
                SubgraphStop(id) => {
                    info!(logger, "Stopping subgraph"; "subgraph_id" => id.to_string());
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
    ) -> Result<(), Error>
    where
        T: RuntimeHostBuilder,
        B: BlockStreamBuilder,
        S: Store + ChainStore,
    {
        let id = manifest.id.clone();
        let id_for_block = manifest.id.clone();
        let id_for_err = manifest.id.clone();
        let store_for_events = store.clone();
        let store_for_errors = store.clone();

        // Request a block stream for this subgraph
        let block_stream_canceler = CancelGuard::new();
        let block_stream_cancel_handle = block_stream_canceler.handle();
        let block_stream = block_stream_builder
            .from_subgraph(&manifest, logger.clone())
            .from_err()
            .cancelable(&block_stream_canceler, || CancelableError::Cancel);

        // Load the subgraph
        let instance = Arc::new(SubgraphInstance::from_manifest(
            &logger,
            manifest,
            host_builder,
        )?);

        // Prepare loggers for different parts of the async processing
        let block_logger = logger.clone();
        let error_logger = logger.clone();

        // Forward block stream events to the subgraph for processing
        tokio::spawn(
            block_stream
                .for_each(move |block| {
                    let id = id_for_block.clone();
                    let instance = instance.clone();
                    let store = store_for_events.clone();
                    let block_stream_cancel_handle = block_stream_cancel_handle.clone();
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
                        })
                        .cloned()
                        .collect();

                    if logs.len() == 0 {
                        info!(logger, "No events found in this block for this subgraph");
                    } else if logs.len() == 1 {
                        info!(logger, "1 event found in this block for this subgraph");
                    } else {
                        info!(
                            logger,
                            "{} events found in this block for this subgraph",
                            logs.len()
                        );
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
                                    )
                                    .map_err(|e| format_err!("Failed to process event: {}", e))
                            })
                        })
                        .and_then(move |entity_operations| {
                            let block = block_for_transact.clone();
                            let logger = logger_for_transact.clone();

                            let block_ptr_now = EthereumBlockPointer::to_parent(&block);
                            let block_ptr_after = EthereumBlockPointer::from(&*block);

                            // Avoid writing to store if block stream has been canceled
                            if block_stream_cancel_handle.is_canceled() {
                                return Err(CancelableError::Cancel);
                            }

                            info!(
                                logger,
                                "Applying {} entity operation(s)",
                                entity_operations.len()
                            );

                            // Transact entity operations into the store and update the
                            // subgraph's block stream pointer
                            store
                                .transact_block_operations(
                                    id.clone(),
                                    block_ptr_now,
                                    block_ptr_after,
                                    entity_operations,
                                )
                                .map_err(|e| {
                                    format_err!(
                                        "Error while processing block stream for a subgraph: {}",
                                        e
                                    )
                                    .into()
                                })
                        })
                })
                .map_err(move |e| match e {
                    CancelableError::Cancel => {
                        debug!(
                            error_logger,
                            "Subgraph block stream shut down cleanly";
                            "id" => id_for_err.to_string()
                        );
                    }
                    CancelableError::Error(e) => {
                        error!(
                            error_logger,
                            "Subgraph instance failed to run: {}", e;
                            "id" => id_for_err.to_string()
                        );

                        // Set subgraph status to Failed
                        let status_ops =
                            SubgraphDeploymentEntity::update_failed_operations(&id_for_err, true);
                        if let Err(e) =
                            store_for_errors.apply_entity_operations(status_ops, EventSource::None)
                        {
                            error!(
                                error_logger,
                                "Failed to set subgraph status to Failed: {}", e;
                                "id" => id_for_err.to_string()
                            );
                        }
                    }
                }),
        );

        // Keep the cancel guard for shutting down the subgraph instance later
        instances.write().unwrap().insert(id, block_stream_canceler);
        Ok(())
    }

    fn stop_subgraph(instances: InstanceShutdownMap, id: SubgraphDeploymentId) {
        // Drop the cancel guard to shut down the subgraph now
        let mut instances = instances.write().unwrap();
        instances.remove(&id);
    }
}

impl EventConsumer<SubgraphAssignmentProviderEvent> for SubgraphInstanceManager {
    /// Get the wrapped event sink.
    fn event_sink(
        &self,
    ) -> Box<Sink<SinkItem = SubgraphAssignmentProviderEvent, SinkError = ()> + Send> {
        let logger = self.logger.clone();
        Box::new(self.input.clone().sink_map_err(move |e| {
            error!(logger, "Component was dropped: {}", e);
        }))
    }
}
