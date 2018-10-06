use failure::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use std::collections::HashMap;
use std::sync::RwLock;

use graph::components::subgraph::SubgraphProviderEvent;
use graph::prelude::{SubgraphInstance as SubgraphInstanceTrait, *};

use super::SubgraphInstance;

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
    ) -> Self
    where
        S: Store + ChainStore + 'static,
        T: RuntimeHostBuilder + 'static,
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
    ) where
        S: Store + ChainStore + 'static,
        T: RuntimeHostBuilder + 'static,
        B: BlockStreamBuilder + 'static,
    {
        // Subgraph instance shutdown senders
        let instances: InstanceShutdownMap = Default::default();

        tokio::spawn(receiver.for_each(move |event| {
            use self::SubgraphProviderEvent::*;

            match event {
                SubgraphStart(name, manifest) => {
                    info!(
                        logger, "Starting subgraph";
                        "subgraph_name" => &name,
                        "subgraph_id" => &manifest.id
                    );

                    Self::start_subgraph(
                        logger.clone(),
                        instances.clone(),
                        host_builder.clone(),
                        block_stream_builder.clone(),
                        store.clone(),
                        name,
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
        name: String,
        manifest: SubgraphManifest,
    ) where
        T: RuntimeHostBuilder,
        B: BlockStreamBuilder,
        S: Store + ChainStore + 'static,
    {
        let id = manifest.id.clone();
        let name_for_log = name.clone();
        let id_for_log = manifest.id.clone();
        let id_for_transact = manifest.id.clone();
        let id_for_err = manifest.id.clone();

        // Request a block stream for this subgraph
        let block_stream_canceler = CancelGuard::new();
        let block_stream = block_stream_builder
            .from_subgraph(name.clone(), &manifest, logger.clone())
            .from_err()
            .cancelable(&block_stream_canceler, || CancelableError::Cancel);

        // Load the subgraph
        let instance = Arc::new(SubgraphInstance::from_manifest(
            name,
            manifest,
            host_builder,
        ));

        // Prepare loggers for different parts of the async processing
        let block_logger = logger.clone();
        let error_logger = logger.clone();

        // Forward block stream events to the subgraph for processing
        tokio::spawn(
            block_stream
                .and_then(move |block| {
                    info!(
                        block_logger, "Process events from block";
                        "block_number" => format!("{:?}", block.block.number.unwrap()),
                        "block_hash" => format!("{:?}", block.block.hash.unwrap()),
                        "subgraph_name" => &name_for_log,
                        "subgraph_id" => &id_for_log
                    );

                    // Extract logs relevant to the subgraph
                    let logs: Vec<_> = block
                        .transaction_receipts
                        .iter()
                        .flat_map(|receipt| {
                            receipt.logs.iter().filter(|log| instance.matches_log(&log))
                        }).cloned()
                        .collect();

                    info!(
                        block_logger,
                        "{} events are relevant for this subgraph",
                        logs.len();
                        "subgraph_name" => &name_for_log,
                        "subgraph_id" => &id_for_log,
                    );

                    // Prepare ownership for async closures
                    let instance = instance.clone();
                    let block = Arc::new(block);
                    let block_forward = block.clone();

                    // Process events one after the other, passing in entity operations
                    // collected previously to every new event being processed
                    stream::iter_ok::<_, CancelableError<Error>>(logs)
                        .fold(vec![], move |entity_operations, log| {
                            let instance = instance.clone();
                            let block_for_processing = block.clone();

                            let transaction = block
                                .transaction_for_log(&log)
                                .map(Arc::new)
                                .ok_or_else(|| format_err!("Found no transaction for event"));

                            future::result(transaction).and_then(move |transaction| {
                                instance
                                    .process_log(
                                        block_for_processing,
                                        transaction,
                                        log,
                                        entity_operations,
                                    ).map_err(|e| format_err!("Failed to process event: {}", e))
                            })
                        }).map(move |operations| (block_forward, operations))
                }).for_each(move |(block, entity_operations)| {
                    let block_ptr_now = EthereumBlockPointer::to_parent(&block);
                    let block_ptr_after = EthereumBlockPointer::from(&*block);

                    // Transact entity operations into the store and update the
                    // subgraph's block stream pointer
                    future::result(store.transact_block_operations(
                        id_for_transact.clone(),
                        block_ptr_now,
                        block_ptr_after,
                        entity_operations,
                    )).map_err(|e| {
                        format_err!("Error while processing block stream for a subgraph: {}", e)
                    }).from_err()
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
