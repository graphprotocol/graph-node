use failure::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use futures::sync::oneshot;
use std::collections::HashMap;
use std::sync::RwLock;

use graph::components::subgraph::SubgraphProviderEvent;
use graph::prelude::{SubgraphInstance as SubgraphInstanceTrait, *};

use super::SubgraphInstance;

type InstanceShutdownMap = Arc<RwLock<HashMap<SubgraphId, oneshot::Sender<()>>>>;

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
        S: Store + 'static,
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
        S: Store + 'static,
        T: RuntimeHostBuilder + 'static,
        B: BlockStreamBuilder + 'static,
    {
        // Subgraph instance shutdown senders
        let instances: InstanceShutdownMap = Default::default();

        tokio::spawn(receiver.for_each(move |event| {
            use self::SubgraphProviderEvent::*;

            match event {
                SubgraphAdded(manifest) => {
                    info!(logger, "Subgraph added"; "id" => &manifest.id);
                    Self::handle_subgraph_added(
                        logger.clone(),
                        instances.clone(),
                        host_builder.clone(),
                        block_stream_builder.clone(),
                        store.clone(),
                        manifest,
                    )
                }
                SubgraphRemoved(id) => {
                    info!(logger, "Subgraph removed"; "id" => &id);
                    Self::handle_subgraph_removed(instances.clone(), id);
                }
            };

            Ok(())
        }));
    }

    fn handle_subgraph_added<B, T, S>(
        logger: Logger,
        instances: InstanceShutdownMap,
        host_builder: T,
        block_stream_builder: B,
        store: Arc<S>,
        manifest: SubgraphManifest,
    ) where
        T: RuntimeHostBuilder,
        B: BlockStreamBuilder,
        S: Store + 'static,
    {
        let id = manifest.id.clone();
        let id_for_transact = manifest.id.clone();

        // Request a block stream for this subgraph
        let (block_stream, _) = block_stream_builder.from_subgraph(&manifest);

        // Load the subgraph
        let instance = Arc::new(SubgraphInstance::from_manifest(manifest, host_builder));

        // Prepare loggers for different parts of the async processing
        let block_logger = logger.clone();
        let error_logger = logger.clone();

        // Use a oneshot channel for shutting down the subgraph instance and block stream
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();

        // Forward block stream events to the subgraph for processing
        tokio::spawn(
            block_stream
                .map(Some)
                .map_err(|e| format_err!("Block stream error: {}", e))
                .select(
                    shutdown_receiver
                        .into_stream()
                        .map(|_| None)
                        .map_err(|e| format_err!("Subgraph shut down: {}", e)),
                ).filter_map(|block| block)
                .and_then(move |block| {
                    info!(block_logger, "Process events from block";
                          "block_number" => format!("{:?}", block.block.number),
                          "block_hash" => format!("{:?}", block.block.hash));

                    // Extract logs relevant to the subgraph
                    let logs: Vec<_> = block
                        .logs
                        .iter()
                        .filter(|log| instance.matches_log(&log))
                        .cloned()
                        .collect();

                    info!(
                        block_logger,
                        "{} events are relevant for this subgraph",
                        logs.len()
                    );

                    // Prepare ownership for async closures
                    let instance = instance.clone();
                    let block = Arc::new(block);
                    let block_forward = block.clone();

                    // Process events one after the other, passing in entity operations
                    // collected previously to every new event being processed
                    stream::iter_ok::<_, Error>(logs)
                        .fold(vec![], move |entity_operations, log| {
                            let instance = instance.clone();
                            let block_for_processing = block.clone();

                            let transaction = block
                                .transaction_for_log(&log)
                                .map(Arc::new)
                                .ok_or(format_err!("Found no transaction for event"));

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
                    let block_ptr_now = EthereumBlockPointer::to_parent(&block.block);
                    let block_ptr_after = EthereumBlockPointer::from(&block.block);

                    // Transact entity operations into the store and update the
                    // subgraph's block stream pointer
                    future::result(store.transact_block_operations(
                        &id_for_transact,
                        block_ptr_now,
                        block_ptr_after,
                        entity_operations,
                    )).map_err(|e| {
                        format_err!("Error while processing block stream for a subgraph: {}", e)
                    })
                }).map_err(move |e| {
                    error!(error_logger, "Subgraph instance failed to run: {}", e);
                }),
        );

        // Remember the shutdown sender to shut down the subgraph instance later
        instances.write().unwrap().insert(id, shutdown_sender);
    }

    fn handle_subgraph_removed(instances: InstanceShutdownMap, id: SubgraphId) {
        // Drop the shutdown sender to shut down the subgraph now
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
