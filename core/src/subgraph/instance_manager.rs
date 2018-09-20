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

        // Request a block stream for this subgraph
        let (block_stream, block_stream_controller) = block_stream_builder.from_subgraph(&manifest);

        // Wrap the stream controller so we can move ownership easier
        let block_stream_controller = Arc::new(block_stream_controller);

        // Load the subgraph
        let instance = Arc::new(SubgraphInstance::from_manifest(manifest, host_builder));

        // Prepare loggers for different parts of the async processing
        let block_logger = logger.clone();
        let stream_err_logger = logger.clone();
        let shutdown_logger = logger.clone();
        let process_err_logger = logger.clone();

        // Use a oneshot channel for shutting down the subgraph instance and block stream
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();

        // Forward block stream events to the subgraph for processing
        tokio::spawn(
            block_stream
                .map(Some)
                .map_err(move |e| {
                    warn!(stream_err_logger, "Block stream error: {}", e);
                }).select(
                    shutdown_receiver
                        .into_stream()
                        .map(|_| None)
                        .map_err(move |_| {
                            info!(shutdown_logger, "Subgraph shut down");
                        }),
                ).filter_map(|block| block)
                .and_then(move |block| {
                    info!(block_logger, "Process events from block";
                          "block_number" => format!("{:?}", block.block.number),
                          "block_hash" => format!("{:?}", block.block.hash));

                    // Translate block logs into Ethereum events
                    let events: Vec<_> = block
                        .logs
                        .iter()
                        .filter_map(|log| instance.parse_log(&block.block, log).ok())
                        .collect();

                    info!(
                        block_logger,
                        "{} events are relevant for this subgraph",
                        events.len()
                    );

                    // Prepare ownership for async closures
                    let instance = instance.clone();
                    let process_err_logger = process_err_logger.clone();

                    // Process events one after the other, passing in entity operations
                    // collected previously to every new event being processed
                    stream::iter_ok::<_, ()>(events)
                        .fold(vec![], move |mut entity_operations, event| {
                            let process_err_logger = process_err_logger.clone();

                            // TODO: Pass in the block, the event's transaction and collected
                            // entity operations so far; the block and transaction should
                            // probably become part of `EthereumEvent` and be added in
                            // `instance.parse_log()` earlier.
                            instance
                                .process_event(event)
                                .map(|ops| {
                                    entity_operations.extend(ops.into_iter());
                                    entity_operations
                                }).map_err(move |e| {
                                    error!(process_err_logger, "Failed to process event: {}", e);
                                })
                        }).map(|operations| (block, operations))
                }).for_each(move |(block, entity_operations)| {
                    let block_stream_controller = block_stream_controller.clone();
                    let block_hash = block.block.hash.expect("encountered block without hash");

                    // Transact entities into the store; if that succeeds, advance the
                    // block stream
                    future::result(store.transact(entity_operations))
                        .and_then(move |_| block_stream_controller.advance(block_hash))
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
