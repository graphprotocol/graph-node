use futures::sync::mpsc::{channel, Receiver, Sender};
use futures::sync::oneshot;
use std::collections::HashMap;
use std::sync::{Mutex, RwLock};

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
        store: Arc<Mutex<S>>,
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
        store: Arc<Mutex<S>>,
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

    fn handle_subgraph_added<B, T>(
        logger: Logger,
        instances: InstanceShutdownMap,
        host_builder: T,
        block_stream_builder: B,
        manifest: SubgraphManifest,
    ) where
        T: RuntimeHostBuilder,
        B: BlockStreamBuilder,
    {
        let id = manifest.id.clone();

        // Request a block stream for this subgraph
        let block_stream = block_stream_builder.from_subgraph(&manifest);

        // Load the subgraph
        let instance = SubgraphInstance::from_manifest(manifest, host_builder);

        // Forward block stream events to the subgraph for processing
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let block_logger = logger.clone();
        let stream_err_logger = logger.clone();
        let shutdown_logger = logger.clone();
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
                ).map(move |block| {
                    match block {
                        Some(block) => {
                            info!(block_logger, "Process {} events from block", block.logs.len();
                                  "block_number" => format!("{:?}", block.block.number),
                                  "block_hash" => format!("{:?}", block.block.hash));

                            // TODO: Process all events in order, collect their results
                            //for async event in block.logs {
                            //    let ops = instance.process_event(event, prev_ops)
                            //}
                            //
                            //let state = {
                            //    block,
                            //    remaining_events: block.logs.clone(),
                            //    operations_so_far: vec![],
                            //};
                            //stream::unfold(state, |mut state| {
                            //    instance.process_event(event.clone(), state.operations_so_far.clone())
                            //        .and_then(move |ops| {
                            //            state.operations_so_far.extend(ops)
                            //            Ok(state)
                            //        })
                            //})

                            Some(vec![])
                        }
                        None => None,
                    }
                }).for_each(|entity_operations: Option<Vec<EntityOperation>>| {
                    //match entity_operations {
                    //    Some(ops) => {
                    //        // TODO: Transact operations into the store
                    //        // TODO: Advance the block stream (unless the block stream does
                    //        //       it by itself)
                    //    },
                    //    None => {
                    //        // Continue
                    //    }
                    //};
                    Ok(())
                }).and_then(|_| Ok(())),
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
