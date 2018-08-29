use futures::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Mutex;

use graph::components::subgraph::RuntimeHostEvent;
use graph::components::subgraph::SubgraphProviderEvent;
use graph::prelude::*;

pub struct RuntimeManager {
    logger: Logger,
    input: Sender<SubgraphProviderEvent>,
}

impl RuntimeManager where {
    /// Creates a new runtime manager.
    pub fn new<S, T>(logger: &Logger, store: Arc<Mutex<S>>, host_builder: T) -> Self
    where
        S: Store + 'static,
        T: RuntimeHostBuilder,
    {
        let logger = logger.new(o!("component" => "RuntimeManager"));

        // Create channel for receiving subgraph provider events.
        let (subgraph_sender, subgraph_receiver) = channel(100);

        // Handle incoming events from the subgraph provider.
        Self::handle_subgraph_events(logger.clone(), store, host_builder, subgraph_receiver);

        RuntimeManager {
            logger,
            input: subgraph_sender,
        }
    }

    /// Handle incoming events from subgraph providers.
    fn handle_subgraph_events<S, T>(
        logger: Logger,
        store: Arc<Mutex<S>>,
        mut host_builder: T,
        receiver: Receiver<SubgraphProviderEvent>,
    ) where
        S: Store + 'static,
        T: RuntimeHostBuilder,
    {
        // Handles each incoming event from the subgraph.
        fn handle_event<S: Store + 'static>(store: Arc<Mutex<S>>, event: RuntimeHostEvent) {
            match event {
                RuntimeHostEvent::EntitySet(store_key, entity, event_source) => {
                    let store = store.lock().unwrap();
                    // TODO this code is incorrect. One TX should be used for entire block.
                    // TODO ensure necessary blocks are in DB
                    let EventSource::EthereumBlock(block_hash) = event_source;
                    let mut tx = store
                        .begin_transaction(
                            SubgraphId(store_key.subgraph.clone()),
                            store
                                .block(block_hash)
                                .expect("failed to load block from block store")
                                .expect(&format!(
                                    "block {:?} missing from block store",
                                    block_hash
                                )),
                        )
                        .unwrap();
                    tx.set(store_key, entity)
                        .expect("Failed to set entity in the store");
                    tx.commit().unwrap();
                }
                RuntimeHostEvent::EntityRemoved(store_key, event_source) => {
                    let store = store.lock().unwrap();
                    // TODO this code is incorrect. One TX should be used for entire block.
                    // TODO ensure necessary blocks are in DB
                    let EventSource::EthereumBlock(block_hash) = event_source;
                    let mut tx = store
                        .begin_transaction(
                            SubgraphId(store_key.subgraph.clone()),
                            store.block(block_hash).unwrap().unwrap(),
                        )
                        .unwrap();
                    tx.delete(store_key)
                        .expect("Failed to delete entity from the store");
                    tx.commit().unwrap();
                }
            }
        }

        let mut runtime_hosts = vec![];

        tokio::spawn(receiver.for_each(move |event| {
            match event {
                SubgraphProviderEvent::SubgraphAdded(manifest) => {
                    info!(logger, "Host mapping runtimes for subgraph";
                          "location" => &manifest.location);

                    // Create a new runtime host for each data source in the subgraph manifest
                    let mut new_hosts = manifest
                        .data_sources
                        .iter()
                        .map(|d| host_builder.build(manifest.clone(), d.clone()));

                    // Forward events from the runtime host to the store; this
                    // Tokio task will terminate when the corresponding subgraph
                    // is removed and the host and its event sender are dropped
                    for mut new_host in new_hosts {
                        let store = store.clone();
                        tokio::spawn(new_host.take_event_stream().unwrap().for_each(
                            move |event| {
                                handle_event(store.clone(), event);
                                Ok(())
                            },
                        ));
                        // Add the new host to the list of managed runtime hosts
                        runtime_hosts.push(new_host);
                    }
                }
                SubgraphProviderEvent::SubgraphRemoved(id) => {
                    // Destroy all runtime hosts for this subgraph; this will
                    // also terminate the host's event stream
                    runtime_hosts.retain(|host| host.subgraph_manifest().id != id);
                }
            }

            Ok(())
        }));
    }
}

impl EventConsumer<SubgraphProviderEvent> for RuntimeManager {
    /// Get the wrapped event sink.
    fn event_sink(&self) -> Box<Sink<SinkItem = SubgraphProviderEvent, SinkError = ()> + Send> {
        let logger = self.logger.clone();
        Box::new(self.input.clone().sink_map_err(move |e| {
            error!(logger, "Component was dropped: {}", e);
        }))
    }
}
