use futures::sync::mpsc::{channel, Receiver, Sender};

use graph::prelude::*;

use super::BlockWriter;
use super::NetworkTracerEvent;

pub struct NetworkIndexer<S> {
    logger: Logger,
    store: Arc<S>,
    input: Sender<NetworkTracerEvent>,
}

impl<S> NetworkIndexer<S>
where
    S: Store + ChainStore,
{
    pub fn new(
        subgraph_id: SubgraphDeploymentId,
        logger: &Logger,
        store: Arc<S>,
        metrics_registry: Arc<dyn MetricsRegistry>,
    ) -> Self {
        let logger = logger.new(o!("component" => "NetworkIndexer"));

        // Create a channel for incoming events.
        let (input, event_stream) = channel(100);

        let indexer = Self {
            logger,
            store,
            input,
        };

        // Process incoming events in the background.
        indexer.process_events(subgraph_id, metrics_registry, event_stream);

        indexer
    }

    fn process_events(
        &self,
        subgraph_id: SubgraphDeploymentId,
        metrics_registry: Arc<dyn MetricsRegistry>,
        event_stream: Receiver<NetworkTracerEvent>,
    ) {
        let logger = self.logger.clone();

        // We use a block writer to write blocks and other entities
        // to the store.
        let block_writer = Arc::new(BlockWriter::new(
            subgraph_id,
            &self.logger,
            self.store.clone(),
            metrics_registry.clone(),
        ));

        tokio::spawn(event_stream.for_each(move |event| {
            let logger = logger.clone();
            let block_writer = block_writer.clone();

            match event {
                // Handle reverts by rolling the network data back to
                // the block prior to the reorg (called the "fork base"
                // in other parts of the code).
                NetworkTracerEvent::RevertTo { .. } => {
                    unimplemented!("Block reversion is not implemented yet");
                }

                // Write any incoming blocks straight to the store.
                // By the time these blocks arrive here, they have been
                // checked for missing data and are guaranteed to represent
                // a continuous, gapless sequence of blocks.
                NetworkTracerEvent::AddBlocks { blocks } => stream::iter_ok::<_, Error>(blocks)
                    .for_each(move |block| block_writer.clone().write(block))
                    .map_err(move |e: Error| {
                        error!(
                            logger,
                            "Network indexer failed";
                            "error" => format!("{:?}", e)
                        );
                    }),
            }
        }));
    }
}

impl<S> EventConsumer<NetworkTracerEvent> for NetworkIndexer<S>
where
    S: Store + ChainStore,
{
    fn event_sink(
        &self,
    ) -> Box<(dyn Sink<SinkItem = NetworkTracerEvent, SinkError = ()> + Send + 'static)> {
        let logger = self.logger.clone();
        Box::new(self.input.clone().sink_map_err(move |e| {
            error!(logger, "Component was dropped: {}", e);
        }))
    }
}
