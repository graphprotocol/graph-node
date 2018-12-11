use futures::sync::mpsc::{channel, Receiver, Sender};
use std::collections::HashSet;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use graph::data::subgraph::schema::{SubgraphEntity, SUBGRAPHS_ID};
use graph::prelude::{SubgraphProvider as SubgraphProviderTrait, *};

pub struct SubgraphProvider<L, S> {
    logger: Logger,
    event_stream: Option<Receiver<SubgraphProviderEvent>>,
    event_sink: Sender<SubgraphProviderEvent>,
    resolver: Arc<L>,
    subgraphs_running: Arc<Mutex<HashSet<SubgraphId>>>,
    store: Arc<S>,
}

impl<L, S> SubgraphProvider<L, S>
where
    L: LinkResolver,
    S: Store,
{
    pub fn new(logger: Logger, resolver: Arc<L>, store: Arc<S>) -> Self {
        let (event_sink, event_stream) = channel(100);

        // Create the subgraph provider
        SubgraphProvider {
            logger: logger.new(o!("component" => "SubgraphProvider")),
            event_stream: Some(event_stream),
            event_sink,
            resolver,
            subgraphs_running: Arc::new(Mutex::new(HashSet::new())),
            store,
        }
    }

    /// Clones but forcing receivers to `None`.
    fn clone(&self) -> Self {
        SubgraphProvider {
            logger: self.logger.clone(),
            event_stream: None,
            event_sink: self.event_sink.clone(),
            resolver: self.resolver.clone(),
            subgraphs_running: self.subgraphs_running.clone(),
            store: self.store.clone(),
        }
    }
}

impl<L, S> SubgraphProviderTrait for SubgraphProvider<L, S>
where
    L: LinkResolver,
    S: Store,
{
    fn start(
        &self,
        id: SubgraphId,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static> {
        let self_clone = self.clone();

        let link = format!("/ipfs/{}", id);

        Box::new(
            SubgraphManifest::resolve(Link { link }, self.resolver.clone())
                .map_err(SubgraphProviderError::ResolveError)
                .and_then(move |subgraph| -> Box<Future<Item = _, Error = _> + Send> {
                    // If subgraph ID already in set
                    if !self_clone
                        .subgraphs_running
                        .lock()
                        .unwrap()
                        .insert(subgraph.id.clone())
                    {
                        return Box::new(future::err(SubgraphProviderError::AlreadyRunning(
                            subgraph.id,
                        )));
                    }

                    // Place subgraph info into store
                    let created_at = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    let entity_ops = SubgraphEntity::new(&subgraph, created_at).write_operations();
                    self_clone
                        .store
                        .apply_entity_operations(entity_ops, EventSource::None)
                        .map_err(|err| {
                            error!(
                                self_clone.logger,
                                "Failed to write subgraph to store: {}", err
                            )
                        })
                        .ok();

                    // Send events to trigger subgraph processing
                    Box::new(
                        self_clone
                            .event_sink
                            .clone()
                            .send(SubgraphProviderEvent::SubgraphStart(subgraph))
                            .map_err(|e| panic!("failed to forward subgraph: {}", e))
                            .map(|_| ()),
                    )
                }),
        )
    }

    fn stop(
        &self,
        id: SubgraphId,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static> {
        // If subgraph ID was in set
        if self.subgraphs_running.lock().unwrap().remove(&id) {
            // Shut down subgraph processing
            Box::new(
                self.event_sink
                    .clone()
                    .send(SubgraphProviderEvent::SubgraphStop(id))
                    .map_err(|e| panic!("failed to forward subgraph shut down event: {}", e))
                    .map(|_| ()),
            )
        } else {
            Box::new(future::err(SubgraphProviderError::NotRunning(id)))
        }
    }
}

impl<L, S> EventProducer<SubgraphProviderEvent> for SubgraphProvider<L, S> {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<Stream<Item = SubgraphProviderEvent, Error = ()> + Send>> {
        self.event_stream
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = SubgraphProviderEvent, Error = ()> + Send>)
    }
}
