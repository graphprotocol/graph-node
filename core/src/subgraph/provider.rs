use futures::sync::mpsc::{channel, Receiver, Sender};
use graph::data::subgraph::schema::SubgraphEntity;
use graph::prelude::{SubgraphProvider as SubgraphProviderTrait, *};
use std::collections::HashSet;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct SubgraphProvider<L, S> {
    logger: slog::Logger,
    event_stream: Option<Receiver<SubgraphProviderEvent>>,
    event_sink: Sender<SubgraphProviderEvent>,
    schema_event_stream: Option<Receiver<SchemaEvent>>,
    schema_event_sink: Sender<SchemaEvent>,
    resolver: Arc<L>,
    subgraphs_running: Arc<Mutex<HashSet<SubgraphId>>>,
    store: Arc<S>,
}

impl<L, S> SubgraphProvider<L, S>
where
    L: LinkResolver,
{
    pub fn new(logger: slog::Logger, resolver: Arc<L>, store: Arc<S>) -> Self {
        let (schema_event_sink, schema_event_stream) = channel(100);
        let (event_sink, event_stream) = channel(100);

        // Create the subgraph provider
        let provider = SubgraphProvider {
            logger: logger.new(o!("component" => "SubgraphProvider")),
            event_stream: Some(event_stream),
            event_sink,
            schema_event_stream: Some(schema_event_stream),
            schema_event_sink,
            resolver,
            subgraphs_running: Arc::new(Mutex::new(HashSet::new())),
            store,
        };

        provider
    }

    fn send_add_events(&self, subgraph: SubgraphManifest) -> impl Future<Item = (), Error = Error> {
        let schema_addition = self
            .schema_event_sink
            .clone()
            .send(SchemaEvent::SchemaAdded(subgraph.schema.clone()))
            .map_err(|e| panic!("failed to forward subgraph schema: {}", e))
            .map(|_| ());

        let subgraph_start = self
            .event_sink
            .clone()
            .send(SubgraphProviderEvent::SubgraphStart(subgraph))
            .map_err(|e| panic!("failed to forward subgraph: {}", e))
            .map(|_| ());

        schema_addition.join(subgraph_start).map(|_| ())
    }

    fn send_remove_events(
        &self,
        id: String,
    ) -> impl Future<Item = (), Error = SubgraphProviderError> + Send + 'static {
        let schema_removal = self
            .schema_event_sink
            .clone()
            .send(SchemaEvent::SchemaRemoved(id.clone()))
            .map_err(|e| panic!("failed to forward schema removal: {}", e))
            .map(|_| ());

        let subgraph_stop = self
            .event_sink
            .clone()
            .send(SubgraphProviderEvent::SubgraphStop(id))
            .map_err(|e| panic!("failed to forward subgraph shut down event: {}", e))
            .map(|_| ());

        schema_removal.join(subgraph_stop).map(|_| ())
    }

    /// Clones but forcing receivers to `None`.
    fn clone(&self) -> Self {
        SubgraphProvider {
            logger: self.logger.clone(),
            event_stream: None,
            event_sink: self.event_sink.clone(),
            schema_event_stream: None,
            schema_event_sink: self.schema_event_sink.clone(),
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
                .and_then(
                    move |mut subgraph| -> Box<Future<Item = _, Error = _> + Send> {
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

                        // Add IDs into schema
                        subgraph
                            .schema
                            .add_subgraph_id_directives(subgraph.id.clone());

                        SubgraphEntity::new(
                            &subgraph,
                            SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs(),
                        ).write_to_store(&*self_clone.store)
                        .map_err(|err| {
                            error!(
                                self_clone.logger,
                                "failed to write subgraph entity to store: {}", err
                            )
                        }).ok();
                        // Send events to trigger subgraph processing
                        Box::new(self_clone.send_add_events(subgraph).from_err())
                    },
                ),
        )
    }

    fn stop(
        &self,
        id: SubgraphId,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static> {
        // If subgraph ID was in set
        if self.subgraphs_running.lock().unwrap().remove(&id) {
            // Shut down subgraph processing
            Box::new(self.send_remove_events(id))
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

impl<L, S> EventProducer<SchemaEvent> for SubgraphProvider<L, S> {
    fn take_event_stream(&mut self) -> Option<Box<Stream<Item = SchemaEvent, Error = ()> + Send>> {
        self.schema_event_stream
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = SchemaEvent, Error = ()> + Send>)
    }
}
