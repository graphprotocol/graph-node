use std::collections::BTreeMap;
use std::sync::Mutex;

use futures::sync::mpsc::{channel, Receiver, Sender};

use graph::prelude::{SubgraphProvider as SubgraphProviderTrait, *};
use graph_graphql::prelude::validate_schema;

pub struct SubgraphProvider<L> {
    _logger: slog::Logger,
    event_stream: Option<Receiver<SubgraphProviderEvent>>,
    event_sink: Sender<SubgraphProviderEvent>,
    schema_event_stream: Option<Receiver<SchemaEvent>>,
    schema_event_sink: Sender<SchemaEvent>,
    resolver: Arc<L>,
    // Maps subgraph name to id.
    subgraphs: Arc<Mutex<BTreeMap<String, String>>>,
}

impl<L: LinkResolver> SubgraphProvider<L> {
    pub fn new(logger: slog::Logger, resolver: Arc<L>) -> Self {
        let (schema_event_sink, schema_event_stream) = channel(100);
        let (event_sink, event_stream) = channel(100);

        // Create the subgraph provider
        let provider = SubgraphProvider {
            _logger: logger.new(o!("component" => "SubgraphProvider")),
            event_stream: Some(event_stream),
            event_sink,
            schema_event_stream: Some(schema_event_stream),
            schema_event_sink,
            resolver,
            subgraphs: Arc::new(Mutex::new(BTreeMap::new())),
        };

        provider
    }

    fn send_remove_events(
        &self,
        name: String,
        id: String,
    ) -> impl Future<Item = (), Error = SubgraphProviderError> + Send + 'static {
        self.schema_event_sink
            .clone()
            .send(SchemaEvent::SchemaRemoved(name, id.clone()))
            .map_err(|e| panic!("failed to forward schema removal: {}", e))
            .map(|_| ())
            .join(
                self.event_sink
                    .clone()
                    .send(SubgraphProviderEvent::SubgraphRemoved(id))
                    .map(|_| ())
                    .map_err(|e| panic!("failed to forward subgraph removal: {}", e)),
            ).map(|_| ())
    }

    /// Clones but forcing receivers to `None`.
    fn clone(&self) -> Self {
        SubgraphProvider {
            _logger: self._logger.clone(),
            event_stream: None,
            event_sink: self.event_sink.clone(),
            schema_event_stream: None,
            schema_event_sink: self.schema_event_sink.clone(),
            resolver: self.resolver.clone(),
            subgraphs: self.subgraphs.clone(),
        }
    }
}

impl<L: LinkResolver> SubgraphProviderTrait for SubgraphProvider<L> {
    fn deploy(
        &self,
        name: String,
        link: String,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static> {
        // Check that the name contains only allowed characters.
        if !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Box::new(Err(SubgraphProviderError::InvalidName(name)).into_future());
        }

        let subgraph_added_name = name.clone();

        let self_clone = self.clone();
        Box::new(
            SubgraphManifest::resolve(name.clone(), Link { link }, self_clone.resolver.clone())
                .map_err(SubgraphProviderError::ResolveError)
                .and_then(
                    // Validate the subgraph schema before deploying the subgraph
                    |subgraph| match validate_schema(&subgraph.schema.document) {
                        Err(e) => Err(SubgraphProviderError::SchemaValidationError(e)),
                        _ => Ok(subgraph),
                    },
                ).and_then(move |mut subgraph| {
                    subgraph
                        .schema
                        .add_subgraph_id_directives(subgraph.id.clone());

                    let old_id = self_clone
                        .subgraphs
                        .lock()
                        .unwrap()
                        .insert(name.clone(), subgraph.id.clone());

                    // If a subgraph is being updated, remove the old subgraph.
                    if let Some(id) = old_id {
                        Box::new(self_clone.send_remove_events(name, id))
                    } else {
                        Box::new(future::ok(()))
                            as Box<Future<Item = _, Error = _> + Send + 'static>
                    }.and_then(move |_| {
                        // Push the subgraph and the schema into their streams
                        self_clone
                            .schema_event_sink
                            .clone()
                            .send(SchemaEvent::SchemaAdded(subgraph.schema.clone()))
                            .map_err(|e| panic!("failed to forward subgraph schema: {}", e))
                            .join(
                                self_clone
                                    .event_sink
                                    .clone()
                                    .send(SubgraphProviderEvent::SubgraphAdded(
                                        subgraph_added_name,
                                        subgraph,
                                    )).map_err(|e| panic!("failed to forward subgraph: {}", e)),
                            ).map(|_| ())
                    })
                }),
        )
    }

    /// Remove the subgraph and signal the removal to the graphql server and the
    /// runtime manager. Error if the subgraph is not present.
    fn remove(
        &self,
        name: String,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static> {
        match self.subgraphs.lock().unwrap().remove(&name) {
            Some(id) => Box::new(self.send_remove_events(name, id)),
            None => Box::new(future::err(SubgraphProviderError::NotFound(name))),
        }
    }

    fn list(&self) -> Vec<(String, String)> {
        self.subgraphs
            .lock()
            .unwrap()
            .iter()
            .map(|(name, id)| (name.clone(), id.clone()))
            .collect()
    }
}

impl<L> EventProducer<SubgraphProviderEvent> for SubgraphProvider<L> {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<Stream<Item = SubgraphProviderEvent, Error = ()> + Send>> {
        self.event_stream
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = SubgraphProviderEvent, Error = ()> + Send>)
    }
}

impl<L> EventProducer<SchemaEvent> for SubgraphProvider<L> {
    fn take_event_stream(&mut self) -> Option<Box<Stream<Item = SchemaEvent, Error = ()> + Send>> {
        self.schema_event_stream
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = SchemaEvent, Error = ()> + Send>)
    }
}

#[test]
fn rejects_name_bad_for_urls() {
    extern crate failure;

    struct FakeLinkResolver;

    impl LinkResolver for FakeLinkResolver {
        fn cat(&self, _: &Link) -> Box<Future<Item = Vec<u8>, Error = failure::Error> + Send> {
            unimplemented!()
        }
    }
    let logger = slog::Logger::root(slog::Discard, o!());
    let provider = Arc::new(SubgraphProvider::new(logger, Arc::new(FakeLinkResolver)));
    let bad = "/../funky%2F:9001".to_owned();
    let result = provider.deploy(bad.clone(), "".to_owned());
    match result.wait() {
        Err(SubgraphProviderError::InvalidName(name)) => assert_eq!(name, bad),
        x => panic!("unexpected test result {:?}", x),
    }
}
