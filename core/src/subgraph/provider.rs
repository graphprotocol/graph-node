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
        let graphql_remove_sink = self.schema_event_sink.clone();
        let host_remove_sink = self.event_sink.clone();
        graphql_remove_sink
            .send(SchemaEvent::SchemaRemoved(name, id.clone()))
            .map_err(|e| panic!("Failed to forward schema removal: {}", e))
            .map(|_| ())
            .join(
                host_remove_sink
                    .send(SubgraphProviderEvent::SubgraphRemoved(id))
                    .map(|_| ())
                    .map_err(|e| panic!("Failed to forward subgraph removal: {}", e)),
            ).map(|_| ())
    }
}

impl<L: LinkResolver> SubgraphProviderTrait for SubgraphProvider<L> {
    fn deploy(
        arc_self: &Arc<Self>,
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

        let arc_self = arc_self.clone();
        Box::new(
            SubgraphManifest::resolve(name.clone(), Link { link }, arc_self.resolver.clone())
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

                    let old_id = arc_self
                        .subgraphs
                        .lock()
                        .unwrap()
                        .insert(name.clone(), subgraph.id.clone());

                    // If a subgraph is being updated, remove the old subgraph.
                    let removal_arc_self = arc_self.clone();
                    if let Some(id) = old_id {
                        Box::new(
                            arc_self
                                .send_remove_events(name, id)
                                .map(move |_| (removal_arc_self, subgraph)),
                        )
                            as Box<Future<Item = _, Error = _> + Send + 'static>
                    } else {
                        Box::new(future::ok::<_, SubgraphProviderError>((arc_self, subgraph)))
                    }
                }).and_then(|(arc_self, subgraph)| {
                    // Push the subgraph and the schema into their streams
                    arc_self
                        .schema_event_sink
                        .clone()
                        .send(SchemaEvent::SchemaAdded(subgraph.schema.clone()))
                        .map_err(|e| panic!("Failed to forward subgraph schema: {}", e))
                        .join(
                            arc_self
                                .event_sink
                                .clone()
                                .send(SubgraphProviderEvent::SubgraphAdded(subgraph))
                                .map_err(|e| panic!("Failed to forward subgraph: {}", e)),
                        ).map(|_| ())
                }),
        )
    }

    fn remove(
        &self,
        name_or_id: String,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static> {
        let mut subgraphs = self.subgraphs.lock().unwrap();

        // Either `name_or_id` is a name,
        let name = if subgraphs.contains_key(&name_or_id) {
            name_or_id
        // or it's an id, so we get the corresponding name.
        } else if let Some(name) = subgraphs.keys().find(|&name| subgraphs[name] == name_or_id) {
            name.to_owned()
        // Otherwise the subgraph is not hosted.
        } else {
            return Box::new(future::err(SubgraphProviderError::NotFound(
                name_or_id.to_owned(),
            )));
        };

        // Remove the subgraph and signal the removal to the graphql server and
        // the runtime manager.
        let id = subgraphs.remove(&name).unwrap();
        Box::new(self.send_remove_events(name, id))
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
    let result = SubgraphProvider::deploy(&provider, bad.clone(), "".to_owned());
    match result.wait() {
        Err(SubgraphProviderError::InvalidName(name)) => assert_eq!(name, bad),
        x => panic!("unexpected test result {:?}", x),
    }
}
