use std::collections::BTreeMap;
use std::sync::Mutex;

use futures::sync::mpsc::{channel, Receiver, Sender};

use graph::data::subgraph::SubgraphProviderError;
use graph::prelude::{SubgraphProvider as SubgraphProviderTrait, *};

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
}

impl<L: LinkResolver> SubgraphProviderTrait for SubgraphProvider<L> {
    fn add(
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

        let schema_event_sink = self.schema_event_sink.clone();
        let event_sink_remove = self.event_sink.clone();
        let event_sink_add = self.event_sink.clone();
        let subgraphs = self.subgraphs.clone();
        Box::new(
            SubgraphManifest::resolve(name.clone(), Link { link }, self.resolver.clone())
                .map_err(SubgraphProviderError::ResolveError)
                .and_then(move |mut subgraph| {
                    subgraph
                        .schema
                        .add_subgraph_id_directives(subgraph.id.clone());

                    let old_id = subgraphs.lock().unwrap().insert(name, subgraph.id.clone());

                    if let Some(id) = old_id {
                        Box::new(
                            event_sink_remove
                                .send(SubgraphProviderEvent::SubgraphRemoved(id))
                                .map_err(|e| -> SubgraphProviderError {
                                    panic!("Failed to forward subgraph removal: {}", e)
                                })
                                .map(move |_| subgraph),
                        ) as Box<Future<Item = _, Error = _> + Send>
                    } else {
                        Box::new(future::ok::<_, SubgraphProviderError>(subgraph))
                    }
                })
                .and_then(move |subgraph: SubgraphManifest| {
                    // Push the subgraph and the schema into their streams
                    schema_event_sink
                        .send(SchemaEvent::SchemaAdded(subgraph.schema.clone()))
                        .map_err(|e| panic!("Failed to forward subgraph schema: {}", e))
                        .join(
                            event_sink_add
                                .send(SubgraphProviderEvent::SubgraphAdded(subgraph))
                                .map_err(|e| panic!("Failed to forward subgraph: {}", e)),
                        )
                        .map_err(|_| SubgraphProviderError::SendError)
                        .map(|_| ())
                }),
        )
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
    let provider = SubgraphProvider::new(logger, Arc::new(FakeLinkResolver));
    let bad = "/../funky%2F:9001".to_owned();
    let result = provider.add(bad.clone(), "".to_owned());
    match result.wait() {
        Err(SubgraphProviderError::InvalidName(name)) => assert_eq!(name, bad),
        x => panic!("unexpected test result {:?}", x),
    }
}
