use futures::sync::mpsc::{channel, Receiver, Sender};

use graph::data::subgraph::SubgraphProviderError;
use graph::prelude::{SubgraphProvider as SubgraphProviderTrait, *};

pub struct SubgraphProvider<L> {
    logger: slog::Logger,
    event_stream: Option<Receiver<SubgraphProviderEvent>>,
    event_sink: Sender<SubgraphProviderEvent>,
    schema_event_stream: Option<Receiver<SchemaEvent>>,
    schema_event_sink: Sender<SchemaEvent>,
    resolver: Arc<L>,
}

impl<L: LinkResolver> SubgraphProvider<L> {
    pub fn new(logger: slog::Logger, resolver: Arc<L>) -> Self {
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

        let send_logger = self.logger.clone();
        let schema_event_sink = self.schema_event_sink.clone();
        let event_sink = self.event_sink.clone();
        Box::new(
            SubgraphManifest::resolve(name, Link { link }, self.resolver.clone())
                .map_err(SubgraphProviderError::ResolveError)
                .and_then(move |mut subgraph| {
                    subgraph
                        .schema
                        .add_subgraph_id_directives(subgraph.id.clone());

                    // Push the subgraph and the schema into their streams
                    let event_logger = send_logger.clone();
                    schema_event_sink
                        .send(SchemaEvent::SchemaAdded(subgraph.schema.clone()))
                        .map_err(move |e| {
                            error!(send_logger, "Failed to forward subgraph schema: {}", e)
                        })
                        .join(
                            event_sink
                                .send(SubgraphProviderEvent::SubgraphAdded(subgraph))
                                .map_err(move |e| {
                                    error!(event_logger, "Failed to forward subgraph: {}", e)
                                }),
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
