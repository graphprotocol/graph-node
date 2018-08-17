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
        link: String,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static> {
        let send_logger = self.logger.clone();
        let schema_event_sink = self.schema_event_sink.clone();
        let event_sink = self.event_sink.clone();
        Box::new(
            SubgraphManifest::resolve(Link { link }, self.resolver.clone())
                .map_err(SubgraphProviderError::ResolveError)
                .and_then(move |subgraph| {
                    let mut schema = subgraph.schema.clone();
                    schema.add_subgraph_id_directives(subgraph.id.clone());

                    // Push the subgraph and the schema into their streams
                    let event_logger = send_logger.clone();
                    schema_event_sink
                        .send(SchemaEvent::SchemaAdded(schema))
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
