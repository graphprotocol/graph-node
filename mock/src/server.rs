use futures::sync::mpsc::{channel, Receiver, Sender};
use graphql_parser;
use std::error::Error;
use std::fmt;
use std::sync::Mutex;

use graph::prelude::*;
use graph_graphql::prelude::api_schema;

#[derive(Debug)]
pub struct MockServeError;

impl Error for MockServeError {
    fn description(&self) -> &str {
        "Mock serve error"
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}

impl fmt::Display for MockServeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Mock serve error")
    }
}

/// A mock `GraphQLServer`.
pub struct MockGraphQLServer<Q> {
    logger: Logger,
    schema_event_sink: Sender<SchemaEvent>,
    schema: Arc<Mutex<Option<Schema>>>,
    query_runner: Arc<Q>,
}

impl<Q> MockGraphQLServer<Q> {
    /// Creates a new mock `GraphQLServer`.
    pub fn new(logger: &Logger, query_runner: Arc<Q>) -> Self {
        // Create channel for handling incoming schema events
        let (schema_event_sink, schema_event_stream) = channel(100);

        // Create a new mock GraphQL server
        let mut server = MockGraphQLServer {
            logger: logger.new(o!("component" => "MockGraphQLServer")),
            schema_event_sink,
            schema: Arc::new(Mutex::new(None)),
            query_runner,
        };

        // Spawn tasks to handle incoming schema events
        server.handle_schema_events(schema_event_stream);

        // Return the new server
        server
    }

    /// Handle incoming schema events
    fn handle_schema_events(&mut self, stream: Receiver<SchemaEvent>) {
        let logger = self.logger.clone();
        let schema = self.schema.clone();

        tokio::spawn(stream.for_each(move |event| {
            info!(logger, "Received schema event"; "event" => format!("{:?}", event));

            if let SchemaEvent::SchemaAdded(new_schema) = event {
                let mut schema = schema.lock().unwrap();
                let derived_schema = match api_schema(&new_schema.document) {
                    Ok(document) => Schema {
                        id: new_schema.id.clone(),
                        document,
                    },
                    Err(e) => {
                        error!(logger, "Error deriving schema {}", e);
                        return Ok(());
                    }
                };
                *schema = Some(derived_schema);
            } else {
                panic!("schema removal is yet not supported")
            }

            Ok(())
        }));
    }
}

impl<Q> GraphQLServer for MockGraphQLServer<Q>
where
    Q: GraphQlRunner + 'static,
{
    type ServeError = MockServeError;

    fn serve(
        &mut self,
        _port: u16,
    ) -> Result<Box<Future<Item = (), Error = ()> + Send>, Self::ServeError> {
        let schema = self.schema.clone();
        let query_runner = self.query_runner.clone();
        let logger = self.logger.clone();

        // Generate mock query requests
        let requests = (0..5)
            .map(|_| {
                let schema = schema.lock().unwrap();
                Query {
                    schema: schema.clone().unwrap(),
                    document: graphql_parser::parse_query("{ allUsers { name }}").unwrap(),
                    variables: None,
                }
            }).collect::<Vec<Query>>();

        // Create task to generate mock queries
        Ok(Box::new(stream::iter_ok(requests).for_each(move |query| {
            let logger = logger.clone();
            query_runner.run_query(query).then(move |result| {
                info!(logger, "Query result: {:?}", result);
                Ok(())
            })
        })))
    }
}

impl<Q> EventConsumer<SchemaEvent> for MockGraphQLServer<Q> {
    fn event_sink(&self) -> Box<Sink<SinkItem = SchemaEvent, SinkError = ()> + Send> {
        Box::new(self.schema_event_sink.clone().sink_map_err(move |e| {
            panic!(
                "Failed to send schema event, receiving component was dropped: {}",
                e
            );
        }))
    }
}
