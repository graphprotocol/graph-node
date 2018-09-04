use futures::sync::mpsc::{channel, Receiver, Sender};
use hyper;
use hyper::Server;

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::RwLock;

use graph::data::schema::Schema;
use graph::prelude::{GraphQLServer as GraphQLServerTrait, *};
use graph_graphql::prelude::api_schema;
use service::GraphQLService;

/// Errors that may occur when starting the server.
#[derive(Debug)]
pub enum GraphQLServeError {
    BindError(hyper::Error),
}

impl Error for GraphQLServeError {
    fn description(&self) -> &str {
        "Failed to start the server"
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}

impl fmt::Display for GraphQLServeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GraphQLServeError::BindError(e) => write!(f, "Failed to bind GraphQL server: {}", e),
        }
    }
}

impl From<hyper::Error> for GraphQLServeError {
    fn from(err: hyper::Error) -> Self {
        GraphQLServeError::BindError(err)
    }
}

/// A GraphQL server based on Hyper.
pub struct GraphQLServer<Q> {
    logger: slog::Logger,
    schema_event_sink: Sender<SchemaEvent>,
    // Maps a subgraph id to its name.
    names: Arc<RwLock<BTreeMap<String, String>>>,
    // Maps a subgraph name to its schema.
    schemas: Arc<RwLock<BTreeMap<String, Schema>>>,
    graphql_runner: Arc<Q>,
}

impl<Q> GraphQLServer<Q> {
    /// Creates a new GraphQL server.
    pub fn new(logger: &slog::Logger, graphql_runner: Arc<Q>) -> Self {
        // Create channel for handling incoming schema events
        let (schema_event_sink, schema_event_stream) = channel(100);

        // Create a new GraphQL server
        let mut server = GraphQLServer {
            logger: logger.new(o!("component" => "GraphQLServer")),
            schema_event_sink,
            names: Arc::new(RwLock::new(BTreeMap::new())),
            schemas: Arc::new(RwLock::new(BTreeMap::new())),
            graphql_runner: graphql_runner,
        };

        // Spawn tasks to handle incoming schema events
        server.handle_schema_events(schema_event_stream);

        // Return the new server.
        server
    }

    /// Handle incoming schema events.
    fn handle_schema_events(&mut self, stream: Receiver<SchemaEvent>) {
        let logger = self.logger.clone();
        let schemas = self.schemas.clone();
        let names = self.names.clone();

        tokio::spawn(stream.for_each(move |event| {
            info!(logger, "Received schema event");

            let mut schemas = schemas.write().unwrap();
            let mut names = names.write().unwrap();
            match event {
                SchemaEvent::SchemaAdded(new_schema) => {
                    let derived_schema = match api_schema(&new_schema.document) {
                        Ok(document) => Schema {
                            name: new_schema.name.clone(),
                            id: new_schema.id.clone(),
                            document,
                        },
                        Err(e) => return Ok(error!(logger, "error deriving schema {}", e)),
                    };

                    schemas.insert(derived_schema.name.clone(), derived_schema);
                    names.insert(new_schema.id.clone(), new_schema.name.clone());
                }
                SchemaEvent::SchemaRemoved(name, id) => {
                    // If the event got this far, the subgraph must be hosted.
                    schemas.remove(&name).expect("subgraph not hosted");
                    names.remove(&id).expect("subgraph not hosted");
                }
            }

            Ok(())
        }));
    }
}

impl<Q> GraphQLServerTrait for GraphQLServer<Q>
where
    Q: GraphQlRunner + Sized + 'static,
{
    type ServeError = GraphQLServeError;

    fn schema_event_sink(&mut self) -> Sender<SchemaEvent> {
        self.schema_event_sink.clone()
    }

    fn serve(
        &mut self,
        port: u16,
    ) -> Result<Box<Future<Item = (), Error = ()> + Send>, Self::ServeError> {
        let logger = self.logger.clone();

        let addr = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port);

        // On every incoming request, launch a new GraphQL service that writes
        // incoming queries to the query sink.
        let graphql_runner = self.graphql_runner.clone();
        let names = self.names.clone();
        let schemas = self.schemas.clone();
        let new_service = move || {
            let service =
                GraphQLService::new(names.clone(), schemas.clone(), graphql_runner.clone());
            future::ok::<GraphQLService<Q>, hyper::Error>(service)
        };

        // Create a task to run the server and handle HTTP requests
        let task = Server::try_bind(&addr.into())?
            .serve(new_service)
            .map_err(move |e| error!(logger, "Server error"; "error" => format!("{}", e)));

        Ok(Box::new(task))
    }
}

#[cfg(test)]
mod tests {
    extern crate graph_mock;

    use std::time::{Duration, Instant};

    use self::graph_mock::MockGraphQlRunner;
    use graph_graphql::schema::ast;

    use super::*;

    #[test]
    fn emits_an_api_schema_after_one_schema_is_added() {
        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        runtime
            .block_on(future::lazy(|| {
                let res: Result<_, ()> = Ok({
                    // Set up the server
                    let logger = Logger::root(slog::Discard, o!());
                    let graphql_runner = Arc::new(MockGraphQlRunner::new(&logger));
                    let mut server = GraphQLServer::new(&logger, graphql_runner);
                    let schema_sink = server.schema_event_sink();

                    // Create an input schema event
                    let input_doc =
                        ::graphql_parser::parse_schema("type User { name: String! }").unwrap();
                    let input_schema = Schema {
                        name: "input-schema".to_string(),
                        id: "input-schema".to_string(),
                        document: input_doc,
                    };
                    let input_event = SchemaEvent::SchemaAdded(input_schema.clone());

                    // Send the input schema event to the server
                    schema_sink.send(input_event).wait().unwrap();

                    // Wait for the schema to be received and extract it.
                    // Wait for thirty seconds for that to happen, otherwise fail the test.
                    let start_time = Instant::now();
                    let max_wait = Duration::from_secs(30);
                    let output_schema = loop {
                        if let Some(schema) = server.schemas.read().unwrap().get("input-schema") {
                            break schema.clone();
                        } else if Instant::now().duration_since(start_time) > max_wait {
                            panic!("Timed out, schema not received")
                        }
                        ::std::thread::yield_now();
                    };

                    assert_eq!(output_schema.id, input_schema.id);

                    // The output schema must include the input schema types
                    assert_eq!(
                        ast::get_named_type(&input_schema.document, &"User".to_string()),
                        ast::get_named_type(&output_schema.document, &"User".to_string())
                    );

                    // The output schema must include a Query type
                    ast::get_named_type(&output_schema.document, &"Query".to_string())
                        .expect("Query type missing in output schema");
                });
                res
            }))
            .unwrap();
    }
}
