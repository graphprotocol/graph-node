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
pub struct GraphQLServer<Q, S> {
    logger: Logger,
    schema_event_sink: Sender<SchemaEvent>,
    // Maps a subgraph id to its schema.
    schemas: Arc<RwLock<BTreeMap<SubgraphId, Schema>>>,
    graphql_runner: Arc<Q>,
    store: Arc<S>,
    node_id: NodeId,
}

impl<Q, S> GraphQLServer<Q, S> {
    /// Creates a new GraphQL server.
    pub fn new(logger: &Logger, graphql_runner: Arc<Q>, store: Arc<S>, node_id: NodeId) -> Self {
        // Create channel for handling incoming schema events
        let (schema_event_sink, schema_event_stream) = channel(100);

        // Create a new GraphQL server
        let mut server = GraphQLServer {
            logger: logger.new(o!("component" => "GraphQLServer")),
            schema_event_sink,
            schemas: Arc::new(RwLock::new(BTreeMap::new())),
            graphql_runner,
            store,
            node_id,
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

        tokio::spawn(stream.for_each(move |event| {
            let mut schemas = schemas.write().unwrap();
            match event {
                SchemaEvent::SchemaAdded(new_schema) => {
                    debug!(logger, "Received SchemaAdded event"; "id" => new_schema.id.to_string());

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

                    schemas.insert(new_schema.id.clone(), derived_schema);
                }
                SchemaEvent::SchemaRemoved(id) => {
                    debug!(logger, "Received SchemaRemoved event"; "id" => id.to_string());

                    // If the event got this far, the subgraph must be hosted.
                    schemas.remove(&id).expect("subgraph not hosted");
                }
            }

            Ok(())
        }));
    }
}

impl<Q, S> GraphQLServerTrait for GraphQLServer<Q, S>
where
    Q: GraphQlRunner + 'static,
    S: SubgraphDeploymentStore,
{
    type ServeError = GraphQLServeError;

    fn serve(
        &mut self,
        port: u16,
        ws_port: u16,
    ) -> Result<Box<Future<Item = (), Error = ()> + Send>, Self::ServeError> {
        let logger = self.logger.clone();

        info!(
            logger,
            "Starting GraphQL HTTP server at: http://localhost:{}", port
        );

        let addr = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port);

        // On every incoming request, launch a new GraphQL service that writes
        // incoming queries to the query sink.
        let graphql_runner = self.graphql_runner.clone();
        let schemas = self.schemas.clone();
        let store = self.store.clone();
        let node_id = self.node_id.clone();
        let new_service = move || {
            let service = GraphQLService::new(
                schemas.clone(),
                graphql_runner.clone(),
                store.clone(),
                ws_port,
                node_id.clone(),
            );
            future::ok::<GraphQLService<Q, S>, hyper::Error>(service)
        };

        // Create a task to run the server and handle HTTP requests
        let task = Server::try_bind(&addr.into())?
            .serve(new_service)
            .map_err(move |e| error!(logger, "Server error"; "error" => format!("{}", e)));

        Ok(Box::new(task))
    }
}

impl<Q, S> EventConsumer<SchemaEvent> for GraphQLServer<Q, S> {
    fn event_sink(&self) -> Box<Sink<SinkItem = SchemaEvent, SinkError = ()> + Send> {
        let logger = self.logger.clone();
        Box::new(self.schema_event_sink.clone().sink_map_err(move |e| {
            error!(
                logger,
                "Failed to send schema event, receiving component was dropped: {}", e
            );
        }))
    }
}
