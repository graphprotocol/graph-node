use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Mutex;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::{handshake::server::Request, Error as WsError};

use graph::prelude::{SubscriptionServer as SubscriptionServerTrait, *};
use graph::tokio::net::TcpListener;
use graph_graphql::prelude::api_schema;

use connection::GraphQlConnection;

/// On drop, cancels all connections to this subgraph.
pub(crate) struct GuardedSchema {
    pub(crate) schema: Schema,
    connection_guards: Vec<CancelGuard>,
}

impl GuardedSchema {
    fn new(schema: Schema) -> Self {
        Self {
            schema,
            connection_guards: Vec::new(),
        }
    }
}

/// A GraphQL subscription server based on Hyper / Websockets.
pub struct SubscriptionServer<Q> {
    logger: Logger,
    graphql_runner: Arc<Q>,
    schema_event_sink: Sender<SchemaEvent>,
    subgraphs: SubgraphRegistry<GuardedSchema>,
}

impl<Q> SubscriptionServer<Q>
where
    Q: GraphQlRunner + 'static,
{
    pub fn new(logger: &Logger, graphql_runner: Arc<Q>) -> Self {
        let logger = logger.new(o!("component" => "SubscriptionServer"));

        let (schema_event_sink, schema_event_stream) = channel(100);

        let mut server = SubscriptionServer {
            logger,
            graphql_runner,
            schema_event_sink,
            subgraphs: SubgraphRegistry::new(),
        };

        // Spawn task to handle incoming schema events
        server.handle_schema_events(schema_event_stream);

        // Return the server
        server
    }

    fn handle_schema_events(&mut self, stream: Receiver<SchemaEvent>) {
        let logger = self.logger.clone();
        let mut subgraphs = self.subgraphs.clone();

        tokio::spawn(stream.for_each(move |event| {
            info!(logger, "Received schema event");

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

                    // Add the subgraph name, ID and schema to the subgraph registry
                    subgraphs.insert(
                        Some(derived_schema.name.clone()),
                        derived_schema.id.clone(),
                        GuardedSchema::new(derived_schema),
                    );
                }
                SchemaEvent::SchemaRemoved(name, _) => {
                    // On removal, the `GuardedSchema` will be dropped and all
                    // connections to it will be terminated.
                    subgraphs.remove_name(name);
                }
            }

            Ok(())
        }));
    }

    fn subgraph_from_url_path(path: PathBuf) -> Option<String> {
        path.iter()
            .nth(1)
            .and_then(|os| os.to_str())
            .map(|s| s.into())
    }
}

impl<Q> SubscriptionServerTrait for SubscriptionServer<Q>
where
    Q: GraphQlRunner + 'static,
{
    type ServeError = ();

    fn serve(
        &mut self,
        port: u16,
    ) -> Result<Box<Future<Item = (), Error = ()> + Send>, Self::ServeError> {
        let logger = self.logger.clone();
        let error_logger = self.logger.clone();
        let subgraphs = self.subgraphs.clone();

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);
        let graphql_runner = self.graphql_runner.clone();

        let socket = TcpListener::bind(&addr).expect("Failed to bind WebSocket port");

        let task = socket
            .incoming()
            .map_err(move |e| {
                warn!(error_logger, "Connection error: {}", e);
            }).for_each(move |stream| {
                let logger = logger.clone();
                let graphql_runner = graphql_runner.clone();

                // Clone subgraph registry to pass it on to connections
                let subgraphs = subgraphs.clone();

                // Subgraph that the request is resolved to (if any)
                let subgraph_id_or_name = Arc::new(Mutex::new(None));
                let accept_subgraph_id_or_name = subgraph_id_or_name.clone();

                accept_hdr_async(stream, move |request: &Request| {
                    // Try to obtain the subgraph ID or name from the URL path.
                    // Return a 404 if the URL path contains no name/ID segment.
                    let path = PathBuf::from(&request.path);
                    *accept_subgraph_id_or_name.lock().unwrap() =
                        Some(Self::subgraph_from_url_path(path).ok_or(WsError::Http(404))?);

                    Ok(Some(vec![(
                        String::from("Sec-WebSocket-Protocol"),
                        String::from("graphql-ws"),
                    )]))
                }).then(move |result| {
                    match result {
                        Ok(ws_stream) => {
                            // Obtain the subgraph ID or name that we resolved the request to
                            let subgraph = subgraph_id_or_name.lock().unwrap().clone().unwrap();

                            // Spawn a GraphQL over WebSocket connection
                            let service = GraphQlConnection::new(
                                &logger,
                                subgraphs.clone(),
                                subgraph.clone(),
                                ws_stream,
                                graphql_runner.clone(),
                            );

                            // Setup cancelation.
                            let guard = CancelGuard::new();
                            let cancel_subgraph = subgraph.clone();
                            let connection = service.into_future().cancelable(&guard, move || {
                                debug!(
                                        logger,
                                        "Canceling subscriptions"; "subgraph" => &cancel_subgraph
                                    )
                            });
                            subgraphs.mutate(&subgraph, |subgraph| {
                                subgraph.connection_guards.push(guard)
                            });

                            tokio::spawn(connection);
                        }
                        Err(e) => {
                            // We gracefully skip over failed connection attempts rather
                            // than tearing down the entire stream
                            warn!(logger, "Failed to establish WebSocket connection: {}", e);
                        }
                    }
                    Ok(())
                })
            });

        Ok(Box::new(task))
    }
}

impl<Q> EventConsumer<SchemaEvent> for SubscriptionServer<Q>
where
    Q: GraphQlRunner + 'static,
{
    fn event_sink(&self) -> Box<Sink<SinkItem = SchemaEvent, SinkError = ()> + Send> {
        let logger = self.logger.clone();
        Box::new(self.schema_event_sink.clone().sink_map_err(move |e| {
            error!(
                logger,
                "Failed to send schema event to subscription server: {}", e
            )
        }))
    }
}
