use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use graph::data::subgraph::schema::SUBGRAPHS_ID;
use graph::prelude::{SubscriptionServer as SubscriptionServerTrait, *};
use graph::tokio::net::TcpListener;
use graph_graphql::prelude::api_schema;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Mutex;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::{handshake::server::Request, Error as WsError};

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
pub struct SubscriptionServer<Q, S> {
    logger: Logger,
    graphql_runner: Arc<Q>,
    schema_event_sink: Sender<SchemaEvent>,
    subgraphs: SubgraphRegistry<GuardedSchema>,
    store: Arc<S>,
}

impl<Q, S> SubscriptionServer<Q, S>
where
    Q: GraphQlRunner + 'static,
    S: Store,
{
    pub fn new(logger: &Logger, graphql_runner: Arc<Q>, store: Arc<S>) -> Self {
        let logger = logger.new(o!("component" => "SubscriptionServer"));

        let (schema_event_sink, schema_event_stream) = channel(100);

        let mut server = SubscriptionServer {
            logger,
            graphql_runner,
            schema_event_sink,
            subgraphs: SubgraphRegistry::new(),
            store,
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
            match event {
                SchemaEvent::SchemaAdded(new_schema) => {
                    debug!(logger, "Received SchemaAdded event"; "id" => &new_schema.id);

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

                    // Add the subgraph name, ID and schema to the subgraph registry
                    subgraphs.insert(
                        derived_schema.id.clone(),
                        GuardedSchema::new(derived_schema),
                    );
                }
                SchemaEvent::SchemaRemoved(id) => {
                    debug!(logger, "Received SchemaRemoved event"; "id" => &id);

                    // On removal, the `GuardedSchema` will be dropped and all
                    // connections to it will be terminated.
                    subgraphs.remove_id(id);
                }
            }

            Ok(())
        }));
    }

    fn subgraph_id_from_url_path(store: Arc<S>, path: &Path) -> Result<String, ()> {
        let mut parts = path.iter();
        if parts.next() != Some("/".as_ref()) {
            return Err(());
        }

        match parts.next().and_then(|s| s.to_str()) {
            Some("subgraphs") => Ok(SUBGRAPHS_ID.to_owned()),
            Some("by-id") => parts
                .next()
                .and_then(|id| id.to_owned().into_string().ok())
                .ok_or(()),
            Some("by-name") => {
                let name = parts
                    .next()
                    .and_then(|name| name.to_owned().into_string().ok())
                    .ok_or(())?;

                store
                    .read_subgraph_name(name)
                    .expect("error reading subgraph name from store")
                    .ok_or(())
                    .and_then(|id_opt| id_opt.ok_or(()))
            }
            _ => Err(()),
        }
    }
}

impl<Q, S> SubscriptionServerTrait for SubscriptionServer<Q, S>
where
    Q: GraphQlRunner + 'static,
    S: Store,
{
    type ServeError = ();

    fn serve(
        &mut self,
        port: u16,
    ) -> Result<Box<Future<Item = (), Error = ()> + Send>, Self::ServeError> {
        let logger = self.logger.clone();
        let error_logger = self.logger.clone();
        let subgraphs = self.subgraphs.clone();

        info!(
            logger,
            "Starting GraphQL WebSocket server at: ws://localhost:{}", port
        );

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);
        let graphql_runner = self.graphql_runner.clone();
        let store = self.store.clone();

        let socket = TcpListener::bind(&addr).expect("Failed to bind WebSocket port");

        let task = socket
            .incoming()
            .map_err(move |e| {
                trace!(error_logger, "Connection error: {}", e);
            }).for_each(move |stream| {
                let logger = logger.clone();
                let graphql_runner = graphql_runner.clone();
                let store = store.clone();

                // Clone subgraph registry to pass it on to connections
                let subgraphs = subgraphs.clone();

                // Subgraph that the request is resolved to (if any)
                let subgraph_id = Arc::new(Mutex::new(None));
                let accept_subgraph_id = subgraph_id.clone();

                accept_hdr_async(stream, move |request: &Request| {
                    // Try to obtain the subgraph ID or name from the URL path.
                    // Return a 404 if the URL path contains no name/ID segment.
                    let path = &request.path;
                    *accept_subgraph_id.lock().unwrap() = Some(
                        Self::subgraph_id_from_url_path(store.clone(), path.as_ref())
                            .map_err(|()| WsError::Http(404))?,
                    );

                    Ok(Some(vec![(
                        String::from("Sec-WebSocket-Protocol"),
                        String::from("graphql-ws"),
                    )]))
                }).then(move |result| {
                    match result {
                        Ok(ws_stream) => {
                            // Obtain the subgraph ID or name that we resolved the request to
                            let subgraph_id = subgraph_id.lock().unwrap().clone().unwrap();

                            // Spawn a GraphQL over WebSocket connection
                            let service = GraphQlConnection::new(
                                &logger,
                                subgraphs.clone(),
                                subgraph_id.clone(),
                                ws_stream,
                                graphql_runner.clone(),
                            );

                            // Setup cancelation.
                            let guard = CancelGuard::new();
                            let cancel_subgraph = subgraph_id.clone();
                            let connection = service.into_future().cancelable(&guard, move || {
                                debug!(
                                        logger,
                                        "Canceling subscriptions"; "subgraph" => &cancel_subgraph
                                    )
                            });
                            subgraphs.mutate(&subgraph_id, |subgraph| {
                                subgraph.connection_guards.push(guard)
                            });

                            tokio::spawn(connection);
                        }
                        Err(e) => {
                            // We gracefully skip over failed connection attempts rather
                            // than tearing down the entire stream
                            trace!(logger, "Failed to establish WebSocket connection: {}", e);
                        }
                    }
                    Ok(())
                })
            });

        Ok(Box::new(task))
    }
}

impl<Q, S> EventConsumer<SchemaEvent> for SubscriptionServer<Q, S>
where
    Q: GraphQlRunner + 'static,
    S: Store,
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
