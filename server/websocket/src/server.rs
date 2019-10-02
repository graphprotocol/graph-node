use graph::data::subgraph::schema::SUBGRAPHS_ID;
use graph::prelude::{SubscriptionServer as SubscriptionServerTrait, *};
use hyper::Uri;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Mutex;
use tokio::net::TcpListener;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::{handshake::server::Request, Error as WsError};

use crate::connection::GraphQlConnection;

/// A GraphQL subscription server based on Hyper / Websockets.
pub struct SubscriptionServer<Q, S> {
    logger: Logger,
    graphql_runner: Arc<Q>,
    store: Arc<S>,
}

impl<Q, S> SubscriptionServer<Q, S>
where
    Q: GraphQlRunner,
    S: SubgraphDeploymentStore + Store,
{
    pub fn new(logger: &Logger, graphql_runner: Arc<Q>, store: Arc<S>) -> Self {
        SubscriptionServer {
            logger: logger.new(o!("component" => "SubscriptionServer")),
            graphql_runner,
            store,
        }
    }

    fn subgraph_id_from_url_path(
        store: Arc<S>,
        path: &str,
    ) -> Result<Option<SubgraphDeploymentId>, Error> {
        let path_segments = {
            let mut segments = path.split("/");

            // Remove leading '/'
            let first_segment = segments.next();
            if first_segment != Some("") {
                return Ok(None);
            }

            segments.collect::<Vec<_>>()
        };

        match path_segments.as_slice() {
            &["subgraphs"] => Ok(Some(SUBGRAPHS_ID.clone())),
            &["subgraphs", "id", subgraph_id] => Ok(SubgraphDeploymentId::new(subgraph_id).ok()),
            &["subgraphs", "name", _] | &["subgraphs", "name", _, _] => {
                let subgraph_name = path_segments[2..].join("/");

                match SubgraphName::new(subgraph_name) {
                    Err(()) => Ok(None),
                    Ok(subgraph_name) => store.resolve_subgraph_name_to_id(subgraph_name),
                }
            }
            _ => Ok(None),
        }
    }
}

impl<Q, S> SubscriptionServerTrait for SubscriptionServer<Q, S>
where
    Q: GraphQlRunner,
    S: SubgraphDeploymentStore + Store,
{
    type ServeError = ();

    fn serve(
        &mut self,
        port: u16,
    ) -> Result<Box<dyn Future<Item = (), Error = ()> + Send>, Self::ServeError> {
        let logger = self.logger.clone();
        let error_logger = self.logger.clone();

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
            })
            .for_each(move |stream| {
                let logger = logger.clone();
                let logger2 = logger.clone();
                let graphql_runner = graphql_runner.clone();
                let store = store.clone();
                let store2 = store.clone();

                // Subgraph that the request is resolved to (if any)
                let subgraph_id = Arc::new(Mutex::new(None));
                let accept_subgraph_id = subgraph_id.clone();

                accept_hdr_async(stream, move |request: &Request| {
                    // Try to obtain the subgraph ID or name from the URL path.
                    // Return a 404 if the URL path contains no name/ID segment.

                    // request.path is straight from the HTTP request line
                    // and might be an absolute URI (according to RFC7230
                    // section 5.3.2, "a server MUST accept the absolute-form
                    // in requests").
                    //
                    // Use hyper's URI parser to extract the path part of the
                    // URI. This will be a no-op in the normal case where
                    // request.path is only a path.
                    let path = request.path.parse::<Uri>()
                        .map_err(|e| {
                            debug!(
                                logger,
                                "Could not parse WebSockets request.path";
                                "error" => e.to_string()
                            );

                            WsError::Http(400)
                        })?
                        .path().to_owned();

                    let subgraph_id = Self::subgraph_id_from_url_path(store.clone(), path.as_ref())
                        .map_err(|e| {
                            error!(
                                logger,
                                "Error resolving subgraph ID from URL path";
                                "error" => e.to_string()
                            );

                            WsError::Http(500)
                        }).and_then(|subgraph_id_opt| {
                            subgraph_id_opt.ok_or_else(|| WsError::Http(404))
                        })?;

                    // Check if the subgraph is deployed
                    match store.is_deployed(&subgraph_id) {
                        Err(_) | Ok(false) => {
                            error!(logger, "Failed to establish WS connection, no data found for subgraph";
                                            "subgraph_id" => subgraph_id.to_string(),
                            );
                            return Err(WsError::Http(404));
                        }
                        Ok(true) => (),
                    }

                    *accept_subgraph_id.lock().unwrap() = Some(subgraph_id);

                    Ok(Some(vec![(
                        String::from("Sec-WebSocket-Protocol"),
                        String::from("graphql-ws"),
                    )]))
                })
                .then(move |result| {
                    match result {
                        Ok(ws_stream) => {
                            // Obtain the subgraph ID or name that we resolved the request to
                            let subgraph_id = subgraph_id.lock().unwrap().clone().unwrap();

                            // Get the subgraph schema
                            let schema = match store2.api_schema(&subgraph_id) {
                                Ok(schema) => schema,
                                Err(e) => {
                                    error!(logger2, "Failed to establish WS connection, could not find schema";
                                                    "subgraph" => subgraph_id.to_string(),
                                                    "error" => e.to_string(),
                                    );
                                    return Ok(())
                                }
                            };

                            // Spawn a GraphQL over WebSocket connection
                            let service = GraphQlConnection::new(
                                &logger2,
                                schema,
                                ws_stream,
                                graphql_runner.clone(),
                            );
                            tokio::spawn(service.into_future());
                        }
                        Err(e) => {
                            // We gracefully skip over failed connection attempts rather
                            // than tearing down the entire stream
                            trace!(logger2, "Failed to establish WebSocket connection: {}", e);
                        }
                    }
                    Ok(())
                })
            });

        Ok(Box::new(task))
    }
}
