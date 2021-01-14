use graph::prelude::{SubscriptionServer as SubscriptionServerTrait, *};
use http::{HeaderValue, Response, StatusCode};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Mutex;
use tokio::net::TcpListener;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::handshake::server::Request;

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
    S: SubgraphStore,
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
        fn id_from_name<S: SubgraphStore>(
            store: Arc<S>,
            name: String,
        ) -> Option<SubgraphDeploymentId> {
            SubgraphName::new(name)
                .ok()
                .map(|subgraph_name| store.deployment_state_from_name(subgraph_name))
                .transpose()
                .map(|state| state.map(|state| state.id))
                .ok()
                .flatten()
        }

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
            &["subgraphs", "id", subgraph_id] => Ok(SubgraphDeploymentId::new(subgraph_id).ok()),
            &["subgraphs", "name", _] | &["subgraphs", "name", _, _] => {
                Ok(id_from_name(store, path_segments[2..].join("/")))
            }
            &["subgraphs", "network", _, _] => {
                Ok(id_from_name(store, path_segments[1..].join("/")))
            }
            _ => Ok(None),
        }
    }
}

#[async_trait]
impl<Q, S> SubscriptionServerTrait for SubscriptionServer<Q, S>
where
    Q: GraphQlRunner,
    S: SubgraphStore,
{
    async fn serve(self, port: u16) {
        info!(
            self.logger,
            "Starting GraphQL WebSocket server at: ws://localhost:{}", port
        );

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);
        let mut socket = TcpListener::bind(&addr)
            .await
            .expect("Failed to bind WebSocket port");

        let mut incoming = socket.incoming();
        while let Some(stream_res) = incoming.next().await {
            let stream = match stream_res {
                Ok(stream) => stream,
                Err(e) => {
                    trace!(self.logger, "Connection error: {}", e);
                    continue;
                }
            };
            let logger = self.logger.clone();
            let logger2 = self.logger.clone();
            let graphql_runner = self.graphql_runner.clone();
            let store = self.store.clone();
            let store2 = self.store.clone();

            // Subgraph that the request is resolved to (if any)
            let subgraph_id = Arc::new(Mutex::new(None));
            let accept_subgraph_id = subgraph_id.clone();

            accept_hdr_async(stream, move |request: &Request, mut response: Response<()>| {
                // Try to obtain the subgraph ID or name from the URL path.
                // Return a 404 if the URL path contains no name/ID segment.
                let path = request.uri().path();
                let subgraph_id = Self::subgraph_id_from_url_path(store.clone(), path.as_ref())
                    .map_err(|e| {
                        error!(
                            logger,
                            "Error resolving subgraph ID from URL path";
                            "error" => e.to_string()
                        );

                        Response::builder().status(StatusCode::INTERNAL_SERVER_ERROR).body(None).unwrap()
                    }).and_then(|subgraph_id_opt| {
                        subgraph_id_opt.ok_or_else(|| {
                            Response::builder().status(StatusCode::NOT_FOUND).body(None).unwrap()
                        })
                    })?;

                // Check if the subgraph is deployed
                match store.is_deployed(&subgraph_id) {
                    Err(_) | Ok(false) => {
                        error!(logger, "Failed to establish WS connection, no data found for subgraph";
                                        "subgraph_id" => subgraph_id.to_string(),
                        );
                        return Err(Response::builder().status(StatusCode::NOT_FOUND).body(None).unwrap());
                    }
                    Ok(true) => (),
                }

                *accept_subgraph_id.lock().unwrap() = Some(subgraph_id);
                response.headers_mut().insert("Sec-WebSocket-Protocol", HeaderValue::from_static("graphql-ws"));
                Ok(response)
            })
            .then(move |result| async move {
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
                                return;
                            }
                        };

                        // Spawn a GraphQL over WebSocket connection
                        let service = GraphQlConnection::new(
                            &logger2,
                            schema,
                            ws_stream,
                            graphql_runner.clone(),
                        );

                        graph::spawn_allow_panic(service.into_future().compat());
                    }
                    Err(e) => {
                        // We gracefully skip over failed connection attempts rather
                        // than tearing down the entire stream
                        trace!(logger2, "Failed to establish WebSocket connection: {}", e);
                    }
                }
            }).await
        }
    }
}
