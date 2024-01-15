use std::net::{Ipv4Addr, SocketAddrV4};

use hyper::service::{service_fn, Service};
use hyper_util::rt::TokioIo;
use thiserror::Error;

use crate::service::GraphQLService;
use graph::{
    prelude::{GraphQLServer as GraphQLServerTrait, GraphQlRunner, *},
    tokio::net::TcpListener,
};

/// Errors that may occur when starting the server.
#[derive(Debug, Error)]
pub enum GraphQLServeError {
    #[error("Bind error: {0}")]
    BindError(String),
}

/// A GraphQL server based on Hyper.
pub struct GraphQLServer<Q> {
    logger: Logger,
    graphql_runner: Arc<Q>,
    node_id: NodeId,
}

impl<Q> GraphQLServer<Q> {
    /// Creates a new GraphQL server.
    pub fn new(logger_factory: &LoggerFactory, graphql_runner: Arc<Q>, node_id: NodeId) -> Self {
        let logger = logger_factory.component_logger(
            "GraphQLServer",
            Some(ComponentLoggerConfig {
                elastic: Some(ElasticComponentLoggerConfig {
                    index: String::from("graphql-server-logs"),
                }),
            }),
        );
        GraphQLServer {
            logger,
            graphql_runner,
            node_id,
        }
    }
}

#[async_trait]
impl<Q> GraphQLServerTrait for GraphQLServer<Q>
where
    Q: GraphQlRunner,
{
    type ServeError = GraphQLServeError;

    async fn serve(&mut self, port: u16, ws_port: u16) -> Result<Result<(), ()>, Self::ServeError> {
        let logger = self.logger.clone();

        info!(
            logger,
            "Starting GraphQL HTTP server at: http://localhost:{}", port
        );

        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port))
            .await
            .map_err(|e| GraphQLServeError::BindError(format!("unable to bind: {}", e)))?;

        let (stream, _) = listener.accept().await.map_err(|e| {
            GraphQLServeError::BindError(format!("unable to accept connections: {}", e))
        })?;
        let stream = TokioIo::new(stream);

        // On every incoming request, launch a new GraphQL service that writes
        // incoming queries to the query sink.
        let logger_for_service = self.logger.clone();
        let graphql_runner = self.graphql_runner.clone();
        let node_id = self.node_id.clone();
        let new_service = service_fn(move |req| {
            let graphql_service = GraphQLService::new(
                logger_for_service.clone(),
                graphql_runner.clone(),
                ws_port,
                node_id.clone(),
            );

            graphql_service.call(req)
        });

        let mut builder =
            hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());

        // Create a task to run the server and handle HTTP requests
        Ok(builder
            .http1()
            .serve_connection(stream, new_service)
            .await
            .map_err(move |e| error!(logger, "Server error"; "error" => format!("{}", e))))
    }
}
