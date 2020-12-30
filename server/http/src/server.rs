use std::net::{Ipv4Addr, SocketAddrV4};

use hyper;
use hyper::service::make_service_fn;
use hyper::Server;

use crate::service::{GraphQLService, GraphQLServiceMetrics};
use graph::prelude::{GraphQLServer as GraphQLServerTrait, *};
use thiserror::Error;

/// Errors that may occur when starting the server.
#[derive(Debug, Error)]
pub enum GraphQLServeError {
    #[error("Bind error: {0}")]
    BindError(hyper::Error),
}

impl From<hyper::Error> for GraphQLServeError {
    fn from(err: hyper::Error) -> Self {
        GraphQLServeError::BindError(err)
    }
}

/// A GraphQL server based on Hyper.
pub struct GraphQLServer<Q> {
    logger: Logger,
    metrics: Arc<GraphQLServiceMetrics>,
    graphql_runner: Arc<Q>,
    node_id: NodeId,
}

impl<Q> GraphQLServer<Q> {
    /// Creates a new GraphQL server.
    pub fn new(
        logger_factory: &LoggerFactory,
        metrics_registry: Arc<impl MetricsRegistry>,
        graphql_runner: Arc<Q>,
        node_id: NodeId,
    ) -> Self {
        let logger = logger_factory.component_logger(
            "GraphQLServer",
            Some(ComponentLoggerConfig {
                elastic: Some(ElasticComponentLoggerConfig {
                    index: String::from("graphql-server-logs"),
                }),
            }),
        );
        let metrics = Arc::new(GraphQLServiceMetrics::new(metrics_registry.clone()));
        GraphQLServer {
            logger,
            metrics,
            graphql_runner,
            node_id,
        }
    }
}

impl<Q> GraphQLServerTrait for GraphQLServer<Q>
where
    Q: GraphQlRunner,
{
    type ServeError = GraphQLServeError;

    fn serve(
        &mut self,
        port: u16,
        ws_port: u16,
    ) -> Result<Box<dyn Future<Item = (), Error = ()> + Send>, Self::ServeError> {
        let logger = self.logger.clone();

        info!(
            logger,
            "Starting GraphQL HTTP server at: http://localhost:{}", port
        );

        let addr = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port);

        // On every incoming request, launch a new GraphQL service that writes
        // incoming queries to the query sink.
        let logger_for_service = self.logger.clone();
        let graphql_runner = self.graphql_runner.clone();
        let metrics = self.metrics.clone();
        let node_id = self.node_id.clone();
        let new_service = make_service_fn(move |_| {
            futures03::future::ok::<_, Error>(GraphQLService::new(
                logger_for_service.clone(),
                metrics.clone(),
                graphql_runner.clone(),
                ws_port,
                node_id.clone(),
            ))
        });

        // Create a task to run the server and handle HTTP requests
        let task = Server::try_bind(&addr.into())?
            .serve(new_service)
            .map_err(move |e| error!(logger, "Server error"; "error" => format!("{}", e)));

        Ok(Box::new(task.compat()))
    }
}
