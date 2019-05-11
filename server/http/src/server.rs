use std::error::Error;
use std::fmt;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::Duration;

use hyper;
use hyper::Server;

use crate::service::GraphQLService;
use graph::prelude::{GraphQLServer as GraphQLServerTrait, *};

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
    graphql_runner: Arc<Q>,
    store: Arc<S>,
    node_id: NodeId,
}

impl<Q, S> GraphQLServer<Q, S> {
    /// Creates a new GraphQL server.
    pub fn new(
        logger: &Logger,
        graphql_runner: Arc<Q>,
        store: Arc<S>,
        node_id: NodeId,
        elastic_config: Option<ElasticLoggingConfig>,
    ) -> Self {
        let term_logger = logger.new(o!("component" => "GraphQLServer"));
        let logger = elastic_config
            .clone()
            .map(|elastic_config| {
                split_logger(
                    term_logger.clone(),
                    elastic_logger(
                        ElasticDrainConfig {
                            general: elastic_config,
                            index: String::from("graphql-server-logs"),
                            document_type: String::from("log"),
                            custom_id_key: String::from("componentId"),
                            custom_id_value: String::from("graphQlServer"),
                            flush_interval: Duration::from_secs(4),
                        },
                        term_logger.clone(),
                    ),
                )
            })
            .unwrap_or(term_logger);
        GraphQLServer {
            logger,
            graphql_runner,
            store,
            node_id,
        }
    }
}

impl<Q, S> GraphQLServerTrait for GraphQLServer<Q, S>
where
    Q: GraphQlRunner,
    S: SubgraphDeploymentStore + Store,
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
        let logger_for_service = self.logger.clone();
        let graphql_runner = self.graphql_runner.clone();
        let store = self.store.clone();
        let node_id = self.node_id.clone();
        let new_service = move || {
            let service = GraphQLService::new(
                logger_for_service.clone(),
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
