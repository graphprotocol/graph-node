use hyper;
use hyper::Server;
use std::error::Error;
use std::fmt;
use std::net::{Ipv4Addr, SocketAddrV4};

use graph::prelude::{IndexNodeServer as IndexNodeServerTrait, *};

use crate::service::IndexNodeService;

/// Errors that may occur when starting the server.
#[derive(Debug)]
pub enum IndexNodeServeError {
    BindError(hyper::Error),
}

impl Error for IndexNodeServeError {
    fn description(&self) -> &str {
        "Failed to start the server"
    }

    fn cause(&self) -> Option<&dyn Error> {
        None
    }
}

impl fmt::Display for IndexNodeServeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IndexNodeServeError::BindError(e) => {
                write!(f, "Failed to bind index node server: {}", e)
            }
        }
    }
}

impl From<hyper::Error> for IndexNodeServeError {
    fn from(err: hyper::Error) -> Self {
        IndexNodeServeError::BindError(err)
    }
}

/// A GraphQL server based on Hyper.
pub struct IndexNodeServer<Q, S> {
    logger: Logger,
    graphql_runner: Arc<Q>,
    store: Arc<S>,
    node_id: NodeId,
}

impl<Q, S> IndexNodeServer<Q, S> {
    /// Creates a new GraphQL server.
    pub fn new(
        logger_factory: &LoggerFactory,
        graphql_runner: Arc<Q>,
        store: Arc<S>,
        node_id: NodeId,
    ) -> Self {
        let logger = logger_factory.component_logger(
            "IndexNodeServer",
            Some(ComponentLoggerConfig {
                elastic: Some(ElasticComponentLoggerConfig {
                    index: String::from("index-node-server-logs"),
                }),
            }),
        );

        IndexNodeServer {
            logger,
            graphql_runner,
            store,
            node_id,
        }
    }
}

impl<Q, S> IndexNodeServerTrait for IndexNodeServer<Q, S>
where
    Q: GraphQlRunner,
    S: SubgraphDeploymentStore + Store,
{
    type ServeError = IndexNodeServeError;

    fn serve(
        &mut self,
        port: u16,
    ) -> Result<Box<dyn Future<Item = (), Error = ()> + Send>, Self::ServeError> {
        let logger = self.logger.clone();

        info!(
            logger,
            "Starting index node server at: http://localhost:{}", port
        );

        let addr = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port);

        // On every incoming request, launch a new GraphQL service that writes
        // incoming queries to the query sink.
        let logger_for_service = self.logger.clone();
        let graphql_runner = self.graphql_runner.clone();
        let store = self.store.clone();
        let node_id = self.node_id.clone();
        let new_service = move || {
            let service = IndexNodeService::new(
                logger_for_service.clone(),
                graphql_runner.clone(),
                store.clone(),
                node_id.clone(),
            );
            future::ok::<IndexNodeService<Q, S>, hyper::Error>(service)
        };

        // Create a task to run the server and handle HTTP requests
        let task = Server::try_bind(&addr.into())?
            .serve(new_service)
            .map_err(move |e| error!(logger, "Server error"; "error" => format!("{}", e)));

        Ok(Box::new(task))
    }
}
