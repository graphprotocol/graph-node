use hyper;
use hyper::service::make_service_fn;
use hyper::Server;
use std::net::{Ipv4Addr, SocketAddrV4};

use graph::{
    components::store::StatusStore,
    prelude::{IndexNodeServer as IndexNodeServerTrait, *},
};

use crate::service::IndexNodeService;
use thiserror::Error;

/// Errors that may occur when starting the server.
#[derive(Debug, Error)]
pub enum IndexNodeServeError {
    #[error("Bind error: {0}")]
    BindError(hyper::Error),
}

impl From<hyper::Error> for IndexNodeServeError {
    fn from(err: hyper::Error) -> Self {
        IndexNodeServeError::BindError(err)
    }
}

/// A GraphQL server based on Hyper.
pub struct IndexNodeServer<Q, S, R, St> {
    logger: Logger,
    graphql_runner: Arc<Q>,
    store: Arc<S>,
    link_resolver: Arc<R>,
    subgraph_store: Arc<St>,
}

impl<Q, S, R, St> IndexNodeServer<Q, S, R, St> {
    /// Creates a new GraphQL server.
    pub fn new(
        logger_factory: &LoggerFactory,
        graphql_runner: Arc<Q>,
        store: Arc<S>,
        link_resolver: Arc<R>,
        subgraph_store: Arc<St>,
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
            link_resolver,
            subgraph_store,
        }
    }
}

impl<Q, S, R, St> IndexNodeServerTrait for IndexNodeServer<Q, S, R, St>
where
    Q: GraphQlRunner,
    S: StatusStore,
    R: LinkResolver,
    St: SubgraphStore,
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
        let service = IndexNodeService::new(
            logger_for_service.clone(),
            graphql_runner.clone(),
            store.clone(),
            self.link_resolver.clone(),
            self.subgraph_store.clone(),
        );
        let new_service =
            make_service_fn(move |_| futures03::future::ok::<_, Error>(service.clone()));

        // Create a task to run the server and handle HTTP requests
        let task = Server::try_bind(&addr.into())?
            .serve(new_service)
            .map_err(move |e| error!(logger, "Server error"; "error" => format!("{}", e)));

        Ok(Box::new(task.compat()))
    }
}
