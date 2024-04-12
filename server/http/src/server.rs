use std::sync::Arc;

use graph::anyhow;
use graph::cheap_clone::CheapClone;
use graph::components::server::server::{start, ServerHandle};
use graph::log::factory::{ComponentLoggerConfig, ElasticComponentLoggerConfig};
use graph::slog::info;

use crate::service::GraphQLService;
use graph::prelude::{GraphQlRunner, Logger, LoggerFactory};

/// A GraphQL server based on Hyper.
pub struct GraphQLServer<Q> {
    logger: Logger,
    graphql_runner: Arc<Q>,
}

impl<Q: GraphQlRunner> GraphQLServer<Q> {
    /// Creates a new GraphQL server.
    pub fn new(logger_factory: &LoggerFactory, graphql_runner: Arc<Q>) -> Self {
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
        }
    }

    pub async fn start(&self, port: u16, ws_port: u16) -> Result<ServerHandle, anyhow::Error> {
        let logger = self.logger.clone();

        info!(
            logger,
            "Starting GraphQL HTTP server at: http://localhost:{}", port
        );

        let graphql_runner = self.graphql_runner.clone();

        let service = Arc::new(GraphQLService::new(logger.clone(), graphql_runner, ws_port));

        start(logger, port, move |req| {
            let service = service.cheap_clone();
            async move { Ok::<_, _>(service.cheap_clone().call(req).await) }
        })
        .await
    }
}
