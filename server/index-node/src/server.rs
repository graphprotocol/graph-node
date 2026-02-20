use graph::{
    amp,
    blockchain::BlockchainMap,
    components::{
        network_provider::AmpClients,
        server::server::{start, ServerHandle},
        store::Store,
    },
    prelude::*,
};

use crate::service::IndexNodeService;

/// A GraphQL server based on Hyper.
pub struct IndexNodeServer<S, AC> {
    logger: Logger,
    blockchain_map: Arc<BlockchainMap>,
    store: Arc<S>,
    link_resolver: Arc<dyn LinkResolver>,
    amp_clients: AmpClients<AC>,
}

impl<S, AC> IndexNodeServer<S, AC>
where
    S: Store,
    AC: amp::Client + Send + Sync + 'static,
{
    /// Creates a new GraphQL server.
    pub fn new(
        logger_factory: &LoggerFactory,
        blockchain_map: Arc<BlockchainMap>,
        store: Arc<S>,
        link_resolver: Arc<dyn LinkResolver>,
        amp_clients: AmpClients<AC>,
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
            blockchain_map,
            store,
            link_resolver,
            amp_clients,
        }
    }

    pub async fn start(&self, port: u16) -> Result<ServerHandle, anyhow::Error> {
        let logger = self.logger.clone();

        info!(
            logger,
            "Starting index node server at: http://localhost:{}", port
        );

        // On every incoming request, launch a new GraphQL service that writes
        // incoming queries to the query sink.
        let logger_for_service = self.logger.clone();
        let store = self.store.clone();
        let service = Arc::new(IndexNodeService::new(
            logger_for_service.clone(),
            self.blockchain_map.clone(),
            store,
            self.link_resolver.clone(),
            self.amp_clients.clone(),
        ));

        start(logger_for_service.clone(), port, move |req| {
            let service = service.clone();
            async move { Ok::<_, _>(service.call(req).await) }
        })
        .await
    }
}
