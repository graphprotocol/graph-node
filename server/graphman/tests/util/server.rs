use std::sync::Arc;

use graph::prelude::LoggerFactory;
use graph_store_postgres::NotificationSender;
use graphman_server::GraphmanServer;
use graphman_server::GraphmanServerConfig;
use lazy_static::lazy_static;
use test_store::LOGGER;
use test_store::METRICS_REGISTRY;
use test_store::PRIMARY_POOL;
use test_store::STORE;
use tokio::sync::OnceCell;

pub const VALID_TOKEN: &str = "123";
pub const INVALID_TOKEN: &str = "abc";

pub const PORT: u16 = 8050;

lazy_static! {
    static ref SERVER: OnceCell<()> = OnceCell::new();
}

pub async fn start() {
    SERVER
        .get_or_init(|| async {
            let logger_factory = LoggerFactory::new(LOGGER.clone(), None, METRICS_REGISTRY.clone());
            let notification_sender = Arc::new(NotificationSender::new(METRICS_REGISTRY.clone()));

            let config = GraphmanServerConfig {
                pool: PRIMARY_POOL.clone(),
                notification_sender,
                store: STORE.clone(),
                logger_factory: &logger_factory,
                auth_token: VALID_TOKEN.to_string(),
            };

            let server = GraphmanServer::new(config).expect("graphman config is valid");

            server
                .start(PORT)
                .await
                .expect("graphman server starts successfully");
        })
        .await;
}
