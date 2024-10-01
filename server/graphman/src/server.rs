use std::net::SocketAddr;
use std::sync::Arc;

use async_graphql::EmptySubscription;
use async_graphql::Schema;
use axum::extract::Extension;
use axum::http::Method;
use axum::routing::get;
use axum::Router;
use graph::log::factory::LoggerFactory;
use graph::prelude::ComponentLoggerConfig;
use graph::prelude::ElasticComponentLoggerConfig;
use graph_store_postgres::connection_pool::ConnectionPool;
use graph_store_postgres::graphman::GraphmanStore;
use graph_store_postgres::NotificationSender;
use graph_store_postgres::Store;
use slog::{info, Logger};
use tokio::sync::Notify;
use tower_http::cors::{Any, CorsLayer};

use crate::auth::AuthToken;
use crate::handlers::graphql_playground_handler;
use crate::handlers::graphql_request_handler;
use crate::handlers::AppState;
use crate::resolvers::MutationRoot;
use crate::resolvers::QueryRoot;
use crate::GraphmanServerError;

#[derive(Clone)]
pub struct GraphmanServer {
    pool: ConnectionPool,
    notification_sender: Arc<NotificationSender>,
    store: Arc<Store>,
    graphman_store: Arc<GraphmanStore>,
    logger: Logger,
    auth_token: AuthToken,
}

#[derive(Clone)]
pub struct GraphmanServerConfig<'a> {
    pub pool: ConnectionPool,
    pub notification_sender: Arc<NotificationSender>,
    pub store: Arc<Store>,
    pub logger_factory: &'a LoggerFactory,
    pub auth_token: String,
}

pub struct GraphmanServerManager {
    notify: Arc<Notify>,
}

impl GraphmanServer {
    pub fn new(config: GraphmanServerConfig) -> Result<Self, GraphmanServerError> {
        let GraphmanServerConfig {
            pool,
            notification_sender,
            store,
            logger_factory,
            auth_token,
        } = config;

        let graphman_store = Arc::new(GraphmanStore::new(pool.clone()));
        let auth_token = AuthToken::new(auth_token)?;

        let logger = logger_factory.component_logger(
            "GraphmanServer",
            Some(ComponentLoggerConfig {
                elastic: Some(ElasticComponentLoggerConfig {
                    index: String::from("graphman-server-logs"),
                }),
            }),
        );

        Ok(Self {
            pool,
            notification_sender,
            store,
            graphman_store,
            logger,
            auth_token,
        })
    }

    pub async fn start(self, port: u16) -> Result<GraphmanServerManager, GraphmanServerError> {
        let Self {
            pool,
            notification_sender,
            store,
            graphman_store,
            logger,
            auth_token,
        } = self;

        info!(
            logger,
            "Starting graphman server at: http://localhost:{}", port,
        );

        let app_state = Arc::new(AppState { auth_token });

        let cors_layer = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods([Method::GET, Method::OPTIONS, Method::POST])
            .allow_headers(Any);

        let schema = Schema::build(QueryRoot, MutationRoot, EmptySubscription)
            .data(pool)
            .data(notification_sender)
            .data(store)
            .data(graphman_store)
            .finish();

        let app = Router::new()
            .route(
                "/",
                get(graphql_playground_handler).post(graphql_request_handler),
            )
            .with_state(app_state)
            .layer(cors_layer)
            .layer(Extension(schema));

        let addr = SocketAddr::from(([0, 0, 0, 0], port));

        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|err| GraphmanServerError::Io(err.into()))?;

        let notify = Arc::new(Notify::new());
        let notify_clone = notify.clone();

        graph::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    notify_clone.notified().await;
                })
                .await
                .unwrap_or_else(|err| panic!("Failed to start graphman server: {err}"));
        });

        Ok(GraphmanServerManager { notify })
    }
}

impl GraphmanServerManager {
    pub fn stop_server(self) {
        self.notify.notify_one()
    }
}
