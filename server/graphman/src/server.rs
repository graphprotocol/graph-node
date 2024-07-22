use std::net::SocketAddr;
use std::sync::Arc;

use async_graphql::{EmptySubscription, Schema};
use axum::extract::Extension;
use axum::http::Method;
use axum::routing::get;
use axum::Router;
use graph::log::factory::LoggerFactory;
use graph::prelude::{ComponentLoggerConfig, ElasticComponentLoggerConfig};
use graph_store_postgres::connection_pool::ConnectionPool;
use graph_store_postgres::NotificationSender;
use graph_store_postgres::Store;
use slog::{info, Logger};
use tower_http::cors::{Any, CorsLayer};

use crate::auth::AuthToken;
use crate::handlers::graphql_playground_handler;
use crate::handlers::graphql_request_handler;
use crate::handlers::AppState;
use crate::resolvers::{MutationRoot, QueryRoot};
use crate::GraphmanServerError;

#[derive(Clone)]
pub struct GraphmanServer {
    pool: ConnectionPool,
    notification_sender: Arc<NotificationSender>,
    store: Arc<Store>,
    logger: Logger,
    auth_token: AuthToken,
    max_query_depth: usize,
    max_query_complexity: usize,
}

#[derive(Clone)]
pub struct GraphmanServerConfig<'a> {
    pub pool: ConnectionPool,
    pub notification_sender: Arc<NotificationSender>,
    pub store: Arc<Store>,
    pub logger_factory: &'a LoggerFactory,
    pub auth_token: String,
    pub max_query_depth: usize,
    pub max_query_complexity: usize,
}

pub struct GraphmanServerManager {
    handle: tokio::task::JoinHandle<()>,
}

impl GraphmanServer {
    pub fn new(config: GraphmanServerConfig) -> Result<Self, GraphmanServerError> {
        let GraphmanServerConfig {
            pool,
            notification_sender,
            store,
            logger_factory,
            auth_token,
            max_query_depth,
            max_query_complexity,
        } = config;

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
            logger,
            auth_token,
            max_query_depth,
            max_query_complexity,
        })
    }

    pub fn start(self, port: u16) -> GraphmanServerManager {
        let Self {
            pool,
            notification_sender,
            store,
            logger,
            auth_token,
            max_query_depth,
            max_query_complexity,
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
            .limit_depth(max_query_depth)
            .limit_complexity(max_query_complexity)
            .data(pool)
            .data(notification_sender)
            .data(store)
            .data(logger)
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

        let handle = tokio::spawn(async move {
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .unwrap_or_else(|err| panic!("Failed to bind graphman tcp listener: {err}"));

            axum::serve(listener, app)
                .await
                .unwrap_or_else(|err| panic!("Failed to start graphman server: {err}"));
        });

        GraphmanServerManager { handle }
    }
}

impl GraphmanServerManager {
    pub fn abort(self) {
        self.handle.abort();
    }
}
