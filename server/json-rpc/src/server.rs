//! Axum-based JSON-RPC server implementation.
//!
//! This module provides the `JsonRpcServer` that serves JSON-RPC requests
//! over HTTP using axum.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use axum::routing::post;
use axum::Router;
use graph::prelude::{NodeId, SubgraphRegistrar};
use slog::{info, Logger};
use thiserror::Error;
use tokio::sync::Notify;

use crate::handlers::{jsonrpc_handler, AppState};

/// Errors that can occur when starting the JSON-RPC server.
#[derive(Debug, Error)]
pub enum JsonRpcServerError {
    #[error("Failed to bind to address: {0}")]
    Bind(#[from] std::io::Error),
}

/// Handle to a running JSON-RPC server.
///
/// Dropping this handle does not stop the server. Use `stop()` for graceful shutdown.
pub struct JsonRpcServer {
    notify: Arc<Notify>,
}

impl JsonRpcServer {
    /// Start the JSON-RPC server.
    ///
    /// # Arguments
    ///
    /// * `port` - The port to listen on for JSON-RPC requests
    /// * `http_port` - The HTTP port used for subgraph route URLs in deploy responses
    /// * `registrar` - The subgraph registrar for handling operations
    /// * `node_id` - Default node ID for deployments
    /// * `logger` - Logger for request/response logging
    pub async fn serve<R>(
        port: u16,
        http_port: u16,
        registrar: Arc<R>,
        node_id: NodeId,
        logger: Logger,
    ) -> Result<Self, JsonRpcServerError>
    where
        R: SubgraphRegistrar,
    {
        let logger = logger.new(graph::prelude::o!("component" => "JsonRpcServer"));

        info!(
            logger,
            "Starting JSON-RPC admin server at: http://localhost:{}", port
        );

        let state = Arc::new(AppState {
            registrar,
            http_port,
            node_id,
            logger,
        });

        let app = Router::new()
            .route("/", post(jsonrpc_handler::<R>))
            .with_state(state);

        let addr: SocketAddr = (Ipv4Addr::new(0, 0, 0, 0), port).into();
        let listener = tokio::net::TcpListener::bind(addr).await?;

        let notify = Arc::new(Notify::new());
        let notify_clone = notify.clone();

        graph::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async move {
                notify_clone.notified().await;
            })
            .await
            .unwrap_or_else(|err| panic!("JSON-RPC server failed: {err}"));
        });

        Ok(Self { notify })
    }

    /// Stop the server gracefully.
    pub fn stop(self) {
        self.notify.notify_one();
    }
}
