//! Error handling for the JSON-RPC admin server.
//!
//! This module defines error codes and mapping from `SubgraphRegistrarError`
//! to JSON-RPC error responses.

use graph::prelude::SubgraphRegistrarError;
use slog::{error, Logger};

use crate::jsonrpc::JsonRpcError;

/// Application-specific error codes for subgraph operations.
///
/// These codes are preserved from the original jsonrpsee implementation
/// to maintain backward compatibility with existing clients.
pub mod error_codes {
    pub const DEPLOY_ERROR: i64 = 0;
    pub const REMOVE_ERROR: i64 = 1;
    pub const CREATE_ERROR: i64 = 2;
    pub const REASSIGN_ERROR: i64 = 3;
    pub const PAUSE_ERROR: i64 = 4;
    pub const RESUME_ERROR: i64 = 5;
}

/// Convert a `SubgraphRegistrarError` to a `JsonRpcError`.
///
/// Logs the error and returns an appropriate JSON-RPC error response.
/// For `Unknown` errors, the message is masked as "internal error" to avoid
/// leaking sensitive information.
pub fn registrar_error_to_jsonrpc(
    logger: &Logger,
    operation: &str,
    e: SubgraphRegistrarError,
    code: i64,
    params: impl std::fmt::Debug,
) -> JsonRpcError {
    error!(logger, "{} failed", operation;
        "error" => format!("{:?}", e),
        "params" => format!("{:?}", params));

    let message = if let SubgraphRegistrarError::Unknown(_) = e {
        "internal error".to_owned()
    } else {
        e.to_string()
    };

    JsonRpcError::new(code, message)
}
