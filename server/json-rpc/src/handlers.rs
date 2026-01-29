//! JSON-RPC request handlers for subgraph operations.
//!
//! This module implements the request dispatch and individual handlers
//! for each JSON-RPC method.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use graph::prelude::{
    DeploymentHash, NodeId, SubgraphName, SubgraphRegistrar, SubgraphRegistrarError, ENV_VARS,
};
use serde::Deserialize;
use serde_json::{self, Value as JsonValue};
use slog::{error, info, Logger};

use crate::jsonrpc::{JsonRpcError, JsonRpcId, JsonRpcRequest, JsonRpcResponse};

/// Application-specific error codes for subgraph operations.
mod error_codes {
    pub const DEPLOY_ERROR: i64 = 0;
    pub const REMOVE_ERROR: i64 = 1;
    pub const CREATE_ERROR: i64 = 2;
    pub const REASSIGN_ERROR: i64 = 3;
    pub const PAUSE_ERROR: i64 = 4;
    pub const RESUME_ERROR: i64 = 5;
}

/// Shared application state for the JSON-RPC server.
pub struct AppState<R> {
    pub registrar: Arc<R>,
    pub http_port: u16,
    pub node_id: NodeId,
    pub logger: Logger,
}

/// Main JSON-RPC request handler.
///
/// Processes incoming JSON-RPC requests, dispatches to the appropriate method handler,
/// and returns the response.
pub async fn jsonrpc_handler<R: SubgraphRegistrar>(
    State(state): State<Arc<AppState<R>>>,
    ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
    fn header<'a>(headers: &'a HeaderMap, key: &str) -> &'a str {
        headers
            .get(key)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("unset")
    }

    // Parse the JSON-RPC request
    let request: JsonRpcRequest = match serde_json::from_str(&body) {
        Ok(req) => req,
        Err(_) => {
            return (StatusCode::OK, Json(JsonRpcResponse::parse_error()));
        }
    };

    // Validate JSON-RPC version
    if !request.is_valid_version() {
        return (StatusCode::OK, Json(JsonRpcResponse::invalid_request()));
    }

    let id = request.id.clone().unwrap_or(JsonRpcId::Null);

    // Log the method call
    info!(
        &state.logger,
        "JSON-RPC call";
        "method" => &request.method,
        "params" => ?request.params,
        "remote_addr" => %remote_addr,
        "x_forwarded_for" => header(&headers, "x-forwarded-for"),
        "x_real_ip" => header(&headers, "x-real-ip"),
        "x_forwarded_proto" => header(&headers, "x-forwarded-proto")
    );

    // Dispatch to the appropriate handler
    let response = match request.method.as_str() {
        "subgraph_create" => handle_create(&state, &request, id.clone()).await,
        "subgraph_deploy" => handle_deploy(&state, &request, id.clone()).await,
        "subgraph_remove" => handle_remove(&state, &request, id.clone()).await,
        "subgraph_reassign" => handle_reassign(&state, &request, id.clone()).await,
        "subgraph_pause" => handle_pause(&state, &request, id.clone()).await,
        "subgraph_resume" => handle_resume(&state, &request, id.clone()).await,
        _ => JsonRpcResponse::error(id, JsonRpcError::method_not_found()),
    };

    (StatusCode::OK, Json(response))
}

/// Parse parameters from a JSON-RPC request.
#[allow(clippy::result_large_err)]
fn parse_params<T: for<'de> Deserialize<'de>>(
    request: &JsonRpcRequest,
    id: JsonRpcId,
) -> Result<T, JsonRpcResponse> {
    let params = request.params.clone().unwrap_or(JsonValue::Null);
    serde_json::from_value(params).map_err(|e| {
        JsonRpcResponse::error(
            id,
            JsonRpcError::invalid_params(format!("Invalid params: {}", e)),
        )
    })
}

/// Convert a registrar result to a JSON-RPC response.
fn to_response<P: Debug>(
    logger: &Logger,
    method: &str,
    error_code: i64,
    params: &P,
    result: Result<JsonValue, SubgraphRegistrarError>,
    id: JsonRpcId,
) -> JsonRpcResponse {
    match result {
        Ok(value) => JsonRpcResponse::success(id, value),
        Err(e) => {
            error!(logger, "{} failed", method;
                "error" => format!("{:?}", e),
                "params" => format!("{:?}", params));

            let message = if let SubgraphRegistrarError::Unknown(_) = e {
                "internal error".to_owned()
            } else {
                e.to_string()
            };

            JsonRpcResponse::error(id, JsonRpcError::new(error_code, message))
        }
    }
}

/// Handler for `subgraph_create`.
async fn handle_create<R: SubgraphRegistrar>(
    state: &AppState<R>,
    request: &JsonRpcRequest,
    id: JsonRpcId,
) -> JsonRpcResponse {
    #[derive(Debug, Deserialize)]
    pub struct SubgraphCreateParams {
        pub name: SubgraphName,
    }

    let params: SubgraphCreateParams = match parse_params(request, id.clone()) {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    let result = state
        .registrar
        .create_subgraph(params.name.clone())
        .await
        .map(|r| serde_json::to_value(r).expect("invalid result"));

    to_response(
        &state.logger,
        "subgraph_create",
        error_codes::CREATE_ERROR,
        &params,
        result,
        id,
    )
}

/// Handler for `subgraph_deploy`.
async fn handle_deploy<R: SubgraphRegistrar>(
    state: &AppState<R>,
    request: &JsonRpcRequest,
    id: JsonRpcId,
) -> JsonRpcResponse {
    #[derive(Debug, Deserialize)]
    pub struct SubgraphDeployParams {
        pub name: SubgraphName,
        pub ipfs_hash: DeploymentHash,
        pub node_id: Option<NodeId>,
        pub debug_fork: Option<DeploymentHash>,
        pub history_blocks: Option<i32>,
    }

    let params: SubgraphDeployParams = match parse_params(request, id.clone()) {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    let node_id = params.node_id.clone().unwrap_or(state.node_id.clone());

    let result = state
        .registrar
        .create_subgraph_version(
            params.name.clone(),
            params.ipfs_hash.clone(),
            node_id,
            params.debug_fork.clone(),
            // Here it doesn't make sense to receive another
            // startBlock, we'll use the one from the manifest.
            None,
            None,
            params.history_blocks,
            false,
        )
        .await
        .map(|_| subgraph_routes(&params.name, state.http_port));

    to_response(
        &state.logger,
        "subgraph_deploy",
        error_codes::DEPLOY_ERROR,
        &params,
        result,
        id,
    )
}

/// Handler for `subgraph_remove`.
async fn handle_remove<R: SubgraphRegistrar>(
    state: &AppState<R>,
    request: &JsonRpcRequest,
    id: JsonRpcId,
) -> JsonRpcResponse {
    #[derive(Debug, Deserialize)]
    pub struct SubgraphRemoveParams {
        pub name: SubgraphName,
    }

    let params: SubgraphRemoveParams = match parse_params(request, id.clone()) {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    let result = state
        .registrar
        .remove_subgraph(params.name.clone())
        .await
        .map(|_| JsonValue::Null);

    to_response(
        &state.logger,
        "subgraph_remove",
        error_codes::REMOVE_ERROR,
        &params,
        result,
        id,
    )
}

/// Handler for `subgraph_reassign`.
async fn handle_reassign<R: SubgraphRegistrar>(
    state: &AppState<R>,
    request: &JsonRpcRequest,
    id: JsonRpcId,
) -> JsonRpcResponse {
    #[derive(Debug, Deserialize)]
    pub struct SubgraphReassignParams {
        pub ipfs_hash: DeploymentHash,
        pub node_id: NodeId,
    }

    let params: SubgraphReassignParams = match parse_params(request, id.clone()) {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    let result = state
        .registrar
        .reassign_subgraph(&params.ipfs_hash, &params.node_id)
        .await
        .map(|_| JsonValue::Null);

    to_response(
        &state.logger,
        "subgraph_reassign",
        error_codes::REASSIGN_ERROR,
        &params,
        result,
        id,
    )
}

// Parameter structs for pause and resume

#[derive(Debug, Deserialize)]
pub struct SubgraphPauseParams {
    pub deployment: DeploymentHash,
}

/// Handler for `subgraph_pause`.
async fn handle_pause<R: SubgraphRegistrar>(
    state: &AppState<R>,
    request: &JsonRpcRequest,
    id: JsonRpcId,
) -> JsonRpcResponse {
    let params: SubgraphPauseParams = match parse_params(request, id.clone()) {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    let result = state
        .registrar
        .pause_subgraph(&params.deployment)
        .await
        .map(|_| JsonValue::Null);

    to_response(
        &state.logger,
        "subgraph_pause",
        error_codes::PAUSE_ERROR,
        &params,
        result,
        id,
    )
}

/// Handler for `subgraph_resume`.
async fn handle_resume<R: SubgraphRegistrar>(
    state: &AppState<R>,
    request: &JsonRpcRequest,
    id: JsonRpcId,
) -> JsonRpcResponse {
    let params: SubgraphPauseParams = match parse_params(request, id.clone()) {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    let result = state
        .registrar
        .resume_subgraph(&params.deployment)
        .await
        .map(|_| JsonValue::Null);

    to_response(
        &state.logger,
        "subgraph_resume",
        error_codes::RESUME_ERROR,
        &params,
        result,
        id,
    )
}

/// Build the subgraph routes response for deploy.
fn subgraph_routes(name: &SubgraphName, http_port: u16) -> JsonValue {
    let http_base_url = ENV_VARS
        .external_http_base_url
        .clone()
        .unwrap_or_else(|| format!(":{}", http_port));

    let mut map = BTreeMap::new();
    map.insert(
        "playground",
        format!("{}/subgraphs/name/{}/graphql", http_base_url, name),
    );
    map.insert(
        "queries",
        format!("{}/subgraphs/name/{}", http_base_url, name),
    );

    serde_json::to_value(map).expect("invalid subgraph routes")
}
