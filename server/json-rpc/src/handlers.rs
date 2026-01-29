//! JSON-RPC request handlers for subgraph operations.
//!
//! This module implements the request dispatch and individual handlers
//! for each JSON-RPC method.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use graph::prelude::{
    DeploymentHash, NodeId, SubgraphName, SubgraphRegistrar, Value as GraphValue, ENV_VARS,
};
use serde::Deserialize;
use serde_json::{self, Value as JsonValue};
use slog::{info, Logger};

use crate::error::{error_codes, registrar_error_to_jsonrpc};
use crate::jsonrpc::{JsonRpcError, JsonRpcId, JsonRpcRequest, JsonRpcResponse};

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
    // Log the incoming request with proxy headers
    info!(
        &state.logger,
        "JSON-RPC request";
        "remote_addr" => %remote_addr,
        "x_forwarded_for" => headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()),
        "x_real_ip" => headers.get("x-real-ip").and_then(|v| v.to_str().ok()),
        "x_forwarded_proto" => headers.get("x-forwarded-proto").and_then(|v| v.to_str().ok())
    );

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
        "params" => ?request.params
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

/// Handler for `subgraph_create`.
async fn handle_create<R: SubgraphRegistrar>(
    state: &AppState<R>,
    request: &JsonRpcRequest,
    id: JsonRpcId,
) -> JsonRpcResponse {
    let params: SubgraphCreateParams = match parse_params(request, id.clone()) {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    info!(&state.logger, "Received subgraph_create request"; "params" => format!("{:?}", params));

    match state.registrar.create_subgraph(params.name.clone()).await {
        Ok(result) => {
            let value = serde_json::to_value(result).expect("invalid subgraph creation result");
            JsonRpcResponse::success(id, value)
        }
        Err(e) => {
            let error = registrar_error_to_jsonrpc(
                &state.logger,
                "subgraph_create",
                e,
                error_codes::CREATE_ERROR,
                &params,
            );
            JsonRpcResponse::error(id, error)
        }
    }
}

/// Handler for `subgraph_deploy`.
async fn handle_deploy<R: SubgraphRegistrar>(
    state: &AppState<R>,
    request: &JsonRpcRequest,
    id: JsonRpcId,
) -> JsonRpcResponse {
    let params: SubgraphDeployParams = match parse_params(request, id.clone()) {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    info!(&state.logger, "Received subgraph_deploy request"; "params" => format!("{:?}", params));

    let node_id = params.node_id.clone().unwrap_or(state.node_id.clone());
    let routes = subgraph_routes(&params.name, state.http_port);

    match state
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
    {
        Ok(_) => JsonRpcResponse::success(id, routes),
        Err(e) => {
            let error = registrar_error_to_jsonrpc(
                &state.logger,
                "subgraph_deploy",
                e,
                error_codes::DEPLOY_ERROR,
                &params,
            );
            JsonRpcResponse::error(id, error)
        }
    }
}

/// Handler for `subgraph_remove`.
async fn handle_remove<R: SubgraphRegistrar>(
    state: &AppState<R>,
    request: &JsonRpcRequest,
    id: JsonRpcId,
) -> JsonRpcResponse {
    let params: SubgraphRemoveParams = match parse_params(request, id.clone()) {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    info!(&state.logger, "Received subgraph_remove request"; "params" => format!("{:?}", params));

    match state.registrar.remove_subgraph(params.name.clone()).await {
        Ok(_) => JsonRpcResponse::success(id, serde_json::to_value(GraphValue::Null).unwrap()),
        Err(e) => {
            let error = registrar_error_to_jsonrpc(
                &state.logger,
                "subgraph_remove",
                e,
                error_codes::REMOVE_ERROR,
                &params,
            );
            JsonRpcResponse::error(id, error)
        }
    }
}

/// Handler for `subgraph_reassign`.
async fn handle_reassign<R: SubgraphRegistrar>(
    state: &AppState<R>,
    request: &JsonRpcRequest,
    id: JsonRpcId,
) -> JsonRpcResponse {
    let params: SubgraphReassignParams = match parse_params(request, id.clone()) {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    info!(&state.logger, "Received subgraph_reassignment request"; "params" => format!("{:?}", params));

    match state
        .registrar
        .reassign_subgraph(&params.ipfs_hash, &params.node_id)
        .await
    {
        Ok(_) => JsonRpcResponse::success(id, serde_json::to_value(GraphValue::Null).unwrap()),
        Err(e) => {
            let error = registrar_error_to_jsonrpc(
                &state.logger,
                "subgraph_reassign",
                e,
                error_codes::REASSIGN_ERROR,
                &params,
            );
            JsonRpcResponse::error(id, error)
        }
    }
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

    info!(&state.logger, "Received subgraph_pause request"; "params" => format!("{:?}", params));

    match state.registrar.pause_subgraph(&params.deployment).await {
        Ok(_) => JsonRpcResponse::success(id, serde_json::to_value(GraphValue::Null).unwrap()),
        Err(e) => {
            let error = registrar_error_to_jsonrpc(
                &state.logger,
                "subgraph_pause",
                e,
                error_codes::PAUSE_ERROR,
                &params,
            );
            JsonRpcResponse::error(id, error)
        }
    }
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

    info!(&state.logger, "Received subgraph_resume request"; "params" => format!("{:?}", params));

    match state.registrar.resume_subgraph(&params.deployment).await {
        Ok(_) => JsonRpcResponse::success(id, serde_json::to_value(GraphValue::Null).unwrap()),
        Err(e) => {
            let error = registrar_error_to_jsonrpc(
                &state.logger,
                "subgraph_resume",
                e,
                error_codes::RESUME_ERROR,
                &params,
            );
            JsonRpcResponse::error(id, error)
        }
    }
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

// Parameter structs for each method

#[derive(Debug, Deserialize)]
pub struct SubgraphCreateParams {
    pub name: SubgraphName,
}

#[derive(Debug, Deserialize)]
pub struct SubgraphDeployParams {
    pub name: SubgraphName,
    pub ipfs_hash: DeploymentHash,
    pub node_id: Option<NodeId>,
    pub debug_fork: Option<DeploymentHash>,
    pub history_blocks: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct SubgraphRemoveParams {
    pub name: SubgraphName,
}

#[derive(Debug, Deserialize)]
pub struct SubgraphReassignParams {
    pub ipfs_hash: DeploymentHash,
    pub node_id: NodeId,
}

#[derive(Debug, Deserialize)]
pub struct SubgraphPauseParams {
    pub deployment: DeploymentHash,
}
