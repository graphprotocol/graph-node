extern crate jsonrpc_http_server;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate graph;

use graph::prelude::{JsonRpcServer as JsonRpcServerTrait, *};
use graph::serde_json;
use jsonrpc_http_server::{
    hyper::{header, Request, Response, StatusCode},
    jsonrpc_core::{
        self, Compatibility, Id, MetaIoHandler, Metadata, MethodCall, Params, Value, Version,
    },
    RequestMiddlewareAction, RestApi, Server, ServerBuilder,
};

use std::collections::BTreeMap;
use std::iter::FromIterator;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::{env, fmt, io};

const GRAPH_MASTER_TOKEN_VAR: &str = "GRAPH_MASTER_TOKEN";
const JSON_RPC_DEPLOY_ERROR: i64 = 0;
const JSON_RPC_REMOVE_ERROR: i64 = 1;
const JSON_RPC_UNAUTHORIZED_ERROR: i64 = 2;
const JSON_RPC_INTERNAL_ERROR: i64 = 3;

#[derive(Debug, Serialize, Deserialize)]
struct SubgraphDeployParams {
    name: String,
    ipfs_hash: String,
}

impl fmt::Display for SubgraphDeployParams {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SubgraphRemoveParams {
    name_or_id: String,
}

impl fmt::Display for SubgraphRemoveParams {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SubgraphAuthorizeParams {
    subgraph_api_keys: BTreeMap<String, String>,
}

impl fmt::Display for SubgraphAuthorizeParams {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Default)]
struct AuthorizationHeader {
    bearer_token: String,
}

impl Metadata for AuthorizationHeader {}

pub struct JsonRpcServer<P, S> {
    provider: Arc<P>,
    store: Arc<S>,
    logger: Logger,
}

impl<P, S> JsonRpcServer<P, S>
where
    P: SubgraphProvider,
    S: Store,
{
    fn require_master_token(auth: AuthorizationHeader) -> Result<(), jsonrpc_core::Error> {
        let master_token = env::var(GRAPH_MASTER_TOKEN_VAR);
        match &master_token {
            Ok(master_token) if *master_token == auth.bearer_token => (), // Authorized.
            Ok(_) => {
                return Err(json_rpc_error(
                    JSON_RPC_UNAUTHORIZED_ERROR,
                    "authorization token is invalid".to_owned(),
                ))
            }
            Err(_) => {
                return Err(json_rpc_error(
                    JSON_RPC_UNAUTHORIZED_ERROR,
                    "internal error".to_owned(),
                ))
            }
        }
        Ok(())
    }

    fn check_subgraph_auth(
        &self,
        subgraph_name: String,
        bearer_token: String,
    ) -> Result<(), jsonrpc_core::Error> {
        if !should_check_auth() {
            return Ok(());
        }

        let is_authd = self
            .store
            .check_subgraph_name_access_token(subgraph_name, bearer_token)
            .map_err(move |e| {
                error!(self.logger, "Failed to check access token: {}", e);
                json_rpc_error(JSON_RPC_INTERNAL_ERROR, "database error".to_owned())
            })?;

        if is_authd {
            return Ok(());
        }

        return Err(json_rpc_error(
            JSON_RPC_UNAUTHORIZED_ERROR,
            "API key is invalid".to_owned(),
        ));
    }

    /// Handler for the `subgraph_deploy` endpoint.
    fn deploy_handler(
        &self,
        params: SubgraphDeployParams,
        auth: AuthorizationHeader,
    ) -> Box<Future<Item = Value, Error = jsonrpc_core::Error> + Send> {
        let logger = self.logger.clone();

        info!(logger, "Received subgraph_deploy request"; "params" => params.to_string());

        if let Err(e) = self.check_subgraph_auth(params.name.clone(), auth.bearer_token) {
            return Box::new(future::err(e));
        }

        Box::new(
            self.provider
                .deploy(params.name, format!("/ipfs/{}", params.ipfs_hash))
                .map_err(move |e| {
                    if let SubgraphProviderError::Unknown(e) = e {
                        error!(logger, "subgraph_deploy failed: {}", e);
                        json_rpc_error(JSON_RPC_DEPLOY_ERROR, "internal error".to_owned())
                    } else {
                        json_rpc_error(JSON_RPC_DEPLOY_ERROR, e.to_string())
                    }
                }).map(|_| Ok(Value::Null))
                .flatten(),
        )
    }

    /// Handler for the `subgraph_remove` endpoint.
    fn remove_handler(
        &self,
        params: SubgraphRemoveParams,
        auth: AuthorizationHeader,
    ) -> Box<Future<Item = Value, Error = jsonrpc_core::Error> + Send> {
        let logger = self.logger.clone();

        info!(logger, "Received subgraph_remove request"; "params" => params.to_string());

        let name_or_id = params.name_or_id;
        if let Err(e) = self.check_subgraph_auth(name_or_id.clone(), auth.bearer_token) {
            return Box::new(future::err(e));
        }

        Box::new(
            self.provider
                .remove(name_or_id)
                .map_err(move |e| {
                    if let SubgraphProviderError::Unknown(e) = e {
                        error!(logger, "subgraph_remove failed: {}", e);
                        json_rpc_error(JSON_RPC_DEPLOY_ERROR, "internal error".to_owned())
                    } else {
                        json_rpc_error(JSON_RPC_REMOVE_ERROR, e.to_string())
                    }
                }).map(|_| Ok(Value::Null))
                .flatten(),
        )
    }

    /// Handler for the `subgraph_authorize` endpoint.
    ///
    /// Sets the access tokens for a set of subgraph names.
    ///
    /// Requires bearer authorization with the master token.
    fn authorize_handler(
        &self,
        params: SubgraphAuthorizeParams,
        auth: AuthorizationHeader,
    ) -> Result<Value, jsonrpc_core::Error> {
        info!(self.logger, "Received subgraph_authorize request"; "params" => params.to_string());
        Self::require_master_token(auth)?;

        for (subgraph_name, access_token) in params.subgraph_api_keys {
            let logger = self.logger.clone();

            self.store
                .authorize_subgraph_name(subgraph_name, access_token)
                .map_err(move |e| {
                    error!(logger, "Failed to authorize subgraph name: {}", e);
                    json_rpc_error(JSON_RPC_INTERNAL_ERROR, "database error".to_owned())
                })?;
        }

        Ok(Value::Null)
    }

    /// Handler for the `subgraph_list` endpoint.
    ///
    /// Returns the names and ids of deployed subgraphs.
    fn list_handler(&self) -> Result<Value, jsonrpc_core::Error> {
        let logger = self.logger.clone();

        info!(logger, "Received subgraph_list request");
        let list = self
            .provider
            .list()
            .map_err(move |e| {
                error!(logger, "Failed to list subgraphs: {}", e);
                json_rpc_error(JSON_RPC_INTERNAL_ERROR, "database error".to_owned())
            })?.into_iter()
            .map(|(name, id_opt)| (name, id_opt.map(Value::String).unwrap_or(Value::Null)));

        Ok(Value::from(serde_json::Map::from_iter(list)))
    }
}

impl<P, S> JsonRpcServerTrait<P, S> for JsonRpcServer<P, S>
where
    P: SubgraphProvider,
    S: Store,
{
    type Server = Server;

    fn serve(
        port: u16,
        provider: Arc<P>,
        store: Arc<S>,
        logger: Logger,
    ) -> Result<Self::Server, io::Error> {
        let addr = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port);

        let mut handler = MetaIoHandler::with_compatibility(Compatibility::Both);

        let arc_self = Arc::new(JsonRpcServer {
            provider,
            store,
            logger: logger.new(o!("component" => "JsonRpcServer")),
        });
        // `subgraph_deploy` handler.
        let me = arc_self.clone();
        handler.add_method_with_meta("subgraph_deploy", move |params: Params, auth| {
            let me = me.clone();
            params
                .parse()
                .into_future()
                .and_then(move |params| me.deploy_handler(params, auth))
        });

        // `subgraph_remove` handler.
        let me = arc_self.clone();
        handler.add_method_with_meta("subgraph_remove", move |params: Params, auth| {
            let me = me.clone();
            params
                .parse()
                .into_future()
                .and_then(move |params| me.remove_handler(params, auth))
        });

        // `subgraph_authorize` handler.
        let me = arc_self.clone();
        handler.add_method_with_meta("subgraph_authorize", move |params: Params, auth| {
            let me = me.clone();
            params
                .parse()
                .into_future()
                .and_then(move |params| me.authorize_handler(params, auth))
        });

        // `subgraph_list` handler.
        let me = arc_self.clone();
        handler.add_method_with_meta("subgraph_list", move |_, _| me.list_handler());

        /// Get the `Authorization: Bearer` header if present.
        fn auth_extractor(request: &Request) -> Option<AuthorizationHeader> {
            request
                .headers()
                .get::<header::Authorization<header::Bearer>>()
                .cloned()
                .map(|bearer| AuthorizationHeader {
                    bearer_token: bearer.token.clone(),
                })
        }

        /// Make sure requests contain a `Authorization: Bearer` header.
        fn require_auth(request: Request) -> RequestMiddlewareAction {
            if !should_check_auth() || auth_extractor(&request).is_some() {
                RequestMiddlewareAction::Proceed {
                    should_continue_on_invalid_cors: false,
                    request,
                }
            } else {
                let mut response = Response::new();
                response.set_status(StatusCode::Unauthorized);
                RequestMiddlewareAction::Respond {
                    should_validate_hosts: true,
                    response: Box::new(future::ok(response)),
                }
            }
        }

        ServerBuilder::with_meta_extractor(handler, |request: &Request| {
            if should_check_auth() {
                // The middleware guarantees the header it's present.
                auth_extractor(request).unwrap()
            } else {
                // Nobody should care about this value.
                AuthorizationHeader::default()
            }
        }).request_middleware(require_auth)
            // Enable REST API:
            // POST /<method>/<param1>/<param2>
            .rest_api(RestApi::Secure)
            .start_http(&addr.into())
    }
}

fn should_check_auth() -> bool {
    match env::var(GRAPH_MASTER_TOKEN_VAR) {
        Err(env::VarError::NotPresent) => false,
        _ => true,
    }
}

fn json_rpc_error(code: i64, message: String) -> jsonrpc_core::Error {
    jsonrpc_core::Error {
        code: jsonrpc_core::ErrorCode::ServerError(code),
        message,
        data: None,
    }
}

pub fn subgraph_deploy_request(name: String, ipfs_hash: String, id: String) -> MethodCall {
    let params = serde_json::to_value(SubgraphDeployParams { name, ipfs_hash })
        .unwrap()
        .as_object()
        .cloned()
        .unwrap();

    MethodCall {
        jsonrpc: Some(Version::V2),
        method: "subgraph_deploy".to_owned(),
        params: Params::Map(params),
        id: Id::Str(id),
    }
}

pub fn parse_response(response: Value) -> Result<(), jsonrpc_core::Error> {
    // serde deserialization of the `id` field to an `Id` struct is somehow
    // incompatible with the `arbitrary-precision` feature which we use, so we
    // need custom parsing logic.
    let object = response.as_object().unwrap();
    if let Some(error) = object.get("error") {
        Err(serde_json::from_value(error.clone()).unwrap())
    } else {
        Ok(())
    }
}
