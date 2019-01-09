extern crate graph;
extern crate jsonrpc_http_server;
extern crate serde;

use graph::prelude::{JsonRpcServer as JsonRpcServerTrait, *};
use graph::serde_json;
use jsonrpc_http_server::{
    jsonrpc_core::{self, Compatibility, IoHandler, Params, Value},
    RestApi, Server, ServerBuilder,
};

use std::collections::BTreeMap;
use std::io;
use std::net::{Ipv4Addr, SocketAddrV4};

const JSON_RPC_DEPLOY_ERROR: i64 = 0;
const JSON_RPC_REMOVE_ERROR: i64 = 1;
const JSON_RPC_CREATE_ERROR: i64 = 2;
const JSON_RPC_INTERNAL_ERROR: i64 = 3;

#[derive(Debug, Deserialize)]
struct SubgraphCreateParams {
    name: SubgraphName,
}

#[derive(Debug, Deserialize)]
struct SubgraphDeployParams {
    name: SubgraphName,
    ipfs_hash: SubgraphId,
    node_id: Option<NodeId>,
}

#[derive(Debug, Deserialize)]
struct SubgraphRemoveParams {
    name: SubgraphName,
}

pub struct JsonRpcServer<R> {
    registrar: Arc<R>,
    http_port: u16,
    ws_port: u16,
    node_id: NodeId,
    logger: Logger,
}

impl<R> JsonRpcServer<R>
where
    R: SubgraphRegistrar,
{
    /// Handler for the `subgraph_create` endpoint.
    fn create_handler(
        &self,
        params: SubgraphCreateParams,
    ) -> Box<Future<Item = Value, Error = jsonrpc_core::Error> + Send> {
        let logger = self.logger.clone();

        info!(logger, "Received subgraph_create request"; "params" => format!("{:?}", params));

        Box::new(
            self.registrar
                .create_subgraph(params.name)
                .map_err(move |e| {
                    if let SubgraphRegistrarError::Unknown(e) = e {
                        error!(logger, "subgraph_create failed: {}", e);
                        json_rpc_error(JSON_RPC_CREATE_ERROR, "internal error".to_owned())
                    } else {
                        json_rpc_error(JSON_RPC_CREATE_ERROR, e.to_string())
                    }
                })
                .map(move |id| Value::String(id)),
        )
    }

    /// Handler for the `subgraph_deploy` endpoint.
    fn deploy_handler(
        &self,
        params: SubgraphDeployParams,
    ) -> Box<Future<Item = Value, Error = jsonrpc_core::Error> + Send> {
        let logger = self.logger.clone();

        info!(logger, "Received subgraph_deploy request"; "params" => format!("{:?}", params));

        let node_id = params.node_id.clone().unwrap_or(self.node_id.clone());
        let routes = subgraph_routes(&params.name, self.http_port, self.ws_port);

        Box::new(
            self.registrar
                .create_subgraph_version(params.name, params.ipfs_hash, node_id)
                .map_err(move |e| {
                    if let SubgraphRegistrarError::Unknown(e) = e {
                        error!(logger, "subgraph_deploy failed: {}", e);
                        json_rpc_error(JSON_RPC_DEPLOY_ERROR, "internal error".to_owned())
                    } else {
                        json_rpc_error(JSON_RPC_DEPLOY_ERROR, e.to_string())
                    }
                })
                .map(move |_| routes),
        )
    }

    /// Handler for the `subgraph_remove` endpoint.
    fn remove_handler(
        &self,
        params: SubgraphRemoveParams,
    ) -> Box<Future<Item = Value, Error = jsonrpc_core::Error> + Send> {
        let logger = self.logger.clone();

        info!(logger, "Received subgraph_remove request"; "params" => format!("{:?}", params));

        Box::new(
            self.registrar
                .remove_subgraph(params.name)
                .map_err(move |e| {
                    if let SubgraphRegistrarError::Unknown(e) = e {
                        error!(logger, "subgraph_remove failed: {}", e);
                        json_rpc_error(JSON_RPC_REMOVE_ERROR, "internal error".to_owned())
                    } else {
                        json_rpc_error(JSON_RPC_REMOVE_ERROR, e.to_string())
                    }
                })
                .map(|_| Ok(Value::Null))
                .flatten(),
        )
    }

    /// Handler for the `subgraph_list` endpoint.
    ///
    /// Returns the names of deployed subgraphs.
    fn list_handler(&self) -> Box<Future<Item = Value, Error = jsonrpc_core::Error> + Send> {
        let logger = self.logger.clone();

        info!(logger, "Received subgraph_list request");

        Box::new(
            self.registrar
                .list_subgraphs()
                .map_err(move |e| {
                    error!(logger, "Failed to list subgraphs: {}", e);
                    json_rpc_error(JSON_RPC_INTERNAL_ERROR, "database error".to_owned())
                })
                .map(|names| {
                    Value::from(
                        names
                            .into_iter()
                            .map(|name| name.to_string())
                            .collect::<Vec<_>>(),
                    )
                }),
        )
    }
}

impl<R> JsonRpcServerTrait<R> for JsonRpcServer<R>
where
    R: SubgraphRegistrar,
{
    type Server = Server;

    fn serve(
        port: u16,
        http_port: u16,
        ws_port: u16,
        registrar: Arc<R>,
        node_id: NodeId,
        logger: Logger,
    ) -> Result<Self::Server, io::Error> {
        let logger = logger.new(o!("component" => "JsonRpcServer"));

        info!(
            logger,
            "Starting JSON-RPC admin server at: http://localhost:{}", port
        );

        let addr = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port);

        let mut handler = IoHandler::with_compatibility(Compatibility::Both);

        let arc_self = Arc::new(JsonRpcServer {
            registrar,
            http_port,
            ws_port,
            node_id,
            logger,
        });

        let me = arc_self.clone();
        handler.add_method("subgraph_create", move |params: Params| {
            let me = me.clone();
            params
                .parse()
                .into_future()
                .and_then(move |params| me.create_handler(params))
        });

        let me = arc_self.clone();
        handler.add_method("subgraph_deploy", move |params: Params| {
            let me = me.clone();
            params
                .parse()
                .into_future()
                .and_then(move |params| me.deploy_handler(params))
        });

        let me = arc_self.clone();
        handler.add_method("subgraph_remove", move |params: Params| {
            let me = me.clone();
            params
                .parse()
                .into_future()
                .and_then(move |params| me.remove_handler(params))
        });

        let me = arc_self.clone();
        handler.add_method("subgraph_list", move |_| me.list_handler());

        ServerBuilder::new(handler)
            // Enable REST API:
            // POST /<method>/<param1>/<param2>
            .rest_api(RestApi::Secure)
            .start_http(&addr.into())
    }
}

fn json_rpc_error(code: i64, message: String) -> jsonrpc_core::Error {
    jsonrpc_core::Error {
        code: jsonrpc_core::ErrorCode::ServerError(code),
        message,
        data: None,
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

fn subgraph_routes(name: &SubgraphName, http_port: u16, ws_port: u16) -> Value {
    let mut map = BTreeMap::new();
    map.insert("playground", format!(":{}/name/{}", http_port, name));
    map.insert("queries", format!(":{}/name/{}/graphql", http_port, name));
    map.insert("subscriptions", format!(":{}/name/{}", ws_port, name));
    jsonrpc_core::to_value(map).unwrap()
}
