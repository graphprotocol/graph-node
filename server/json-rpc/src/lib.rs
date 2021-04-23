extern crate graph;
extern crate jsonrpc_http_server;
extern crate lazy_static;
extern crate serde;

use graph::prelude::futures03::channel::{mpsc, oneshot};
use graph::prelude::futures03::SinkExt;
use graph::prelude::serde_json;
use graph::prelude::{JsonRpcServer as JsonRpcServerTrait, *};
use jsonrpc_http_server::{
    jsonrpc_core::{self, Compatibility, IoHandler, Params, Value},
    RestApi, Server, ServerBuilder,
};
use lazy_static::lazy_static;

use std::collections::BTreeMap;
use std::env;
use std::io;
use std::net::{Ipv4Addr, SocketAddrV4};

lazy_static! {
    static ref EXTERNAL_HTTP_BASE_URL: Option<String> = env::var_os("EXTERNAL_HTTP_BASE_URL")
        .map(|s| s.into_string().expect("invalid external HTTP base URL"));
    static ref EXTERNAL_WS_BASE_URL: Option<String> = env::var_os("EXTERNAL_WS_BASE_URL")
        .map(|s| s.into_string().expect("invalid external WS base URL"));
}

const JSON_RPC_DEPLOY_ERROR: i64 = 0;
const JSON_RPC_REMOVE_ERROR: i64 = 1;
const JSON_RPC_CREATE_ERROR: i64 = 2;
const JSON_RPC_REASSIGN_ERROR: i64 = 3;

#[derive(Debug, Deserialize)]
struct SubgraphCreateParams {
    name: SubgraphName,
}

#[derive(Debug, Deserialize)]
struct SubgraphDeployParams {
    name: SubgraphName,
    ipfs_hash: DeploymentHash,
    node_id: Option<NodeId>,
}

#[derive(Debug, Deserialize)]
struct SubgraphRemoveParams {
    name: SubgraphName,
}

#[derive(Debug, Deserialize)]
struct SubgraphReassignParams {
    ipfs_hash: DeploymentHash,
    node_id: NodeId,
}

pub struct JsonRpcServer<R> {
    registrar: Arc<R>,
    http_port: u16,
    ws_port: u16,
    node_id: NodeId,
    logger: Logger,
}

impl<R: SubgraphRegistrar> JsonRpcServer<R> {
    /// Handler for the `subgraph_create` endpoint.
    async fn create_handler(
        &self,
        params: SubgraphCreateParams,
    ) -> Result<Value, jsonrpc_core::Error> {
        info!(&self.logger, "Received subgraph_create request"; "params" => format!("{:?}", params));

        match self.registrar.create_subgraph(params.name.clone()).await {
            Ok(result) => {
                Ok(serde_json::to_value(result).expect("invalid subgraph creation result"))
            }
            Err(e) => Err(json_rpc_error(
                &self.logger,
                "subgraph_create",
                e,
                JSON_RPC_CREATE_ERROR,
                params,
            )),
        }
    }

    /// Handler for the `subgraph_deploy` endpoint.
    async fn deploy_handler(
        &self,
        params: SubgraphDeployParams,
    ) -> Result<Value, jsonrpc_core::Error> {
        info!(&self.logger, "Received subgraph_deploy request"; "params" => format!("{:?}", params));

        let node_id = params.node_id.clone().unwrap_or(self.node_id.clone());
        let routes = subgraph_routes(&params.name, self.http_port, self.ws_port);
        match self
            .registrar
            .create_subgraph_version(params.name.clone(), params.ipfs_hash.clone(), node_id)
            .await
        {
            Ok(_) => Ok(routes),
            Err(e) => Err(json_rpc_error(
                &self.logger,
                "subgraph_deploy",
                e,
                JSON_RPC_DEPLOY_ERROR,
                params,
            )),
        }
    }

    /// Handler for the `subgraph_remove` endpoint.
    async fn remove_handler(
        &self,
        params: SubgraphRemoveParams,
    ) -> Result<Value, jsonrpc_core::Error> {
        info!(&self.logger, "Received subgraph_remove request"; "params" => format!("{:?}", params));

        match self.registrar.remove_subgraph(params.name.clone()).await {
            Ok(_) => Ok(Value::Null),
            Err(e) => Err(json_rpc_error(
                &self.logger,
                "subgraph_remove",
                e,
                JSON_RPC_REMOVE_ERROR,
                params,
            )),
        }
    }

    /// Handler for the `subgraph_assign` endpoint.
    async fn reassign_handler(
        &self,
        params: SubgraphReassignParams,
    ) -> Result<Value, jsonrpc_core::Error> {
        let logger = self.logger.clone();

        info!(logger, "Received subgraph_reassignment request"; "params" => format!("{:?}", params));

        match self
            .registrar
            .reassign_subgraph(&params.ipfs_hash, &params.node_id)
            .await
        {
            Ok(_) => Ok(Value::Null),
            Err(e) => Err(json_rpc_error(
                &logger,
                "subgraph_reassign",
                e,
                JSON_RPC_REASSIGN_ERROR,
                params,
            )),
        }
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

        let (task_sender, task_receiver) =
            mpsc::channel::<Box<dyn std::future::Future<Output = ()> + Send + Unpin>>(100);
        graph::spawn(task_receiver.for_each(|f| {
            async {
                // Blocking due to store interactions. Won't be blocking after #905.
                graph::spawn_blocking(f);
            }
        }));

        // This is a hack required because the json-rpc crate is not updated to tokio 0.2.
        // We should watch the `jsonrpsee` crate and switch to that once it's ready.
        async fn tokio02_spawn<I: Send + 'static, ER: Send + 'static>(
            mut task_sink: mpsc::Sender<Box<dyn std::future::Future<Output = ()> + Send + Unpin>>,
            future: impl std::future::Future<Output = Result<I, ER>> + Send + Unpin + 'static,
        ) -> Result<I, ER>
        where
            I: Debug,
            ER: Debug,
        {
            let (return_sender, return_receiver) = oneshot::channel();
            task_sink
                .send(Box::new(future.map(move |res| {
                    return_sender.send(res).expect("`return_receiver` dropped");
                })))
                .await
                .expect("task receiver dropped");
            return_receiver.await.expect("`return_sender` dropped")
        }

        let me = arc_self.clone();
        let sender = task_sender.clone();
        handler.add_method("subgraph_create", move |params: Params| {
            let me = me.clone();
            Box::pin(tokio02_spawn(
                sender.clone(),
                async move {
                    let params = params.parse()?;
                    me.create_handler(params).await
                }
                .boxed(),
            ))
            .compat()
        });

        let me = arc_self.clone();
        let sender = task_sender.clone();

        handler.add_method("subgraph_deploy", move |params: Params| {
            let me = me.clone();
            Box::pin(tokio02_spawn(
                sender.clone(),
                async move {
                    let params = params.parse()?;
                    me.deploy_handler(params).await
                }
                .boxed(),
            ))
            .compat()
        });

        let me = arc_self.clone();
        let sender = task_sender.clone();
        handler.add_method("subgraph_remove", move |params: Params| {
            let me = me.clone();
            Box::pin(tokio02_spawn(
                sender.clone(),
                async move {
                    let params = params.parse()?;
                    me.remove_handler(params).await
                }
                .boxed(),
            ))
            .compat()
        });

        let me = arc_self.clone();
        let sender = task_sender.clone();
        handler.add_method("subgraph_reassign", move |params: Params| {
            let me = me.clone();
            Box::pin(tokio02_spawn(
                sender.clone(),
                async move {
                    let params = params.parse()?;
                    me.reassign_handler(params).await
                }
                .boxed(),
            ))
            .compat()
        });

        ServerBuilder::new(handler)
            // Enable REST API:
            // POST /<method>/<param1>/<param2>
            .rest_api(RestApi::Secure)
            .start_http(&addr.into())
    }
}

fn json_rpc_error(
    logger: &Logger,
    operation: &str,
    e: SubgraphRegistrarError,
    code: i64,
    params: impl std::fmt::Debug,
) -> jsonrpc_core::Error {
    error!(logger, "{} failed", operation;
        "error" => format!("{:?}", e),
        "params" => format!("{:?}", params));

    let message = if let SubgraphRegistrarError::Unknown(_) = e {
        "internal error".to_owned()
    } else {
        e.to_string()
    };

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
    let http_base_url = EXTERNAL_HTTP_BASE_URL
        .clone()
        .unwrap_or_else(|| format!(":{}", http_port));
    let ws_base_url = EXTERNAL_WS_BASE_URL
        .clone()
        .unwrap_or_else(|| format!(":{}", ws_port));

    let mut map = BTreeMap::new();
    map.insert(
        "playground",
        format!("{}/subgraphs/name/{}/graphql", http_base_url, name),
    );
    map.insert(
        "queries",
        format!("{}/subgraphs/name/{}", http_base_url, name),
    );
    map.insert(
        "subscriptions",
        format!("{}/subgraphs/name/{}", ws_base_url, name),
    );
    jsonrpc_core::to_value(map).unwrap()
}
