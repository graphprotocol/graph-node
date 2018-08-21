extern crate jsonrpc_http_server;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate graph;

use graph::prelude::{JsonRpcServer as JsonRpcServerTrait, *};
use graph::serde_json;
use jsonrpc_http_server::{
    jsonrpc_core::{self, IoHandler, Params, Value},
    RestApi, Server, ServerBuilder,
};
use std::fmt;
use std::io;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

#[derive(Debug, Serialize, Deserialize)]
struct SubgraphAddParams {
    name: String,
    ipfs_hash: String,
}

impl fmt::Display for SubgraphAddParams {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{:?}", self)
    }
}

pub struct JsonRpcServer {}

impl JsonRpcServerTrait for JsonRpcServer {
    type Server = Server;

    fn serve(
        port: u16,
        provider: Arc<impl SubgraphProvider>,
        logger: Logger,
    ) -> Result<Self::Server, io::Error> {
        let addr = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port);

        let mut handler = IoHandler::new();

        // `subgraph_add` handler.
        let add_provider = provider.clone();
        let add_logger = logger.clone();
        let subgraph_added = AtomicBool::new(false);
        let add_handler = move |params: SubgraphAddParams| {
            let provider = add_provider.clone();
            info!(add_logger, "Received subgraph_add request"; "params" => params.to_string());
            provider
                .add(format!("/ipfs/{}", params.ipfs_hash))
                .map_err(|e| json_rpc_error(0, e.to_string()))
                .map(|_| Ok(Value::Null))
                .flatten()
        };
        handler.add_method("subgraph_add", move |params: Params| {
            let handler = add_handler.clone();
            let added = subgraph_added.load(Ordering::SeqCst);
            subgraph_added.store(true, Ordering::SeqCst);
            if added {
                Err(json_rpc_error(
                    1,
                    "adding multiple subgraphs is not yet supported".to_owned(),
                ))
            } else {
                Ok(())
            }.into_future()
                .and_then(|_| params.parse())
                .and_then(handler)
        });

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
