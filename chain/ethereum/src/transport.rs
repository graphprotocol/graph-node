use jsonrpc_core::types::Call;
use jsonrpc_core::Value;

use web3::transports::{http, ipc, ws};
use web3::RequestId;

use graph::prelude::*;
use std::future::Future;

/// Abstraction over the different web3 transports.
#[derive(Clone, Debug)]
pub enum Transport {
    RPC(http::Http),
    IPC(ipc::Ipc),
    WS(ws::WebSocket),
}

impl Transport {
    /// Creates an IPC transport.
    #[cfg(unix)]
    pub async fn new_ipc(ipc: &str) -> Self {
        ipc::Ipc::new(ipc)
            .await
            .map(|transport| Transport::IPC(transport))
            .expect("Failed to connect to Ethereum IPC")
    }

    /// Creates a WebSocket transport.
    pub async fn new_ws(ws: &str) -> Self {
        ws::WebSocket::new(ws)
            .await
            .map(|transport| Transport::WS(transport))
            .expect("Failed to connect to Ethereum WS")
    }

    /// Creates a JSON-RPC over HTTP transport.
    ///
    /// Note: JSON-RPC over HTTP doesn't always support subscribing to new
    /// blocks (one such example is Infura's HTTP endpoint).
    pub fn new_rpc(rpc: &str, headers: ::http::HeaderMap) -> Self {
        http::Http::with_headers(rpc, headers)
            .map(|transport| Transport::RPC(transport))
            .expect("Failed to connect to Ethereum RPC")
    }
}

impl web3::Transport for Transport {
    type Out = Box<dyn Future<Output = Result<Value, web3::error::Error>> + Send + Unpin>;

    fn prepare(&self, method: &str, params: Vec<Value>) -> (RequestId, Call) {
        match self {
            Transport::RPC(http) => http.prepare(method, params),
            Transport::IPC(ipc) => ipc.prepare(method, params),
            Transport::WS(ws) => ws.prepare(method, params),
        }
    }

    fn send(&self, id: RequestId, request: Call) -> Self::Out {
        match self {
            Transport::RPC(http) => Box::new(http.send(id, request)),
            Transport::IPC(ipc) => Box::new(ipc.send(id, request)),
            Transport::WS(ws) => Box::new(ws.send(id, request)),
        }
    }
}

impl web3::BatchTransport for Transport {
    type Batch = Box<
        dyn Future<Output = Result<Vec<Result<Value, web3::error::Error>>, web3::error::Error>>
            + Send
            + Unpin,
    >;

    fn send_batch<T>(&self, requests: T) -> Self::Batch
    where
        T: IntoIterator<Item = (RequestId, Call)>,
    {
        match self {
            Transport::RPC(http) => Box::new(http.send_batch(requests)),
            Transport::IPC(ipc) => Box::new(ipc.send_batch(requests)),
            Transport::WS(ws) => Box::new(ws.send_batch(requests)),
        }
    }
}
