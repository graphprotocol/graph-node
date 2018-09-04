use futures::prelude::*;
use graph::serde_json::Value;
use jsonrpc_core::types::Call;
use web3;
use web3::transports::http;
use web3::transports::ipc;
use web3::transports::ws;
use web3::RequestId;

pub use web3::transports::EventLoopHandle;

/// Abstraction over the different web3 transports.
#[derive(Clone, Debug)]
pub enum Transport {
    RPC(http::Http),
    IPC(ipc::Ipc),
    WS(ws::WebSocket),
}

impl Transport {
    /// Creates an IPC transport.
    pub fn new_ipc(ipc: &str) -> (EventLoopHandle, Self) {
        ipc::Ipc::new(ipc)
            .map(|(event_loop, transport)| (event_loop, Transport::IPC(transport)))
            .expect("Failed to connect to Ethereum RPC")
    }

    /// Creates a WebSocket transport.
    pub fn new_ws(ws: &str) -> (EventLoopHandle, Self) {
        ws::WebSocket::new(ws)
            .map(|(event_loop, transport)| (event_loop, Transport::WS(transport)))
            .expect("Failed to connect to Ethereum WS")
    }

    /// Creates a JSON-RPC over HTTP transport.
    ///
    /// Note: JSON-RPC over HTTP doesn't always support subscribing to new
    /// blocks (one such example is Infura's HTTP endpoint).
    pub fn new_rpc(rpc: &str) -> (EventLoopHandle, Self) {
        http::Http::new(rpc)
            .map(|(event_loop, transport)| (event_loop, Transport::RPC(transport)))
            .expect("Failed to connect to Ethereum WS")
    }
}

impl web3::Transport for Transport {
    type Out = Box<Future<Item = Value, Error = web3::error::Error> + Send>;

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
        Future<Item = Vec<Result<Value, web3::error::Error>>, Error = web3::error::Error> + Send,
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
