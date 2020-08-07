use jsonrpc_core::types::Call;
use serde_json::Value;

use web3::transports::{http, ws};
use web3::RequestId;

use graph::prelude::*;

use super::config::ETHEREUM_CONFIG;

/// Abstraction over the different web3 transports.
#[derive(Clone, Debug)]
pub enum Transport {
    RPC(http::Http),
    WS(ws::WebSocket),
}

impl Transport {
    /// Creates a WebSocket transport.
    pub fn new_ws(ws: &str) -> Self {
        ws::WebSocket::new(ws)
            .map(|transport| Transport::WS(transport))
            .expect("Failed to connect to Ethereum WS")
    }

    /// Creates a JSON-RPC over HTTP transport.
    ///
    /// Note: JSON-RPC over HTTP doesn't always support subscribing to new
    /// blocks (one such example is Infura's HTTP endpoint).
    pub fn new_rpc(rpc: &str) -> Self {
        // let max_parallel_http: usize = env::var_os("ETHEREUM_RPC_MAX_PARALLEL_REQUESTS")
        //     .map(|s| s.to_str().unwrap().parse().unwrap())
        //     .unwrap_or(64);

        let cfg = ETHEREUM_CONFIG.rpc.get(rpc);
        let headers = cfg.map(|cfg| cfg.http_headers.clone()).unwrap_or_default();

        http::Http::with_headers(rpc, headers)
            .map(|transport| Transport::RPC(transport))
            .expect("Failed to connect to Ethereum RPC")
    }
}

impl web3::Transport for Transport {
    type Out = Box<dyn Future<Item = Value, Error = web3::error::Error> + Send + Unpin>;

    fn prepare(&self, method: &str, params: Vec<Value>) -> (RequestId, Call) {
        match self {
            Transport::RPC(http) => http.prepare(method, params),
            Transport::WS(ws) => ws.prepare(method, params),
        }
    }

    fn send(&self, id: RequestId, request: Call) -> Self::Out {
        match self {
            Transport::RPC(http) => Box::new(http.send(id, request)),
            Transport::WS(ws) => Box::new(ws.send(id, request)),
        }
    }
}

impl web3::BatchTransport for Transport {
    type Batch = Box<
        dyn Future<Item = Vec<Result<Value, web3::error::Error>>, Error = web3::error::Error>
            + Send
            + Unpin,
    >;

    fn send_batch<T>(&self, requests: T) -> Self::Batch
    where
        T: IntoIterator<Item = (RequestId, Call)>,
    {
        match self {
            Transport::RPC(http) => Box::new(http.send_batch(requests)),
            Transport::WS(ws) => Box::new(ws.send_batch(requests)),
        }
    }
}
