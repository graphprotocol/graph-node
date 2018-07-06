use futures::prelude::*;
use jsonrpc_core::types::Call;
use serde_json::Value;
use web3;
use web3::transports::http;
use web3::transports::ipc;
use web3::transports::ws;
use web3::transports::EventLoopHandle;
use web3::RequestId;

/// Abstraction over the different web3 transports.
#[derive(Clone, Debug)]
pub enum Transport {
    RPC(http::Http),
    IPC(ipc::Ipc),
    WS(ws::WebSocket),
}

impl Transport {
    /// Uses the best of the given RPC, IPC and WS endpoints/paths.
    pub fn preferred_transport(
        rpc: Option<&str>,
        ipc: Option<&str>,
        ws: Option<&str>,
    ) -> Option<(EventLoopHandle, Self)> {
        match (rpc, ipc, ws) {
            // IPC is fastest, so it is always preferred
            (_, Some(s), _) => {
                let (event_loop, transport) =
                    ipc::Ipc::new(s).expect("Failed to connect to Ethereum RPC");
                Some((event_loop, Transport::IPC(transport)))
            }
            // Next up is WebSockets, since they are guaranteed to support
            // subscribing to new blocks and should be faster than HTTP as well
            (_, _, Some(s)) => {
                let (event_loop, transport) =
                    ws::WebSocket::new(s).expect("Failed to connect to Ethereum WS");
                Some((event_loop, Transport::WS(transport)))
            }
            // Last is JSON-RPC over HTTP; which doesn't always support subscribing
            // to new blocks
            (Some(s), _, _) => {
                let (event_loop, transport) =
                    http::Http::new(s).expect("Failed to connect to Ethereum WS");
                Some((event_loop, Transport::RPC(transport)))
            }
            _ => None,
        }
    }
}

impl web3::Transport for Transport {
    type Out = Box<Future<Item = Value, Error = web3::error::Error>>;

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
