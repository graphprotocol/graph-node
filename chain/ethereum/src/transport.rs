use graph::prelude::*;
use hyper::header::{HeaderMap, HeaderName, HeaderValue};
use jsonrpc_core::types::Call;
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::ops::Deref;
use std::str::FromStr;

use toml;
use web3::transports::{http, ipc, ws};
use web3::RequestId;

pub use web3::transports::EventLoopHandle;

fn deserialize_http_headers<'de, D>(deserializer: D) -> Result<HeaderMap, D::Error>
where
    D: Deserializer<'de>,
{
    let kvs: HashMap<String, String> = Deserialize::deserialize(deserializer)?;
    let mut headers = HeaderMap::new();
    for (k, v) in kvs.into_iter() {
        headers.insert(
            k.parse::<HeaderName>().expect("invalid HTTP header name"),
            v.parse::<HeaderValue>().expect("invalid HTTP header value"),
        );
    }
    Ok(headers)
}

#[derive(Deserialize, Debug)]
struct EthereumNodeConfig {
    #[serde(deserialize_with = "deserialize_http_headers")]
    http_headers: HeaderMap,
}

#[derive(Deserialize, Debug)]
struct EthereumNodeConfigs(HashMap<String, EthereumNodeConfig>);

impl FromStr for EthereumNodeConfigs {
    type Err = toml::de::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(toml::from_str(s)?))
    }
}

impl Deref for EthereumNodeConfigs {
    type Target = HashMap<String, EthereumNodeConfig>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

lazy_static! {
    static ref ETHEREUM_NODE_CONFIGS: Option<EthereumNodeConfigs> =
        std::env::var("GRAPH_EXPERIMENTAL_ETHEREUM_NODE_CONFIGS")
            .ok()
            .map(|s| fs::read_to_string(s)
                .expect("Failed to read Ethereum node config file")
                .parse()
                .expect("Failed to parse Ethereum node config file (must be a .toml file)"));
}

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
            .expect("Failed to connect to Ethereum IPC")
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
        let max_parallel_http: usize = env::var_os("ETHEREUM_RPC_MAX_PARALLEL_REQUESTS")
            .map(|s| s.to_str().unwrap().parse().unwrap())
            .unwrap_or(64);

        let node_config = ETHEREUM_NODE_CONFIGS.as_ref().and_then(|m| m.get(rpc));
        let headers = node_config
            .map(|cfg| cfg.http_headers.clone())
            .unwrap_or_default();

        http::Http::with_max_parallel_and_headers(rpc, max_parallel_http, headers)
            .map(|(event_loop, transport)| (event_loop, Transport::RPC(transport)))
            .expect("Failed to connect to Ethereum RPC")
    }
}

impl web3::Transport for Transport {
    type Out = Box<dyn Future<Item = Value, Error = web3::error::Error> + Send>;

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
        dyn Future<Item = Vec<Result<Value, web3::error::Error>>, Error = web3::error::Error>
            + Send,
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
