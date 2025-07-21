use graph::components::network_provider::ProviderName;
use graph::endpoint::EndpointMetrics;

use graph::prelude::*;
use graph::url::Url;
use std::sync::Arc;

// Alloy imports for transport types
use graph::prelude::alloy::transports::{http::Http, ipc::IpcConnect, ws::WsConnect};

/// Abstraction over different transport types for Alloy providers.
#[derive(Clone, Debug)]
pub enum Transport {
    RPC {
        client: Http<reqwest::Client>,
        metrics: Arc<EndpointMetrics>,
        provider: ProviderName,
        url: String,
    },
    IPC(IpcConnect<String>),
    WS(WsConnect),
}

impl Transport {
    /// Creates an IPC transport.
    #[cfg(unix)]
    pub async fn new_ipc(ipc: &str) -> Self {
        let transport = IpcConnect::new(ipc.to_string());

        Transport::IPC(transport)
    }

    #[cfg(not(unix))]
    pub async fn new_ipc(_ipc: &str) -> Self {
        panic!("IPC connections are not supported on non-Unix platforms")
    }

    /// Creates a WebSocket transport.
    pub async fn new_ws(ws: &str) -> Self {
        let transport = WsConnect::new(ws.to_string());

        Transport::WS(transport)
    }

    /// Creates a JSON-RPC over HTTP transport.
    ///
    /// Note: JSON-RPC over HTTP doesn't always support subscribing to new
    /// blocks (one such example is Infura's HTTP endpoint).
    pub fn new_rpc(
        rpc: Url,
        headers: graph::http::HeaderMap,
        metrics: Arc<EndpointMetrics>,
        provider: impl AsRef<str>,
    ) -> Self {
        // Unwrap: This only fails if something is wrong with the system's TLS config.
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .unwrap();

        let rpc_url = rpc.to_string();

        Transport::RPC {
            client: Http::with_client(client, rpc),
            metrics,
            provider: provider.as_ref().into(),
            url: rpc_url,
        }
    }
}

/*
impl web3::Transport for Transport {
    type Out = Pin<Box<dyn Future<Output = Result<Value, web3::error::Error>> + Send + 'static>>;

    fn prepare(&self, method: &str, params: Vec<Value>) -> (RequestId, Call) {
        match self {
            Transport::RPC {
                client,
                metrics: _,
                provider: _,
                url: _,
            } => client.prepare(method, params),
            Transport::IPC { transport, path: _ } => transport.prepare(method, params),
            Transport::WS { transport, url: _ } => transport.prepare(method, params),
        }
    }

    fn send(&self, id: RequestId, request: Call) -> Self::Out {
        match self {
            Transport::RPC {
                client,
                metrics,
                provider,
                url: _,
            } => {
                let metrics = metrics.cheap_clone();
                let client = client.clone();
                let method = match request {
                    Call::MethodCall(ref m) => m.method.as_str(),
                    _ => "unknown",
                };

                let labels = RequestLabels {
                    provider: provider.clone(),
                    req_type: method.into(),
                    conn_type: graph::endpoint::ConnectionType::Rpc,
                };
                let out = async move {
                    let out = client.send(id, request).await;
                    match out {
                        Ok(_) => metrics.success(&labels),
                        Err(_) => metrics.failure(&labels),
                    }

                    out
                };

                Box::pin(out)
            }
            Transport::IPC { transport, path: _ } => Box::pin(transport.send(id, request)),
            Transport::WS { transport, url: _ } => Box::pin(transport.send(id, request)),
        }
    }
}
*/

/*
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
            Transport::RPC {
                client,
                metrics: _,
                provider: _,
                url: _,
            } => Box::new(client.send_batch(requests)),
            Transport::IPC { transport, path: _ } => Box::new(transport.send_batch(requests)),
            Transport::WS { transport, url: _ } => Box::new(transport.send_batch(requests)),
        }
    }
}
*/
