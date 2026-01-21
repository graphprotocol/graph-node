use graph::components::network_provider::ProviderName;
use graph::endpoint::{ConnectionType, EndpointMetrics, RequestLabels};
use graph::prelude::alloy::rpc::json_rpc::{RequestPacket, ResponsePacket};
use graph::prelude::*;
use graph::url::Url;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::Service;

use alloy::transports::{TransportError, TransportFut};

use graph::prelude::alloy::transports::{http::Http, ipc::IpcConnect, ws::WsConnect};

/// Abstraction over different transport types for Alloy providers.
#[derive(Clone, Debug)]
pub enum Transport {
    RPC(alloy::rpc::client::RpcClient),
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
    pub fn new_rpc(
        rpc: Url,
        headers: graph::http::HeaderMap,
        metrics: Arc<EndpointMetrics>,
        provider: impl AsRef<str>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .expect("Failed to build HTTP client");

        let http_transport = Http::with_client(client, rpc);
        let metrics_transport = MetricsHttp::new(http_transport, metrics, provider.as_ref().into());
        let rpc_client = alloy::rpc::client::RpcClient::new(metrics_transport, false);

        Transport::RPC(rpc_client)
    }
}

/// Custom HTTP transport wrapper that collects metrics
#[derive(Clone)]
pub struct MetricsHttp {
    inner: Http<reqwest::Client>,
    metrics: Arc<EndpointMetrics>,
    provider: ProviderName,
}

impl MetricsHttp {
    pub fn new(
        inner: Http<reqwest::Client>,
        metrics: Arc<EndpointMetrics>,
        provider: ProviderName,
    ) -> Self {
        Self {
            inner,
            metrics,
            provider,
        }
    }
}

// Implement tower::Service trait for MetricsHttp to intercept RPC calls
impl Service<RequestPacket> for MetricsHttp {
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: RequestPacket) -> Self::Future {
        let metrics = self.metrics.clone();
        let provider = self.provider.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            // Extract method name from request
            let method = match &request {
                RequestPacket::Single(req) => req.method().to_string(),
                RequestPacket::Batch(reqs) => reqs
                    .first()
                    .map(|r| r.method().to_string())
                    .unwrap_or_else(|| "batch".to_string()),
            };

            let labels = RequestLabels {
                provider,
                req_type: method.into(),
                conn_type: ConnectionType::Rpc,
            };

            // Call inner transport and track metrics
            let result = inner.call(request).await;

            match &result {
                Ok(_) => metrics.success(&labels),
                Err(_) => metrics.failure(&labels),
            }

            result
        })
    }
}
