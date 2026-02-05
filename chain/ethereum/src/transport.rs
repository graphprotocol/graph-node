use crate::json_patch;
use alloy::transports::{TransportError, TransportErrorKind, TransportFut};
use graph::components::network_provider::ProviderName;
use graph::endpoint::{ConnectionType, EndpointMetrics, RequestLabels};
use graph::prelude::alloy::rpc::json_rpc::{RequestPacket, ResponsePacket};
use graph::prelude::alloy::transports::{ipc::IpcConnect, ws::WsConnect};
use graph::prelude::*;
use graph::url::Url;
use serde_json::Value;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::Service;

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
    ///
    /// Set `no_eip2718` to true for chains that don't return the `type` field
    /// in transaction receipts (pre-EIP-2718 chains). Use provider feature `no_eip2718`.
    pub fn new_rpc(
        rpc: Url,
        headers: graph::http::HeaderMap,
        metrics: Arc<EndpointMetrics>,
        provider: impl AsRef<str>,
        no_eip2718: bool,
    ) -> Self {
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .expect("Failed to build HTTP client");

        let patching_transport = PatchingHttp::new(client, rpc, no_eip2718);
        let metrics_transport =
            MetricsHttp::new(patching_transport, metrics, provider.as_ref().into());
        let rpc_client = alloy::rpc::client::RpcClient::new(metrics_transport, false);

        Transport::RPC(rpc_client)
    }
}

/// Custom HTTP transport wrapper that collects metrics
#[derive(Clone)]
pub struct MetricsHttp {
    inner: PatchingHttp,
    metrics: Arc<EndpointMetrics>,
    provider: ProviderName,
}

impl MetricsHttp {
    pub fn new(inner: PatchingHttp, metrics: Arc<EndpointMetrics>, provider: ProviderName) -> Self {
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

/// HTTP transport that patches receipts for chains that don't support EIP-2718 (typed transactions).
/// When `no_eip2718` is set, adds missing `type` field to receipts.
#[derive(Clone)]
pub struct PatchingHttp {
    client: reqwest::Client,
    url: Url,
    no_eip2718: bool,
}

impl PatchingHttp {
    pub fn new(client: reqwest::Client, url: Url, no_eip2718: bool) -> Self {
        Self {
            client,
            url,
            no_eip2718,
        }
    }

    fn is_receipt_method(method: &str) -> bool {
        method == "eth_getTransactionReceipt" || method == "eth_getBlockReceipts"
    }

    fn patch_rpc_response(response: &mut Value) -> bool {
        response
            .get_mut("result")
            .map(json_patch::patch_receipts)
            .unwrap_or(false)
    }

    fn patch_response(body: &[u8]) -> Option<Vec<u8>> {
        let mut json: Value = serde_json::from_slice(body).ok()?;

        let patched = match &mut json {
            Value::Object(_) => Self::patch_rpc_response(&mut json),
            Value::Array(batch) => {
                let mut patched = false;
                for r in batch {
                    patched |= Self::patch_rpc_response(r);
                }
                patched
            }
            _ => false,
        };

        if patched {
            serde_json::to_vec(&json).ok()
        } else {
            None
        }
    }
}

impl Service<RequestPacket> for PatchingHttp {
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: RequestPacket) -> Self::Future {
        let client = self.client.clone();
        let url = self.url.clone();
        let no_eip2718 = self.no_eip2718;

        let should_patch = if no_eip2718 {
            match &request {
                RequestPacket::Single(req) => Self::is_receipt_method(req.method()),
                RequestPacket::Batch(reqs) => {
                    reqs.iter().any(|r| Self::is_receipt_method(r.method()))
                }
            }
        } else {
            false
        };

        Box::pin(async move {
            let resp = client
                .post(url)
                .json(&request)
                .headers(request.headers())
                .send()
                .await
                .map_err(TransportErrorKind::custom)?;

            let status = resp.status();
            let body = resp.bytes().await.map_err(TransportErrorKind::custom)?;

            if !status.is_success() {
                return Err(TransportErrorKind::http_error(
                    status.as_u16(),
                    String::from_utf8_lossy(&body).into_owned(),
                ));
            }

            if should_patch {
                if let Some(patched) = Self::patch_response(&body) {
                    return serde_json::from_slice(&patched).map_err(|err| {
                        TransportError::deser_err(err, String::from_utf8_lossy(&patched))
                    });
                }
            }
            serde_json::from_slice(&body)
                .map_err(|err| TransportError::deser_err(err, String::from_utf8_lossy(&body)))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patch_response_single() {
        let body = br#"{"jsonrpc":"2.0","id":1,"result":{"status":"0x1"}}"#;
        let patched = PatchingHttp::patch_response(body).unwrap();
        let json: Value = serde_json::from_slice(&patched).unwrap();
        assert_eq!(json["result"]["type"], "0x0");
    }

    #[test]
    fn patch_response_returns_none_when_type_exists() {
        let body = br#"{"jsonrpc":"2.0","id":1,"result":{"status":"0x1","type":"0x2"}}"#;
        assert!(PatchingHttp::patch_response(body).is_none());
    }

    #[test]
    fn patch_response_batch() {
        let body = br#"[{"jsonrpc":"2.0","id":1,"result":{"status":"0x1"}},{"jsonrpc":"2.0","id":2,"result":{"status":"0x1"}}]"#;
        let patched = PatchingHttp::patch_response(body).unwrap();
        let json: Value = serde_json::from_slice(&patched).unwrap();
        assert_eq!(json[0]["result"]["type"], "0x0");
        assert_eq!(json[1]["result"]["type"], "0x0");
    }
}
