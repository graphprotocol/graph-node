use alloy::transports::{TransportError, TransportErrorKind, TransportFut};
use graph::components::ethereum::json_patch;
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

/// Compression method for RPC requests.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Compression {
    #[default]
    None,
    Gzip,
    Brotli,
    Deflate,
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Compression::None => write!(f, "none"),
            Compression::Gzip => write!(f, "gzip"),
            Compression::Brotli => write!(f, "brotli"),
            Compression::Deflate => write!(f, "deflate"),
        }
    }
}

/// Abstraction over different transport types for Alloy providers.
#[derive(Clone, Debug)]
pub enum Transport {
    RPC(alloy::rpc::client::RpcClient),
    IPC(IpcConnect<String>),
    WS(WsConnect),
}

impl Transport {
    /// Creates an IPC transport.
    ///
    /// Accepts both a raw file path (`/tmp/geth.ipc`) and an `ipc://` URL
    /// (`ipc:///tmp/geth.ipc`). When a URL is provided the file path is
    /// extracted so that the underlying IPC connector receives a plain path.
    #[cfg(unix)]
    pub async fn new_ipc(ipc: &str) -> Self {
        let path = Url::parse(ipc)
            .ok()
            .map(|u| u.path().to_string())
            .unwrap_or_else(|| ipc.to_string());
        let transport = IpcConnect::new(path);

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
        compression: Compression,
    ) -> Self {
        let mut client_builder = reqwest::Client::builder().default_headers(headers);

        match compression {
            Compression::None => {}
            Compression::Gzip => {
                client_builder = client_builder.gzip(true);
            }
            Compression::Brotli => {
                client_builder = client_builder.brotli(true);
            }
            Compression::Deflate => {
                client_builder = client_builder.deflate(true);
            }
        }

        let client = client_builder.build().expect("Failed to build HTTP client");

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

    fn patch_rpc_response(method: Option<&str>, response: &mut Value) -> bool {
        let mut patched = response
            .get_mut("result")
            .map(|result| {
                // Celo pre-Gingerbread blocks omit `sha3Uncles`; alloy needs it.
                if let Some(m) = method
                    && json_patch::is_block_method(m)
                    && let Some(result_obj) = result.as_object_mut()
                {
                    return json_patch::patch_uncles_hash(result_obj);
                }
                false
            })
            .unwrap_or(false);
        patched |= response
            .get_mut("result")
            .map(json_patch::patch_receipts)
            .unwrap_or(false);
        patched
    }

    fn patch_response(method: Option<&str>, body: &[u8]) -> Option<Vec<u8>> {
        let mut json: Value = serde_json::from_slice(body).ok()?;

        let patched = match &mut json {
            Value::Object(_) => Self::patch_rpc_response(method, &mut json),
            Value::Array(batch) => {
                let mut patched = false;
                for r in batch {
                    patched |= Self::patch_rpc_response(method, r);
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

        // Receipt patching is gated on `no_eip2718` (only chains without typed
        // transactions need it). Block patching (missing `sha3Uncles` on
        // pre-Gingerbread Celo) runs unconditionally: it only fires when the
        // field is absent, which standard chains never hit.
        let (patch_receipts, is_block_call) = match &request {
            RequestPacket::Single(req) => (
                no_eip2718 && Self::is_receipt_method(req.method()),
                json_patch::is_block_method(req.method()),
            ),
            RequestPacket::Batch(reqs) => (
                no_eip2718 && reqs.iter().any(|r| Self::is_receipt_method(r.method())),
                reqs.iter().any(|r| json_patch::is_block_method(r.method())),
            ),
        };
        // `patch_method` borrows `request`; convert to owned so it can move into
        // the async block.
        let patch_method: Option<String> = if is_block_call {
            match &request {
                RequestPacket::Single(req) => Some(req.method().to_string()),
                RequestPacket::Batch(reqs) => reqs
                    .iter()
                    .find(|r| json_patch::is_block_method(r.method()))
                    .map(|r| r.method().to_string()),
            }
        } else {
            None
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

            let should_patch = patch_receipts || patch_method.is_some();
            if should_patch
                && let Some(patched) = Self::patch_response(patch_method.as_deref(), &body)
            {
                return serde_json::from_slice(&patched).map_err(|err| {
                    TransportError::deser_err(err, String::from_utf8_lossy(&patched))
                });
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
        let patched = PatchingHttp::patch_response(None, body).unwrap();
        let json: Value = serde_json::from_slice(&patched).unwrap();
        assert_eq!(json["result"]["type"], "0x0");
    }

    #[test]
    fn patch_response_returns_none_when_type_exists() {
        let body = br#"{"jsonrpc":"2.0","id":1,"result":{"status":"0x1","type":"0x2"}}"#;
        assert!(PatchingHttp::patch_response(None, body).is_none());
    }

    #[test]
    fn patch_response_batch() {
        let body = br#"[{"jsonrpc":"2.0","id":1,"result":{"status":"0x1"}},{"jsonrpc":"2.0","id":2,"result":{"status":"0x1"}}]"#;
        let patched = PatchingHttp::patch_response(None, body).unwrap();
        let json: Value = serde_json::from_slice(&patched).unwrap();
        assert_eq!(json[0]["result"]["type"], "0x0");
        assert_eq!(json[1]["result"]["type"], "0x0");
    }

    #[test]
    fn patch_response_block_adds_missing_sha3_uncles() {
        // Pre-Gingerbread Celo block: no `sha3Uncles` in the header.
        let body = br#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x1","parentHash":"0xaa..bb","timestamp":"0x0"}}"#;
        let patched = PatchingHttp::patch_response(Some("eth_getBlockByNumber"), body).unwrap();
        let json: Value = serde_json::from_slice(&patched).unwrap();
        assert_eq!(
            json["result"]["sha3Uncles"],
            "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347"
        );
    }

    #[test]
    fn patch_response_block_keeps_existing_sha3_uncles() {
        // `type` present so the receipt patch is a no-op; only the uncles patch
        // could fire, and the field already exists.
        let body =
            br#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x1","type":"0x2","sha3Uncles":"0x1234"}}"#;
        assert!(PatchingHttp::patch_response(Some("eth_getBlockByNumber"), body).is_none());
    }

    #[test]
    fn patch_response_non_block_method_leaves_block_alone() {
        // A result without sha3Uncles from a non-block method is not patched by
        // the uncles rule (the receipt patch may still add `type`).
        let body = br#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x1","type":"0x2"}}"#;
        assert!(PatchingHttp::patch_response(Some("eth_getLogs"), body).is_none());
    }
}
