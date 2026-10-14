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

/// Which patches to apply to a JSON-RPC response.
#[derive(Clone, Copy, Debug)]
struct Patches {
    /// Add a missing `type` field to receipts.
    receipts: bool,
    /// Add a missing `sha3Uncles` field to blocks.
    uncles_hash: bool,
}

impl Patches {
    fn any(&self) -> bool {
        self.receipts || self.uncles_hash
    }
}

/// HTTP transport that patches responses that alloy can't deserialize:
/// - When `no_eip2718` is set, adds a missing `type` field to receipts, for
///   chains that don't support EIP-2718 (typed transactions).
/// - Adds a missing `sha3Uncles` field to blocks, on every chain. Celo blocks
///   before the Gingerbread hard fork don't have it. Responses that already
///   contain the field are not parsed.
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

    fn is_block_method(method: &str) -> bool {
        method == "eth_getBlockByNumber" || method == "eth_getBlockByHash"
    }

    fn request_has(request: &RequestPacket, pred: fn(&str) -> bool) -> bool {
        match request {
            RequestPacket::Single(req) => pred(req.method()),
            RequestPacket::Batch(reqs) => reqs.iter().any(|r| pred(r.method())),
        }
    }

    /// Whether a response may contain a block without `sha3Uncles`. This
    /// is a text search, so that block responses that have the field, which
    /// is all of them on most chains, are not parsed twice.
    fn may_lack_uncles_hash(body: &[u8]) -> bool {
        !std::str::from_utf8(body).is_ok_and(|body| body.contains(r#""sha3Uncles""#))
    }

    fn patch_rpc_response(response: &mut Value, patches: Patches) -> bool {
        let Some(result) = response.get_mut("result") else {
            return false;
        };
        let mut patched = false;
        if patches.receipts {
            patched |= json_patch::patch_receipts(result);
        }
        if patches.uncles_hash {
            patched |= json_patch::patch_missing_uncles_hash(result);
        }
        patched
    }

    fn patch_response(body: &[u8], patches: Patches) -> Option<Vec<u8>> {
        let mut json: Value = serde_json::from_slice(body).ok()?;

        let patched = match &mut json {
            Value::Object(_) => Self::patch_rpc_response(&mut json, patches),
            Value::Array(batch) => {
                let mut patched = false;
                for r in batch {
                    patched |= Self::patch_rpc_response(r, patches);
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

        let receipts = self.no_eip2718 && Self::request_has(&request, Self::is_receipt_method);
        let blocks = Self::request_has(&request, Self::is_block_method);

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

            let patches = Patches {
                receipts,
                // In a batch, one block having the field says nothing about
                // the others, so a batch is always parsed.
                uncles_hash: blocks
                    && (matches!(request, RequestPacket::Batch(_))
                        || Self::may_lack_uncles_hash(&body)),
            };
            if patches.any()
                && let Some(patched) = Self::patch_response(&body, patches)
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
    use graph::components::ethereum::AnyBlock;

    const RECEIPTS: Patches = Patches {
        receipts: true,
        uncles_hash: false,
    };

    const UNCLES_HASH: Patches = Patches {
        receipts: false,
        uncles_hash: true,
    };

    /// Celo mainnet block 18946750 (before Gingerbread), as returned by
    /// forno.celo.org. It has no `sha3Uncles` field.
    const CELO_PRE_GINGERBREAD_BLOCK: &str = r#"{"baseFeePerGas":"0x12a05f200","difficulty":"0x0","epochSnarkData":null,"extraData":"0xd983010700846765746889676f312e31372e3133856c696e7578000000000000f8ccc0c080b841a2dabeefc7d980e7da2dc0279a009f071892f2da13513fad2c78564b02ac62ad1846f48b2c374cecdb3008fbe58d3381809d62d0542a8468b94f4e9b22be904300f8418e303ed3fcd8eaff6d5f7fbc5f79bcb0f84388e266f10d9cb40a8aa194fd4d496e0481f03e0f0115b090c8510253278495ea59cd861dfd688fd62a7a263d290180f8418e3fffffffffffffffffffefffffffb0450595e2afb27d47dbababe99a3f093d4c0f9e7a4b555fff11980cf43ca2a7a5583c75877c4ed993908a49a1b8d4290080","gasLimit":"0x1312d00","gasUsed":"0xd647e","hash":"0x8e4a323d009fdd25a67f58d883abbc5b5f029f213a2e64f39dd73e0efecc083a","logsBloom":"0x00800000000200000000240040020000000000000000000000000000000000000000080000000000000000002000000800000000000000000000000000200000000000100002000200000008000000000840002004000000000000000000000000000000000400000004400000008008000000000000000040910010000001000402000080000000400080000400000800000000000040800000000000008100000000800000000000000000000010000000000000000000000000000400010000000002000000020000100000002000000800000000000000000000400000000000000000000000020004020000000000000240000020000000000000000000","miner":"0x8e5db67c3ca13c3487fa291953ade27b503ed6bd","number":"0x1211abe","parentHash":"0x265cb829255b379e0812f857b162450e8330dd34cd21082af15b502afb960539","randomness":{"committed":"0x5a6dd907afeb4dbe106605e77f3eee2d8778dee1e99d9bc74705fec15540d61c","revealed":"0xf8158ddf7f63993ecefc39d4ca064aeb987c15d62b24807b079a1c8b29d3d8f6"},"receiptsRoot":"0xf06c82b478ebd9f5281756a1876d7526a6a952e7ecad6b040748c700378bfca2","size":"0x64b","stateRoot":"0x55d3594e686901a30be71777d8cc11575da5cfa2c78c57303933a3db6cf18b87","timestamp":"0x64476aa4","totalDifficulty":"0x1211abf","transactions":["0xc06b5d3f226083ea603fdfffaf910aef6794709f0b7ac2226f828ba053fbc015","0xd46049a51bbd0baf617d63cc264694d2c9ea4cda11ac348ea5bb813844611d0a","0x2422aea2e4815963e7c6d2fefa0698cb827ef180dd662364203cf416262419f8","0x973372c269483c298d40d111d5b342f6d30d66bb46cfab6cf910e5252ab9cb9e"],"transactionsRoot":"0x7fc667ab6e209550d53bd09f04fbd8fd12f277cc12733ecf4af1c95b99b51fd3"}"#;

    fn rpc_response(id: u64, result: &str) -> String {
        format!(r#"{{"jsonrpc":"2.0","id":{id},"result":{result}}}"#)
    }

    #[test]
    fn celo_pre_gingerbread_block_needs_patch() {
        assert!(serde_json::from_str::<AnyBlock>(CELO_PRE_GINGERBREAD_BLOCK).is_err());
    }

    #[test]
    fn patch_response_celo_block_deserializes() {
        let body = rpc_response(1, CELO_PRE_GINGERBREAD_BLOCK);
        let patched = PatchingHttp::patch_response(body.as_bytes(), UNCLES_HASH).unwrap();
        let json: Value = serde_json::from_slice(&patched).unwrap();

        let block: AnyBlock = serde_json::from_value(json["result"].clone()).unwrap();
        assert_eq!(block.header.number, 18946750);
        assert_eq!(block.header.ommers_hash, alloy::primitives::B256::ZERO);
    }

    #[test]
    fn patch_response_celo_block_batch() {
        let body = format!(
            "[{},{}]",
            rpc_response(1, CELO_PRE_GINGERBREAD_BLOCK),
            rpc_response(2, "null")
        );
        let patched = PatchingHttp::patch_response(body.as_bytes(), UNCLES_HASH).unwrap();
        let json: Value = serde_json::from_slice(&patched).unwrap();
        serde_json::from_value::<AnyBlock>(json[0]["result"].clone()).unwrap();
        assert!(json[1]["result"].is_null());
    }

    #[test]
    fn patch_response_uncles_hash_returns_none_when_present() {
        let body = rpc_response(1, r#"{"parentHash":"0x1","sha3Uncles":"0x2"}"#);
        assert!(PatchingHttp::patch_response(body.as_bytes(), UNCLES_HASH).is_none());
    }

    #[test]
    fn patch_response_receipts_only_leaves_blocks_alone() {
        let body = rpc_response(1, r#"{"parentHash":"0x1","type":"0x2"}"#);
        assert!(PatchingHttp::patch_response(body.as_bytes(), RECEIPTS).is_none());
    }

    #[test]
    fn may_lack_uncles_hash() {
        let missing = rpc_response(1, CELO_PRE_GINGERBREAD_BLOCK);
        assert!(PatchingHttp::may_lack_uncles_hash(missing.as_bytes()));

        let present = rpc_response(1, r#"{"parentHash":"0x1","sha3Uncles":"0x2"}"#);
        assert!(!PatchingHttp::may_lack_uncles_hash(present.as_bytes()));
    }

    #[test]
    fn patch_response_single() {
        let body = br#"{"jsonrpc":"2.0","id":1,"result":{"status":"0x1"}}"#;
        let patched = PatchingHttp::patch_response(body, RECEIPTS).unwrap();
        let json: Value = serde_json::from_slice(&patched).unwrap();
        assert_eq!(json["result"]["type"], "0x0");
    }

    #[test]
    fn patch_response_returns_none_when_type_exists() {
        let body = br#"{"jsonrpc":"2.0","id":1,"result":{"status":"0x1","type":"0x2"}}"#;
        assert!(PatchingHttp::patch_response(body, RECEIPTS).is_none());
    }

    #[test]
    fn patch_response_batch() {
        let body = br#"[{"jsonrpc":"2.0","id":1,"result":{"status":"0x1"}},{"jsonrpc":"2.0","id":2,"result":{"status":"0x1"}}]"#;
        let patched = PatchingHttp::patch_response(body, RECEIPTS).unwrap();
        let json: Value = serde_json::from_slice(&patched).unwrap();
        assert_eq!(json[0]["result"]["type"], "0x0");
        assert_eq!(json[1]["result"]["type"], "0x0");
    }
}
