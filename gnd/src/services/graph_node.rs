//! JSON-RPC client for communicating with Graph Node admin API.
//!
//! This client is used by the `create`, `remove`, and `deploy` commands to
//! interact with a Graph Node instance.

use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, USER_AGENT};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

/// Version string for User-Agent header
const GND_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Errors that can occur when communicating with a Graph Node
#[derive(Debug, Error)]
pub enum GraphNodeError {
    #[error("Invalid node URL: {0}")]
    InvalidUrl(#[from] url::ParseError),

    #[error("Unsupported protocol: {protocol}. The Graph Node URL must be http:// or https://")]
    UnsupportedProtocol { protocol: String },

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON-RPC error: {message}")]
    JsonRpc { code: i64, message: String },
}

/// A client for communicating with a Graph Node's JSON-RPC admin API
#[derive(Debug, Clone)]
pub struct GraphNodeClient {
    client: reqwest::Client,
    url: Url,
}

impl GraphNodeClient {
    /// Create a new client for the given Graph Node URL.
    ///
    /// The URL should be the admin JSON-RPC endpoint (e.g., `http://localhost:8020`).
    /// An optional access token can be provided for authentication.
    pub fn new(node_url: &str, access_token: Option<&str>) -> Result<Self, GraphNodeError> {
        let url = Url::parse(node_url)?;

        // Validate protocol
        match url.scheme() {
            "http" | "https" => {}
            other => {
                return Err(GraphNodeError::UnsupportedProtocol {
                    protocol: other.to_string(),
                })
            }
        }

        // Build headers
        let mut headers = HeaderMap::new();
        headers.insert(
            USER_AGENT,
            HeaderValue::from_str(&format!("gnd/{}", GND_VERSION))
                .expect("valid user agent string"),
        );

        if let Some(token) = access_token {
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {}", token))
                    .expect("valid authorization header"),
            );
        }

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_secs(120))
            .build()?;

        Ok(Self { client, url })
    }

    /// Create a subgraph with the given name.
    ///
    /// This registers the subgraph name with the Graph Node but does not deploy any code.
    pub async fn create_subgraph(&self, name: &str) -> Result<(), GraphNodeError> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "subgraph_create",
            params: SubgraphNameParams { name },
        };

        self.call::<serde_json::Value>(request).await?;
        Ok(())
    }

    /// Remove a subgraph with the given name.
    ///
    /// This unregisters the subgraph name from the Graph Node.
    pub async fn remove_subgraph(&self, name: &str) -> Result<(), GraphNodeError> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "subgraph_remove",
            params: SubgraphNameParams { name },
        };

        self.call::<serde_json::Value>(request).await?;
        Ok(())
    }

    /// Make a JSON-RPC call to the Graph Node
    async fn call<T: for<'de> Deserialize<'de>>(
        &self,
        request: JsonRpcRequest<'_>,
    ) -> Result<T, GraphNodeError> {
        let response = self
            .client
            .post(self.url.clone())
            .json(&request)
            .send()
            .await?
            .json::<JsonRpcResponse<T>>()
            .await?;

        if let Some(error) = response.error {
            return Err(GraphNodeError::JsonRpc {
                code: error.code,
                message: error.message,
            });
        }

        // If there's no error, there should be a result
        response.result.ok_or_else(|| GraphNodeError::JsonRpc {
            code: -1,
            message: "No result in response".to_string(),
        })
    }
}

#[derive(Debug, Serialize)]
struct JsonRpcRequest<'a> {
    jsonrpc: &'static str,
    id: u32,
    method: &'static str,
    params: SubgraphNameParams<'a>,
}

#[derive(Debug, Serialize)]
struct SubgraphNameParams<'a> {
    name: &'a str,
}

#[derive(Debug, Deserialize)]
struct JsonRpcResponse<T> {
    #[allow(dead_code)]
    jsonrpc: String,
    #[allow(dead_code)]
    id: u32,
    result: Option<T>,
    error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_url() {
        let result = GraphNodeClient::new("not-a-valid-url", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_unsupported_protocol() {
        let result = GraphNodeClient::new("ftp://example.com", None);
        assert!(matches!(
            result,
            Err(GraphNodeError::UnsupportedProtocol { protocol }) if protocol == "ftp"
        ));
    }

    #[test]
    fn test_valid_http_url() {
        let result = GraphNodeClient::new("http://localhost:8020", None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_valid_https_url() {
        let result = GraphNodeClient::new("https://example.com/admin", None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_with_access_token() {
        let result = GraphNodeClient::new("http://localhost:8020", Some("test-token"));
        assert!(result.is_ok());
    }
}
