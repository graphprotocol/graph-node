//! JSON-RPC 2.0 types for the admin server.
//!
//! This module implements the JSON-RPC 2.0 protocol types according to the specification:
//! https://www.jsonrpc.org/specification

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// JSON-RPC 2.0 request ID.
///
/// The ID can be a string, number, or null (for notifications).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JsonRpcId {
    String(String),
    Number(i64),
    #[default]
    Null,
}

/// JSON-RPC 2.0 request object.
#[derive(Debug, Clone, Deserialize)]
pub struct JsonRpcRequest {
    /// JSON-RPC version. Must be "2.0".
    pub jsonrpc: String,

    /// Method name to invoke.
    pub method: String,

    /// Method parameters (optional).
    #[serde(default)]
    pub params: Option<JsonValue>,

    /// Request ID (optional for notifications).
    #[serde(default)]
    pub id: Option<JsonRpcId>,
}

impl JsonRpcRequest {
    /// Returns true if this request has a valid JSON-RPC version.
    pub fn is_valid_version(&self) -> bool {
        self.jsonrpc == "2.0"
    }
}

/// JSON-RPC 2.0 error object.
#[derive(Debug, Clone, Serialize)]
pub struct JsonRpcError {
    /// Error code.
    pub code: i64,

    /// Human-readable error message.
    pub message: String,

    /// Additional error data (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<JsonValue>,
}

impl JsonRpcError {
    /// Create a new error with the given code and message.
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    /// Parse error (-32700): Invalid JSON was received.
    pub fn parse_error() -> Self {
        Self::new(-32700, "Parse error")
    }

    /// Invalid request (-32600): The JSON sent is not a valid Request object.
    pub fn invalid_request() -> Self {
        Self::new(-32600, "Invalid Request")
    }

    /// Method not found (-32601): The method does not exist / is not available.
    pub fn method_not_found() -> Self {
        Self::new(-32601, "Method not found")
    }

    /// Invalid params (-32602): Invalid method parameter(s).
    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self::new(-32602, message)
    }
}

/// JSON-RPC 2.0 response object.
#[derive(Debug, Clone, Serialize)]
pub struct JsonRpcResponse {
    /// JSON-RPC version. Always "2.0".
    pub jsonrpc: &'static str,

    /// Result on success (mutually exclusive with error).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<JsonValue>,

    /// Error on failure (mutually exclusive with result).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,

    /// Request ID (same as the request, or null for notifications).
    pub id: JsonRpcId,
}

impl JsonRpcResponse {
    /// Create a successful response.
    pub fn success(id: JsonRpcId, result: JsonValue) -> Self {
        Self {
            jsonrpc: "2.0",
            result: Some(result),
            error: None,
            id,
        }
    }

    /// Create an error response.
    pub fn error(id: JsonRpcId, error: JsonRpcError) -> Self {
        Self {
            jsonrpc: "2.0",
            result: None,
            error: Some(error),
            id,
        }
    }

    /// Create an error response for a parse error (when we don't have an ID).
    pub fn parse_error() -> Self {
        Self::error(JsonRpcId::Null, JsonRpcError::parse_error())
    }

    /// Create an error response for an invalid request (when we don't have an ID).
    pub fn invalid_request() -> Self {
        Self::error(JsonRpcId::Null, JsonRpcError::invalid_request())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_request_with_string_id() {
        let json = r#"{"jsonrpc":"2.0","method":"test","id":"abc"}"#;
        let req: JsonRpcRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.id, Some(JsonRpcId::String("abc".to_string())));
    }

    #[test]
    fn deserialize_request_with_number_id() {
        let json = r#"{"jsonrpc":"2.0","method":"test","id":123}"#;
        let req: JsonRpcRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.id, Some(JsonRpcId::Number(123)));
    }

    #[test]
    fn deserialize_request_with_null_id() {
        // When id is explicitly null, serde treats it as None due to Option<JsonRpcId>
        // This is acceptable per JSON-RPC 2.0 spec as null id means notification
        let json = r#"{"jsonrpc":"2.0","method":"test","id":null}"#;
        let req: JsonRpcRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.id, None);
    }

    #[test]
    fn deserialize_request_without_id() {
        let json = r#"{"jsonrpc":"2.0","method":"test"}"#;
        let req: JsonRpcRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.id, None);
    }

    #[test]
    fn serialize_success_response() {
        let resp = JsonRpcResponse::success(JsonRpcId::Number(1), serde_json::json!({"ok": true}));
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains(r#""jsonrpc":"2.0""#));
        assert!(json.contains(r#""result":{"ok":true}"#));
        assert!(json.contains(r#""id":1"#));
        assert!(!json.contains("error"));
    }

    #[test]
    fn serialize_error_response() {
        let resp = JsonRpcResponse::error(
            JsonRpcId::String("req-1".to_string()),
            JsonRpcError::new(-32601, "Method not found"),
        );
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains(r#""jsonrpc":"2.0""#));
        assert!(json.contains(r#""error""#));
        assert!(json.contains(r#""code":-32601"#));
        assert!(json.contains(r#""id":"req-1""#));
        assert!(!json.contains("result"));
    }
}
