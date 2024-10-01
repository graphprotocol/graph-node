use anyhow::anyhow;
use axum::http::HeaderMap;
use graph::http::header::AUTHORIZATION;

use crate::GraphmanServerError;

/// Contains a valid authentication token and checks HTTP headers for valid tokens.
#[derive(Clone)]
pub struct AuthToken {
    token: Vec<u8>,
}

impl AuthToken {
    pub fn new(token: impl AsRef<str>) -> Result<Self, GraphmanServerError> {
        let token = token.as_ref().trim().as_bytes().to_vec();

        if token.is_empty() {
            return Err(GraphmanServerError::InvalidAuthToken(anyhow!(
                "auth token can not be empty"
            )));
        }

        Ok(Self { token })
    }

    pub fn headers_contain_correct_token(&self, headers: &HeaderMap) -> bool {
        let header_token = headers
            .get(AUTHORIZATION)
            .and_then(|header| header.as_bytes().strip_prefix(b"Bearer "));

        let Some(header_token) = header_token else {
            return false;
        };

        let mut token_is_correct = true;

        // We compare every byte of the tokens to prevent token size leaks and timing attacks.
        for i in 0..std::cmp::max(self.token.len(), header_token.len()) {
            if self.token.get(i) != header_token.get(i) {
                token_is_correct = false;
            }
        }

        token_is_correct
    }
}

pub fn unauthorized_graphql_message() -> serde_json::Value {
    serde_json::json!({
        "errors": [
            {
                "message": "You are not authorized to access this resource",
                "extensions": {
                    "code": "UNAUTHORIZED"
                }
            }
        ],
        "data": null
    })
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    fn header_value(s: &str) -> HeaderValue {
        s.try_into().unwrap()
    }

    fn bearer_value(s: &str) -> HeaderValue {
        header_value(&format!("Bearer {s}"))
    }

    #[test]
    fn require_non_empty_tokens() {
        assert!(AuthToken::new("").is_err());
        assert!(AuthToken::new("  ").is_err());
        assert!(AuthToken::new("\n\n").is_err());
        assert!(AuthToken::new("\t\t").is_err());
    }

    #[test]
    fn check_missing_header() {
        let token_a = AuthToken::new("123").unwrap();
        let token_b = AuthToken::new("abc").unwrap();

        let headers = HeaderMap::new();

        assert!(!token_a.headers_contain_correct_token(&headers));
        assert!(!token_b.headers_contain_correct_token(&headers));
    }

    #[test]
    fn check_empty_header() {
        let token_a = AuthToken::new("123").unwrap();
        let token_b = AuthToken::new("abc").unwrap();

        let mut headers = HeaderMap::new();

        headers.insert(AUTHORIZATION, header_value(""));

        assert!(!token_a.headers_contain_correct_token(&headers));
        assert!(!token_b.headers_contain_correct_token(&headers));

        headers.insert(AUTHORIZATION, bearer_value(""));

        assert!(!token_a.headers_contain_correct_token(&headers));
        assert!(!token_b.headers_contain_correct_token(&headers));
    }

    #[test]
    fn check_token_prefix() {
        let token_a = AuthToken::new("123").unwrap();
        let token_b = AuthToken::new("abc").unwrap();

        let mut headers = HeaderMap::new();

        headers.insert(AUTHORIZATION, header_value("12"));

        assert!(!token_a.headers_contain_correct_token(&headers));
        assert!(!token_b.headers_contain_correct_token(&headers));

        headers.insert(AUTHORIZATION, bearer_value("12"));

        assert!(!token_a.headers_contain_correct_token(&headers));
        assert!(!token_b.headers_contain_correct_token(&headers));
    }

    #[test]
    fn validate_tokens() {
        let token_a = AuthToken::new("123").unwrap();
        let token_b = AuthToken::new("abc").unwrap();

        let mut headers = HeaderMap::new();

        headers.insert(AUTHORIZATION, bearer_value("123"));

        assert!(token_a.headers_contain_correct_token(&headers));
        assert!(!token_b.headers_contain_correct_token(&headers));

        headers.insert(AUTHORIZATION, bearer_value("abc"));

        assert!(!token_a.headers_contain_correct_token(&headers));
        assert!(token_b.headers_contain_correct_token(&headers));
    }
}
