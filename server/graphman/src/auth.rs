use anyhow::anyhow;
use axum::http::HeaderMap;
use constant_time_eq::constant_time_eq;
use graph::http::header::AUTHORIZATION;

use crate::GraphmanServerError;

// A fixed token size facilitates constant-time string comparison, preventing timing attacks.
//
// Note: The hex string representation of an SHA-256 hash corresponds to 64 characters.
const AUTH_TOKEN_CHAR_COUNT: usize = 64;

/// Contains a valid authentication token and checks HTTP headers for valid tokens.
#[derive(Clone)]
pub struct AuthToken {
    token: [u8; AUTH_TOKEN_CHAR_COUNT],
}

impl AuthToken {
    pub fn new(token: impl AsRef<str>) -> Result<Self, GraphmanServerError> {
        let Ok(token) = token.as_ref().as_bytes().try_into() else {
            return Err(GraphmanServerError::InvalidAuthToken(anyhow!(
                "auth token should contain exactly {AUTH_TOKEN_CHAR_COUNT} characters"
            )));
        };

        Ok(Self { token })
    }

    pub fn headers_contain_correct_token(&self, headers: &HeaderMap) -> bool {
        let header_token = headers
            .get(AUTHORIZATION)
            .and_then(|header| header.as_bytes().strip_prefix(b"Bearer "));

        let Some(header_token) = header_token else {
            return false;
        };

        if header_token.len() != AUTH_TOKEN_CHAR_COUNT {
            return false;
        }

        constant_time_eq(&self.token, header_token)
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

    const TOKEN_A: &str = "16b9f2912007922fe48f5343d131850f01f2095edbc97bf38d36efc3178bafc2";
    const TOKEN_B: &str = "f7f4d77d5b5a0718043e0dc606baf489872520371618d72f37eae434cedba5f6";

    fn header_value(s: &str) -> HeaderValue {
        s.try_into().unwrap()
    }

    fn bearer_value(s: &str) -> HeaderValue {
        header_value(&format!("Bearer {s}"))
    }

    #[test]
    fn require_tokens_of_fixed_length() {
        assert!(AuthToken::new("").is_err(), "fail on empty token");

        for i in 1..AUTH_TOKEN_CHAR_COUNT {
            let s = &TOKEN_A[..i];
            assert!(AuthToken::new(s).is_err(), "fail on token of length {i}");
        }

        assert!(AuthToken::new(TOKEN_A).is_ok(), "accept token");
        assert!(AuthToken::new(TOKEN_B).is_ok(), "accept token");
    }

    #[test]
    fn check_missing_header() {
        let token_a = AuthToken::new(TOKEN_A).unwrap();
        let token_b = AuthToken::new(TOKEN_B).unwrap();

        let headers = HeaderMap::new();

        assert!(!token_a.headers_contain_correct_token(&headers));
        assert!(!token_b.headers_contain_correct_token(&headers));
    }

    #[test]
    fn check_empty_header() {
        let token_a = AuthToken::new(TOKEN_A).unwrap();
        let token_b = AuthToken::new(TOKEN_B).unwrap();

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
        let token_a = AuthToken::new(TOKEN_A).unwrap();
        let token_b = AuthToken::new(TOKEN_B).unwrap();

        let mut headers = HeaderMap::new();

        for i in 1..AUTH_TOKEN_CHAR_COUNT {
            headers.insert(AUTHORIZATION, header_value(&TOKEN_A[..i]));

            assert!(!token_a.headers_contain_correct_token(&headers));
            assert!(!token_b.headers_contain_correct_token(&headers));

            headers.insert(AUTHORIZATION, bearer_value(&TOKEN_A[..i]));

            assert!(!token_a.headers_contain_correct_token(&headers));
            assert!(!token_b.headers_contain_correct_token(&headers));
        }
    }

    #[test]
    fn validate_tokens() {
        let token_a = AuthToken::new(TOKEN_A).unwrap();
        let token_b = AuthToken::new(TOKEN_B).unwrap();

        let mut headers = HeaderMap::new();

        headers.insert(AUTHORIZATION, bearer_value(TOKEN_A));

        assert!(token_a.headers_contain_correct_token(&headers));
        assert!(!token_b.headers_contain_correct_token(&headers));

        headers.insert(AUTHORIZATION, bearer_value(TOKEN_B));

        assert!(!token_a.headers_contain_correct_token(&headers));
        assert!(token_b.headers_contain_correct_token(&headers));
    }
}
