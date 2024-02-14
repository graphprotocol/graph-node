use graph::prelude::serde_json;
use http::{response::Parts, HeaderMap, HeaderValue, StatusCode};
use http_body_util::BodyExt;
use hyper::{body::Body, header::ACCESS_CONTROL_ALLOW_ORIGIN, Response};

/// Asserts that the response is a successful GraphQL response; returns its `"data"` field.
pub async fn assert_successful_response(
    response: Response<impl Body>,
) -> serde_json::Map<String, serde_json::Value> {
    let (parts, body) = response.into_parts();

    let body = body
        .collect()
        .await
        .map_err(|_| anyhow::anyhow!("failed to decode body"))
        .unwrap()
        .to_bytes();
    let body = String::from_utf8(body.to_vec()).unwrap();

    assert_successful_response_string(&parts.headers, body).await
}

/// Asserts that the response is a successful GraphQL response; returns its `"data"` field.
pub async fn assert_successful_response_string(
    headers: &HeaderMap<HeaderValue>,
    body: String,
) -> serde_json::Map<String, serde_json::Value> {
    assert_expected_headers(headers);

    let json: serde_json::Value =
        serde_json::from_str(&body).expect("GraphQL response is not valid JSON");

    json.as_object()
        .expect("GraphQL response must be an object")
        .get("data")
        .expect("GraphQL response must contain a \"data\" field")
        .as_object()
        .expect("GraphQL \"data\" field must be an object")
        .clone()
}

/// Asserts that the response is a failed GraphQL response; returns its `"errors"` field.
pub async fn assert_error_response(
    parts: Parts,
    body: String,
    expected_status: StatusCode,
    graphql_response: bool,
) -> Vec<serde_json::Value> {
    assert_eq!(parts.status, expected_status);
    assert_expected_headers(&parts.headers);

    // In case of a non-graphql response, return the body.
    if !graphql_response {
        return vec![serde_json::Value::String(body)];
    }

    let json: serde_json::Value =
        serde_json::from_str(&body).expect("GraphQL response is not valid JSON");

    json.as_object()
        .expect("GraphQL response must be an object")
        .get("errors")
        .expect("GraphQL error response must contain an \"errors\" field")
        .as_array()
        .expect("GraphQL \"errors\" field must be a vector")
        .clone()
}

#[track_caller]
pub fn assert_expected_headers(headers: &HeaderMap<HeaderValue>) {
    assert_eq!(
        headers
            .get(ACCESS_CONTROL_ALLOW_ORIGIN)
            .expect("Missing CORS Header"),
        &"*"
    );
}
