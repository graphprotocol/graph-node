use graph::prelude::serde_json;
use graph::prelude::*;
use http::StatusCode;
use hyper::{Body, Response};

/// Asserts that the response is a successful GraphQL response; returns its `"data"` field.
pub fn assert_successful_response(
    response: Response<Body>,
) -> serde_json::Map<String, serde_json::Value> {
    assert_eq!(response.status(), StatusCode::OK);

    futures03::executor::block_on(
        hyper::body::to_bytes(response.into_body())
            .map_ok(|chunk| {
                let json: serde_json::Value =
                    serde_json::from_slice(&chunk).expect("GraphQL response is not valid JSON");

                json.as_object()
                    .expect("GraphQL response must be an object")
                    .get("data")
                    .expect("GraphQL response must contain a \"data\" field")
                    .as_object()
                    .expect("GraphQL \"data\" field must be an object")
                    .clone()
            })
            .map_err(|e| panic!("Truncated response body {:?}", e)),
    )
    .unwrap()
}

/// Asserts that the response is a failed GraphQL response; returns its `"errors"` field.
pub fn assert_error_response(
    response: Response<Body>,
    expected_status: StatusCode,
) -> Vec<serde_json::Value> {
    assert_eq!(response.status(), expected_status);

    futures03::executor::block_on(
        hyper::body::to_bytes(response.into_body())
            .map_ok(|chunk| {
                let json: serde_json::Value =
                    serde_json::from_slice(&chunk).expect("GraphQL response is not valid JSON");

                json.as_object()
                    .expect("GraphQL response must be an object")
                    .get("errors")
                    .expect("GraphQL error response must contain an \"errors\" field")
                    .as_array()
                    .expect("GraphQL \"errors\" field must be a vector")
                    .clone()
            })
            .map_err(|e| panic!("Truncated response body {:?}", e)),
    )
    .unwrap()
}
