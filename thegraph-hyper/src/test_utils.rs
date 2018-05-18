use futures::prelude::*;
use http::StatusCode;
use hyper::{Body, Response};
use serde_json;
use tokio_core::reactor::Core;

pub fn assert_successful_response(
    core: &mut Core,
    response: Response<Body>,
) -> serde_json::Map<String, serde_json::Value> {
    assert_eq!(response.status(), StatusCode::OK);

    let chunk = core.run(response.into_body().concat2())
        .expect("Truncated response body");
    let json: serde_json::Value =
        serde_json::from_slice(&chunk).expect("GraphQL response is not valid JSON");

    json.as_object()
        .expect("GraphQL response must be an object")
        .get("data")
        .expect("GraphQL response must contain a \"data\" field")
        .as_object()
        .expect("GraphQL \"data\" field must be an object")
        .clone()
}

pub fn assert_error_response(
    core: &mut Core,
    response: Response<Body>,
    expected_status: StatusCode,
) -> Vec<serde_json::Value> {
    assert_eq!(response.status(), expected_status);

    let chunk = core.run(response.into_body().concat2())
        .expect("Truncated response body");
    let json: serde_json::Value =
        serde_json::from_slice(&chunk).expect("GraphQL response is not valid JSON");

    json.as_object()
        .expect("GraphQL response must be an object")
        .get("errors")
        .expect("GraphQL error response must contain an \"errors\" field")
        .as_array()
        .expect("GraphQL \"errors\" field must be a vector")
        .clone()
}
