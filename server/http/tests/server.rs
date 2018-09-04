extern crate futures;
extern crate graph;
extern crate graph_server_http;
extern crate graphql_parser;
extern crate http;
extern crate hyper;

use graphql_parser::query as q;
use http::StatusCode;
use hyper::{Body, Client, Request};
use std::collections::BTreeMap;
use std::iter::FromIterator;

use graph::prelude::*;

use graph_server_http::test_utils;
use graph_server_http::GraphQLServer as HyperGraphQLServer;

/// A simple stupid query runner for testing.
pub struct TestGraphQlRunner;

impl GraphQlRunner for TestGraphQlRunner {
    fn run_query(&self, _query: Query) -> QueryResultFuture {
        Box::new(future::ok(QueryResult::new(Some(q::Value::Object(
            BTreeMap::from_iter(
                vec![(
                    String::from("name"),
                    q::Value::String(String::from("Jordi")),
                )].into_iter(),
            ),
        )))))
    }

    fn run_subscription(&self, _subscription: Subscription) -> SubscriptionResultFuture {
        unimplemented!();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn test_schema() -> Schema {
        Schema {
            name: "test-schema".to_string(),
            id: "test-schema".to_string(),
            document: Default::default(),
        }
    }

    #[test]
    fn rejects_empty_json() {
        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        runtime
            .block_on(futures::lazy(|| {
                let logger = slog::Logger::root(slog::Discard, o!());

                let query_runner = Arc::new(TestGraphQlRunner);
                let mut server = HyperGraphQLServer::new(&logger, query_runner);
                let http_server = server.serve(8001).expect("Failed to start GraphQL server");

                // Create a simple schema and send it to the server
                let schema = test_schema();
                let id = schema.id.clone();

                server
                    .schema_event_sink()
                    .send(SchemaEvent::SchemaAdded(schema))
                    .wait()
                    .expect("Failed to send schema to server");

                // Launch the server to handle a single request
                tokio::spawn(http_server.fuse());

                // Send an empty JSON POST request
                let client = Client::new();
                let request = Request::post(format!("http://localhost:8001/{}/graphql", id))
                    .body(Body::from("{}"))
                    .unwrap();

                // The response must be a query error
                client.request(request).and_then(|response| {
                    let errors =
                        test_utils::assert_error_response(response, StatusCode::BAD_REQUEST);

                    let message = errors[0]
                        .as_object()
                        .expect("Query error is not an object")
                        .get("message")
                        .expect("Error contains no message")
                        .as_str()
                        .expect("Error message is not a string");
                    assert_eq!(message, "The \"query\" field missing in request data");
                    Ok(())
                })
            }))
            .unwrap()
    }

    #[test]
    fn rejects_invalid_queries() {
        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        runtime
            .block_on(futures::lazy(|| {
                let logger = slog::Logger::root(slog::Discard, o!());

                let query_runner = Arc::new(TestGraphQlRunner);
                let mut server = HyperGraphQLServer::new(&logger, query_runner);
                let http_server = server.serve(8002).expect("Failed to start GraphQL server");

                // Launch the server to handle a single request
                tokio::spawn(http_server.fuse());

                // Create a simple schema and send it to the server
                let schema = test_schema();
                let id = schema.id.clone();

                server
                    .schema_event_sink()
                    .send(SchemaEvent::SchemaAdded(schema))
                    .wait()
                    .expect("Failed to send schema to server");

                // Send an broken query request
                let client = Client::new();
                let request = Request::post(format!("http://localhost:8002/{}/graphql", id))
                    .body(Body::from("{\"query\": \"<L<G<>M>\"}"))
                    .unwrap();

                // The response must be a query error
                client.request(request).and_then(|response| {
                    let errors =
                        test_utils::assert_error_response(response, StatusCode::BAD_REQUEST);

                    let message = errors[0]
                        .as_object()
                        .expect("Query error is not an object")
                        .get("message")
                        .expect("Error contains no message")
                        .as_str()
                        .expect("Error message is not a string");

                    assert_eq!(
                        message,
                        "Unexpected `unexpected character \
                         \'<\'`\nExpected `{`, `query`, `mutation`, \
                         `subscription` or `fragment`"
                    );

                    let locations = errors[0]
                        .as_object()
                        .expect("Query error is not an object")
                        .get("locations")
                        .expect("Query error contains not locations")
                        .as_array()
                        .expect("Query error \"locations\" field is not an array");

                    let location = locations[0]
                        .as_object()
                        .expect("Query error location is not an object");

                    let line = location
                        .get("line")
                        .expect("Query error location is missing a \"line\" field")
                        .as_u64()
                        .expect("Query error location \"line\" field is not a u64");

                    assert_eq!(line, 1);

                    let column = location
                        .get("column")
                        .expect("Query error location is missing a \"column\" field")
                        .as_u64()
                        .expect("Query error location \"column\" field is not a u64");

                    assert_eq!(column, 1);
                    Ok(())
                })
            }))
            .unwrap()
    }

    #[test]
    fn accepts_valid_queries() {
        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        runtime
            .block_on(futures::lazy(|| {
                let logger = slog::Logger::root(slog::Discard, o!());

                let query_runner = Arc::new(TestGraphQlRunner);
                let mut server = HyperGraphQLServer::new(&logger, query_runner);
                let http_server = server.serve(8003).expect("Failed to start GraphQL server");

                // Launch the server to handle a single request
                tokio::spawn(http_server.fuse());

                // Create a simple schema and send it to the server
                let schema = test_schema();
                let id = schema.id.clone();

                server
                    .schema_event_sink()
                    .send(SchemaEvent::SchemaAdded(schema))
                    .wait()
                    .expect("Failed to send schema to server");

                // Send a valid example query
                let client = Client::new();
                let request = Request::post(format!("http://localhost:8003/{}/graphql", id))
                    .body(Body::from("{\"query\": \"{ name }\"}"))
                    .unwrap();

                // The response must be a 200
                client.request(request).and_then(|response| {
                    let data = test_utils::assert_successful_response(response);

                    // The JSON response should match the simulated query result
                    let name = data
                        .get("name")
                        .expect("Query result data has no \"name\" field")
                        .as_str()
                        .expect("Query result field \"name\" is not a string");
                    assert_eq!(name, "Jordi".to_string());

                    Ok(())
                })
            }))
            .unwrap()
    }
}
