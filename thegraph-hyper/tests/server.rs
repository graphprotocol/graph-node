extern crate futures;
extern crate graphql_parser;
extern crate http;
extern crate hyper;
extern crate serde_json;
#[macro_use]
extern crate slog;
extern crate thegraph;
extern crate thegraph_hyper;
extern crate tokio;
extern crate tokio_core;

use futures::prelude::*;
use futures::sync::mpsc::Receiver;
use graphql_parser::query::Value;
use http::StatusCode;
use hyper::{Body, Client, Request};
use std::collections::BTreeMap;
use tokio_core::reactor::Core;

use thegraph::components::schema::SchemaProviderEvent;
use thegraph::prelude::*;

use thegraph_hyper::test_utils;
use thegraph_hyper::GraphQLServer as HyperGraphQLServer;

/// Helper function that simulates running a single incoming query and then
/// closing the query stream.
fn simulate_running_one_query(core: &Core, query_stream: Receiver<Query>) {
    core.handle().spawn(
        query_stream
            .for_each(move |query| {
                let mut map = BTreeMap::new();
                map.insert("name".to_string(), Value::String("Jordi".to_string()));
                let data = Value::Object(map);
                let result = QueryResult::new(Some(data));
                query.result_sender.send(result).unwrap();
                Ok(())
            })
            .fuse(),
    );
}

#[test]
fn rejects_empty_json() {
    let mut core = Core::new().unwrap();
    let logger = slog::Logger::root(slog::Discard, o!());

    let mut server = HyperGraphQLServer::new(&logger, core.handle());
    let query_stream = server.query_stream().unwrap();
    let http_server = server.serve().expect("Failed to start GraphQL server");

    // Create a simple schema and send it to the server
    let schema = Schema {
        id: "test-schema".to_string(),
        document: graphql_parser::parse_schema(
            "\
             scalar String \
             type Query { name: String } \
             ",
        ).unwrap(),
    };
    server
        .schema_provider_event_sink()
        .send(SchemaProviderEvent::SchemaChanged(Some(schema)))
        .wait()
        .expect("Failed to send schema to server");

    // Launch the server to handle a single request
    simulate_running_one_query(&core, query_stream);
    core.handle().spawn(http_server.fuse());

    // Send an empty JSON POST request
    let client = Client::new();
    let request = Request::post("http://localhost:8000/graphql")
        .body(Body::from("{}"))
        .unwrap();
    let work = client.request(request);

    // The response must be a query error
    let response = core.run(work).expect("Should return a response");
    let errors = test_utils::assert_error_response(&mut core, response, StatusCode::BAD_REQUEST);

    let message = errors[0]
        .as_object()
        .expect("Query error is not an object")
        .get("message")
        .expect("Error contains no message")
        .as_str()
        .expect("Error message is not a string");

    assert_eq!(message, "The \"query\" field missing in request data");
}

#[test]
fn rejects_invalid_queries() {
    let mut core = Core::new().unwrap();
    let logger = slog::Logger::root(slog::Discard, o!());

    let mut server = HyperGraphQLServer::new(&logger, core.handle());
    let query_stream = server.query_stream().unwrap();
    let http_server = server.serve().expect("Failed to start GraphQL server");

    // Launch the server to handle a single request
    simulate_running_one_query(&core, query_stream);
    core.handle().spawn(http_server.fuse());

    // Create a simple schema and send it to the server
    let schema = Schema {
        id: "test-schema".to_string(),
        document: graphql_parser::parse_schema(
            "\
             scalar String \
             type Query { name: String } \
             ",
        ).unwrap(),
    };
    server
        .schema_provider_event_sink()
        .send(SchemaProviderEvent::SchemaChanged(Some(schema)))
        .wait()
        .expect("Failed to send schema to server");

    // Send an broken query request
    let client = Client::new();
    let request = Request::post("http://localhost:8000/graphql")
        .body(Body::from("{\"query\": \"<L<G<>M>\"}"))
        .unwrap();
    let work = client.request(request);

    // The response must be a query error
    let response = core.run(work).expect("Should return a response");
    let errors = test_utils::assert_error_response(&mut core, response, StatusCode::BAD_REQUEST);

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
}

#[test]
fn accepts_valid_queries() {
    let mut core = Core::new().unwrap();
    let logger = slog::Logger::root(slog::Discard, o!());

    let mut server = HyperGraphQLServer::new(&logger, core.handle());
    let query_stream = server.query_stream().unwrap();
    let http_server = server.serve().expect("Failed to start GraphQL server");

    // Launch the server to handle a single request
    simulate_running_one_query(&core, query_stream);
    core.handle().spawn(http_server.fuse());

    // Create a simple schema and send it to the server
    let schema = Schema {
        id: "test-schema".to_string(),
        document: graphql_parser::parse_schema(
            "\
             scalar String \
             type Query { name: String } \
             ",
        ).unwrap(),
    };
    server
        .schema_provider_event_sink()
        .send(SchemaProviderEvent::SchemaChanged(Some(schema)))
        .wait()
        .expect("Failed to send schema to server");

    // Send a valid example query
    let client = Client::new();
    let request = Request::post("http://localhost:8000/graphql")
        .body(Body::from("{\"query\": \"{ name }\"}"))
        .unwrap();
    let work = client.request(request);

    // The response must be a 200
    let response = core.run(work).expect("Should return a response");
    let data = test_utils::assert_successful_response(&mut core, response);

    // The JSON response should match the simulated query result
    let name = data.get("name")
        .expect("Query result data has no \"name\" field")
        .as_str()
        .expect("Query result field \"name\" is not a string");
    assert_eq!(name, "Jordi".to_string());
}
