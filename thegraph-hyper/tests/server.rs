extern crate futures;
extern crate http;
extern crate hyper;
extern crate thegraph;
extern crate thegraph_hyper;
extern crate tokio;
extern crate tokio_core;

use futures::prelude::*;
use futures::sync::mpsc::Receiver;
use http::StatusCode;
use hyper::{Body, Client, Request};
use thegraph::common::util::log::logger;
use thegraph::prelude::*;
use thegraph_hyper::HyperGraphQLServer;
use tokio_core::reactor::Core;

use thegraph::common::query::{Query, QueryResult};

/// Helper function that simulates running a single incoming query and then
/// closing the query stream.
fn simulate_running_one_query(core: &Core, query_stream: Receiver<Query>) {
    core.handle().spawn(
        query_stream
            .for_each(move |query| {
                query.result_sender.send(QueryResult {}).unwrap();
                Ok(())
            })
            .fuse(),
    );
}

#[test]
fn rejects_invalid_queries() {
    let mut core = Core::new().unwrap();
    let logger = logger();

    let mut server = HyperGraphQLServer::new(&logger, core.handle());
    let query_stream = server.query_stream().unwrap();
    let http_server = server.serve().expect("Failed to start GraphQL server");

    // Launch the server to handle a single request
    simulate_running_one_query(&core, query_stream);
    core.handle().spawn(http_server.fuse());

    // Send an empty JSON POST request
    let client = Client::new();
    let request = Request::post("http://localhost:8000")
        .body(Body::from("{}"))
        .unwrap();
    let work = client.request(request);

    // The response must be a client error
    let response = core.run(work).unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn accepts_valid_queries() {
    let mut core = Core::new().unwrap();
    let logger = logger();

    let mut server = HyperGraphQLServer::new(&logger, core.handle());
    let query_stream = server.query_stream().unwrap();
    let http_server = server.serve().expect("Failed to start GraphQL server");

    // Launch the server to handle a single request
    simulate_running_one_query(&core, query_stream);
    core.handle().spawn(http_server.fuse());

    // Send a valid example query
    let client = Client::new();
    let request = Request::post("http://localhost:8000")
        .body(Body::from("{\"query\": \"{ users { name }}\"}"))
        .unwrap();
    let work = client.request(request);

    // The response must be a 200
    let response = core.run(work).unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // The body should match the simulated query result
    let body = core.run(response.into_body().concat2().map(|chunk| chunk.to_vec()))
        .unwrap();
    assert_eq!(body, "QueryResult".as_bytes());
}
