extern crate futures;
extern crate graph;
#[cfg(test)]
extern crate graph_mock;
extern crate graph_server_http;
extern crate graphql_parser;
extern crate http;
extern crate hyper;

use graphql_parser::query as q;
use http::StatusCode;
use hyper::{Body, Client, Request};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use graph::prelude::*;

use graph_server_http::test_utils;
use graph_server_http::GraphQLServer as HyperGraphQLServer;

use crate::tokio::timer::Delay;

/// A simple stupid query runner for testing.
pub struct TestGraphQlRunner;

impl GraphQlRunner for TestGraphQlRunner {
    fn run_query_with_complexity(
        &self,
        _query: Query,
        _complexity: Option<u64>,
    ) -> QueryResultFuture {
        unimplemented!();
    }

    fn run_query(&self, query: Query) -> QueryResultFuture {
        Box::new(future::ok(QueryResult::new(Some(q::Value::Object(
            if query.variables.is_some()
                && query
                    .variables
                    .as_ref()
                    .unwrap()
                    .get(&String::from("equals"))
                    .is_some()
                && query
                    .variables
                    .unwrap()
                    .get(&String::from("equals"))
                    .unwrap()
                    == &q::Value::String(String::from("John"))
            {
                BTreeMap::from_iter(
                    vec![(String::from("name"), q::Value::String(String::from("John")))]
                        .into_iter(),
                )
            } else {
                BTreeMap::from_iter(
                    vec![(
                        String::from("name"),
                        q::Value::String(String::from("Jordi")),
                    )]
                    .into_iter(),
                )
            },
        )))))
    }

    fn run_subscription(&self, _subscription: Subscription) -> SubscriptionResultFuture {
        unreachable!();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use graph::web3::types::H256;
    use graph_mock::MockStore;

    fn mock_store(id: SubgraphDeploymentId) -> Arc<MockStore> {
        let schema = Schema::parse("scalar Foo", id.clone()).unwrap();
        let manifest = SubgraphManifest {
            id: id.clone(),
            location: "".to_owned(),
            spec_version: "".to_owned(),
            description: None,
            repository: None,
            schema: schema.clone(),
            data_sources: vec![],
        };

        let store = Arc::new(MockStore::new(vec![(id, schema)]));
        store
            .apply_entity_operations(
                SubgraphDeploymentEntity::new(
                    &manifest,
                    false,
                    false,
                    EthereumBlockPointer {
                        hash: H256::zero(),
                        number: 0,
                    },
                    0,
                )
                .create_operations(&manifest.id),
                None,
            )
            .unwrap();

        store
    }

    #[test]
    fn rejects_empty_json() {
        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        runtime
            .block_on(futures::lazy(|| {
                let logger = Logger::root(slog::Discard, o!());
                let id = SubgraphDeploymentId::new("testschema").unwrap();
                let query_runner = Arc::new(TestGraphQlRunner);
                let store = mock_store(id.clone());
                let node_id = NodeId::new("test").unwrap();
                let mut server = HyperGraphQLServer::new(&logger, query_runner, store, node_id);
                let http_server = server
                    .serve(8001, 8002)
                    .expect("Failed to start GraphQL server");

                // Launch the server to handle a single request
                tokio::spawn(http_server.fuse());
                // Give some time for the server to start.
                Delay::new(Instant::now() + Duration::from_secs(2))
                    .map_err(|e| panic!("failed to start server: {:?}", e))
                    .and_then(move |()| {
                        // Send an empty JSON POST request
                        let client = Client::new();
                        let request =
                            Request::post(format!("http://localhost:8001/subgraphs/id/{}", id))
                                .body(Body::from("{}"))
                                .unwrap();

                        // The response must be a query error
                        client.request(request)
                    })
                    .and_then(|response| {
                        let errors =
                            test_utils::assert_error_response(response, StatusCode::BAD_REQUEST);

                        let message = errors[0]
                            .as_object()
                            .expect("Query error is not an object")
                            .get("message")
                            .expect("Error contains no message")
                            .as_str()
                            .expect("Error message is not a string");
                        assert_eq!(message, "GraphQL server error (client error): The \"query\" field missing in request data");
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
                let logger = Logger::root(slog::Discard, o!());
                let id = SubgraphDeploymentId::new("testschema").unwrap();
                let query_runner = Arc::new(TestGraphQlRunner);
                let store = mock_store(id.clone());
                let node_id = NodeId::new("test").unwrap();
                let mut server = HyperGraphQLServer::new(&logger, query_runner, store, node_id);
                let http_server = server
                    .serve(8002, 8003)
                    .expect("Failed to start GraphQL server");

                // Launch the server to handle a single request
                tokio::spawn(http_server.fuse());
                // Give some time for the server to start.
                Delay::new(Instant::now() + Duration::from_secs(2))
                    .map_err(|e| panic!("failed to start server: {:?}", e))
                    .and_then(move |()| {
                        // Send an broken query request
                        let client = Client::new();
                        let request =
                            Request::post(format!("http://localhost:8002/subgraphs/id/{}", id))
                                .body(Body::from("{\"query\": \"<L<G<>M>\"}"))
                                .unwrap();

                        // The response must be a query error
                        client.request(request)
                    })
                    .and_then(|response| {
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
                let logger = Logger::root(slog::Discard, o!());
                let id = SubgraphDeploymentId::new("testschema").unwrap();
                let query_runner = Arc::new(TestGraphQlRunner);
                let store = mock_store(id.clone());
                let node_id = NodeId::new("test").unwrap();
                let mut server = HyperGraphQLServer::new(&logger, query_runner, store, node_id);
                let http_server = server
                    .serve(8003, 8004)
                    .expect("Failed to start GraphQL server");

                // Launch the server to handle a single request
                tokio::spawn(http_server.fuse());
                // Give some time for the server to start.
                Delay::new(Instant::now() + Duration::from_secs(2))
                    .map_err(|e| panic!("failed to start server: {:?}", e))
                    .and_then(move |()| {
                        // Send a valid example query
                        let client = Client::new();
                        let request =
                            Request::post(format!("http://localhost:8003/subgraphs/id/{}", id))
                                .body(Body::from("{\"query\": \"{ name }\"}"))
                                .unwrap();

                        // The response must be a 200
                        client.request(request)
                    })
                    .and_then(|response| {
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

    #[test]
    fn accepts_valid_queries_with_variables() {
        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        runtime
            .block_on(futures::lazy(|| {
                let logger = Logger::root(slog::Discard, o!());

                let id = SubgraphDeploymentId::new("testschema").unwrap();
                let query_runner = Arc::new(TestGraphQlRunner);
                let store = mock_store(id.clone());
                let node_id = NodeId::new("test").unwrap();
                let mut server = HyperGraphQLServer::new(&logger, query_runner, store, node_id);
                let http_server = server
                    .serve(8005, 8006)
                    .expect("Failed to start GraphQL server");

                // Launch the server to handle a single request
                tokio::spawn(http_server.fuse());
                // Give some time for the server to start.
                Delay::new(Instant::now() + Duration::from_secs(2))
                    .map_err(|e| panic!("failed to start server: {:?}", e))
                    .and_then(move |()| {
                        // Send a valid example query
                        let client = Client::new();
                        let request =
                            Request::post(format!("http://localhost:8005/subgraphs/id/{}", id))
                                .body(Body::from(
                                    "
                            {
                              \"query\": \" \
                                query name($equals: String!) { \
                                  name(equals: $equals) \
                                } \
                              \",
                              \"variables\": { \"equals\": \"John\" }
                            }
                            ",
                                ))
                                .unwrap();

                        // The response must be a 200
                        client.request(request)
                    })
                    .and_then(|response| {
                        let data = test_utils::assert_successful_response(response);

                        // The JSON response should match the simulated query result
                        let name = data
                            .get("name")
                            .expect("Query result data has no \"name\" field")
                            .as_str()
                            .expect("Query result field \"name\" is not a string");
                        assert_eq!(name, "John".to_string());

                        Ok(())
                    })
            }))
            .unwrap()
    }
}
