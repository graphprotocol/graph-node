use http::StatusCode;
use hyper::{Body, Client, Request};
use std::collections::BTreeMap;
use std::time::Duration;

use graph::data::{
    graphql::effort::LoadManager,
    query::{QueryResults, QueryTarget},
};
use graph::prelude::*;

use graph_server_http::test_utils;
use graph_server_http::GraphQLServer as HyperGraphQLServer;

use tokio::time::delay_for;

/// A simple stupid query runner for testing.
pub struct TestGraphQlRunner;

#[async_trait]
impl GraphQlRunner for TestGraphQlRunner {
    async fn run_query_with_complexity(
        self: Arc<Self>,
        _query: Query,
        _target: QueryTarget,
        _complexity: Option<u64>,
        _max_depth: Option<u8>,
        _max_first: Option<u32>,
        _max_skip: Option<u32>,
        _nested_resolver: bool,
    ) -> QueryResults {
        unimplemented!();
    }

    async fn run_query(
        self: Arc<Self>,
        query: Query,
        _target: QueryTarget,
        _: bool,
    ) -> QueryResults {
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
                vec![(String::from("name"), q::Value::String(String::from("John")))].into_iter(),
            )
        } else {
            BTreeMap::from_iter(
                vec![(
                    String::from("name"),
                    q::Value::String(String::from("Jordi")),
                )]
                .into_iter(),
            )
        }
        .into()
    }

    async fn run_subscription(
        self: Arc<Self>,
        _subscription: Subscription,
        _target: QueryTarget,
    ) -> Result<SubscriptionResult, SubscriptionError> {
        unreachable!();
    }

    fn load_manager(&self) -> Arc<LoadManager> {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use graph_mock::MockMetricsRegistry;

    lazy_static! {
        static ref USERS: DeploymentHash = DeploymentHash::new("users").unwrap();
    }

    #[test]
    fn rejects_empty_json() {
        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        runtime
            .block_on(async {
                let logger = Logger::root(slog::Discard, o!());
                let logger_factory = LoggerFactory::new(logger, None);
                let metrics_registry = Arc::new(MockMetricsRegistry::new());
                let id = USERS.clone();
                let query_runner = Arc::new(TestGraphQlRunner);
                let node_id = NodeId::new("test").unwrap();
                let mut server = HyperGraphQLServer::new(&logger_factory, metrics_registry, query_runner, node_id);
                let http_server = server
                    .serve(8001, 8002)
                    .expect("Failed to start GraphQL server");

                // Launch the server to handle a single request
                tokio::spawn(http_server.fuse().compat());
                // Give some time for the server to start.
                delay_for(Duration::from_secs(2))
                    .then(move |()| {
                        // Send an empty JSON POST request
                        let client = Client::new();
                        let request =
                            Request::post(format!("http://localhost:8001/subgraphs/id/{}", id))
                                .body(Body::from("{}"))
                                .unwrap();

                        // The response must be a query error
                        client.request(request)
                    })
                    .map_ok(|response| {
                        let errors =
                            test_utils::assert_error_response(response, StatusCode::BAD_REQUEST, false);

                        let message = errors[0]
                            .as_str()
                            .expect("Error message is not a string");
                        assert_eq!(message, "GraphQL server error (client error): The \"query\" field is missing in request data");
                    }).await.unwrap()
            })
    }

    #[test]
    fn rejects_invalid_queries() {
        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let logger = Logger::root(slog::Discard, o!());
            let logger_factory = LoggerFactory::new(logger, None);
            let metrics_registry = Arc::new(MockMetricsRegistry::new());
            let id = USERS.clone();
            let query_runner = Arc::new(TestGraphQlRunner);
            let node_id = NodeId::new("test").unwrap();
            let mut server =
                HyperGraphQLServer::new(&logger_factory, metrics_registry, query_runner, node_id);
            let http_server = server
                .serve(8002, 8003)
                .expect("Failed to start GraphQL server");

            // Launch the server to handle a single request
            tokio::spawn(http_server.fuse().compat());
            // Give some time for the server to start.
            delay_for(Duration::from_secs(2))
                .then(move |()| {
                    // Send an broken query request
                    let client = Client::new();
                    let request =
                        Request::post(format!("http://localhost:8002/subgraphs/id/{}", id))
                            .body(Body::from("{\"query\": \"<L<G<>M>\"}"))
                            .unwrap();

                    // The response must be a query error
                    client.request(request)
                })
                .map_ok(|response| {
                    let errors = test_utils::assert_error_response(response, StatusCode::OK, true);

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
                })
                .await
                .unwrap()
        })
    }

    #[test]
    fn accepts_valid_queries() {
        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let logger = Logger::root(slog::Discard, o!());
            let logger_factory = LoggerFactory::new(logger, None);
            let metrics_registry = Arc::new(MockMetricsRegistry::new());
            let id = USERS.clone();
            let query_runner = Arc::new(TestGraphQlRunner);
            let node_id = NodeId::new("test").unwrap();
            let mut server =
                HyperGraphQLServer::new(&logger_factory, metrics_registry, query_runner, node_id);
            let http_server = server
                .serve(8003, 8004)
                .expect("Failed to start GraphQL server");

            // Launch the server to handle a single request
            tokio::spawn(http_server.fuse().compat());
            // Give some time for the server to start.
            delay_for(Duration::from_secs(2))
                .then(move |()| {
                    // Send a valid example query
                    let client = Client::new();
                    let request =
                        Request::post(format!("http://localhost:8003/subgraphs/id/{}", id))
                            .body(Body::from("{\"query\": \"{ name }\"}"))
                            .unwrap();

                    // The response must be a 200
                    client.request(request)
                })
                .map_ok(|response| {
                    let data = test_utils::assert_successful_response(response);

                    // The JSON response should match the simulated query result
                    let name = data
                        .get("name")
                        .expect("Query result data has no \"name\" field")
                        .as_str()
                        .expect("Query result field \"name\" is not a string");
                    assert_eq!(name, "Jordi".to_string());
                })
                .await
                .unwrap()
        });
    }

    #[test]
    fn accepts_valid_queries_with_variables() {
        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        let _ = runtime.block_on(async {
            let logger = Logger::root(slog::Discard, o!());
            let logger_factory = LoggerFactory::new(logger, None);
            let metrics_registry = Arc::new(MockMetricsRegistry::new());
            let id = USERS.clone();
            let query_runner = Arc::new(TestGraphQlRunner);
            let node_id = NodeId::new("test").unwrap();
            let mut server =
                HyperGraphQLServer::new(&logger_factory, metrics_registry, query_runner, node_id);
            let http_server = server
                .serve(8005, 8006)
                .expect("Failed to start GraphQL server");

            // Launch the server to handle a single request
            tokio::spawn(http_server.fuse().compat());
            // Give some time for the server to start.
            delay_for(Duration::from_secs(2))
                .then(move |()| {
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
                .map_ok(|response| {
                    async {
                        let data = test_utils::assert_successful_response(response);

                        // The JSON response should match the simulated query result
                        let name = data
                            .get("name")
                            .expect("Query result data has no \"name\" field")
                            .as_str()
                            .expect("Query result field \"name\" is not a string");
                        assert_eq!(name, "John".to_string());
                    }
                })
                .await
                .unwrap()
        });
    }
}
