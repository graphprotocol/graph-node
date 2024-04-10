use graph::http::StatusCode;
use std::time::Duration;

use graph::data::{
    query::{QueryResults, QueryTarget},
    value::{Object, Word},
};
use graph::prelude::*;
use graph_server_http::GraphQLServer as HyperGraphQLServer;

use tokio::time::sleep;

pub struct TestGraphQLMetrics;

impl GraphQLMetrics for TestGraphQLMetrics {
    fn observe_query_execution(&self, _duration: Duration, _results: &QueryResults) {}
    fn observe_query_parsing(&self, _duration: Duration, _results: &QueryResults) {}
    fn observe_query_validation(&self, _duration: Duration, _id: &DeploymentHash) {}
    fn observe_query_validation_error(&self, _error_codes: Vec<&str>, _id: &DeploymentHash) {}
    fn observe_query_blocks_behind(&self, _blocks_behind: i32, _id: &DeploymentHash) {}
}

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
    ) -> QueryResults {
        unimplemented!();
    }

    async fn run_query(self: Arc<Self>, query: Query, _target: QueryTarget) -> QueryResults {
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
                == &r::Value::String(String::from("John"))
        {
            Object::from_iter(
                vec![(Word::from("name"), r::Value::String(String::from("John")))].into_iter(),
            )
        } else {
            Object::from_iter(
                vec![(Word::from("name"), r::Value::String(String::from("Jordi")))].into_iter(),
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

    fn metrics(&self) -> Arc<dyn GraphQLMetrics> {
        Arc::new(TestGraphQLMetrics)
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::Ordering;

    use super::*;
    use graph::http::header::{ACCESS_CONTROL_ALLOW_ORIGIN, CONTENT_TYPE};
    use graph::hyper::header::{ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS};
    use graph::prelude::reqwest::{Client, Response};

    lazy_static! {
        static ref USERS: DeploymentHash = DeploymentHash::new("users").unwrap();
    }

    pub async fn assert_successful_response(
        response: Response,
    ) -> serde_json::Map<String, serde_json::Value> {
        assert_expected_headers(&response, true);
        let body = response.bytes().await.unwrap().to_vec();
        let json: serde_json::Value =
            serde_json::from_slice(&body).expect("GraphQL response is not valid JSON");

        json.as_object()
            .expect("GraphQL response must be an object")
            .get("data")
            .expect("GraphQL response must contain a \"data\" field")
            .as_object()
            .expect("GraphQL \"data\" field must be an object")
            .clone()
    }

    pub async fn assert_error_response(
        response: Response,
        expected_status: StatusCode,
        graphql_response: bool,
    ) -> Vec<serde_json::Value> {
        assert_eq!(response.status(), expected_status);
        assert_expected_headers(&response, false);
        let body = response.bytes().await.unwrap().to_vec();
        let body = String::from_utf8(body).unwrap();

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
    pub fn assert_expected_headers(response: &Response, success: bool) {
        #[track_caller]
        fn assert_header(response: &Response, header: &str, value: &str) {
            let hdrs = response.headers();
            let value = Some(value.parse().unwrap());
            assert_eq!(
                value.as_ref(),
                hdrs.get(header),
                "Header {} has unexpected value",
                header
            );
        }

        assert_header(response, ACCESS_CONTROL_ALLOW_ORIGIN.as_str(), "*");
        if success {
            assert_header(
                response,
                ACCESS_CONTROL_ALLOW_HEADERS.as_str(),
                "Content-Type, User-Agent",
            );
            assert_header(
                response,
                ACCESS_CONTROL_ALLOW_METHODS.as_str(),
                "GET, OPTIONS, POST",
            );
            assert_header(response, CONTENT_TYPE.as_str(), "application/json");

            assert_header(response, "Graph-Attestable", "true");
        }
    }

    #[tokio::test]
    async fn rejects_empty_json() {
        let logger = Logger::root(slog::Discard, o!());
        let logger_factory = LoggerFactory::new(logger, None, Arc::new(MetricsRegistry::mock()));
        let id = USERS.clone();
        let query_runner = Arc::new(TestGraphQlRunner);
        let server = HyperGraphQLServer::new(&logger_factory, query_runner);
        let server_handle = server
            .start(8007, 8008)
            .await
            .expect("Failed to start GraphQL server");
        while !server_handle.accepting.load(Ordering::SeqCst) {
            sleep(Duration::from_millis(20)).await;
        }

        // Send an empty JSON POST request
        let client = Client::new();
        let request = client
            .post(format!("http://localhost:8007/subgraphs/id/{}", id))
            .header(CONTENT_TYPE, "text/plain")
            .body("{}")
            .build()
            .unwrap();

        // The response must be a query error
        let response = client.execute(request).await.unwrap();
        let errors = assert_error_response(response, StatusCode::BAD_REQUEST, false).await;

        let message = errors[0].as_str().expect("Error message is not a string");
        assert_eq!(message, "{\"error\":\"GraphQL server error (client error): The \\\"query\\\" field is missing in request data\"}");
    }

    #[tokio::test]
    async fn rejects_invalid_queries() {
        let logger = Logger::root(slog::Discard, o!());
        let logger_factory = LoggerFactory::new(logger, None, Arc::new(MetricsRegistry::mock()));
        let id = USERS.clone();
        let query_runner = Arc::new(TestGraphQlRunner);
        let server = HyperGraphQLServer::new(&logger_factory, query_runner);
        let server_handle = server
            .start(8002, 8003)
            .await
            .expect("Failed to start GraphQL server");
        while !server_handle.accepting.load(Ordering::SeqCst) {
            sleep(Duration::from_millis(20)).await;
        }

        // Send an broken query request
        let client = Client::new();
        let request = client
            .post(format!("http://localhost:8002/subgraphs/id/{}", id))
            .header(CONTENT_TYPE, "text/plain")
            .body("{\"query\": \"<L<G<>M>\"}")
            .build()
            .unwrap();

        // The response must be a query error
        let response = client.execute(request).await.unwrap();
        let errors = assert_error_response(response, StatusCode::OK, true).await;

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

    #[tokio::test]
    async fn accepts_valid_queries() {
        let logger = Logger::root(slog::Discard, o!());
        let logger_factory = LoggerFactory::new(logger, None, Arc::new(MetricsRegistry::mock()));
        let id = USERS.clone();
        let query_runner = Arc::new(TestGraphQlRunner);
        let server = HyperGraphQLServer::new(&logger_factory, query_runner);
        let server_handle = server
            .start(8003, 8004)
            .await
            .expect("Failed to start GraphQL server");
        while !server_handle.accepting.load(Ordering::SeqCst) {
            sleep(Duration::from_millis(20)).await;
        }

        // Send a valid example query
        let client = Client::new();
        let request = client
            .post(format!("http://localhost:8003/subgraphs/id/{}", id))
            .header(CONTENT_TYPE, "plain/text")
            .body("{\"query\": \"{ name }\"}")
            .build()
            .unwrap();

        // The response must be a 200
        let response = client.execute(request).await.unwrap();
        let data = assert_successful_response(response).await;

        // The JSON response should match the simulated query result
        let name = data
            .get("name")
            .expect("Query result data has no \"name\" field")
            .as_str()
            .expect("Query result field \"name\" is not a string");
        assert_eq!(name, "Jordi".to_string());
    }

    #[tokio::test]
    async fn accepts_valid_queries_with_variables() {
        let logger = Logger::root(slog::Discard, o!());
        let logger_factory = LoggerFactory::new(logger, None, Arc::new(MetricsRegistry::mock()));
        let id = USERS.clone();
        let query_runner = Arc::new(TestGraphQlRunner);
        let server = HyperGraphQLServer::new(&logger_factory, query_runner);
        let server_handle = server
            .start(8005, 8006)
            .await
            .expect("Failed to start GraphQL server");
        while !server_handle.accepting.load(Ordering::SeqCst) {
            sleep(Duration::from_millis(20)).await;
        }

        // Send a valid example query
        let client = Client::new();
        let request = client
            .post(format!("http://localhost:8005/subgraphs/id/{}", id))
            .header(CONTENT_TYPE, "plain/text")
            .body(
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
            )
            .build()
            .unwrap();

        // The response must be a 200
        let response = client.execute(request).await.unwrap();
        let data = assert_successful_response(response).await;

        // The JSON response should match the simulated query result
        let name = data
            .get("name")
            .expect("Query result data has no \"name\" field")
            .as_str()
            .expect("Query result field \"name\" is not a string");
        assert_eq!(name, "John".to_string());
    }
}
