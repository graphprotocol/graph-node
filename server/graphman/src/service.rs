use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use graph::components::server::query::GraphQLServerError;
use graph::prelude::*;
use http::header::{ACCESS_CONTROL_ALLOW_ORIGIN, CONTENT_TYPE};
use hyper::service::Service;
use hyper::{Body, Method, Request, Response, StatusCode};

use crate::schema::create_schema;
use crate::schema::GQLContext;

pub type GraphQLServiceResult = Result<Response<Body>, GraphQLServerError>;
/// An asynchronous response to a GraphQL request.
pub type GraphQLServiceResponse =
    Pin<Box<dyn std::future::Future<Output = GraphQLServiceResult> + Send>>;

/// A Hyper Service that serves GraphQL over a POST / endpoint.
#[derive(Debug)]
pub struct GraphQLService<Q> {
    logger: Logger,
    graphql_runner: Arc<Q>,
    ws_port: u16,
    node_id: NodeId,
}

impl<Q> Clone for GraphQLService<Q> {
    fn clone(&self) -> Self {
        Self {
            logger: self.logger.clone(),
            graphql_runner: self.graphql_runner.clone(),
            ws_port: self.ws_port,
            node_id: self.node_id.clone(),
        }
    }
}

impl<Q> GraphQLService<Q>
where
    Q: GraphQlRunner,
{
    /// Creates a new GraphQL service.
    pub fn new(logger: Logger, graphql_runner: Arc<Q>, ws_port: u16, node_id: NodeId) -> Self {
        GraphQLService {
            logger,
            graphql_runner,
            ws_port,
            node_id,
        }
    }

    fn graphiql_html(&self) -> String {
        include_str!("../../graphql_utils/assets/index.html")
            .replace("__WS_PORT__", format!("{}", self.ws_port).as_str())
    }

    async fn index(self) -> GraphQLServiceResult {
        Ok(Response::builder()
            .status(200)
            .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .header(CONTENT_TYPE, "text/plain")
            .body(Body::from(String::from(
                "Graphman is a maintenance tool for Graph Node, helping with diagnosis and resolution of different day-to-day and exceptional tasks.",
            )))
            .unwrap())
    }

    /// Serves a dynamically created file.
    fn serve_dynamic_file(&self, contents: String) -> GraphQLServiceResponse {
        async {
            Ok(Response::builder()
                .status(200)
                .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                .header(CONTENT_TYPE, "text/html")
                .body(Body::from(contents))
                .unwrap())
        }
        .boxed()
    }

    fn handle_graphiql(&self) -> GraphQLServiceResponse {
        self.serve_dynamic_file(self.graphiql_html())
    }

    fn handle_operation(&self, req: Request<Body>) -> GraphQLServiceResponse {
        async {
            let root_node = create_schema();
            let ctx = GQLContext {};

            let res = juniper_hyper::graphql(Arc::new(root_node), Arc::new(ctx), req).await;

            Ok(res)
        }
        .boxed()
    }

    /// Handles 404s.
    fn handle_not_found(&self) -> GraphQLServiceResponse {
        async {
            let json: String = serde_json::to_string("Not Found").unwrap();

            Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header(CONTENT_TYPE, "text/plain")
                .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                .body(Body::from(json))
                .unwrap())
        }
        .boxed()
    }

    fn handle_call(self, req: Request<Body>) -> GraphQLServiceResponse {
        let method = req.method().clone();

        let path = req.uri().path().to_owned();
        let path_segments = {
            let mut segments = path.split('/');

            // Remove leading '/'
            assert_eq!(segments.next(), Some(""));

            segments.collect::<Vec<_>>()
        };

        match (method, path_segments.as_slice()) {
            (Method::GET, [""]) => self.index().boxed(),
            (Method::GET, &["graphql"]) => self.handle_graphiql(),
            (Method::POST, &["graphql"]) => self.handle_operation(req),

            // (Method::GET, path @ ["subgraphs", "id", _])
            // | (Method::GET, path @ ["subgraphs", "name", _])
            // | (Method::GET, path @ ["subgraphs", "name", _, _])
            // | (Method::GET, path @ ["subgraphs", "network", _, _]) => {
            //     let dest = format!("/{}/graphql", path.join("/"));
            //     self.handle_temp_redirect(dest).boxed()
            // }

            // (Method::POST, &["subgraphs", "id", subgraph_id]) => {
            //     self.handle_graphql_query_by_id(subgraph_id.to_owned(), req)
            // }
            // (Method::OPTIONS, ["subgraphs", "id", _]) => self.handle_graphql_options(req),
            // (Method::POST, &["subgraphs", "name", subgraph_name]) => self
            //     .handle_graphql_query_by_name(subgraph_name.to_owned(), req)
            //     .boxed(),
            // (Method::POST, ["subgraphs", "name", subgraph_name_part1, subgraph_name_part2]) => {
            //     let subgraph_name = format!("{}/{}", subgraph_name_part1, subgraph_name_part2);
            //     self.handle_graphql_query_by_name(subgraph_name, req)
            //         .boxed()
            // }
            // (Method::POST, ["subgraphs", "network", subgraph_name_part1, subgraph_name_part2]) => {
            //     let subgraph_name =
            //         format!("network/{}/{}", subgraph_name_part1, subgraph_name_part2);
            //     self.handle_graphql_query_by_name(subgraph_name, req)
            //         .boxed()
            // }

            // (Method::OPTIONS, ["subgraphs", "name", _])
            // | (Method::OPTIONS, ["subgraphs", "name", _, _])
            // | (Method::OPTIONS, ["subgraphs", "network", _, _]) => self.handle_graphql_options(req),
            _ => self.handle_not_found(),
        }
    }
}

impl<Q> Service<Request<Body>> for GraphQLService<Q>
where
    Q: GraphQlRunner,
{
    type Response = Response<Body>;
    type Error = GraphQLServerError;
    type Future = GraphQLServiceResponse;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let logger = self.logger.clone();
        let service = self.clone();

        // Returning Err here will prevent the client from receiving any response.
        // Instead, we generate a Response with an error code and return Ok
        Box::pin(async move {
            let result = service.handle_call(req).await;
            match result {
                Ok(response) => Ok(response),
                Err(err @ GraphQLServerError::ClientError(_)) => Ok(Response::builder()
                    .status(400)
                    .header(CONTENT_TYPE, "text/plain")
                    .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                    .body(Body::from(err.to_string()))
                    .unwrap()),
                Err(err @ GraphQLServerError::QueryError(_)) => {
                    error!(logger, "GraphQLService call failed: {}", err);

                    Ok(Response::builder()
                        .status(400)
                        .header(CONTENT_TYPE, "text/plain")
                        .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                        .body(Body::from(format!("Query error: {}", err)))
                        .unwrap())
                }
                Err(err @ GraphQLServerError::InternalError(_)) => {
                    error!(logger, "GraphQLService call failed: {}", err);

                    Ok(Response::builder()
                        .status(500)
                        .header(CONTENT_TYPE, "text/plain")
                        .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                        .body(Body::from(format!("Internal server error: {}", err)))
                        .unwrap())
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use graph::data::value::{Object, Word};
    use http::status::StatusCode;
    use hyper::service::Service;
    use hyper::{Body, Method, Request};

    use graph::data::{
        graphql::effort::LoadManager,
        query::{QueryResults, QueryTarget},
    };
    use graph::prelude::*;

    use graphql_utils::test_utils;

    use super::GraphQLService;

    /// A simple stupid query runner for testing.
    pub struct TestGraphQlRunner;

    pub struct TestGraphQLMetrics;

    lazy_static! {
        static ref USERS: DeploymentHash = DeploymentHash::new("users").unwrap();
    }

    impl GraphQLMetrics for TestGraphQLMetrics {
        fn observe_query_execution(&self, _duration: Duration, _results: &QueryResults) {}
        fn observe_query_parsing(&self, _duration: Duration, _results: &QueryResults) {}
        fn observe_query_validation(&self, _duration: Duration, _id: &DeploymentHash) {}
        fn observe_query_validation_error(&self, _error_codes: Vec<&str>, _id: &DeploymentHash) {}
    }

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

        async fn run_query(self: Arc<Self>, _query: Query, _target: QueryTarget) -> QueryResults {
            QueryResults::from(Object::from_iter(
                vec![(Word::from("name"), r::Value::String(String::from("Jordi")))].into_iter(),
            ))
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

        fn metrics(&self) -> Arc<dyn GraphQLMetrics> {
            Arc::new(TestGraphQLMetrics)
        }
    }

    #[test]
    fn posting_invalid_query_yields_error_response() {
        let logger = Logger::root(slog::Discard, o!());
        let subgraph_id = USERS.clone();
        let graphql_runner = Arc::new(TestGraphQlRunner);

        let node_id = NodeId::new("test").unwrap();
        let mut service = GraphQLService::new(logger, graphql_runner, 8001, node_id);

        let request = Request::builder()
            .method(Method::POST)
            .uri(format!(
                "http://localhost:8000/subgraphs/id/{}",
                subgraph_id
            ))
            .body(Body::from("{}"))
            .unwrap();

        let response =
            futures03::executor::block_on(service.call(request)).expect("Should return a response");
        let errors = test_utils::assert_error_response(response, StatusCode::BAD_REQUEST, false);

        let message = errors[0].as_str().expect("Error message is not a string");

        assert_eq!(
            message,
            "GraphQL server error (client error): The \"query\" field is missing in request data"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn posting_valid_queries_yields_result_response() {
        let logger = Logger::root(slog::Discard, o!());
        let subgraph_id = USERS.clone();
        let graphql_runner = Arc::new(TestGraphQlRunner);

        let node_id = NodeId::new("test").unwrap();
        let mut service = GraphQLService::new(logger, graphql_runner, 8001, node_id);

        let request = Request::builder()
            .method(Method::POST)
            .uri(format!(
                "http://localhost:8000/subgraphs/id/{}",
                subgraph_id
            ))
            .body(Body::from("{\"query\": \"{ name }\"}"))
            .unwrap();

        // The response must be a 200
        let response = tokio::spawn(service.call(request))
            .await
            .unwrap()
            .expect("Should return a response");
        let data = test_utils::assert_successful_response(response);

        // The body should match the simulated query result
        let name = data
            .get("name")
            .expect("Query result data has no \"name\" field")
            .as_str()
            .expect("Query result field \"name\" is not a string");
        assert_eq!(name, "Jordi".to_string());
    }
}
