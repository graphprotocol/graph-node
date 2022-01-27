use std::convert::TryFrom;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

use graph::prelude::*;
use graph::{components::server::query::GraphQLServerError, data::query::QueryTarget};
use http::header;
use http::header::{
    ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
    CONTENT_TYPE, LOCATION,
};
use hyper::service::Service;
use hyper::{Body, Method, Request, Response, StatusCode};

use crate::request::GraphQLRequest;

pub struct GraphQLServiceMetrics {
    query_execution_time: Box<HistogramVec>,
    failed_query_execution_time: Box<HistogramVec>,
}

impl fmt::Debug for GraphQLServiceMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GraphQLServiceMetrics {{ }}")
    }
}

impl GraphQLServiceMetrics {
    pub fn new(registry: Arc<impl MetricsRegistry>) -> Self {
        let query_execution_time = registry
            .new_histogram_vec(
                "query_execution_time",
                "Execution time for successful GraphQL queries",
                vec![String::from("deployment")],
                vec![0.1, 0.5, 1.0, 10.0, 100.0],
            )
            .expect("failed to create `query_execution_time` histogram");

        let failed_query_execution_time = registry
            .new_histogram_vec(
                "query_failed_execution_time",
                "Execution time for failed GraphQL queries",
                vec![String::from("deployment")],
                vec![0.1, 0.5, 1.0, 10.0, 100.0],
            )
            .expect("failed to create `query_failed_execution_time` histogram");

        Self {
            query_execution_time,
            failed_query_execution_time,
        }
    }

    pub fn observe_query_execution_time(&self, duration: f64, deployment_id: String) {
        self.query_execution_time
            .with_label_values(vec![deployment_id.as_ref()].as_slice())
            .observe(duration);
    }

    pub fn observe_failed_query_execution_time(&self, duration: f64, deployment_id: String) {
        self.failed_query_execution_time
            .with_label_values(vec![deployment_id.as_ref()].as_slice())
            .observe(duration);
    }
}

pub type GraphQLServiceResult = Result<Response<Body>, GraphQLServerError>;
/// An asynchronous response to a GraphQL request.
pub type GraphQLServiceResponse =
    Pin<Box<dyn std::future::Future<Output = GraphQLServiceResult> + Send>>;

/// A Hyper Service that serves GraphQL over a POST / endpoint.
#[derive(Debug)]
pub struct GraphQLService<Q> {
    logger: Logger,
    metrics: Arc<GraphQLServiceMetrics>,
    graphql_runner: Arc<Q>,
    ws_port: u16,
    node_id: NodeId,
}

impl<Q> Clone for GraphQLService<Q> {
    fn clone(&self) -> Self {
        Self {
            logger: self.logger.clone(),
            metrics: self.metrics.clone(),
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
    pub fn new(
        logger: Logger,
        metrics: Arc<GraphQLServiceMetrics>,
        graphql_runner: Arc<Q>,
        ws_port: u16,
        node_id: NodeId,
    ) -> Self {
        GraphQLService {
            logger,
            metrics,
            graphql_runner,
            ws_port,
            node_id,
        }
    }

    fn graphiql_html(&self) -> String {
        include_str!("../assets/index.html")
            .replace("__WS_PORT__", format!("{}", self.ws_port).as_str())
    }

    async fn index(self) -> GraphQLServiceResult {
        Ok(Response::builder()
            .status(200)
            .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .header(CONTENT_TYPE, "text/plain")
            .body(Body::from(String::from(
                "Access deployed subgraphs by deployment ID at \
                /subgraphs/id/<ID> or by name at /subgraphs/name/<NAME>",
            )))
            .unwrap())
    }

    /// Serves a static file.
    fn serve_file(
        &self,
        contents: &'static str,
        content_type: &'static str,
    ) -> GraphQLServiceResponse {
        async move {
            Ok(Response::builder()
                .status(200)
                .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                .header(CONTENT_TYPE, content_type)
                .body(Body::from(contents))
                .unwrap())
        }
        .boxed()
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

    async fn handle_graphql_query_by_name(
        self,
        subgraph_name: String,
        request: Request<Body>,
    ) -> GraphQLServiceResult {
        let subgraph_name = SubgraphName::new(subgraph_name.as_str()).map_err(|()| {
            GraphQLServerError::ClientError(format!("Invalid subgraph name {:?}", subgraph_name))
        })?;

        self.handle_graphql_query(subgraph_name.into(), request.into_body())
            .await
    }

    fn handle_graphql_query_by_id(
        self,
        id: String,
        request: Request<Body>,
    ) -> GraphQLServiceResponse {
        let res = DeploymentHash::new(id)
            .map_err(|id| GraphQLServerError::ClientError(format!("Invalid subgraph id `{}`", id)));
        match res {
            Err(_) => self.handle_not_found(),
            Ok(id) => self
                .handle_graphql_query(id.into(), request.into_body())
                .boxed(),
        }
    }

    async fn handle_graphql_query(
        self,
        target: QueryTarget,
        request_body: Body,
    ) -> GraphQLServiceResult {
        let service = self.clone();
        let service_metrics = self.metrics.clone();

        let start = Instant::now();
        let body = hyper::body::to_bytes(request_body)
            .map_err(|_| GraphQLServerError::InternalError("Failed to read request body".into()))
            .await?;
        let query = GraphQLRequest::new(body).compat().await;

        let result = match query {
            Ok(query) => service.graphql_runner.run_query(query, target).await,
            Err(GraphQLServerError::QueryError(e)) => QueryResult::from(e).into(),
            Err(e) => return Err(e),
        };

        if let Some(id) = result.first().and_then(|res| res.deployment.clone()) {
            service_metrics
                .observe_query_execution_time(start.elapsed().as_secs_f64(), id.to_string());
        }

        Ok(result.as_http_response())
    }

    // Handles OPTIONS requests
    fn handle_graphql_options(&self, _request: Request<Body>) -> GraphQLServiceResponse {
        async {
            Ok(Response::builder()
                .status(200)
                .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                .header(ACCESS_CONTROL_ALLOW_HEADERS, "Content-Type, User-Agent")
                .header(ACCESS_CONTROL_ALLOW_METHODS, "GET, OPTIONS, POST")
                .header(CONTENT_TYPE, "text/html")
                .body(Body::from(""))
                .unwrap())
        }
        .boxed()
    }

    /// Handles 302 redirects
    async fn handle_temp_redirect(self, destination: String) -> GraphQLServiceResult {
        header::HeaderValue::try_from(destination)
            .map_err(|_| {
                GraphQLServerError::ClientError("invalid characters in redirect URL".into())
            })
            .map(|loc_header_val| {
                Response::builder()
                    .status(StatusCode::FOUND)
                    .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                    .header(LOCATION, loc_header_val)
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::from("Redirecting..."))
                    .unwrap()
            })
    }

    /// Handles 404s.
    fn handle_not_found(&self) -> GraphQLServiceResponse {
        async {
            Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header(CONTENT_TYPE, "text/plain")
                .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                .body(Body::from("Not found"))
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
            (Method::GET, ["graphiql.css"]) => {
                self.serve_file(include_str!("../assets/graphiql.css"), "text/css")
            }
            (Method::GET, ["graphiql.min.js"]) => {
                self.serve_file(include_str!("../assets/graphiql.min.js"), "text/javascript")
            }

            (Method::GET, &["subgraphs", "id", _, "graphql"])
            | (Method::GET, &["subgraphs", "name", _, "graphql"])
            | (Method::GET, &["subgraphs", "name", _, _, "graphql"])
            | (Method::GET, &["subgraphs", "network", _, _, "graphql"])
            | (Method::GET, &["subgraphs", "graphql"]) => self.handle_graphiql(),

            (Method::GET, path @ ["subgraphs", "id", _])
            | (Method::GET, path @ ["subgraphs", "name", _])
            | (Method::GET, path @ ["subgraphs", "name", _, _])
            | (Method::GET, path @ ["subgraphs", "network", _, _])
            | (Method::GET, path @ ["subgraphs"]) => {
                let dest = format!("/{}/graphql", path.join("/"));
                self.handle_temp_redirect(dest).boxed()
            }

            (Method::POST, &["subgraphs", "id", subgraph_id]) => {
                self.handle_graphql_query_by_id(subgraph_id.to_owned(), req)
            }
            (Method::OPTIONS, ["subgraphs", "id", _]) => self.handle_graphql_options(req),
            (Method::POST, &["subgraphs", "name", subgraph_name]) => self
                .handle_graphql_query_by_name(subgraph_name.to_owned(), req)
                .boxed(),
            (Method::POST, ["subgraphs", "name", subgraph_name_part1, subgraph_name_part2]) => {
                let subgraph_name = format!("{}/{}", subgraph_name_part1, subgraph_name_part2);
                self.handle_graphql_query_by_name(subgraph_name, req)
                    .boxed()
            }
            (Method::POST, ["subgraphs", "network", subgraph_name_part1, subgraph_name_part2]) => {
                let subgraph_name =
                    format!("network/{}/{}", subgraph_name_part1, subgraph_name_part2);
                self.handle_graphql_query_by_name(subgraph_name, req)
                    .boxed()
            }

            (Method::OPTIONS, ["subgraphs", "name", _])
            | (Method::OPTIONS, ["subgraphs", "name", _, _])
            | (Method::OPTIONS, ["subgraphs", "network", _, _]) => self.handle_graphql_options(req),

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
    use graph::data::value::Object;
    use http::status::StatusCode;
    use hyper::service::Service;
    use hyper::{Body, Method, Request};

    use graph::data::{
        graphql::effort::LoadManager,
        query::{QueryResults, QueryTarget},
    };
    use graph::prelude::*;
    use graph_mock::MockMetricsRegistry;

    use crate::test_utils;

    use super::GraphQLService;
    use super::GraphQLServiceMetrics;

    /// A simple stupid query runner for testing.
    pub struct TestGraphQlRunner;

    lazy_static! {
        static ref USERS: DeploymentHash = DeploymentHash::new("users").unwrap();
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
                vec![(
                    String::from("name"),
                    r::Value::String(String::from("Jordi")),
                )]
                .into_iter(),
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
    }

    #[test]
    fn posting_invalid_query_yields_error_response() {
        let logger = Logger::root(slog::Discard, o!());
        let metrics_registry = Arc::new(MockMetricsRegistry::new());
        let metrics = Arc::new(GraphQLServiceMetrics::new(metrics_registry));
        let subgraph_id = USERS.clone();
        let graphql_runner = Arc::new(TestGraphQlRunner);

        let node_id = NodeId::new("test").unwrap();
        let mut service = GraphQLService::new(logger, metrics, graphql_runner, 8001, node_id);

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
        let metrics_registry = Arc::new(MockMetricsRegistry::new());
        let metrics = Arc::new(GraphQLServiceMetrics::new(metrics_registry));
        let subgraph_id = USERS.clone();
        let graphql_runner = Arc::new(TestGraphQlRunner);

        let node_id = NodeId::new("test").unwrap();
        let mut service = GraphQLService::new(logger, metrics, graphql_runner, 8001, node_id);

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
