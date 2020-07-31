use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;
use std::ops::Deref;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

use graph::components::server::query::GraphQLServerError;
use graph::data::subgraph::schema::{SubgraphEntity, SUBGRAPHS_ID};
use graph::prelude::*;
use http::header;
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
                "subgraph_query_execution_time",
                "Execution time for GraphQL queries",
                HashMap::new(),
                vec![String::from("subgraph_deployment")],
                vec![0.1, 0.5, 1.0, 10.0, 100.0],
            )
            .expect("failed to create `subgraph_query_execution_time` histogram");

        let failed_query_execution_time = registry
            .new_histogram_vec(
                "subgraph_failed_query_execution_time",
                "Execution time for failed GraphQL queries",
                HashMap::new(),
                vec![String::from("subgraph_deployment")],
                vec![0.1, 0.5, 1.0, 10.0, 100.0],
            )
            .expect("failed to create `subgraph_failed_query_execution_time` histogram");

        Self {
            query_execution_time,
            failed_query_execution_time,
        }
    }

    pub fn observe_query_execution_time(&self, duration: f64, deployment_id: String) {
        self.query_execution_time
            .with_label_values(vec![deployment_id.as_ref()].as_slice())
            .observe(duration.clone());
    }

    pub fn observe_failed_query_execution_time(&self, duration: f64, deployment_id: String) {
        self.failed_query_execution_time
            .with_label_values(vec![deployment_id.as_ref()].as_slice())
            .observe(duration.clone());
    }
}

pub type GraphQLServiceResult = Result<Response<Body>, GraphQLServerError>;
/// An asynchronous response to a GraphQL request.
pub type GraphQLServiceResponse =
    Pin<Box<dyn std::future::Future<Output = GraphQLServiceResult> + Send>>;

/// A Hyper Service that serves GraphQL over a POST / endpoint.
#[derive(Debug)]
pub struct GraphQLService<Q, S> {
    logger: Logger,
    metrics: Arc<GraphQLServiceMetrics>,
    graphql_runner: Arc<Q>,
    store: Arc<S>,
    ws_port: u16,
    node_id: NodeId,
}

impl<Q, S> Clone for GraphQLService<Q, S> {
    fn clone(&self) -> Self {
        Self {
            logger: self.logger.clone(),
            metrics: self.metrics.clone(),
            graphql_runner: self.graphql_runner.clone(),
            store: self.store.clone(),
            ws_port: self.ws_port,
            node_id: self.node_id.clone(),
        }
    }
}

impl<Q, S> GraphQLService<Q, S>
where
    Q: GraphQlRunner,
    S: SubgraphDeploymentStore + Store,
{
    /// Creates a new GraphQL service.
    pub fn new(
        logger: Logger,
        metrics: Arc<GraphQLServiceMetrics>,
        graphql_runner: Arc<Q>,
        store: Arc<S>,
        ws_port: u16,
        node_id: NodeId,
    ) -> Self {
        GraphQLService {
            logger,
            metrics,
            graphql_runner,
            store,
            ws_port,
            node_id,
        }
    }

    fn graphiql_html(&self) -> String {
        include_str!("../assets/index.html")
            .replace("__WS_PORT__", format!("{}", self.ws_port).as_str())
    }

    async fn index(self) -> GraphQLServiceResult {
        // Ask for two to find out if there is more than one
        let entity_query = SubgraphEntity::query().first(2);

        let mut subgraph_entities = self
            .store
            .find(entity_query)
            .map_err(|e| GraphQLServerError::InternalError(e.to_string()))?;

        // If there is only one subgraph, redirect to it
        match subgraph_entities.len() {
            0 => Ok(Response::builder()
                .status(200)
                .body(Body::from(String::from("No subgraphs deployed yet")))
                .unwrap()),
            1 => {
                let subgraph_entity = subgraph_entities.pop().unwrap();
                let subgraph_name = subgraph_entity
                    .get("name")
                    .expect("subgraph entity without name");
                let name = format!("/subgraphs/name/{}", subgraph_name);
                self.handle_temp_redirect(name).await
            }
            _ => Ok(Response::builder()
                .status(200)
                .body(Body::from(String::from(
                    "Multiple subgraphs deployed. \
                     Try /subgraphs/id/<ID> or \
                     /subgraphs/name/<NAME>",
                )))
                .unwrap()),
        }
    }

    /// Serves a static file.
    fn serve_file(&self, contents: &'static str) -> GraphQLServiceResponse {
        async move {
            Ok(Response::builder()
                .status(200)
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

        let store = self.store.cheap_clone();
        let subgraph_id =
            tokio::task::spawn_blocking(move || store.resolve_subgraph_name_to_id(subgraph_name))
                .await
                .unwrap() // Propagate panics.
                .map_err(|e| {
                    GraphQLServerError::InternalError(format!(
                        "Error resolving subgraph name: {}",
                        e
                    ))
                })
                .and_then(|subgraph_id_opt| {
                    subgraph_id_opt.ok_or(GraphQLServerError::ClientError(
                        "Subgraph name not found".to_owned(),
                    ))
                })?;

        self.handle_graphql_query(subgraph_id, request.into_body())
            .await
    }

    fn handle_graphql_query_by_id(
        self,
        id: String,
        request: Request<Body>,
    ) -> GraphQLServiceResponse {
        match SubgraphDeploymentId::new(id) {
            Err(()) => self.handle_not_found(),
            Ok(id) => self.handle_graphql_query(id, request.into_body()).boxed(),
        }
    }

    async fn handle_graphql_query(
        self,
        id: SubgraphDeploymentId,
        request_body: Body,
    ) -> GraphQLServiceResult {
        let service = self.clone();
        let service_metrics = self.metrics.clone();
        let sd_id = id.clone();

        match self.store.is_deployed(&id) {
            Err(e) => {
                return Err(GraphQLServerError::InternalError(e.to_string()));
            }
            Ok(false) => {
                return Err(GraphQLServerError::ClientError(format!(
                    "No data found for subgraph {}",
                    id
                )));
            }
            Ok(true) => (),
        }

        let schema = match self.store.api_schema(&id) {
            Ok(schema) => schema,
            Err(e) => {
                return Err(GraphQLServerError::InternalError(e.to_string()));
            }
        };

        let network = match self.store.network_name(&id) {
            Ok(network) => network,
            Err(e) => {
                return Err(GraphQLServerError::InternalError(e.to_string()));
            }
        };

        let start = Instant::now();
        let body = hyper::body::to_bytes(request_body)
            .map_err(|_| GraphQLServerError::InternalError("Failed to read request body".into()))
            .await?;
        let query = GraphQLRequest::new(body, schema, network).compat().await;

        let result = match query {
            Ok(query) => {
                let query_text = query.query_text.cheap_clone();
                let variables_text = query.variables_text.cheap_clone();

                let result =
                    graph::spawn_blocking_allow_panic(service.graphql_runner.run_query(query))
                        .await;

                match result {
                    Ok(res) => res,

                    // `Err(JoinError)` means a panic.
                    Err(e) => {
                        let e = e.into_panic();
                        let e = match e
                            .downcast_ref::<String>()
                            .map(|s| s.as_str())
                            .or(e.downcast_ref::<&'static str>().map(|&s| s))
                        {
                            Some(e) => e.to_string(),
                            None => "panic is not a string".to_string(),
                        };
                        let err = QueryExecutionError::Panic(e);
                        error!(
                            self.logger,
                            "panic when processing graphql query";
                            "panic" => err.to_string(),
                            "query" => query_text,
                            "variables" => variables_text,
                        );
                        Arc::new(QueryResult::from(err))
                    }
                }
            }
            Err(GraphQLServerError::QueryError(e)) => Arc::new(QueryResult::from(e)),
            Err(e) => return Err(e),
        };

        service_metrics
            .observe_query_execution_time(start.elapsed().as_secs_f64(), sd_id.deref().to_string());

        Ok(result.as_http_response())
    }

    // Handles OPTIONS requests
    fn handle_graphql_options(&self, _request: Request<Body>) -> GraphQLServiceResponse {
        async {
            Ok(Response::builder()
                .status(200)
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Headers", "Content-Type, User-Agent")
                .header("Access-Control-Allow-Methods", "GET, OPTIONS, POST")
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
                    .header(header::LOCATION, loc_header_val)
                    .body(Body::from("Redirecting..."))
                    .unwrap()
            })
    }

    /// Handles 404s.
    fn handle_not_found(&self) -> GraphQLServiceResponse {
        async {
            Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
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
                self.serve_file(include_str!("../assets/graphiql.css"))
            }
            (Method::GET, ["graphiql.min.js"]) => {
                self.serve_file(include_str!("../assets/graphiql.min.js"))
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

            // `/subgraphs` acts as an alias to `/subgraphs/id/SUBGRAPHS_ID`
            (Method::POST, &["subgraphs"]) => {
                self.handle_graphql_query_by_id(SUBGRAPHS_ID.to_string(), req)
            }
            (Method::OPTIONS, ["subgraphs"]) => self.handle_graphql_options(req),

            _ => self.handle_not_found(),
        }
    }
}

impl<Q, S> Service<Request<Body>> for GraphQLService<Q, S>
where
    Q: GraphQlRunner,
    S: SubgraphDeploymentStore + Store,
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
                    .header("Content-Type", "text/plain")
                    .body(Body::from(err.to_string()))
                    .unwrap()),
                Err(err @ GraphQLServerError::QueryError(_)) => {
                    error!(logger, "GraphQLService call failed: {}", err);

                    Ok(Response::builder()
                        .status(400)
                        .header("Content-Type", "text/plain")
                        .body(Body::from(format!("Query error: {}", err)))
                        .unwrap())
                }
                Err(err @ GraphQLServerError::InternalError(_)) => {
                    error!(logger, "GraphQLService call failed: {}", err);

                    Ok(Response::builder()
                        .status(500)
                        .header("Content-Type", "text/plain")
                        .body(Body::from(format!("Internal server error: {}", err)))
                        .unwrap())
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use http::status::StatusCode;
    use hyper::service::Service;
    use hyper::{Body, Method, Request};
    use std::collections::BTreeMap;

    use graph::data::graphql::effort::LoadManager;
    use graph::prelude::*;
    use graph_mock::{mock_store_with_users_subgraph, MockMetricsRegistry};
    use graphql_parser::query as q;

    use crate::test_utils;

    use super::GraphQLService;
    use super::GraphQLServiceMetrics;

    /// A simple stupid query runner for testing.
    pub struct TestGraphQlRunner;

    #[async_trait]
    impl GraphQlRunner for TestGraphQlRunner {
        async fn run_query_with_complexity(
            &self,
            _query: Query,
            _complexity: Option<u64>,
            _max_depth: Option<u8>,
            _max_first: Option<u32>,
        ) -> Arc<QueryResult> {
            unimplemented!();
        }

        async fn run_query(self: Arc<Self>, _query: Query) -> Arc<QueryResult> {
            Arc::new(QueryResult::new(Some(q::Value::Object(
                BTreeMap::from_iter(
                    vec![(
                        String::from("name"),
                        q::Value::String(String::from("Jordi")),
                    )]
                    .into_iter(),
                ),
            ))))
        }

        fn run_subscription(&self, _subscription: Subscription) -> SubscriptionResultFuture {
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
        let (store, subgraph_id) = mock_store_with_users_subgraph();
        let graphql_runner = Arc::new(TestGraphQlRunner);

        let node_id = NodeId::new("test").unwrap();
        let mut service =
            GraphQLService::new(logger, metrics, graphql_runner, store, 8001, node_id);

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

    #[tokio::test(threaded_scheduler)]
    async fn posting_valid_queries_yields_result_response() {
        let logger = Logger::root(slog::Discard, o!());
        let metrics_registry = Arc::new(MockMetricsRegistry::new());
        let metrics = Arc::new(GraphQLServiceMetrics::new(metrics_registry));
        let (store, subgraph_id) = mock_store_with_users_subgraph();
        let graphql_runner = Arc::new(TestGraphQlRunner);

        let node_id = NodeId::new("test").unwrap();
        let mut service =
            GraphQLService::new(logger, metrics, graphql_runner, store, 8001, node_id);

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
