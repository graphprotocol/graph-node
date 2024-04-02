use std::convert::TryFrom;
use std::env;
use std::sync::Arc;
use std::time::Instant;

use graph::cheap_clone::CheapClone;
use graph::components::graphql::GraphQlRunner;
use graph::components::server::query::ServerResponse;
use graph::components::server::query::ServerResult;
use graph::components::versions::ApiVersion;
use graph::data::query::QueryResult;
use graph::data::subgraph::DeploymentHash;
use graph::data::subgraph::SubgraphName;
use graph::env::ENV_VARS;
use graph::http_body_util::{BodyExt, Full};
use graph::hyper::header::{
    ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
    CONTENT_LENGTH, CONTENT_TYPE, LOCATION,
};
use graph::hyper::{body::Body, header::HeaderValue};
use graph::hyper::{Method, Request, Response, StatusCode};
use graph::prelude::serde_json;
use graph::prelude::serde_json::json;
use graph::semver::VersionReq;
use graph::slog::error;
use graph::slog::Logger;
use graph::url::form_urlencoded;
use graph::{components::server::query::ServerError, data::query::QueryTarget};

use crate::request::parse_graphql_request;

fn client_error(msg: impl Into<String>) -> ServerResponse {
    let response_obj = json!({
        "error": msg.into()
    });
    let response_str = serde_json::to_string(&response_obj).unwrap();

    Response::builder()
        .status(400)
        .header(CONTENT_TYPE, "application/json")
        .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .body(Full::from(response_str))
        .unwrap()
}

/// A Hyper Service that serves GraphQL over a POST / endpoint.
#[derive(Debug)]
pub struct GraphQLService<Q> {
    logger: Logger,
    graphql_runner: Arc<Q>,
    ws_port: u16,
}

impl<Q> GraphQLService<Q>
where
    Q: GraphQlRunner,
{
    /// Creates a new GraphQL service.
    pub fn new(logger: Logger, graphql_runner: Arc<Q>, ws_port: u16) -> Self {
        GraphQLService {
            logger,
            graphql_runner,
            ws_port,
        }
    }

    fn graphiql_html(&self) -> String {
        include_str!("../assets/index.html")
            .replace("__WS_PORT__", format!("{}", self.ws_port).as_str())
    }

    async fn index(&self) -> ServerResult {
        let response_obj = json!({
            "message": "Access deployed subgraphs by deployment ID at \
                        /subgraphs/id/<ID> or by name at /subgraphs/name/<NAME>"
        });
        let response_str = serde_json::to_string(&response_obj).unwrap();

        Ok(Response::builder()
            .status(200)
            .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .header(CONTENT_TYPE, "application/json")
            .body(Full::from(response_str))
            .unwrap())
    }

    /// Serves a dynamically created file.
    fn serve_dynamic_file(&self, contents: String) -> ServerResponse {
        Response::builder()
            .status(200)
            .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .header(CONTENT_TYPE, "text/html; charset=utf-8")
            .body(Full::from(contents))
            .unwrap()
    }

    fn handle_graphiql(&self) -> ServerResult {
        Ok(self.serve_dynamic_file(self.graphiql_html()))
    }

    fn resolve_api_version<T>(&self, request: &Request<T>) -> Result<ApiVersion, ServerError> {
        let mut version = ApiVersion::default();

        if let Some(query) = request.uri().query() {
            let potential_version_requirement = query.split('&').find_map(|pair| {
                if pair.starts_with("api-version=") {
                    if let Some(version_requirement) = pair.split('=').nth(1) {
                        return Some(version_requirement);
                    }
                }
                None
            });

            if let Some(version_requirement) = potential_version_requirement {
                version = ApiVersion::new(
                    &VersionReq::parse(version_requirement)
                        .map_err(|error| ServerError::ClientError(error.to_string()))?,
                )
                .map_err(ServerError::ClientError)?;
            }
        }

        Ok(version)
    }

    async fn handle_graphql_query_by_name<T: Body>(
        &self,
        subgraph_name: String,
        request: Request<T>,
    ) -> ServerResult {
        let version = self.resolve_api_version(&request)?;
        let subgraph_name = SubgraphName::new(subgraph_name.as_str()).map_err(|()| {
            ServerError::ClientError(format!("Invalid subgraph name {:?}", subgraph_name))
        })?;

        self.handle_graphql_query(QueryTarget::Name(subgraph_name, version), request)
            .await
    }

    async fn handle_graphql_query_by_id<T: Body>(
        &self,
        id: String,
        request: Request<T>,
    ) -> ServerResult {
        let id = DeploymentHash::new(id)
            .map_err(|id| ServerError::ClientError(format!("Invalid subgraph id `{}`", id)))?;
        let version = self.resolve_api_version(&request)?;

        self.handle_graphql_query(QueryTarget::Deployment(id, version), request)
            .await
    }

    async fn handle_graphql_query<T: Body>(
        &self,
        target: QueryTarget,
        request: Request<T>,
    ) -> ServerResult {
        let start = Instant::now();
        let trace = {
            !ENV_VARS.graphql.query_trace_token.is_empty()
                && request
                    .headers()
                    .get("X-GraphTraceQuery")
                    .map(|v| {
                        v.to_str()
                            .map(|s| s == &ENV_VARS.graphql.query_trace_token)
                            .unwrap_or(false)
                    })
                    .unwrap_or(false)
        };
        let body = request
            .collect()
            .await
            .map_err(|_| ServerError::InternalError("Failed to read request body".into()))?
            .to_bytes();
        let query = parse_graphql_request(&body, trace);
        let query_parsing_time = start.elapsed();

        let mut result = match query {
            Ok(query) => {
                self.graphql_runner
                    .cheap_clone()
                    .run_query(query, target)
                    .await
            }
            Err(ServerError::QueryError(e)) => QueryResult::from(e).into(),
            Err(e) => return Err(e),
        };

        result.trace.query_parsing(query_parsing_time);
        self.graphql_runner
            .metrics()
            .observe_query_parsing(query_parsing_time, &result);
        self.graphql_runner
            .metrics()
            .observe_query_execution(start.elapsed(), &result);

        Ok(result.as_http_response())
    }

    // Handles OPTIONS requests
    fn handle_graphql_options<T>(&self, _request: Request<T>) -> ServerResult {
        Ok(Response::builder()
            .status(200)
            .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .header(ACCESS_CONTROL_ALLOW_HEADERS, "Content-Type, User-Agent")
            .header(ACCESS_CONTROL_ALLOW_METHODS, "GET, OPTIONS, POST")
            .header(CONTENT_TYPE, "text/html; charset=utf-8")
            .body(Full::from(""))
            .unwrap())
    }

    /// Handles 302 redirects
    fn handle_temp_redirect(&self, destination: String) -> ServerResult {
        HeaderValue::try_from(destination)
            .map_err(|_| ServerError::ClientError("invalid characters in redirect URL".into()))
            .map(|loc_header_val| {
                Response::builder()
                    .status(StatusCode::FOUND)
                    .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                    .header(LOCATION, loc_header_val)
                    .header(CONTENT_TYPE, "text/plain; charset=utf-8")
                    .body(Full::from("Redirecting..."))
                    .unwrap()
            })
    }

    fn handle_not_found(&self) -> ServerResult {
        let response_obj = json!({
            "message": "Not found"
        });
        let response_str = serde_json::to_string(&response_obj).unwrap();

        Ok(Response::builder()
            .status(200)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .body(Full::from(response_str))
            .unwrap())
    }

    fn handle_mutations(&self) -> ServerResult {
        Ok(client_error("Can't use mutations with GET method"))
    }
    /// Handles requests without content type.
    fn handle_requests_without_content_type(&self) -> ServerResult {
        Ok(client_error("Content-Type header is required"))
    }

    /// Handles requests without body.
    fn handle_requests_without_body(&self) -> ServerResult {
        Ok(client_error("Body is required"))
    }

    fn has_request_body<T>(&self, req: &Request<T>) -> bool {
        if let Some(length) = req.headers().get(CONTENT_LENGTH) {
            if let Ok(length) = length.to_str() {
                if let Ok(length) = length.parse::<usize>() {
                    return length > 0;
                }
            }
        }
        false
    }

    async fn handle_call<T: Body>(&self, req: Request<T>) -> ServerResult {
        let method = req.method().clone();

        let path = req.uri().path().to_owned();
        let path_segments = {
            let mut segments = path.split('/');

            // Remove leading '/'
            assert_eq!(segments.next(), Some(""));

            segments.collect::<Vec<_>>()
        };

        let headers = req.headers();
        let content_type = headers.get("content-type");

        let less_strict_graphql_compliance = env::var("LESS_STRICT_GRAPHQL_COMPLIANCE").is_ok();

        if !less_strict_graphql_compliance {
            if method == Method::POST && (content_type.is_none()) {
                return self.handle_requests_without_content_type();
            }

            if method == Method::POST && !self.has_request_body(&req) {
                return self.handle_requests_without_body();
            }
        }

        // Filter out empty strings from path segments
        fn filter_and_join_segments(segments: &[&str]) -> String {
            segments
                .iter()
                .filter(|&&segment| !segment.is_empty())
                .map(|&segment| segment)
                .collect::<Vec<&str>>()
                .join("/")
        }

        let is_mutation = req
            .uri()
            .query()
            .and_then(|query_str| {
                form_urlencoded::parse(query_str.as_bytes())
                    .find(|(key, _)| key == "query")
                    .map(|(_, value)| value.into_owned())
            })
            .unwrap_or_else(|| String::new())
            .trim()
            .to_lowercase()
            .starts_with("mutation");
        match (method, path_segments.as_slice()) {
            (Method::GET, [""]) => self.index().await,
            (Method::GET, &["subgraphs", "id", _, "graphql"])
            | (Method::GET, &["subgraphs", "name", .., "graphql"])
            | (Method::GET, &["subgraphs", "network", _, _, "graphql"])
            | (Method::GET, &["subgraphs", "graphql"]) => self.handle_graphiql(),

            (Method::GET, _path @ ["subgraphs", "name", ..]) if is_mutation => {
                self.handle_mutations()
            }
            (Method::GET, path @ ["subgraphs", "id", _])
            | (Method::GET, path @ ["subgraphs", "name", ..])
            | (Method::GET, path @ ["subgraphs", "network", _, _]) => {
                let filtered_path = filter_and_join_segments(path);
                let dest = format!("/{}/graphql", filtered_path);
                self.handle_temp_redirect(dest)
            }

            (Method::POST, &["subgraphs", "id", subgraph_id]) => {
                self.handle_graphql_query_by_id(subgraph_id.to_owned(), req)
                    .await
            }
            (Method::OPTIONS, ["subgraphs", "id", _]) => self.handle_graphql_options(req),
            (Method::POST, path @ ["subgraphs", "name", ..]) => {
                let subgraph_name = filter_and_join_segments(&path[2..]);
                self.handle_graphql_query_by_name(subgraph_name, req).await
            }

            (Method::OPTIONS, ["subgraphs", "name", ..]) => self.handle_graphql_options(req),

            _ => self.handle_not_found(),
        }
    }

    pub async fn call<T: Body + std::fmt::Debug>(&self, req: Request<T>) -> ServerResponse {
        // Returning Err here will prevent the client from receiving any response.
        // Instead, we generate a Response with an error code and return Ok
        let result = self.handle_call(req).await;

        match result {
            Ok(response) => response,
            Err(err @ ServerError::ClientError(_)) => {
                let response_obj = json!({
                    "error": err.to_string()
                });
                let response_str = serde_json::to_string(&response_obj).unwrap();

                Response::builder()
                    .status(400)
                    .header(CONTENT_TYPE, "application/json")
                    .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                    .body(Full::from(response_str))
                    .unwrap()
            }
            Err(err @ ServerError::QueryError(_)) => {
                error!(self.logger, "GraphQLService call failed: {}", err);

                let response_obj = json!({
                    "QueryError": err.to_string()
                });
                let response_str = serde_json::to_string(&response_obj).unwrap();

                Response::builder()
                    .status(400)
                    .header(CONTENT_TYPE, "application/json")
                    .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                    .body(Full::from(response_str))
                    .unwrap()
            }
            Err(err @ ServerError::InternalError(_)) => {
                error!(self.logger, "GraphQLService call failed: {}", err);

                Response::builder()
                    .status(500)
                    .header(CONTENT_TYPE, "text/plain; charset=utf-8")
                    .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                    .body(Full::from(format!("Internal server error: {}", err)))
                    .unwrap()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use graph::data::value::{Object, Word};
    use graph::http_body_util::{BodyExt, Full};
    use graph::hyper::body::Bytes;
    use graph::hyper::header::{CONTENT_LENGTH, CONTENT_TYPE};
    use graph::hyper::{Method, Request, StatusCode};
    use graph::prelude::serde_json::json;

    use graph::data::query::{QueryResults, QueryTarget};
    use graph::prelude::*;

    use crate::test_utils;

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
        fn observe_query_blocks_behind(&self, _blocks_behind: i32, _id: &DeploymentHash) {}
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

        fn metrics(&self) -> Arc<dyn GraphQLMetrics> {
            Arc::new(TestGraphQLMetrics)
        }
    }

    #[tokio::test]
    async fn querying_not_found_routes_responds_correctly() {
        let logger = Logger::root(slog::Discard, o!());
        let graphql_runner = Arc::new(TestGraphQlRunner);

        let service = GraphQLService::new(logger, graphql_runner, 8001);

        let request: Request<Full<Bytes>> = Request::builder()
            .method(Method::GET)
            .header(CONTENT_TYPE, "text/plain; charset=utf-8")
            .uri("http://localhost:8000/not_found_route".to_string())
            .body(Full::from("{}"))
            .unwrap();

        let response = service.call(request.into()).await;

        let content_type_header = response.status();
        assert_eq!(content_type_header, StatusCode::OK);

        let content_type_header = response.headers().get(CONTENT_TYPE).unwrap();
        assert_eq!(content_type_header, "application/json");

        let body_bytes = response.body().clone().collect().await.unwrap().to_bytes();
        let json: serde_json::Result<serde_json::Value> =
            serde_json::from_str(String::from_utf8(body_bytes.to_vec()).unwrap().as_str());

        assert!(json.is_ok(), "Response body is not valid JSON");

        assert_eq!(json.unwrap(), serde_json::json!({"message": "Not found"}));
    }

    #[tokio::test]
    async fn posting_invalid_query_yields_error_response() {
        let logger = Logger::root(slog::Discard, o!());
        let subgraph_id = USERS.clone();
        let graphql_runner = Arc::new(TestGraphQlRunner);

        let service = GraphQLService::new(logger, graphql_runner, 8001);

        let request: Request<Full<Bytes>> = Request::builder()
            .method(Method::POST)
            .header(CONTENT_TYPE, "text/plain; charset=utf-8")
            .header(CONTENT_LENGTH, 100)
            .uri(format!(
                "http://localhost:8000/subgraphs/id/{}",
                subgraph_id
            ))
            .body(Full::from("{}"))
            .unwrap();

        let response = service.call(request).await;
        let errors =
            test_utils::assert_error_response(response, StatusCode::BAD_REQUEST, false).await;

        let message = errors[0].as_str().expect("Error message is not a string");

        let response = json!({
            "error": "GraphQL server error (client error): The \"query\" field is missing in request data".to_string()
        });

        assert_eq!(message, response.to_string());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn posting_valid_queries_yields_result_response() {
        let logger = Logger::root(slog::Discard, o!());
        let subgraph_id = USERS.clone();
        let graphql_runner = Arc::new(TestGraphQlRunner);

        let service = GraphQLService::new(logger, graphql_runner, 8001);

        let request: Request<Full<Bytes>> = Request::builder()
            .method(Method::POST)
            .header(CONTENT_TYPE, "text/plain; charset=utf-8")
            .header(CONTENT_LENGTH, 100)
            .uri(format!(
                "http://localhost:8000/subgraphs/id/{}",
                subgraph_id
            ))
            .body(Full::from("{\"query\": \"{ name }\"}"))
            .unwrap();

        // The response must be a 200
        let response = service.call(request).await;

        let data = test_utils::assert_successful_response(response).await;

        // The body should match the simulated query result
        let name = data
            .get("name")
            .expect("Query result data has no \"name\" field")
            .as_str()
            .expect("Query result field \"name\" is not a string");
        assert_eq!(name, "Jordi".to_string());
    }
}
