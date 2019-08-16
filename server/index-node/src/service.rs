use http::header;
use hyper::service::Service;
use hyper::{Body, Method, Request, Response, StatusCode};
use std::time::Instant;

use graph::components::server::query::GraphQLServerError;
use graph::prelude::*;
use graph_graphql::prelude::{execute_query, QueryExecutionOptions};

use crate::request::IndexNodeRequest;
use crate::resolver::IndexNodeResolver;
use crate::response::IndexNodeResponse;
use crate::schema::SCHEMA;

/// An asynchronous response to a GraphQL request.
pub type IndexNodeServiceResponse =
    Box<dyn Future<Item = Response<Body>, Error = GraphQLServerError> + Send>;

/// A Hyper Service that serves GraphQL over a POST / endpoint.
#[derive(Debug)]
pub struct IndexNodeService<Q, S> {
    logger: Logger,
    graphql_runner: Arc<Q>,
    store: Arc<S>,
    node_id: NodeId,
}

impl<Q, S> Clone for IndexNodeService<Q, S> {
    fn clone(&self) -> Self {
        Self {
            logger: self.logger.clone(),
            graphql_runner: self.graphql_runner.clone(),
            store: self.store.clone(),
            node_id: self.node_id.clone(),
        }
    }
}

impl<Q, S> IndexNodeService<Q, S>
where
    Q: GraphQlRunner,
    S: SubgraphDeploymentStore + Store,
{
    /// Creates a new GraphQL service.
    pub fn new(logger: Logger, graphql_runner: Arc<Q>, store: Arc<S>, node_id: NodeId) -> Self {
        IndexNodeService {
            logger,
            graphql_runner,
            store,
            node_id,
        }
    }

    fn graphiql_html(&self) -> String {
        include_str!("../assets/index.html").into()
    }

    /// Serves a static file.
    fn serve_file(&self, contents: &'static str) -> IndexNodeServiceResponse {
        Box::new(future::ok(
            Response::builder()
                .status(200)
                .body(Body::from(contents))
                .unwrap(),
        ))
    }

    /// Serves a dynamically created file.
    fn serve_dynamic_file(&self, contents: String) -> IndexNodeServiceResponse {
        Box::new(future::ok(
            Response::builder()
                .status(200)
                .body(Body::from(contents))
                .unwrap(),
        ))
    }

    fn index(&self) -> IndexNodeServiceResponse {
        Box::new(future::ok(
            Response::builder()
                .status(200)
                .body(Body::from("OK"))
                .unwrap(),
        ))
    }

    fn handle_graphiql(&self) -> IndexNodeServiceResponse {
        self.serve_dynamic_file(self.graphiql_html())
    }

    fn handle_graphql_query(&self, request_body: Body) -> IndexNodeServiceResponse {
        let logger = self.logger.clone();
        let store = self.store.clone();
        let result_logger = self.logger.clone();
        let graphql_runner = self.graphql_runner.clone();

        // Obtain the schema for the index node GraphQL API
        let schema = SCHEMA.clone();

        let start = Instant::now();
        Box::new(
            request_body
                .concat2()
                .map_err(|_| GraphQLServerError::from("Failed to read request body"))
                .and_then(move |body| IndexNodeRequest::new(body, schema))
                .and_then(move |query| {
                    let logger = logger.clone();
                    let graphql_runner = graphql_runner.clone();

                    // Run the query using the index node resolver
                    Box::new(future::ok(execute_query(
                        &query,
                        QueryExecutionOptions {
                            logger: logger.clone(),
                            resolver: IndexNodeResolver::new(&logger, graphql_runner, store),
                            deadline: None,
                            max_complexity: None,
                            max_depth: 100,
                            max_first: std::i64::MAX,
                        },
                    )))
                })
                .then(move |result| {
                    let elapsed = start.elapsed().as_millis();
                    match result {
                        Ok(_) => info!(
                            result_logger,
                            "GraphQL query served";
                            "query_time_ms" => elapsed,
                            "code" => LogCode::GraphQlQuerySuccess,
                        ),
                        Err(ref e) => error!(
                            result_logger,
                            "GraphQL query failed";
                            "error" => e.to_string(),
                            "query_time_ms" => elapsed,
                            "code" => LogCode::GraphQlQueryFailure,
                        ),
                    }
                    IndexNodeResponse::new(result)
                }),
        )
    }

    // Handles OPTIONS requests
    fn handle_graphql_options(&self, _request: Request<Body>) -> IndexNodeServiceResponse {
        Box::new(future::ok(
            Response::builder()
                .status(200)
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Headers", "Content-Type")
                .header("Access-Control-Allow-Methods", "GET, OPTIONS, POST")
                .body(Body::from(""))
                .unwrap(),
        ))
    }

    /// Handles 302 redirects
    fn handle_temp_redirect(&self, destination: &str) -> IndexNodeServiceResponse {
        Box::new(future::result(
            header::HeaderValue::from_str(destination)
                .map_err(|_| GraphQLServerError::from("invalid characters in redirect URL"))
                .map(|loc_header_val| {
                    Response::builder()
                        .status(StatusCode::FOUND)
                        .header(header::LOCATION, loc_header_val)
                        .body(Body::from("Redirecting..."))
                        .unwrap()
                }),
        ))
    }

    /// Handles 404s.
    fn handle_not_found(&self) -> IndexNodeServiceResponse {
        Box::new(future::ok(
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("Not found"))
                .unwrap(),
        ))
    }

    fn handle_call(&mut self, req: Request<Body>) -> IndexNodeServiceResponse {
        let method = req.method().clone();

        let path = req.uri().path().to_owned();
        let path_segments = {
            let mut segments = path.split('/');

            // Remove leading '/'
            assert_eq!(segments.next(), Some(""));

            segments.collect::<Vec<_>>()
        };

        match (method, path_segments.as_slice()) {
            (Method::GET, [""]) => self.index(),
            (Method::GET, ["graphiql.css"]) => {
                self.serve_file(include_str!("../assets/graphiql.css"))
            }
            (Method::GET, ["graphiql.min.js"]) => {
                self.serve_file(include_str!("../assets/graphiql.min.js"))
            }

            (Method::GET, path @ ["graphql"]) => {
                let dest = format!("/{}/playground", path.join("/"));
                self.handle_temp_redirect(&dest)
            }
            (Method::GET, ["graphql", "playground"]) => self.handle_graphiql(),

            (Method::POST, ["graphql"]) => self.handle_graphql_query(req.into_body()),
            (Method::OPTIONS, ["graphql"]) => self.handle_graphql_options(req),

            _ => self.handle_not_found(),
        }
    }
}

impl<Q, S> Service for IndexNodeService<Q, S>
where
    Q: GraphQlRunner,
    S: SubgraphDeploymentStore + Store,
{
    type ReqBody = Body;
    type ResBody = Body;
    type Error = GraphQLServerError;
    type Future = IndexNodeServiceResponse;

    fn call(&mut self, req: Request<Self::ReqBody>) -> Self::Future {
        let logger = self.logger.clone();

        // Returning Err here will prevent the client from receiving any response.
        // Instead, we generate a Response with an error code and return Ok
        Box::new(self.handle_call(req).then(move |result| match result {
            Ok(response) => Ok(response),
            Err(err @ GraphQLServerError::Canceled(_)) => {
                error!(logger, "IndexNodeService call failed: {}", err);

                Ok(Response::builder()
                    .status(500)
                    .header("Content-Type", "text/plain")
                    .body(Body::from("Internal server error (operation canceled)"))
                    .unwrap())
            }
            Err(err @ GraphQLServerError::ClientError(_)) => {
                debug!(logger, "IndexNodeService call failed: {}", err);

                Ok(Response::builder()
                    .status(400)
                    .header("Content-Type", "text/plain")
                    .body(Body::from(format!("Invalid request: {}", err)))
                    .unwrap())
            }
            Err(err @ GraphQLServerError::QueryError(_)) => {
                error!(logger, "IndexNodeService call failed: {}", err);

                Ok(Response::builder()
                    .status(500)
                    .header("Content-Type", "text/plain")
                    .body(Body::from(format!("Query error: {}", err)))
                    .unwrap())
            }
            Err(err @ GraphQLServerError::InternalError(_)) => {
                error!(logger, "IndexNodeService call failed: {}", err);

                Ok(Response::builder()
                    .status(500)
                    .header("Content-Type", "text/plain")
                    .body(Body::from(format!("Internal server error: {}", err)))
                    .unwrap())
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use graph::web3::types::H256;
    use graph_mock::MockStore;
    use graphql_parser::query as q;
    use http::status::StatusCode;
    use hyper::service::Service;
    use hyper::{Body, Method, Request};
    use std::collections::BTreeMap;

    use graph::data::subgraph::schema::*;
    use graph::prelude::*;

    use super::IndexNodeService;
    use crate::test_utils;

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

        fn run_query(&self, _query: Query) -> QueryResultFuture {
            Box::new(future::ok(QueryResult::new(Some(q::Value::Object(
                BTreeMap::from_iter(
                    vec![(
                        String::from("name"),
                        q::Value::String(String::from("Jordi")),
                    )]
                    .into_iter(),
                ),
            )))))
        }

        fn run_subscription(&self, _subscription: Subscription) -> SubscriptionResultFuture {
            unreachable!();
        }
    }

    #[test]
    fn posting_invalid_query_yields_error_response() {
        let logger = Logger::root(slog::Discard, o!());
        let id = SubgraphDeploymentId::new("testschema").unwrap();
        let schema = Schema::parse(
            "\
             scalar String \
             type Query @entity { name: String } \
             ",
            id.clone(),
        )
        .unwrap();
        let manifest = SubgraphManifest {
            id: id.clone(),
            location: "".to_owned(),
            spec_version: "".to_owned(),
            description: None,
            repository: None,
            schema: schema.clone(),
            data_sources: vec![],
        };

        let graphql_runner = Arc::new(TestGraphQlRunner);
        let store = Arc::new(MockStore::new(vec![(id.clone(), schema)]));
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
                .create_operations(&id),
                None,
            )
            .unwrap();

        let node_id = NodeId::new("test").unwrap();
        let mut service = IndexNodeService::new(logger, graphql_runner, store, 8001, node_id);

        let request = Request::builder()
            .method(Method::POST)
            .uri(format!("http://localhost:8000/subgraphs/id/{}", id))
            .body(Body::from("{}"))
            .unwrap();

        let response = service
            .call(request)
            .wait()
            .expect("Should return a response");
        let errors = test_utils::assert_error_response(response, StatusCode::BAD_REQUEST);

        let message = errors[0]
            .as_object()
            .expect("Query error is not an object")
            .get("message")
            .expect("Error contains no message")
            .as_str()
            .expect("Error message is not a string");

        assert_eq!(
            message,
            "GraphQL server error (client error): The \"query\" field missing in request data"
        );
    }

    #[test]
    fn posting_valid_queries_yields_result_response() {
        let logger = Logger::root(slog::Discard, o!());
        let id = SubgraphDeploymentId::new("testschema").unwrap();
        let schema = Schema::parse(
            "\
             scalar String \
             type Query @entity { name: String } \
             ",
            id.clone(),
        )
        .unwrap();
        let manifest = SubgraphManifest {
            id: id.clone(),
            location: "".to_owned(),
            spec_version: "".to_owned(),
            description: None,
            repository: None,
            schema: schema.clone(),
            data_sources: vec![],
        };
        let graphql_runner = Arc::new(TestGraphQlRunner);
        let store = Arc::new(MockStore::new(vec![(id.clone(), schema)]));

        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        runtime
            .block_on(future::lazy(move || {
                let res: Result<_, ()> = Ok({
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
                            .create_operations(&id),
                            None,
                        )
                        .unwrap();

                    let node_id = NodeId::new("test").unwrap();
                    let mut service =
                        IndexNodeService::new(logger, graphql_runner, store, 8001, node_id);

                    let request = Request::builder()
                        .method(Method::POST)
                        .uri(format!("http://localhost:8000/subgraphs/id/{}", id))
                        .body(Body::from("{\"query\": \"{ name }\"}"))
                        .unwrap();

                    // The response must be a 200
                    let response = service
                        .call(request)
                        .wait()
                        .expect("Should return a response");
                    let data = test_utils::assert_successful_response(response);

                    // The body should match the simulated query result
                    let name = data
                        .get("name")
                        .expect("Query result data has no \"name\" field")
                        .as_str()
                        .expect("Query result field \"name\" is not a string");
                    assert_eq!(name, "Jordi".to_string());
                });
                res
            }))
            .unwrap()
    }
}
