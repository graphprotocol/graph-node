use std::ops::Deref;
use std::time::Instant;

use graph::components::server::query::GraphQLServerError;
use graph::data::subgraph::schema::{SubgraphEntity, SUBGRAPHS_ID};
use graph::prelude::*;
use http::header;
use hyper::service::Service;
use hyper::{Body, Method, Request, Response, StatusCode};

use crate::request::GraphQLRequest;
use crate::response::GraphQLResponse;

/// An asynchronous response to a GraphQL request.
pub type GraphQLServiceResponse =
    Box<dyn Future<Item = Response<Body>, Error = GraphQLServerError> + Send>;

/// A Hyper Service that serves GraphQL over a POST / endpoint.
#[derive(Debug)]
pub struct GraphQLService<Q, S> {
    logger: Logger,
    graphql_runner: Arc<Q>,
    store: Arc<S>,
    ws_port: u16,
    node_id: NodeId,
}

impl<Q, S> Clone for GraphQLService<Q, S> {
    fn clone(&self) -> Self {
        Self {
            logger: self.logger.clone(),
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
        graphql_runner: Arc<Q>,
        store: Arc<S>,
        ws_port: u16,
        node_id: NodeId,
    ) -> Self {
        GraphQLService {
            logger,
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

    fn index(&self) -> GraphQLServiceResponse {
        let service = self.clone();

        // Ask for two to find out if there is more than one
        let entity_query = SubgraphEntity::query().range(EntityRange::first(2));

        Box::new(
            future::result(self.store.find(entity_query))
                .map_err(|e| GraphQLServerError::InternalError(e.to_string()))
                .and_then(move |mut subgraph_entities| -> GraphQLServiceResponse {
                    // If there is only one subgraph, redirect to it
                    match subgraph_entities.len() {
                        0 => Box::new(future::ok(
                            Response::builder()
                                .status(200)
                                .body(Body::from(String::from("No subgraphs deployed yet")))
                                .unwrap(),
                        )),
                        1 => {
                            let subgraph_entity = subgraph_entities.pop().unwrap();
                            let subgraph_name = subgraph_entity
                                .get("name")
                                .expect("subgraph entity without name");
                            return service.handle_temp_redirect(&format!(
                                "/subgraphs/name/{}",
                                subgraph_name
                            ));
                        }
                        _ => Box::new(future::ok(
                            Response::builder()
                                .status(200)
                                .body(Body::from(String::from(
                                    "Multiple subgraphs deployed. \
                                     Try /subgraphs/id/<ID> or \
                                     /subgraphs/name/<NAME>",
                                )))
                                .unwrap(),
                        )),
                    }
                }),
        )
    }

    /// Serves a static file.
    fn serve_file(&self, contents: &'static str) -> GraphQLServiceResponse {
        Box::new(future::ok(
            Response::builder()
                .status(200)
                .body(Body::from(contents))
                .unwrap(),
        ))
    }

    /// Serves a dynamically created file.
    fn serve_dynamic_file(&self, contents: String) -> GraphQLServiceResponse {
        Box::new(future::ok(
            Response::builder()
                .status(200)
                .body(Body::from(contents))
                .unwrap(),
        ))
    }

    fn handle_graphiql(&self) -> GraphQLServiceResponse {
        self.serve_dynamic_file(self.graphiql_html())
    }

    fn handle_graphql_query_by_name(
        &self,
        subgraph_name: String,
        request: Request<Body>,
    ) -> GraphQLServiceResponse {
        let service = self.clone();

        Box::new(
            SubgraphName::new(subgraph_name.as_str())
                .map_err(|()| {
                    GraphQLServerError::ClientError(format!(
                        "Invalid subgraph name {:?}",
                        subgraph_name
                    ))
                })
                .and_then(|subgraph_name| {
                    self.store
                        .resolve_subgraph_name_to_id(subgraph_name)
                        .map_err(|e| {
                            GraphQLServerError::InternalError(format!(
                                "Error resolving subgraph name: {}",
                                e
                            ))
                        })
                })
                .into_future()
                .and_then(|subgraph_id_opt| {
                    subgraph_id_opt.ok_or(GraphQLServerError::ClientError(
                        "Subgraph name not found".to_owned(),
                    ))
                })
                .and_then(move |subgraph_id| {
                    service.handle_graphql_query(&subgraph_id, request.into_body())
                }),
        )
    }

    fn handle_graphql_query_by_id(
        &self,
        id: String,
        request: Request<Body>,
    ) -> GraphQLServiceResponse {
        match SubgraphDeploymentId::new(id) {
            Err(()) => self.handle_not_found(),
            Ok(id) => self.handle_graphql_query(&id, request.into_body()),
        }
    }

    fn handle_graphql_query(
        &self,
        id: &SubgraphDeploymentId,
        request_body: Body,
    ) -> GraphQLServiceResponse {
        let service = self.clone();
        let logger = self.logger.clone();
        let sd_id = id.clone();

        match self.store.is_deployed(id) {
            Err(e) => {
                return Box::new(future::err(GraphQLServerError::InternalError(
                    e.to_string(),
                )));
            }
            Ok(false) => {
                return Box::new(future::err(GraphQLServerError::ClientError(format!(
                    "No data found for subgraph {}",
                    id
                ))));
            }
            Ok(true) => (),
        }

        let schema = match self.store.api_schema(id) {
            Ok(schema) => schema,
            Err(e) => {
                return Box::new(future::err(GraphQLServerError::InternalError(
                    e.to_string(),
                )));
            }
        };

        let start = Instant::now();
        Box::new(
            request_body
                .concat2()
                .map_err(|_| GraphQLServerError::from("Failed to read request body"))
                .and_then(move |body| GraphQLRequest::new(body, schema))
                .and_then(move |query| {
                    // Run the query using the query runner
                    service
                        .graphql_runner
                        .run_query(query)
                        .map_err(|e| GraphQLServerError::from(e))
                })
                .then(move |result| {
                    let elapsed = start.elapsed().as_millis();
                    match result {
                        Ok(_) => info!(
                            logger,
                            "GraphQL query served";
                            "subgraph_deployment" => sd_id.deref(),
                            "query_time_ms" => elapsed,
                            "code" => LogCode::GraphQlQuerySuccess,
                        ),
                        Err(ref e) => error!(
                            logger,
                            "GraphQL query failed";
                            "subgraph_deployment" => sd_id.deref(),
                            "error" => e.to_string(),
                            "query_time_ms" => elapsed,
                            "code" => LogCode::GraphQlQueryFailure,
                        ),
                    }
                    GraphQLResponse::new(result)
                }),
        )
    }

    // Handles OPTIONS requests
    fn handle_graphql_options(&self, _request: Request<Body>) -> GraphQLServiceResponse {
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
    fn handle_temp_redirect(&self, destination: &str) -> GraphQLServiceResponse {
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
    fn handle_not_found(&self) -> GraphQLServiceResponse {
        Box::new(future::ok(
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("Not found"))
                .unwrap(),
        ))
    }

    fn handle_call(&mut self, req: Request<Body>) -> GraphQLServiceResponse {
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

            (Method::GET, &["subgraphs", "id", _, "graphql"])
            | (Method::GET, &["subgraphs", "name", _, "graphql"])
            | (Method::GET, &["subgraphs", "name", _, _, "graphql"])
            | (Method::GET, &["subgraphs", "graphql"]) => self.handle_graphiql(),

            (Method::GET, path @ ["subgraphs", "id", _])
            | (Method::GET, path @ ["subgraphs", "name", _])
            | (Method::GET, path @ ["subgraphs", "name", _, _])
            | (Method::GET, path @ ["subgraphs"]) => {
                let dest = format!("/{}/graphql", path.join("/"));
                self.handle_temp_redirect(&dest)
            }

            (Method::POST, &["subgraphs", "id", subgraph_id]) => {
                self.handle_graphql_query_by_id(subgraph_id.to_owned(), req)
            }
            (Method::OPTIONS, ["subgraphs", "id", _]) => self.handle_graphql_options(req),
            (Method::POST, &["subgraphs", "name", subgraph_name]) => {
                self.handle_graphql_query_by_name(subgraph_name.to_owned(), req)
            }
            (Method::POST, ["subgraphs", "name", subgraph_name_part1, subgraph_name_part2]) => {
                let subgraph_name = format!("{}/{}", subgraph_name_part1, subgraph_name_part2);
                self.handle_graphql_query_by_name(subgraph_name, req)
            }
            (Method::OPTIONS, ["subgraphs", "name", _])
            | (Method::OPTIONS, ["subgraphs", "name", _, _]) => self.handle_graphql_options(req),

            // `/subgraphs` acts as an alias to `/subgraphs/id/SUBGRAPHS_ID`
            (Method::POST, &["subgraphs"]) => {
                self.handle_graphql_query_by_id(SUBGRAPHS_ID.to_string(), req)
            }
            (Method::OPTIONS, ["subgraphs"]) => self.handle_graphql_options(req),

            _ => self.handle_not_found(),
        }
    }
}

impl<Q, S> Service for GraphQLService<Q, S>
where
    Q: GraphQlRunner,
    S: SubgraphDeploymentStore + Store,
{
    type ReqBody = Body;
    type ResBody = Body;
    type Error = GraphQLServerError;
    type Future = GraphQLServiceResponse;

    fn call(&mut self, req: Request<Self::ReqBody>) -> Self::Future {
        let logger = self.logger.clone();

        // Returning Err here will prevent the client from receiving any response.
        // Instead, we generate a Response with an error code and return Ok
        Box::new(self.handle_call(req).then(move |result| match result {
            Ok(response) => Ok(response),
            Err(err @ GraphQLServerError::Canceled(_)) => {
                error!(logger, "GraphQLService call failed: {}", err);

                Ok(Response::builder()
                    .status(500)
                    .header("Content-Type", "text/plain")
                    .body(Body::from("Internal server error (operation canceled)"))
                    .unwrap())
            }
            Err(err @ GraphQLServerError::ClientError(_)) => {
                debug!(logger, "GraphQLService call failed: {}", err);

                Ok(Response::builder()
                    .status(400)
                    .header("Content-Type", "text/plain")
                    .body(Body::from(format!("Invalid request: {}", err)))
                    .unwrap())
            }
            Err(err @ GraphQLServerError::QueryError(_)) => {
                error!(logger, "GraphQLService call failed: {}", err);

                Ok(Response::builder()
                    .status(500)
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
        }))
    }
}

#[cfg(test)]
mod tests {
    use graph_mock::MockStore;
    use graphql_parser::query as q;
    use http::status::StatusCode;
    use hyper::service::Service;
    use hyper::{Body, Method, Request};
    use std::collections::BTreeMap;
    use web3::types::H256;

    use graph::data::subgraph::schema::*;
    use graph::prelude::*;

    use super::GraphQLService;
    use crate::test_utils;

    /// A simple stupid query runner for testing.
    pub struct TestGraphQlRunner;

    impl GraphQlRunner for TestGraphQlRunner {
        fn run_query_with_complexity(
            &self,
            _query: Query,
            _complexity: Option<u64>,
            _max_depth: Option<u8>,
            _max_first: Option<u32>,
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
        let store = Arc::new(MockStore::user_store());
        let id = MockStore::user_subgraph_id();
        let schema = store
            .input_schema(&id)
            .expect("Failed to get schema")
            .as_ref()
            .clone();
        let manifest = SubgraphManifest {
            id: id.clone(),
            location: "".to_owned(),
            spec_version: "".to_owned(),
            description: None,
            repository: None,
            schema,
            data_sources: vec![],
            templates: vec![],
        };

        let graphql_runner = Arc::new(TestGraphQlRunner);
        store
            .apply_metadata_operations(
                SubgraphDeploymentEntity::new(
                    &manifest,
                    false,
                    false,
                    None,
                    Some(EthereumBlockPointer {
                        hash: H256::zero(),
                        number: 0,
                    }),
                )
                .create_operations(&id),
            )
            .unwrap();

        let node_id = NodeId::new("test").unwrap();
        let mut service = GraphQLService::new(logger, graphql_runner, store, 8001, node_id);

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
        let store = Arc::new(MockStore::user_store());
        let id = MockStore::user_subgraph_id();
        let schema = store
            .input_schema(&id)
            .expect("Failed to get schema")
            .as_ref()
            .clone();
        let manifest = SubgraphManifest {
            id: id.clone(),
            location: "".to_owned(),
            spec_version: "".to_owned(),
            description: None,
            repository: None,
            schema,
            data_sources: vec![],
            templates: vec![],
        };
        let graphql_runner = Arc::new(TestGraphQlRunner);

        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        runtime
            .block_on(future::lazy(move || {
                let res: Result<_, ()> = Ok({
                    store
                        .apply_metadata_operations(
                            SubgraphDeploymentEntity::new(
                                &manifest,
                                false,
                                false,
                                None,
                                Some(EthereumBlockPointer {
                                    hash: H256::zero(),
                                    number: 0,
                                }),
                            )
                            .create_operations(&id),
                        )
                        .unwrap();

                    let node_id = NodeId::new("test").unwrap();
                    let mut service =
                        GraphQLService::new(logger, graphql_runner, store, 8001, node_id);

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
