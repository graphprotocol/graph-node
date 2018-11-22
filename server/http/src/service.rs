use graph::components::server::query::GraphQLServerError;
use graph::data::subgraph::schema::SUBGRAPHS_ID;
use graph::prelude::*;
use http::header;
use hyper::service::Service;
use hyper::{Body, Method, Request, Response, StatusCode};
use std::collections::BTreeMap;
use std::sync::RwLock;

use request::GraphQLRequest;
use response::GraphQLResponse;

/// An asynchronous response to a GraphQL request.
pub type GraphQLServiceResponse =
    Box<Future<Item = Response<Body>, Error = GraphQLServerError> + Send>;

/// A Hyper Service that serves GraphQL over a POST / endpoint.
#[derive(Debug)]
pub struct GraphQLService<Q, S> {
    // Maps IDs to schemas.
    schemas: Arc<RwLock<BTreeMap<SubgraphId, Schema>>>,
    graphql_runner: Arc<Q>,
    store: Arc<S>,
    ws_port: u16,
}

impl<Q, S> Clone for GraphQLService<Q, S> {
    fn clone(&self) -> Self {
        Self {
            ws_port: self.ws_port,
            schemas: self.schemas.clone(),
            graphql_runner: self.graphql_runner.clone(),
            store: self.store.clone(),
        }
    }
}

impl<Q, S> GraphQLService<Q, S>
where
    Q: GraphQlRunner + 'static,
    S: Store,
{
    /// Creates a new GraphQL service.
    pub fn new(
        schemas: Arc<RwLock<BTreeMap<SubgraphId, Schema>>>,
        graphql_runner: Arc<Q>,
        store: Arc<S>,
        ws_port: u16,
    ) -> Self {
        GraphQLService {
            ws_port,
            schemas,
            graphql_runner,
            store,
        }
    }

    fn graphiql_html(&self) -> String {
        include_str!("../assets/index.html")
            .replace("__WS_PORT__", format!("{}", self.ws_port).as_str())
    }

    fn index(&self) -> GraphQLServiceResponse {
        let service = self.clone();

        Box::new(
            future::result(self.store.read_all_subgraph_names())
                .map_err(|e| GraphQLServerError::InternalError(e.to_string()))
                .and_then(
                    move |mut subgraph_name_mappings| -> GraphQLServiceResponse {
                        // If there is only one subgraph, redirect to it
                        if subgraph_name_mappings.len() == 1 {
                            let (subgraph_name, subgraph_id_opt) =
                                subgraph_name_mappings.pop().unwrap();

                            if let Some(_) = subgraph_id_opt {
                                return service.handle_temp_redirect(&format!(
                                    "/subgraphs/name/{}",
                                    subgraph_name
                                ));
                            }
                        }

                        service.handle_not_found()
                    },
                ),
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

    fn handle_graphiql_by_name(&self, name: &str) -> GraphQLServiceResponse {
        let service = self.clone();

        let graphiql_html = self.graphiql_html();

        Box::new(
            future::result(self.store.read_subgraph_name(name.to_owned()))
                .map_err(|e| GraphQLServerError::InternalError(e.to_string()))
                .and_then(move |id_opt_opt| match id_opt_opt {
                    None | Some(None) => service.handle_not_found(),
                    Some(Some(_)) => service.serve_dynamic_file(graphiql_html),
                }),
        )
    }

    fn handle_graphiql_by_id(&self, id: SubgraphId) -> GraphQLServiceResponse {
        if self.schemas.read().unwrap().contains_key(&id) {
            self.serve_dynamic_file(self.graphiql_html())
        } else {
            self.handle_not_found()
        }
    }

    fn handle_graphql_query_by_name(
        &self,
        name: &str,
        request: Request<Body>,
    ) -> GraphQLServiceResponse {
        let name_clone1 = name.to_owned();
        let name_clone2 = name.to_owned();
        let service = self.clone();

        Box::new(
            future::result(self.store.read_subgraph_name(name.to_owned()))
                .map_err(|e| GraphQLServerError::InternalError(e.to_string()))
                .and_then(move |id_opt_opt| {
                    id_opt_opt.ok_or_else(|| {
                        GraphQLServerError::ClientError(format!(
                            "subgraph with name {:?} not found",
                            name_clone1.clone()
                        ))
                    })
                }).and_then(move |id_opt| {
                    id_opt.ok_or_else(|| {
                        GraphQLServerError::ClientError(format!(
                            "subgraph {} is not yet deployed",
                            name_clone2.clone()
                        ))
                    })
                }).and_then(move |id| service.handle_graphql_query(id, request.into_body())),
        )
    }

    fn handle_graphql_query_by_id(
        &self,
        id: SubgraphId,
        request: Request<Body>,
    ) -> GraphQLServiceResponse {
        self.handle_graphql_query(id, request.into_body())
    }

    fn handle_graphql_query(&self, id: SubgraphId, request_body: Body) -> GraphQLServiceResponse {
        let service = self.clone();

        let schemas_rwlock = service.schemas.clone();
        let schemas = schemas_rwlock.read().unwrap();

        let schema_opt = schemas.get(&id).map(|s| s.to_owned());

        match schema_opt {
            None => service.handle_not_found(),
            Some(schema) => Box::new(
                request_body
                    .concat2()
                    .map_err(|_| GraphQLServerError::from("Failed to read request body"))
                    .and_then(move |body| GraphQLRequest::new(body, schema.clone()))
                    .and_then(move |query| {
                        // Run the query using the query runner
                        service
                            .graphql_runner
                            .run_query(query)
                            .map_err(|e| GraphQLServerError::from(e))
                    }).then(|result| GraphQLResponse::new(result)),
            ),
        }
    }

    // Handles OPTIONS requests
    fn handle_graphql_options(&self, _request: Request<Body>) -> GraphQLServiceResponse {
        Box::new(future::ok(
            Response::builder()
                .status(200)
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Headers", "Content-Type")
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
}

impl<Q, S> Service for GraphQLService<Q, S>
where
    Q: GraphQlRunner + 'static,
    S: Store,
{
    type ReqBody = Body;
    type ResBody = Body;
    type Error = GraphQLServerError;
    type Future = GraphQLServiceResponse;

    fn call(&mut self, req: Request<Self::ReqBody>) -> Self::Future {
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

            (Method::GET, &["subgraphs", "id", subgraph_id]) => {
                self.handle_graphiql_by_id(subgraph_id.to_owned())
            }
            (Method::POST, &["subgraphs", "id", subgraph_id, "graphql"]) => {
                self.handle_graphql_query_by_id(subgraph_id.to_owned(), req)
            }
            (Method::OPTIONS, ["subgraphs", "id", _, "graphql"]) => {
                self.handle_graphql_options(req)
            }

            (Method::GET, ["subgraphs", "name", subgraph_name]) => {
                self.handle_graphiql_by_name(subgraph_name)
            }
            (Method::POST, ["subgraphs", "name", subgraph_name, "graphql"]) => {
                self.handle_graphql_query_by_name(subgraph_name, req)
            }
            (Method::OPTIONS, ["subgraphs", "name", _, "graphql"]) => {
                self.handle_graphql_options(req)
            }

            // `/subgraphs` acts as an alias to `/subgraphs/id/SUBGRAPHS_ID`
            (Method::GET, &["subgraphs"]) => self.handle_graphiql_by_id(SUBGRAPHS_ID.to_owned()),
            (Method::POST, &["subgraphs", "graphql"]) => {
                self.handle_graphql_query_by_id(SUBGRAPHS_ID.to_owned(), req)
            }
            (Method::OPTIONS, ["subgraphs"]) => self.handle_graphql_options(req),

            _ => self.handle_not_found(),
        }
    }
}

#[cfg(test)]
mod tests {
    use graph_mock::MockStore;
    use graphql_parser;
    use graphql_parser::query as q;
    use http::status::StatusCode;
    use hyper::service::Service;
    use hyper::{Body, Method, Request};
    use std::collections::BTreeMap;
    use std::iter::once;
    use std::iter::FromIterator;
    use std::sync::RwLock;

    use graph::prelude::*;

    use super::GraphQLService;
    use test_utils;

    /// A simple stupid query runner for testing.
    pub struct TestGraphQlRunner;

    impl GraphQlRunner for TestGraphQlRunner {
        fn run_query(&self, _query: Query) -> QueryResultFuture {
            Box::new(future::ok(QueryResult::new(Some(q::Value::Object(
                BTreeMap::from_iter(
                    vec![(
                        String::from("name"),
                        q::Value::String(String::from("Jordi")),
                    )].into_iter(),
                ),
            )))))
        }

        fn run_subscription(&self, _subscription: Subscription) -> SubscriptionResultFuture {
            unimplemented!();
        }
    }

    #[test]
    fn posting_invalid_query_yields_error_response() {
        let id = "test-schema".to_string();
        let schema = Arc::new(RwLock::new(BTreeMap::from_iter(once((
            id.clone(),
            Schema {
                id: id.clone(),
                document: graphql_parser::parse_schema(
                    "\
                     scalar String \
                     type Query { name: String } \
                     ",
                ).unwrap(),
            },
        )))));
        let graphql_runner = Arc::new(TestGraphQlRunner);
        let store = Arc::new(MockStore::new());
        let mut service = GraphQLService::new(schema, graphql_runner, store, 8001);

        let request = Request::builder()
            .method(Method::POST)
            .uri(format!("http://localhost:8000/subgraphs/id/{}/graphql", id))
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

        assert_eq!(message, "The \"query\" field missing in request data");
    }

    #[test]
    fn posting_valid_queries_yields_result_response() {
        let graphql_runner = Arc::new(TestGraphQlRunner);
        let store = Arc::new(MockStore::new());
        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        runtime
            .block_on(future::lazy(|| {
                let res: Result<_, ()> = Ok({
                    let id = "test-schema".to_string();
                    let schema = Arc::new(RwLock::new(BTreeMap::from_iter(once((
                        id.clone(),
                        Schema {
                            id: id.clone(),
                            document: graphql_parser::parse_schema(
                                "\
                                 scalar String \
                                 type Query { name: String } \
                                 ",
                            ).unwrap(),
                        },
                    )))));

                    let mut service = GraphQLService::new(schema, graphql_runner, store, 8001);

                    let request = Request::builder()
                        .method(Method::POST)
                        .uri(format!("http://localhost:8000/subgraphs/id/{}/graphql", id))
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
            })).unwrap()
    }
}
