use hyper::service::Service;
use hyper::{Body, Method, Request, Response, StatusCode};
use itertools::Itertools;
use std::collections::BTreeMap;
use std::sync::RwLock;

use graph::components::server::query::GraphQLServerError;
use graph::prelude::*;

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
}

impl<Q, S> Clone for GraphQLService<Q, S> {
    fn clone(&self) -> Self {
        Self {
            schemas: self.schemas.clone(),
            graphql_runner: self.graphql_runner.clone(),
            store: self.store.clone(),
        }
    }
}

impl<Q, S> GraphQLService<Q, S>
where
    Q: GraphQlRunner + 'static,
    S: Store + 'static,
{
    /// Creates a new GraphQL service.
    pub fn new(
        schemas: Arc<RwLock<BTreeMap<SubgraphId, Schema>>>,
        graphql_runner: Arc<Q>,
        store: Arc<S>,
    ) -> Self {
        GraphQLService {
            schemas,
            graphql_runner,
            store,
        }
    }

    fn index(&self) -> GraphQLServiceResponse {
        self.handle_not_found()
    }

    /// Serves a GraphiQL index.html.
    fn serve_file(&self, contents: &'static str) -> GraphQLServiceResponse {
        Box::new(future::ok(
            Response::builder()
                .status(200)
                .body(Body::from(contents))
                .unwrap(),
        ))
    }

    /// Handles GraphQL queries received via POST /.
    fn handle_graphql_query(
        &self,
        name_or_id: &str,
        request: Request<Body>,
    ) -> GraphQLServiceResponse {
        let name_or_id = name_or_id.to_owned();
        let service = self.clone();

        Box::new(
            future::result(self.store.read_subgraph_name(name_or_id.clone()))
                .map_err(|e| GraphQLServerError::InternalError(e.to_string()))
                .and_then(move |id_opt_opt| -> GraphQLServiceResponse {
                    let service = service.clone();
                    let schemas_rwlock = service.schemas.clone();
                    let schemas = schemas_rwlock.read().unwrap();

                    let id = match id_opt_opt {
                        // If name_or_id matched a name with non-null ID
                        Some(Some(id)) => id,

                        // Otherwise, treat name_or_id as an ID
                        _ => name_or_id.clone(),
                    };

                    let schema_opt = schemas.get(&id).map(|s| s.to_owned());

                    match schema_opt {
                        None => service.handle_not_found(),
                        Some(schema) => Box::new(
                            request
                                .into_body()
                                .concat2()
                                .map_err(|_| {
                                    GraphQLServerError::from("Failed to read request body")
                                }).and_then(move |body| GraphQLRequest::new(body, schema.clone()))
                                .and_then(move |query| {
                                    // Run the query using the query runner
                                    service
                                        .graphql_runner
                                        .run_query(query)
                                        .map_err(|e| GraphQLServerError::from(e))
                                }).then(|result| GraphQLResponse::new(result)),
                        ),
                    }
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
                .body(Body::from(""))
                .unwrap(),
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
    S: Store + 'static,
{
    type ReqBody = Body;
    type ResBody = Body;
    type Error = GraphQLServerError;
    type Future = GraphQLServiceResponse;

    fn call(&mut self, req: Request<Self::ReqBody>) -> Self::Future {
        let method = req.method().clone();
        let uri = req.uri().clone();
        match (method, uri.path()) {
            (Method::GET, "/") => self.index(),

            (Method::GET, "/graphiql.css") => {
                self.serve_file(include_str!("../assets/graphiql.css"))
            }
            (Method::GET, "/graphiql.min.js") => {
                self.serve_file(include_str!("../assets/graphiql.min.js"))
            }

            // Request is relative to a subgraph.
            (method, path) => {
                let mut path = path.split('/');
                let _empty = path.next();
                let name_or_id = path.next();
                let rest = path.join("/");

                match (method, name_or_id, rest.as_str()) {
                    // GraphiQL
                    (Method::GET, Some(_), "") => {
                        self.serve_file(include_str!("../assets/index.html"))
                    }

                    // POST / receives GraphQL queries
                    (Method::POST, Some(name_or_id), "graphql") => {
                        self.handle_graphql_query(&name_or_id, req)
                    }

                    // OPTIONS / allows to check for GraphQL HTTP features
                    (Method::OPTIONS, Some(_), "graphql") => self.handle_graphql_options(req),

                    // Everything else results in a 404
                    _ => self.handle_not_found(),
                }
            }
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
        let mut service = GraphQLService::new(schema, graphql_runner, store);

        let request = Request::builder()
            .method(Method::POST)
            .uri(format!("http://localhost:8000/{}/graphql", id))
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

                    let mut service = GraphQLService::new(schema, graphql_runner, store);

                    let request = Request::builder()
                        .method(Method::POST)
                        .uri(format!("http://localhost:8000/{}/graphql", id))
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
