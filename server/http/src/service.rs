use futures::sync::mpsc::Sender;
use http::header;
use hyper::service::Service;
use hyper::{Body, Method, Request, Response, StatusCode};
use itertools::Itertools;
use std::collections::BTreeMap;
use std::sync::RwLock;

use graph::components::server::GraphQLServerError;
use graph::prelude::*;

use request::GraphQLRequest;
use response::GraphQLResponse;

/// An asynchronous response to a GraphQL request.
pub type GraphQLServiceResponse =
    Box<Future<Item = Response<Body>, Error = GraphQLServerError> + Send>;

/// A Hyper Service that serves GraphQL over a POST / endpoint.
#[derive(Debug)]
pub struct GraphQLService {
    schemas: Arc<RwLock<BTreeMap<String, Schema>>>,
    query_sink: Sender<Query>,
}

impl GraphQLService {
    /// Creates a new GraphQL service.
    pub fn new(schemas: Arc<RwLock<BTreeMap<String, Schema>>>, query_sink: Sender<Query>) -> Self {
        GraphQLService {
            schemas,
            query_sink,
        }
    }

    fn index(&self) -> GraphQLServiceResponse {
        let len = self.schemas.read().unwrap().len();
        match self.schemas.read().unwrap().values().next().clone() {
            // If there's only 1 schema, redirect to it.
            Some(schema) if len == 1 => Box::new(future::ok(
                Response::builder()
                    .status(StatusCode::PERMANENT_REDIRECT)
                    .header(
                        header::LOCATION,
                        header::HeaderValue::from_str(&format!("/{}", schema.name))
                            .expect("invalid subgraph name"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )),
            _ => self.handle_not_found(),
        }
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
    fn handle_graphql_query(&self, name: &str, request: Request<Body>) -> GraphQLServiceResponse {
        let query_sink = self.query_sink.clone();
        let schema = self
            .schemas
            .read()
            .unwrap()
            .get(name)
            .expect("schema not found")
            .clone();

        Box::new(
            request
                .into_body()
                .concat2()
                .map_err(|_| GraphQLServerError::from("Failed to read request body"))
                .and_then(move |body| GraphQLRequest::new(body, schema.clone()))
                .and_then(move |(query, receiver)| {
                    // Forward the query to the system
                    query_sink
                        .send(query)
                        .wait()
                        .expect("Failed to forward incoming query");

                    // Continue with waiting to receive a result
                    receiver.map_err(|e| GraphQLServerError::from(e))
                })
                .then(|result| GraphQLResponse::new(result)),
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

impl Service for GraphQLService {
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
                let name = path.next();
                let rest = path.join("/");

                match (method, name, rest.as_str()) {
                    // GraphiQL
                    (Method::GET, Some(_), "") => {
                        self.serve_file(include_str!("../assets/index.html"))
                    }

                    // POST / receives GraphQL queries
                    (Method::POST, Some(name), "graphql") => self.handle_graphql_query(&name, req),

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
    use futures::sync::mpsc::channel;
    use graphql_parser;
    use graphql_parser::query::Value;
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

    #[test]
    fn posting_invalid_query_yields_error_response() {
        let id = "test-schema".to_string();
        let schema = Arc::new(RwLock::new(BTreeMap::from_iter(once((
            id.clone(),
            Schema {
                name: id.clone(),
                id: id.clone(),
                document: graphql_parser::parse_schema(
                    "\
                     scalar String \
                     type Query { name: String } \
                     ",
                ).unwrap(),
            },
        )))));
        let (query_sink, _) = channel(1);
        let mut service = GraphQLService::new(schema, query_sink);

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
        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        runtime
            .block_on(future::lazy(|| {
                let res: Result<_, ()> = Ok({
                    let id = "test-schema".to_string();
                    let schema = Arc::new(RwLock::new(BTreeMap::from_iter(once((
                        id.clone(),
                        Schema {
                            name: id.clone(),
                            id: id.clone(),
                            document: graphql_parser::parse_schema(
                                "\
                                 scalar String \
                                 type Query { name: String } \
                                 ",
                            ).unwrap(),
                        },
                    )))));
                    let (query_sink, query_stream) = channel(1);
                    let mut service = GraphQLService::new(schema, query_sink);

                    tokio::spawn(
                        query_stream
                            .for_each(move |query| {
                                let mut map = BTreeMap::new();
                                map.insert("name".to_string(), Value::String("Jordi".to_string()));
                                let data = Value::Object(map);
                                let result = QueryResult::new(Some(data));
                                query.result_sender.send(result).unwrap();
                                Ok(())
                            })
                            .fuse(),
                    );

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
            }))
            .unwrap()
    }
}
