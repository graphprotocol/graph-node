use futures::future;
use futures::prelude::*;
use futures::sync::mpsc::Sender;
use hyper::service::Service;
use hyper::{Body, Method, Request, Response, StatusCode};
use std::sync::{Arc, Mutex};

use thegraph::components::server::GraphQLServerError;
use thegraph::prelude::*;

use request::GraphQLRequest;
use response::GraphQLResponse;

/// An asynchronous response to a GraphQL request.
pub type GraphQLServiceResponse =
    Box<Future<Item = Response<Body>, Error = GraphQLServerError> + Send>;

/// A Hyper Service that serves GraphQL over a POST / endpoint.
#[derive(Debug)]
pub struct GraphQLService {
    schema: Arc<Mutex<Option<Schema>>>,
    query_sink: Sender<Query>,
}

impl GraphQLService {
    /// Creates a new GraphQL service.
    pub fn new(schema: Arc<Mutex<Option<Schema>>>, query_sink: Sender<Query>) -> Self {
        GraphQLService { schema, query_sink }
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
    fn handle_graphql_query(&self, request: Request<Body>) -> GraphQLServiceResponse {
        let query_sink = self.query_sink.clone();
        let schema = self.schema.clone();

        Box::new(
            request
                .into_body()
                .concat2()
                .map_err(|_| GraphQLServerError::from("Failed to read request body"))
                .and_then(move |body| {
                    let schema = schema.lock().unwrap();
                    GraphQLRequest::new(body, schema.clone())
                })
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
    fn handle_not_found(&self, _req: Request<Body>) -> GraphQLServiceResponse {
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
        match (req.method(), req.uri().path()) {
            // GraphiQL
            (&Method::GET, "/") => self.serve_file(include_str!("../assets/index.html")),
            (&Method::GET, "/graphiql.css") => {
                self.serve_file(include_str!("../assets/graphiql.css"))
            }
            (&Method::GET, "/graphiql.min.js") => {
                self.serve_file(include_str!("../assets/graphiql.min.js"))
            }

            // POST / receives GraphQL queries
            (&Method::POST, "/graphql") => self.handle_graphql_query(req),

            // OPTIONS / allows to check for GraphQL HTTP features
            (&Method::OPTIONS, "/graphql") => self.handle_graphql_options(req),

            // Everything else results in a 404
            _ => self.handle_not_found(req),
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::prelude::*;
    use futures::sync::mpsc::channel;
    use graphql_parser;
    use graphql_parser::query::Value;
    use http::status::StatusCode;
    use hyper::service::Service;
    use hyper::{Body, Method, Request};
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use tokio_core::reactor::Core;

    use thegraph::data::query::QueryResult;
    use thegraph::data::schema::Schema;

    use super::GraphQLService;
    use test_utils;

    #[test]
    fn posting_invalid_query_yields_error_response() {
        let mut core = Core::new().unwrap();

        let schema = Arc::new(Mutex::new(Some(Schema {
            id: "test-schema".to_string(),
            document: graphql_parser::parse_schema(
                "\
                 scalar String \
                 type Query { name: String } \
                 ",
            ).unwrap(),
        })));
        let (query_sink, _) = channel(1);
        let mut service = GraphQLService::new(schema, query_sink);

        let request = Request::builder()
            .method(Method::POST)
            .uri("http://localhost:8000/")
            .body(Body::from("{}"))
            .unwrap();

        let response = core.run(service.call(request))
            .expect("Should return a response");
        let errors =
            test_utils::assert_error_response(&mut core, response, StatusCode::BAD_REQUEST);

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
        let mut core = Core::new().unwrap();

        let schema = Arc::new(Mutex::new(Some(Schema {
            id: "test-schema".to_string(),
            document: graphql_parser::parse_schema(
                "\
                 scalar String \
                 type Query { name: String } \
                 ",
            ).unwrap(),
        })));
        let (query_sink, query_stream) = channel(1);
        let mut service = GraphQLService::new(schema, query_sink);

        core.handle().spawn(
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
            .uri("http://localhost:8000/")
            .body(Body::from("{\"query\": \"{ name }\"}"))
            .unwrap();

        // The response must be a 200
        let response = core.run(service.call(request))
            .expect("Should return a response");
        let data = test_utils::assert_successful_response(&mut core, response);

        // The body should match the simulated query result
        let name = data.get("name")
            .expect("Query result data has no \"name\" field")
            .as_str()
            .expect("Query result field \"name\" is not a string");
        assert_eq!(name, "Jordi".to_string());
    }
}
