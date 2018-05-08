use futures::prelude::*;
use futures::future;
use futures::sync::mpsc::Sender;
use hyper::{Body, Method, Request, Response, StatusCode};
use hyper::service::Service;

use thegraph::common::query::Query;
use thegraph::common::server::GraphQLServerError;

use request::GraphQLRequest;
use response::GraphQLResponse;

/// An asynchronous response to a GraphQL request.
pub type GraphQLServiceResponse =
    Box<Future<Item = Response<Body>, Error = GraphQLServerError> + Send>;

/// A Hyper Service that serves GraphQL over a POST / endpoint.
#[derive(Debug)]
pub struct GraphQLService {
    query_sink: Sender<Query>,
}

impl GraphQLService {
    /// Creates a new GraphQL service.
    pub fn new(query_sink: Sender<Query>) -> Self {
        GraphQLService { query_sink }
    }

    /// Handles GraphQL queries received via POST /.
    fn handle_graphql_query(&self, request: Request<Body>) -> GraphQLServiceResponse {
        let query_sink = self.query_sink.clone();

        Box::new(
            request
                .into_body()
                .concat2()
                .map_err(|_| GraphQLServerError::from("Failed to read request body"))
                .and_then(|body| GraphQLRequest::new(body))
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
            // POST / receives GraphQL queries
            (&Method::POST, "/") => self.handle_graphql_query(req),

            // Everything else results in a 404
            _ => self.handle_not_found(req),
        }
    }
}
