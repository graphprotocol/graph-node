use futures::prelude::*;
use futures::sync::oneshot;
use graphql_parser;
use hyper::Chunk;
use serde_json;

use thegraph::components::server::GraphQLServerError;
use thegraph::data::query::{Query, QueryError, QueryResult};

/// Future for a query parsed from an HTTP request.
pub struct GraphQLRequest {
    body: Chunk,
}

impl GraphQLRequest {
    /// Creates a new GraphQLRequest future based on an HTTP request and a result sender.
    pub fn new(body: Chunk) -> Self {
        GraphQLRequest { body }
    }
}

impl Future for GraphQLRequest {
    type Item = (Query, oneshot::Receiver<QueryResult>);
    type Error = GraphQLServerError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // Parse request body as JSON
        let json: serde_json::Value = serde_json::from_slice(&self.body)
            .or_else(|e| Err(GraphQLServerError::ClientError(format!("{}", e))))?;

        // Ensure the JSON data is an object
        let obj = json.as_object().ok_or_else(|| {
            GraphQLServerError::ClientError(String::from("Request data is not an object"))
        })?;

        // Ensure the JSON data has a "query" field
        let query_value = obj.get("query").ok_or_else(|| {
            GraphQLServerError::ClientError(String::from(
                "The \"query\" field missing in request data",
            ))
        })?;

        // Ensure the "query" field is a string
        let query_string = query_value.as_str().ok_or_else(|| {
            GraphQLServerError::ClientError(String::from("The\"query\" field is not a string"))
        })?;

        // Parse the "query" field of the JSON body
        let document = graphql_parser::parse_query(query_string)
            .or_else(|e| Err(GraphQLServerError::from(QueryError::from(e))))?;

        // Create a one-shot channel to allow another part of the system
        // to notify the service when the query has completed
        let (sender, receiver) = oneshot::channel();

        Ok(Async::Ready((
            Query {
                document,
                result_sender: sender,
            },
            receiver,
        )))
    }
}

#[cfg(test)]
mod tests {
    use graphql_parser;
    use hyper;
    use tokio_core::reactor::Core;

    use super::GraphQLRequest;

    #[test]
    fn rejects_invalid_json() {
        let mut core = Core::new().unwrap();
        let request = GraphQLRequest::new(hyper::Chunk::from("!@#)%"));
        let result = core.run(request);
        result.expect_err("Should reject invalid JSON");
    }

    #[test]
    fn rejects_json_without_query_field() {
        let mut core = Core::new().unwrap();
        let request = GraphQLRequest::new(hyper::Chunk::from("{}"));
        let result = core.run(request);
        result.expect_err("Should reject JSON without query field");
    }

    #[test]
    fn rejects_json_with_non_string_query_field() {
        let mut core = Core::new().unwrap();
        let request = GraphQLRequest::new(hyper::Chunk::from("{\"query\": 5}"));
        let result = core.run(request);
        result.expect_err("Should reject JSON with a non-string query field");
    }

    #[test]
    fn rejects_broken_queries() {
        let mut core = Core::new().unwrap();
        let request = GraphQLRequest::new(hyper::Chunk::from("{\"query\": \"foo\"}"));
        let result = core.run(request);
        result.expect_err("Should reject broken queries");
    }

    #[test]
    fn accepts_valid_queries() {
        let mut core = Core::new().unwrap();
        let request = GraphQLRequest::new(hyper::Chunk::from("{\"query\": \"{ user { name } }\"}"));
        let result = core.run(request);
        let (query, _) = result.expect("Should accept valid queries");
        assert_eq!(
            query.document,
            graphql_parser::parse_query("{ user { name } }").unwrap()
        );
    }
}
