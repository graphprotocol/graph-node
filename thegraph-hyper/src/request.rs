use futures::prelude::*;
use futures::sync::oneshot;
use graphql_parser;
use serde_json;

use thegraph::common::query::{Query, QueryError, QueryResult};
use thegraph::common::server::GraphQLServerError;

/// Future for a query parsed from an HTTP request.
pub struct GraphQLRequest {
    data: serde_json::Value,
}

impl GraphQLRequest {
    /// Creates a new GraphQLRequest future based on an HTTP request and a result sender.
    pub fn new(data: serde_json::Value) -> Self {
        GraphQLRequest { data }
    }
}

impl Future for GraphQLRequest {
    type Item = (Query, oneshot::Receiver<QueryResult>);
    type Error = GraphQLServerError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // Ensure the JSON data is an object
        let obj = self.data
            .as_object()
            .ok_or_else(|| GraphQLServerError::InternalError("Request data is not an object"))?;

        // Ensure the JSON data has a "query" field
        let query_value = obj.get("query").ok_or_else(|| {
            GraphQLServerError::InternalError("The \"query\" field missing in request data")
        })?;

        // Ensure the "query" field is a string
        let query_string = query_value.as_str().ok_or_else(|| {
            GraphQLServerError::InternalError("The\"query\" field is not a string")
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
