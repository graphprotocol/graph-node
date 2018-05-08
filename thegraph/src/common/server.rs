use futures::prelude::*;
use futures::sync::oneshot::Canceled;
use futures::sync::mpsc::{Receiver, Sender};
use serde_json;
use std::error::Error;
use std::fmt;
use super::query::{Query, QueryError};
use super::schema::SchemaProviderEvent;
use super::store::StoreEvent;
use super::util::stream::StreamError;

/// Errors that can occur while processing incoming requests.
#[derive(Debug)]
pub enum GraphQLServerError {
    Canceled(Canceled),
    JSONError(serde_json::Error),
    QueryError(QueryError),
    InternalError(&'static str),
}

impl From<Canceled> for GraphQLServerError {
    fn from(e: Canceled) -> Self {
        GraphQLServerError::Canceled(e)
    }
}

impl From<serde_json::Error> for GraphQLServerError {
    fn from(e: serde_json::Error) -> Self {
        GraphQLServerError::JSONError(e)
    }
}

impl From<QueryError> for GraphQLServerError {
    fn from(e: QueryError) -> Self {
        GraphQLServerError::QueryError(e)
    }
}

impl From<&'static str> for GraphQLServerError {
    fn from(s: &'static str) -> Self {
        GraphQLServerError::InternalError(s)
    }
}

impl fmt::Display for GraphQLServerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &GraphQLServerError::Canceled(ref e) => write!(f, "Query was canceled: {}", e),
            &GraphQLServerError::JSONError(ref e) => write!(f, "JSON error: {}", e),
            &GraphQLServerError::QueryError(ref e) => write!(f, "Query error: {}", e),
            &GraphQLServerError::InternalError(ref s) => write!(f, "Internal error: {}", s),
        }
    }
}

impl Error for GraphQLServerError {
    fn description(&self) -> &str {
        "GraphQLServerError"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            &GraphQLServerError::Canceled(ref e) => Some(e),
            &GraphQLServerError::JSONError(ref e) => Some(e),
            &GraphQLServerError::QueryError(ref e) => Some(e),
            &GraphQLServerError::InternalError(_) => None,
        }
    }
}

/// Common trait for GraphQL server implementations.
pub trait GraphQLServer {
    type ServeError;

    /// Sender to which others should write whenever the schema that the server
    /// should serve changes.
    fn schema_provider_event_sink(&mut self) -> Sender<SchemaProviderEvent>;

    /// Sender to which others should write store events that might require
    /// subscription queries to re-run.
    fn store_event_sink(&mut self) -> Sender<StoreEvent>;

    /// Receiver from which others can read incoming queries for processing.
    /// Can only be called once. Any consecutive call will result in a StreamError.
    fn query_stream(&mut self) -> Result<Receiver<Query>, StreamError>;

    /// Creates a new Tokio task that, when spawned, brings up the GraphQL server.
    fn serve(&mut self) -> Result<Box<Future<Item = (), Error = ()> + Send>, Self::ServeError>;
}
