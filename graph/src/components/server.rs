use futures::prelude::*;
use futures::sync::mpsc::{Receiver, Sender};
use futures::sync::oneshot::Canceled;
use serde::ser::*;
use std::error::Error;
use std::fmt;
use std::io;
use std::sync::Arc;

use super::store::StoreEvent;
use super::subgraph::{SchemaEvent, SubgraphProvider};
use data::query::{Query, QueryError};
use prelude::Logger;
use util::stream::StreamError;

/// Errors that can occur while processing incoming requests.
#[derive(Debug)]
pub enum GraphQLServerError {
    Canceled(Canceled),
    ClientError(String),
    QueryError(QueryError),
    InternalError(String),
}

impl From<Canceled> for GraphQLServerError {
    fn from(e: Canceled) -> Self {
        GraphQLServerError::Canceled(e)
    }
}

impl From<QueryError> for GraphQLServerError {
    fn from(e: QueryError) -> Self {
        GraphQLServerError::QueryError(e)
    }
}

impl From<&'static str> for GraphQLServerError {
    fn from(s: &'static str) -> Self {
        GraphQLServerError::InternalError(String::from(s))
    }
}

impl From<String> for GraphQLServerError {
    fn from(s: String) -> Self {
        GraphQLServerError::InternalError(s)
    }
}

impl fmt::Display for GraphQLServerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &GraphQLServerError::Canceled(_) => write!(f, "Query was canceled"),
            &GraphQLServerError::ClientError(ref s) => write!(f, "{}", s),
            &GraphQLServerError::QueryError(ref e) => write!(f, "{}", e),
            &GraphQLServerError::InternalError(ref s) => write!(f, "{}", s),
        }
    }
}

impl Error for GraphQLServerError {
    fn description(&self) -> &str {
        "Failed to process the GraphQL request"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            &GraphQLServerError::Canceled(ref e) => Some(e),
            &GraphQLServerError::ClientError(_) => None,
            &GraphQLServerError::QueryError(ref e) => Some(e),
            &GraphQLServerError::InternalError(_) => None,
        }
    }
}

impl Serialize for GraphQLServerError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let &GraphQLServerError::QueryError(ref e) = self {
            serializer.serialize_some(e)
        } else {
            let mut map = serializer.serialize_map(Some(1))?;
            let msg = format!("{}", self);
            map.serialize_entry("message", msg.as_str())?;
            map.end()
        }
    }
}

/// Common trait for GraphQL server implementations.
pub trait GraphQLServer {
    type ServeError;

    /// Sender to which others should write whenever the schema that the server
    /// should serve changes.
    fn schema_event_sink(&mut self) -> Sender<SchemaEvent>;

    /// Receiver from which others can read incoming queries for processing.
    /// Can only be called once. Any consecutive call will result in a StreamError.
    fn query_stream(&mut self) -> Result<Receiver<Query>, StreamError>;

    /// Creates a new Tokio task that, when spawned, brings up the GraphQL server.
    fn serve(
        &mut self,
        port: u16,
    ) -> Result<Box<Future<Item = (), Error = ()> + Send>, Self::ServeError>;
}

pub trait JsonRpcServer {
    type Server;

    fn serve(
        port: u16,
        provider: Arc<impl SubgraphProvider>,
        logger: Logger,
    ) -> Result<Self::Server, io::Error>;
}
