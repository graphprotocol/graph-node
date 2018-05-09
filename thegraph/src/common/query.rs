use futures::sync::oneshot;
use futures::sync::mpsc::Sender;
use graphql_parser::query;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::string::FromUtf8Error;

/// A GraphQL query as submitted by a client, either directly or through a subscription.
#[derive(Debug)]
pub struct Query {
    pub document: query::Document,
    pub result_sender: oneshot::Sender<QueryResult>,
}

/// The result of running a query, if successful.
#[derive(Debug, Serialize)]
pub struct QueryResult {
    pub data: Option<HashMap<String, String>>,
}

impl QueryResult {
    pub fn new(data: Option<HashMap<String, String>>) -> Self {
        QueryResult { data }
    }
}

/// Error caused while running a [Query](struct.Query.html).
#[derive(Debug)]
pub enum QueryError {
    EncodingError(FromUtf8Error),
    ParseError(query::ParseError),
}

impl From<FromUtf8Error> for QueryError {
    fn from(e: FromUtf8Error) -> Self {
        QueryError::EncodingError(e)
    }
}

impl From<query::ParseError> for QueryError {
    fn from(e: query::ParseError) -> Self {
        QueryError::ParseError(e)
    }
}

impl Error for QueryError {
    fn description(&self) -> &str {
        "Query error"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            &QueryError::EncodingError(ref e) => Some(e),
            _ => None,
        }
    }
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &QueryError::EncodingError(ref e) => write!(f, "QueryError: {}", e),
            &QueryError::ParseError(ref e) => write!(f, "QueryError: {}", e),
        }
    }
}

/// Common trait for query runners that run queries against a [Store](../store/trait.Store.html).
pub trait QueryRunner {
    // Sender to which others can write queries that need to be run.
    fn query_sink(&mut self) -> Sender<Query>;
}
