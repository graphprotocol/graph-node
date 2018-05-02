use futures::sync::oneshot;
use futures::sync::mpsc::Sender;
use std::error::Error;
use std::fmt;

/// A GraphQL query as submitted by a client, either directly or through a subscription.
#[derive(Debug)]
pub struct Query<T> {
    pub request: T,
    pub result_sender: oneshot::Sender<QueryResult>,
}

/// The result of running a query, if successful.
#[derive(Debug)]
pub struct QueryResult;

/// Error caused while running a [Query](struct.Query.html).
#[derive(Debug)]
pub enum QueryError {}

impl Error for QueryError {
    fn description(&self) -> &str {
        "Failed to run query"
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "QueryError: {}", self.description())
    }
}

/// Common trait for query runners that run queries against a [Store](../store/trait.Store.html).
pub trait QueryRunner<T> {
    // Sender to which others can write queries that need to be run.
    fn query_sink(&mut self) -> Sender<Query<T>>;
}
