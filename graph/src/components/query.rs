use futures::prelude::*;

use data::query::{Query, QueryError, QueryResult};

/// A component that can run GraphqL queries against a [Store](../store/trait.Store.html).
pub trait QueryRunner: Send + Sync {
    /// Runs a GraphQL query and returns its result.
    fn run_query(&self, query: Query)
        -> Box<Future<Item = QueryResult, Error = QueryError> + Send>;
}
