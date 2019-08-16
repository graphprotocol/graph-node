use futures::prelude::*;

use crate::data::query::{Query, QueryError, QueryResult};
use crate::data::subscription::{Subscription, SubscriptionError, SubscriptionResult};

/// Future for query results.
pub type QueryResultFuture = Box<dyn Future<Item = QueryResult, Error = QueryError> + Send>;

/// Future for subscription results.
pub type SubscriptionResultFuture =
    Box<dyn Future<Item = SubscriptionResult, Error = SubscriptionError> + Send>;

/// A component that can run GraphqL queries against a [Store](../store/trait.Store.html).
pub trait GraphQlRunner: Send + Sync + 'static {
    /// Runs a GraphQL query and returns its result.
    fn run_query(&self, query: Query) -> QueryResultFuture;

    /// Runs a GraphqL query up to the given complexity. Overrides the global complexity limit.
    fn run_query_with_complexity(
        &self,
        query: Query,
        max_complexity: Option<u64>,
        max_depth: Option<u8>,
        max_first: Option<u32>,
    ) -> QueryResultFuture;

    /// Runs a GraphQL subscription and returns a stream of results.
    fn run_subscription(&self, subscription: Subscription) -> SubscriptionResultFuture;
}
