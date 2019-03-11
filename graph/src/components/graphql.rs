use futures::prelude::*;

use crate::data::query::{Query, QueryError, QueryResult};
use crate::data::subscription::{Subscription, SubscriptionError, SubscriptionResult};

/// Future for query results.
pub type QueryResultFuture = Box<Future<Item = QueryResult, Error = QueryError> + Send>;

/// Future for subscription results.
pub type SubscriptionResultFuture =
    Box<Future<Item = SubscriptionResult, Error = SubscriptionError> + Send>;

/// A component that can run GraphqL queries against a [Store](../store/trait.Store.html).
pub trait GraphQlRunner: Send + Sync + 'static {
    /// Runs a GraphQL query and returns its result.
    fn run_query(&self, query: Query) -> QueryResultFuture;

    /// Runs a GraphQL subscription and returns a stream of results.
    fn run_subscription(&self, subscription: Subscription) -> SubscriptionResultFuture;
}
