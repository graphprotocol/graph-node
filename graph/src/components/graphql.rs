use futures::prelude::*;

use crate::data::query::{Query, QueryError, QueryResult};
use crate::data::subscription::{Subscription, SubscriptionError, SubscriptionResult};

use async_trait::async_trait;
use failure::format_err;
use failure::Error;
use futures03::compat::Future01CompatExt;
use graphql_parser::query as q;

/// Future for query results.
pub type QueryResultFuture = Box<dyn Future<Item = QueryResult, Error = QueryError> + Send>;

/// Future for subscription results.
pub type SubscriptionResultFuture =
    Box<dyn Future<Item = SubscriptionResult, Error = SubscriptionError> + Send>;

/// A component that can run GraphqL queries against a [Store](../store/trait.Store.html).
#[async_trait]
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

    async fn query_metadata(&self, query: Query) -> Result<q::Value, Error> {
        self.run_query_with_complexity(query, None, None, None)
            .compat()
            .await
            .map_err(move |e| format_err!("Failed to query metadata: {}", e))
            .and_then(move |result| {
                if result.errors.is_some() {
                    Err(format_err!("Failed to query metadata: {:?}", result.errors))
                } else {
                    result.data.ok_or_else(|| format_err!("No metadata found"))
                }
            })
    }
}
