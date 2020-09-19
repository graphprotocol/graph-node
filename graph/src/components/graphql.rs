use futures::prelude::*;

use crate::data::graphql::effort::LoadManager;
use crate::data::query::{CacheStatus, Query, QueryResult};
use crate::data::subgraph::DeploymentState;
use crate::data::subscription::{Subscription, SubscriptionError, SubscriptionResult};

use async_trait::async_trait;
use failure::format_err;
use failure::Error;
use graphql_parser::query as q;
use std::sync::Arc;
use std::time::Duration;

/// Future for subscription results.
pub type SubscriptionResultFuture =
    Box<dyn Future<Item = SubscriptionResult, Error = SubscriptionError> + Send>;

/// A component that can run GraphqL queries against a [Store](../store/trait.Store.html).
#[async_trait]
pub trait GraphQlRunner: Send + Sync + 'static {
    /// Runs a GraphQL query and returns its result.
    async fn run_query(
        self: Arc<Self>,
        query: Query,
        state: DeploymentState,
        nested_resolver: bool,
    ) -> Arc<QueryResult>;

    /// Runs a GraphqL query up to the given complexity. Overrides the global complexity limit.
    async fn run_query_with_complexity(
        self: Arc<Self>,
        query: Query,
        state: DeploymentState,
        max_complexity: Option<u64>,
        max_depth: Option<u8>,
        max_first: Option<u32>,
        max_skip: Option<u32>,
        nested_resolver: bool,
    ) -> Arc<QueryResult>;

    /// Runs a GraphQL subscription and returns a stream of results.
    async fn run_subscription(
        self: Arc<Self>,
        subscription: Subscription,
    ) -> Result<SubscriptionResult, SubscriptionError>;

    async fn query_metadata(self: Arc<Self>, query: Query) -> Result<Arc<q::Value>, Error> {
        let state = DeploymentState::meta();
        let result = self
            .run_query_with_complexity(query, state, None, None, None, None, false)
            .await;

        // Metadata queries are not cached.
        Arc::try_unwrap(result)
            .unwrap()
            .to_result()
            .map_err(|errors| format_err!("Failed to query metadata: {:?}", errors))
            .and_then(|data| {
                data.map(|data| Ok(Arc::new(data)))
                    .unwrap_or_else(|| Err(format_err!("No metadata found")))
            })
    }

    fn load_manager(&self) -> Arc<LoadManager>;
}

#[async_trait]
pub trait QueryLoadManager: Send + Sync {
    async fn query_permit(&self) -> tokio::sync::OwnedSemaphorePermit;

    fn record_work(&self, shape_hash: u64, duration: Duration, cache_status: CacheStatus);
}
